package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const rainstormLogRoot = "/rainstorm_logs"

type taskLogRecord struct {
	RecordType string `json:"type"`
	TupleID    string `json:"tupleId"`
	Stage      int    `json:"stage"`
	From       TaskID `json:"from"`
	To         TaskID `json:"to"`
	Key        string `json:"key,omitempty"`
	Value      string `json:"value,omitempty"`
}

type taskState struct {
	mu sync.Mutex

	processed map[string]bool
	pending   map[string]RainstormTupleMessage
	store     map[string]int

	inputCount   int64
	metricWindow time.Duration
	lastMetricAt time.Time
}

var (
	logFlushMu      sync.Mutex
	logFlushCounter = make(map[string]int)
)

func logKey(jobID string, tid TaskID) string {
	return fmt.Sprintf("%s-%d-%d", jobID, tid.Stage, tid.Index)
}

func newTaskState() *taskState {
	return &taskState{
		processed:    make(map[string]bool),
		pending:      make(map[string]RainstormTupleMessage),
		store:        make(map[string]int),
		metricWindow: time.Duration(RAINSTORM_METRICS_INTERVAL_MS) * time.Millisecond,
		lastMetricAt: time.Now(),
	}
}

func (ts *taskState) markProcessed(id string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.processed[id] = true
	ts.inputCount++
}

func (ts *taskState) alreadyProcessed(id string) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.processed[id]
}

func (ts *taskState) addPending(msg RainstormTupleMessage) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.pending[msg.TupleID] = msg
}

func (ts *taskState) ackPending(tupleID string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	delete(ts.pending, tupleID)
}

func (ts *taskState) snapshotPending() []RainstormTupleMessage {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	out := make([]RainstormTupleMessage, 0, len(ts.pending))
	for _, v := range ts.pending {
		out = append(out, v)
	}
	return out
}

func taskLogHDFSPath(jobID string, stage, idx int) string {
	return fmt.Sprintf("%s/job-%s/s%d_t%d.log", rainstormLogRoot, jobID, stage, idx)
}

func taskLogLocalPath(jobID string, stage, idx int) string {
	base := filepath.Join(STORAGE_LOCATION, "rainstorm_logs", fmt.Sprintf("job-%s", jobID))
	_ = os.MkdirAll(base, 0o755)
	return filepath.Join(base, fmt.Sprintf("s%d_t%d.log", stage, idx))
}

func appendRecordToLog(jobID string, tid TaskID, rec taskLogRecord) {
	localPath := taskLogLocalPath(jobID, tid.Stage, tid.Index)

	f, err := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		nodeLogger.Log("ERROR", "RainStorm log open failed: %v", err)
		return
	}
	defer f.Close()

	b, _ := json.Marshal(rec)
	if _, err := f.Write(append(b, '\n')); err != nil {
		nodeLogger.Log("ERROR", "RainStorm log write failed: %v", err)
		return
	}

	k := logKey(jobID, tid)
	logFlushMu.Lock()
	logFlushCounter[k]++
	n := logFlushCounter[k]
	logFlushMu.Unlock()

	if rec.RecordType != "output" && n%50 != 0 {
		return
	}

	hdfsName := taskLogHDFSPath(jobID, tid.Stage, tid.Index)
	if err := appendLocalFileToHyDFS(hdfsName, localPath); err != nil {
		nodeLogger.Log("ERROR", "RainStorm: append log to HyDFS failed: %v", err)
	}
}

func replayTaskLog(jobID string, tid TaskID, ts *taskState) {
	hdfsName := taskLogHDFSPath(jobID, tid.Stage, tid.Index)
	localPath := taskLogLocalPath(jobID, tid.Stage, tid.Index)

	if err := MergeHDFSFileOptimized(hdfsName); err != nil {
		nodeLogger.Log("WARN", "RainStorm: merge failed for log %s: %v", hdfsName, err)
	}

	if err := hdfsGet(hdfsName, localPath); err != nil {
		nodeLogger.Log("INFO", "RainStorm: no existing log for %s: %v", hdfsName, err)
		return
	}

	f, err := os.Open(localPath)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		var rec taskLogRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}
		switch rec.RecordType {
		case "processed":
			ts.processed[rec.TupleID] = true
		case "sent":
			msg := RainstormTupleMessage{
				MsgType:  TupleMsgData,
				JobID:    jobID,
				Stage:    rec.To.Stage,
				FromTask: rec.From,
				ToTask:   rec.To,
				TupleID:  rec.TupleID,
				Key:      rec.Key,
				Value:    rec.Value,
			}
			ts.pending[rec.TupleID] = msg
		case "acked":
			delete(ts.pending, rec.TupleID)
		}
	}
}

func startLocalTaskRuntime(payload TaskPayload) {
	tid := payload.TaskID

	localTasksMu.Lock()
	if _, exists := localTasks[tid]; exists {
		localTasksMu.Unlock()
		return
	}
	runtime := &localTaskRuntime{
		payload:        payload,
		incomingTuples: make(chan RainstormTupleMessage, 1024),
		stopCh:         make(chan struct{}),
	}
	localTasks[tid] = runtime
	localTasksMu.Unlock()

	go taskMainLoop(runtime)
}

func stopLocalTaskRuntime(tid TaskID) {
	localTasksMu.Lock()
	rt, ok := localTasks[tid]
	if ok {
		close(rt.stopCh)
		delete(localTasks, tid)
	}
	localTasksMu.Unlock()
}

func routeTupleToLocalTask(tm RainstormTupleMessage) {
	localTasksMu.RLock()
	rt, ok := localTasks[tm.ToTask]
	localTasksMu.RUnlock()
	if !ok {
		return
	}
	select {
	case rt.incomingTuples <- tm:
	default:
	}
}

func taskMainLoop(rt *localTaskRuntime) {
	payload := rt.payload
	tid := payload.TaskID
	jobID := tid.JobID

	nodeLogger.Log("INFO", "RainStorm: starting task %s on node %s", tid.String(), NODE_ID)

	state := newTaskState()
	if payload.ExactlyOnce {
		replayTaskLog(jobID, tid, state)
	}

	if tid.Stage == 0 && tid.Index == 0 {
		go runSourceForStage0(payload)
	}

	metricsTicker := time.NewTicker(state.metricWindow)
	defer metricsTicker.Stop()

	resendTicker := time.NewTicker(3 * time.Second)
	defer resendTicker.Stop()

	for {
		select {
		case <-rt.stopCh:
			nodeLogger.Log("INFO", "RainStorm: stopping task %s on node %s", tid.String(), NODE_ID)
			return

		case tm := <-rt.incomingTuples:
			if tm.MsgType == TupleMsgAck {
				state.ackPending(tm.AckForTupleID)
				if payload.ExactlyOnce {
					appendRecordToLog(jobID, tid, taskLogRecord{
						RecordType: "acked",
						TupleID:    tm.AckForTupleID,
						Stage:      tid.Stage,
						From:       tm.FromTask,
						To:         tm.ToTask,
					})
				}
				continue
			}

			if payload.ExactlyOnce && state.alreadyProcessed(tm.TupleID) {
				nodeLogger.Log("INFO",
					"RainStorm: duplicate tuple rejected at task %s (tupleId=%s key=%s)",
					tid.String(), tm.TupleID, tm.Key)

				appendRecordToLog(jobID, tid, taskLogRecord{
					RecordType: "duplicate",
					TupleID:    tm.TupleID,
					Stage:      tid.Stage,
					From:       tm.FromTask,
					To:         tm.ToTask,
					Key:        tm.Key,
					Value:      tm.Value,
				})

				sendAckForTuple(tm, tid)
				continue
			}

			// Mark processed (for dedup).
			if payload.ExactlyOnce {
				state.markProcessed(tm.TupleID)
				appendRecordToLog(jobID, tid, taskLogRecord{
					RecordType: "processed",
					TupleID:    tm.TupleID,
					Stage:      tid.Stage,
					From:       tm.FromTask,
					To:         tm.ToTask,
					Key:        tm.Key,
					Value:      tm.Value,
				})
			} else {
				state.inputCount++
			}

			outTuples := runOperator(payload, state, tm)

			for _, out := range outTuples {
				if payload.ExactlyOnce {
					totalStages := getJobStages(out.JobID)
					if out.Stage < totalStages {
						state.addPending(out)
						appendRecordToLog(jobID, tid, taskLogRecord{
							RecordType: "sent",
							TupleID:    out.TupleID,
							Stage:      out.Stage,
							From:       out.FromTask,
							To:         out.ToTask,
							Key:        out.Key,
							Value:      out.Value,
						})
					} else {
						appendRecordToLog(jobID, tid, taskLogRecord{
							RecordType: "output",
							TupleID:    out.TupleID,
							Stage:      tid.Stage,
							From:       out.FromTask,
							To:         out.ToTask,
							Key:        out.Key,
							Value:      out.Value,
						})
					}
				}

				sendTuple(out)
			}

			sendAckForTuple(tm, tid)

		case <-metricsTicker.C:
			reportMetrics(payload, state)

		case <-resendTicker.C:
			if !payload.ExactlyOnce {
				continue
			}
			for _, p := range state.snapshotPending() {
				sendTuple(p)
			}
		}
	}
}

func runOperator(payload TaskPayload, ts *taskState, tm RainstormTupleMessage) []RainstormTupleMessage {
	op := payload.OpExe
	var args []string
	if payload.OpArgs != nil {
		args = payload.OpArgs
	}
	key := tm.Key
	val := tm.Value

	outKey := key
	outVal := val

	switch {
	case op == "identity":

	case op == "csv_first3":
		cols := strings.Split(val, ",")
		if len(cols) >= 3 {
			outVal = strings.Join(cols[:3], ",")
		} else {
			outVal = strings.Join(cols, ",")
		}
		return []RainstormTupleMessage{
			makeNextStageTuple(payload, outKey, outVal, tm),
		}

	case strings.HasPrefix(op, "grep"):
		pat := ""
		if len(args) > 0 {
			pat = args[0]
		} else if idx := strings.Index(op, ":"); idx != -1 {
			pat = op[idx+1:]
		}
		if !strings.Contains(val, pat) {
			return nil
		}

	case strings.HasPrefix(op, "replace"):
		old, newVal := "", ""

		if len(args) >= 2 {
			old, newVal = args[0], args[1]
		} else if len(args) == 1 && strings.Contains(args[0], ",") {
			pair := strings.SplitN(args[0], ",", 2)
			if len(pair) == 2 {
				old, newVal = pair[0], pair[1]
			}
		} else {
			parts := strings.SplitN(op, ":", 3)
			if len(parts) == 3 {
				old, newVal = parts[1], parts[2]
			}
		}

		if old == "" {
			outVal = val
		} else {
			outVal = strings.ReplaceAll(val, old, newVal)
		}

	case op == "tokenize":
		words := strings.Fields(val)
		var out []RainstormTupleMessage
		for _, w := range words {
			key2 := w
			val2 := fmt.Sprintf("%s,1", w)
			out = append(out, makeNextStageTuple(payload, key2, val2, tm))
		}
		return out

	case op == "aggregateByKeys":
		if len(args) < 1 {
			nodeLogger.Log("ERROR", "aggregateByKeys: missing column index argument")
			return nil
		}

		n, err := strconv.Atoi(args[0])
		if err != nil || n <= 0 {
			nodeLogger.Log("ERROR", "aggregateByKeys: invalid column index %q", args[0])
			return nil
		}
		colIdx := n - 1

		r := csv.NewReader(strings.NewReader(val))
		r.Comma = ','
		r.FieldsPerRecord = -1
		r.LazyQuotes = true

		record, err := r.Read()
		if err != nil {
			nodeLogger.Log("ERROR", "aggregateByKeys: CSV parse error for line %q: %v", val, err)
			return nil
		}

		var k string
		if colIdx < len(record) {
			k = record[colIdx]
		} else {
			k = ""
		}

		ts.mu.Lock()
		ts.store[k] += 1
		newTotal := ts.store[k]
		ts.mu.Unlock()

		outKey = k
		outVal = fmt.Sprintf("%s,%d", k, newTotal)

	case strings.HasPrefix(op, "filter_csv"):

		parts := strings.Split(op, ":")
		if len(parts) < 3 {
			return nil
		}

		colIdx, err := strconv.Atoi(parts[1])
		if err != nil {
			nodeLogger.Log("ERROR", "Invalid column index for filter_csv: %v", err)
			return nil
		}
		targetVal := parts[2]

		columns := strings.Split(val, ",")

		if colIdx < len(columns) {
			if strings.Contains(columns[colIdx], targetVal) {
				outKey = key
				outVal = val
			} else {
				return nil
			}
		} else {
			return nil
		}

	case op == "geo_grid":

		columns := strings.Split(val, ",")
		const LatCol = 0
		const LonCol = 1

		if len(columns) > LonCol {
			latStr := strings.TrimSpace(columns[LatCol])
			lonStr := strings.TrimSpace(columns[LonCol])

			if strings.Contains(latStr, "--") {
				parts := strings.Split(latStr, "--")
				if len(parts) > 1 {
					latStr = "-" + parts[1]
				}
			}
			lat, err1 := strconv.ParseFloat(latStr, 64)
			lon, err2 := strconv.ParseFloat(lonStr, 64)

			if err1 == nil && err2 == nil {
				gridKey := fmt.Sprintf("%.1f_%.1f", lat, lon)
				outKey = gridKey
				outVal = fmt.Sprintf("%s,1", gridKey)
			} else {
				return nil
			}
		} else {
			return nil
		}
	}

	return []RainstormTupleMessage{makeNextStageTuple(payload, outKey, outVal, tm)}
}

func makeNextStageTuple(payload TaskPayload, key, val string, tm RainstormTupleMessage) RainstormTupleMessage {
	nextStage := payload.TaskID.Stage + 1
	return RainstormTupleMessage{
		MsgType:  TupleMsgData,
		JobID:    payload.TaskID.JobID,
		Stage:    nextStage,
		FromTask: payload.TaskID,
		TupleID:  newTupleID(),
		Key:      key,
		Value:    val,
	}
}

func sendTuple(tm RainstormTupleMessage) {
	totalStages := getJobStages(tm.JobID)
	if totalStages <= 0 {
		return
	}

	if tm.Stage >= totalStages {
		fmt.Println(tm.Value)
		writeFinalOutputToHyDFS(tm.JobID, tm.Value)
		return
	}

	tasksPerStage := getJobTasksPerStage(tm.JobID)
	if tasksPerStage <= 0 {
		return
	}

	idx := (ComputeRingPosition(tm.Key)%tasksPerStage + tasksPerStage) % tasksPerStage

	toTask := TaskID{
		JobID: tm.JobID,
		Stage: tm.Stage,
		Index: idx,
	}
	tm.ToTask = toTask

	jobsMu.RLock()
	j, ok := jobs[tm.JobID]
	jobsMu.RUnlock()
	if !ok || j == nil {
		return
	}

	members := GetSortedRingMembers()
	if len(members) == 0 {
		return
	}

	globalIdx := tm.Stage*j.TasksPerStage + tm.ToTask.Index
	rm := members[globalIdx%len(members)]

	member, ok := GetMember(rm.Id)
	if !ok {
		return
	}

	msg := Message{Kind: RS_TUPLE, Data: tm}
	if err := SendMessage(member, msg); err != nil {
		nodeLogger.Log("ERROR", "RainStorm: failed to send tuple %+v to %s: %v", tm, rm.Id, err)
	}
}

func sendAckForTuple(input RainstormTupleMessage, thisTask TaskID) {
	if input.FromTask.Stage < 0 || input.FromTask.Index < 0 {
		return
	}

	ack := RainstormTupleMessage{
		MsgType:       TupleMsgAck,
		JobID:         input.JobID,
		Stage:         input.Stage,
		FromTask:      thisTask,
		ToTask:        input.FromTask,
		AckForTupleID: input.TupleID,
	}

	jobsMu.RLock()
	j, ok := jobs[input.JobID]
	jobsMu.RUnlock()
	if !ok || j == nil {
		return
	}

	members := GetSortedRingMembers()
	if len(members) == 0 {
		return
	}

	globalIdx := input.FromTask.Stage*j.TasksPerStage + input.FromTask.Index
	if globalIdx < 0 {
		return
	}
	rm := members[globalIdx%len(members)]

	member, ok := GetMember(rm.Id)
	if !ok {
		return
	}

	msg := Message{Kind: RS_TUPLE, Data: ack}
	_ = SendMessage(member, msg)
}

func reportMetrics(payload TaskPayload, ts *taskState) {
	now := time.Now()
	elapsed := now.Sub(ts.lastMetricAt)
	if elapsed <= 0 {
		return
	}
	ts.mu.Lock()
	count := ts.inputCount
	ts.inputCount = 0
	ts.lastMetricAt = now
	ts.mu.Unlock()

	tps := float64(count) / elapsed.Seconds()
	m := RainstormMetrics{
		JobID:    payload.TaskID.JobID,
		Stage:    payload.TaskID.Stage,
		TaskIdx:  payload.TaskID.Index,
		InputTPS: tps,
		Window:   int(elapsed.Seconds()),
	}
	nodeLogger.Log("INFO",
		"RainStorm metrics: job=%s stage=%d task=%d TPS=%.2f window=%.2fs",
		payload.TaskID.JobID, payload.TaskID.Stage, payload.TaskID.Index, tps, elapsed.Seconds())

	if !IsLeaderNode {
		leaderID, ok := GetNodeIDFromIP(LEADER_SERVER_HOST)
		if !ok {
			return
		}
		leaderMember, ok := GetMember(leaderID)
		if !ok {
			return
		}
		msg := Message{Kind: RS_METRICS, Data: m}
		_ = SendMessage(leaderMember, msg)
	}

}
