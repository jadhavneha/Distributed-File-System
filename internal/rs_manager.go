// rs_manager.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type rainstormJob struct {
	JobID         string
	NStages       int
	TasksPerStage int

	Ops []string

	HyDFSSrcFile  string
	HyDFSDestFile string

	ExactlyOnce      bool
	AutoscaleEnabled bool

	InputRate int
	LW        int
	HW        int
}

var (
	jobsMu sync.RWMutex
	jobs   = make(map[string]*rainstormJob)
)

func getJobStages(jobID string) int {
	jobsMu.RLock()
	defer jobsMu.RUnlock()
	if j, ok := jobs[jobID]; ok {
		return j.NStages
	}
	return 0
}

func getJobTasksPerStage(jobID string) int {
	jobsMu.RLock()
	defer jobsMu.RUnlock()
	if j, ok := jobs[jobID]; ok {
		return j.TasksPerStage
	}
	return 0
}

func appendLocalFileToHyDFS(hdfsName, localPath string) error {
	return hdfsAppend(localPath, hdfsName)
}

func getHyDFSFileToLocal(hdfsName, localPath string) error {
	return hdfsGet(hdfsName, localPath)
}

func writeFinalOutputToHyDFS(jobID, line string) {
	jobsMu.RLock()
	j := jobs[jobID]
	jobsMu.RUnlock()
	if j == nil {
		return
	}

	localTmpDir := filepath.Join(STORAGE_LOCATION, "rainstorm_output")
	_ = os.MkdirAll(localTmpDir, 0o755)

	tmpFile := filepath.Join(
		localTmpDir,
		fmt.Sprintf("%s-%d.out.tmp", jobID, time.Now().UnixNano()),
	)

	if err := os.WriteFile(tmpFile, []byte(line+"\n"), 0o644); err != nil {
		return
	}

	_ = appendLocalFileToHyDFS(j.HyDFSDestFile, tmpFile)
	_ = os.Remove(tmpFile)
}

func StartRainStormJob(
	nStages int,
	nTasks int,
	ops []string,
	hydfsSrc string,
	hydfsDest string,
	exactlyOnce bool,
	autoscale bool,
	inputRate int,
	lw int,
	hw int,
) error {
	if !IsLeaderNode {
		return fmt.Errorf("RainStorm jobs must be started from the leader node")
	}

	testPath := filepath.Join(STORAGE_LOCATION, "rainstorm_src", "check_input.tmp")
	_ = os.MkdirAll(filepath.Dir(testPath), 0o755)
	if err := getHyDFSFileToLocal(hydfsSrc, testPath); err != nil {
		return fmt.Errorf("cannot read source file %s from HyDFS: %v", hydfsSrc, err)
	}
	_ = os.Remove(testPath)

	if len(ops) != nStages {
		return fmt.Errorf("expected %d ops, got %d", nStages, len(ops))
	}

	jobID := fmt.Sprintf("%d", time.Now().UnixNano())

	job := &rainstormJob{
		JobID:            jobID,
		NStages:          nStages,
		TasksPerStage:    nTasks,
		Ops:              ops,
		HyDFSSrcFile:     hydfsSrc,
		HyDFSDestFile:    hydfsDest,
		ExactlyOnce:      exactlyOnce,
		AutoscaleEnabled: autoscale,
		InputRate:        inputRate,
		LW:               lw,
		HW:               hw,
	}

	jobsMu.Lock()
	jobs[jobID] = job
	jobsMu.Unlock()

	nodeLogger.Log("INFO", "RainStorm: starting job %s with %d stages * %d tasks", jobID, nStages, nTasks)

	members := GetSortedRingMembers()
	if len(members) == 0 {
		return fmt.Errorf("no workers to schedule RainStorm job")
	}

	taskIdx := 0
	for stage := 0; stage < nStages; stage++ {
		opSpec := ops[stage]
		for idx := 0; idx < nTasks; idx++ {
			m := members[taskIdx%len(members)]
			taskIdx++

			tid := TaskID{JobID: jobID, Stage: stage, Index: idx}
			exe, args := parseOpSpec(opSpec)

			payload := TaskPayload{
				TaskID:           tid,
				WorkerNodeID:     m.Id,
				OpExe:            exe,
				OpArgs:           args,
				NStages:          nStages,
				TasksPerStage:    nTasks,
				HyDFSSrcFile:     hydfsSrc,
				HyDFSDestFile:    hydfsDest,
				ExactlyOnce:      exactlyOnce,
				AutoscaleEnabled: autoscale,
				InputRate:        inputRate,
				LW:               lw,
				HW:               hw,
			}

			registerTaskAssignment(tid, m.Id, payload)

			logFile := taskLogLocalPath(jobID, stage, idx)
			nodeLogger.Log("INFO",
				"RainStorm: starting task %s on VM=%s pid=%d op=%s log=%s",
				tid.String(), m.Id, os.Getpid(), exe, logFile)

			msg := Message{Kind: RS_START_TASK, Data: payload}
			memberInfo, ok := GetMember(m.Id)
			if !ok {
				return fmt.Errorf("failed to find member info for node %s", m.Id)
			}

			if err := SendMessage(memberInfo, msg); err != nil {
				return fmt.Errorf("failed to start task %s on node %s: %v", tid.String(), m.Id, err)
			}
		}
	}

	return nil
}

func parseOpSpec(spec string) (string, []string) {
	parts := strings.Split(spec, ":")
	exe := parts[0]
	if len(parts) > 1 {
		return exe, parts[1:]
	}
	return exe, nil
}

func runSourceForStage0(payload TaskPayload) {
	jobID := payload.TaskID.JobID

	jobsMu.RLock()
	job := jobs[jobID]
	jobsMu.RUnlock()
	if job == nil {
		return
	}

	localPath := filepath.Join(STORAGE_LOCATION, "rainstorm_src", fmt.Sprintf("%s_input.log", jobID))
	_ = os.MkdirAll(filepath.Dir(localPath), 0o755)

	if err := getHyDFSFileToLocal(job.HyDFSSrcFile, localPath); err != nil {
		nodeLogger.Log("ERROR", "RainStorm source: failed to fetch %s: %v", job.HyDFSSrcFile, err)
		return
	}

	f, err := os.Open(localPath)
	if err != nil {
		nodeLogger.Log("ERROR", "RainStorm source: open local src failed: %v", err)
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNo := 0
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	linesThisSec := 0
	rate := job.InputRate
	if rate <= 0 {
		rate = 1000
	}

	members := GetSortedRingMembers()
	if len(members) == 0 {
		nodeLogger.Log("ERROR", "RainStorm source: no ring members")
		return
	}

	for scanner.Scan() {
		line := scanner.Text()
		key := fmt.Sprintf("%s:%d", job.HyDFSSrcFile, lineNo)
		lineNo++

		tm := RainstormTupleMessage{
			MsgType:  TupleMsgData,
			JobID:    jobID,
			Stage:    0,
			FromTask: TaskID{JobID: jobID, Stage: -1, Index: -1},
			TupleID:  newTupleID(),
			Key:      key,
			Value:    line,
		}

		tasksPerStage := job.TasksPerStage
		if tasksPerStage <= 0 {
			tasksPerStage = 1
		}

		idx := (ComputeRingPosition(tm.Key)%tasksPerStage + tasksPerStage) % tasksPerStage
		tm.ToTask = TaskID{
			JobID: jobID,
			Stage: 0,
			Index: idx,
		}

		globalIdx := 0*job.TasksPerStage + tm.ToTask.Index
		rm := members[globalIdx%len(members)]

		if rm.Id == NODE_ID {
			localTasksMu.RLock()
			rt, ok := localTasks[tm.ToTask]
			localTasksMu.RUnlock()
			if ok {
				select {
				case rt.incomingTuples <- tm:
				default:
				}
			}
		} else {
			member, ok := GetMember(rm.Id)
			if !ok {
				nodeLogger.Log("ERROR", "RainStorm source: owner %s not in membership", rm.Id)
			} else {
				msg := Message{Kind: RS_TUPLE, Data: tm}
				if err := SendMessage(member, msg); err != nil {
					nodeLogger.Log("ERROR",
						"RainStorm source: failed to send tuple to %s: %v", rm.Id, err)
				}
			}
		}

		linesThisSec++
		if linesThisSec >= rate {
			<-ticker.C
			linesThisSec = 0
		}
	}

	if err := scanner.Err(); err != nil {
		nodeLogger.Log("ERROR", "RainStorm source: scan error: %v", err)
	} else {
		nodeLogger.Log("INFO", "RainStorm: job %s source finished streaming (end of run)", jobID)
	}
}

func HandleRainstormStartTask(msg *Message) {
	payload, err := decodeTaskPayload(msg)
	if err != nil {
		nodeLogger.Log("ERROR", "RS_START_TASK decode failed: %v", err)
		return
	}

	jobID := payload.TaskID.JobID
	jobsMu.Lock()
	if _, ok := jobs[jobID]; !ok {
		jobs[jobID] = &rainstormJob{
			JobID:            jobID,
			NStages:          payload.NStages,
			TasksPerStage:    payload.TasksPerStage,
			HyDFSSrcFile:     payload.HyDFSSrcFile,
			HyDFSDestFile:    payload.HyDFSDestFile,
			ExactlyOnce:      payload.ExactlyOnce,
			AutoscaleEnabled: payload.AutoscaleEnabled,
			InputRate:        payload.InputRate,
			LW:               payload.LW,
			HW:               payload.HW,
		}
	}
	jobsMu.Unlock()

	startLocalTaskRuntime(payload)
}

func HandleRainstormStopTask(msg *Message) {
	payload, err := decodeTaskPayload(msg)
	if err != nil {
		nodeLogger.Log("ERROR", "RS_STOP_TASK decode failed: %v", err)
		return
	}
	stopLocalTaskRuntime(payload.TaskID)
}

func HandleRainstormTuple(msg *Message) {
	tm, err := decodeTupleMessage(msg)
	if err != nil {
		nodeLogger.Log("ERROR", "RS_TUPLE decode failed: %v", err)
		return
	}
	routeTupleToLocalTask(tm)
}

type stageLoad struct {
	lastUpdate time.Time
	avgTps     float64
	taskCount  int
}

var (
	stageLoadsMu sync.Mutex
	stageLoads   = make(map[string]*stageLoad)
)

func stageKey(jobID string, stage int) string {
	return jobID + ":" + strconv.Itoa(stage)
}

func HandleRainstormMetrics(msg *Message) {
	if !IsLeaderNode {
		return
	}
	m, err := decodeMetrics(msg)
	if err != nil {
		nodeLogger.Log("ERROR", "RS_METRICS decode failed: %v", err)
		return
	}

	nodeLogger.Log("INFO",
		"RainStorm metrics (leader): job=%s stage=%d task=%d TPS=%.2f window=%ds",
		m.JobID, m.Stage, m.TaskIdx, m.InputTPS, m.Window)

	key := stageKey(m.JobID, m.Stage)

	stageLoadsMu.Lock()
	sl, ok := stageLoads[key]
	if !ok {
		sl = &stageLoad{}
		stageLoads[key] = sl
	}
	sl.lastUpdate = time.Now()
	sl.avgTps = (sl.avgTps + m.InputTPS) / 2.0
	jobsMu.RLock()
	j := jobs[m.JobID]
	jobsMu.RUnlock()
	if j != nil {
		sl.taskCount = j.TasksPerStage
	}
	stageLoadsMu.Unlock()

	if j == nil || !j.AutoscaleEnabled || j.ExactlyOnce {
		return
	}

	if sl.avgTps > float64(j.HW*j.TasksPerStage) {
		nodeLogger.Log("INFO",
			"RainStorm autoscale event: job=%s stage=%d direction=UP avgTPS=%.2f threshold=%d",
			m.JobID, m.Stage, sl.avgTps, j.HW*j.TasksPerStage)
		scaleStage(m.JobID, m.Stage, +1)
	} else if sl.avgTps < float64(j.LW*j.TasksPerStage) {
		nodeLogger.Log("INFO",
			"RainStorm autoscale event: job=%s stage=%d direction=DOWN avgTPS=%.2f threshold=%d",
			m.JobID, m.Stage, sl.avgTps, j.LW*j.TasksPerStage)
		scaleStage(m.JobID, m.Stage, -1)
	}
}

func scaleStage(jobID string, stage int, delta int) {
	jobsMu.Lock()
	j := jobs[jobID]
	if j == nil {
		jobsMu.Unlock()
		return
	}
	oldCount := j.TasksPerStage
	newCount := oldCount + delta
	if newCount <= 0 {
		jobsMu.Unlock()
		return
	}
	j.TasksPerStage = newCount
	jobsMu.Unlock()

	nodeLogger.Log("INFO", "RainStorm autoscale: job %s stage %d from %d → %d tasks", jobID, stage, oldCount, newCount)

	bcastUpdateTaskCount(jobID, stage, newCount)

	if delta > 0 {
		newIdx := newCount - 1
		members := GetSortedRingMembers()
		if len(members) == 0 {
			return
		}

		globalIdx := stage*newCount + newIdx
		m := members[globalIdx%len(members)]

		tid := TaskID{JobID: jobID, Stage: stage, Index: newIdx}
		opSpec := j.Ops[stage]
		exe, args := parseOpSpec(opSpec)

		payload := TaskPayload{
			TaskID:           tid,
			WorkerNodeID:     m.Id,
			OpExe:            exe,
			OpArgs:           args,
			NStages:          j.NStages,
			TasksPerStage:    newCount,
			HyDFSSrcFile:     j.HyDFSSrcFile,
			HyDFSDestFile:    j.HyDFSDestFile,
			ExactlyOnce:      j.ExactlyOnce,
			AutoscaleEnabled: j.AutoscaleEnabled,
			InputRate:        j.InputRate,
			LW:               j.LW,
			HW:               j.HW,
		}

		registerTaskAssignment(tid, m.Id, payload)

		msg := Message{Kind: RS_START_TASK, Data: payload}
		memberInfo, ok := GetMember(m.Id)
		if ok {
			_ = SendMessage(memberInfo, msg)
		}
	}
}

type updateTaskCountPayload struct {
	JobID        string `json:"jobId"`
	Stage        int    `json:"stage"`
	NewTaskCount int    `json:"newTaskCount"`
}

func bcastUpdateTaskCount(jobID string, stage, newCount int) {
	payload := updateTaskCountPayload{
		JobID:        jobID,
		Stage:        stage,
		NewTaskCount: newCount,
	}
	msg := Message{Kind: RS_UPDATE_TASK_COUNT, Data: payload}
	for _, rm := range GetSortedRingMembers() {
		member, ok := GetMember(rm.Id)
		if !ok {
			continue
		}
		_ = SendMessage(member, msg)
	}
}

func HandleUpdateTaskCount(msg *Message) {
	var payload updateTaskCountPayload
	switch data := msg.Data.(type) {
	case string:
		_ = json.Unmarshal([]byte(data), &payload)
	default:
		b, _ := json.Marshal(data)
		_ = json.Unmarshal(b, &payload)
	}

	jobsMu.Lock()
	if j, ok := jobs[payload.JobID]; ok {
		j.TasksPerStage = payload.NewTaskCount
	}
	jobsMu.Unlock()

	localTasksMu.Lock()
	for tid, rt := range localTasks {
		if tid.JobID == payload.JobID && tid.Stage == payload.Stage {
			rt.payload.TasksPerStage = payload.NewTaskCount
		}
	}
	localTasksMu.Unlock()
}

func OnNodeFailure(failedNodeID string) {
	if !IsLeaderNode {
		return
	}

	nodeLogger.Log("INFO", "RainStorm: Node %s failed. Checking for lost tasks...", failedNodeID)

	taskRegistry.mu.Lock()
	defer taskRegistry.mu.Unlock()

	var lostTasks []TaskPayload

	for _, info := range taskRegistry.tasks {
		if info.nodeID == failedNodeID {
			lostTasks = append(lostTasks, info.payload)
		}
	}

	if len(lostTasks) == 0 {
		return
	}

	members := GetSortedRingMembers()
	if len(members) == 0 {
		nodeLogger.Log("ERROR", "RainStorm: No live nodes available to reschedule tasks!")
		return
	}

	for _, payload := range lostTasks {

		globalIdx := payload.TaskID.Stage*payload.TasksPerStage + payload.TaskID.Index

		newOwner := members[globalIdx%len(members)]

		nodeLogger.Log("INFO", "RainStorm: Rescheduling task %s from %s -> %s",
			payload.TaskID.String(), failedNodeID, newOwner.Id)

		payload.WorkerNodeID = newOwner.Id
		taskRegistry.tasks[payload.TaskID] = taskInfo{
			nodeID:  newOwner.Id,
			payload: payload,
		}

		msg := Message{Kind: RS_START_TASK, Data: payload}
		memberInfo, ok := GetMember(newOwner.Id)
		if ok {
			go SendMessage(memberInfo, msg)
		}
	}
}

func GetLatestRainstormJobID() string {
	jobsMu.RLock()
	defer jobsMu.RUnlock()

	var latest string
	for id := range jobs {
		if id > latest {
			latest = id
		}
	}
	return latest
}
