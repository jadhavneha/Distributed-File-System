package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type TaskID struct {
	JobID string `json:"jobId"`
	Stage int    `json:"stage"`
	Index int    `json:"index"`
}

func (t TaskID) String() string {
	return fmt.Sprintf("job=%s,s=%d,t=%d", t.JobID, t.Stage, t.Index)
}

type TaskPayload struct {
	TaskID        TaskID   `json:"taskId"`
	WorkerNodeID  string   `json:"workerNodeId"`
	OpExe         string   `json:"opExe"`
	OpArgs        []string `json:"opArgs"`
	NStages       int      `json:"nStages"`
	TasksPerStage int      `json:"tasksPerStage"`

	HyDFSSrcFile  string `json:"hydfsSrcFile"`
	HyDFSDestFile string `json:"hydfsDestFile"`

	ExactlyOnce      bool `json:"exactlyOnce"`
	AutoscaleEnabled bool `json:"autoscaleEnabled"`

	InputRate int `json:"inputRate"`
	LW        int `json:"lw"`
	HW        int `json:"hw"`
}

type TupleMessageType string

const (
	TupleMsgData TupleMessageType = "data"
	TupleMsgAck  TupleMessageType = "ack"
)

type RainstormTupleMessage struct {
	MsgType TupleMessageType `json:"msgType"`

	JobID    string `json:"jobId"`
	Stage    int    `json:"stage"`
	FromTask TaskID `json:"fromTask"`
	ToTask   TaskID `json:"toTask"`

	TupleID string `json:"tupleId,omitempty"`
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`

	AckForTupleID string `json:"ackForTupleId,omitempty"`
}

type RainstormMetrics struct {
	JobID    string  `json:"jobId"`
	Stage    int     `json:"stage"`
	TaskIdx  int     `json:"taskIdx"`
	InputTPS float64 `json:"inputTps"`
	Window   int     `json:"windowSec"`
}

type taskInfo struct {
	nodeID  string
	payload TaskPayload
}

var taskRegistry = struct {
	mu    sync.RWMutex
	tasks map[TaskID]taskInfo
}{
	tasks: make(map[TaskID]taskInfo),
}

func registerTaskAssignment(tid TaskID, nodeID string, payload TaskPayload) {
	taskRegistry.mu.Lock()
	defer taskRegistry.mu.Unlock()
	taskRegistry.tasks[tid] = taskInfo{nodeID: nodeID, payload: payload}
}

func lookupTaskPayload(tid TaskID) (TaskPayload, bool) {
	taskRegistry.mu.RLock()
	defer taskRegistry.mu.RUnlock()
	info, ok := taskRegistry.tasks[tid]
	if !ok {
		return TaskPayload{}, false
	}
	return info.payload, true
}

func updateTaskNode(tid TaskID, newNodeID string) {
	taskRegistry.mu.Lock()
	defer taskRegistry.mu.Unlock()
	info, ok := taskRegistry.tasks[tid]
	if !ok {
		return
	}
	info.nodeID = newNodeID
	taskRegistry.tasks[tid] = info
}

type localTaskRuntime struct {
	payload TaskPayload

	incomingTuples chan RainstormTupleMessage
	stopCh         chan struct{}
}

var localTasksMu sync.RWMutex
var localTasks = make(map[TaskID]*localTaskRuntime)

func decodeTaskPayload(msg *Message) (TaskPayload, error) {
	var payload TaskPayload
	switch data := msg.Data.(type) {
	case string:
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			return TaskPayload{}, err
		}
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return TaskPayload{}, err
		}
		if err := json.Unmarshal(b, &payload); err != nil {
			return TaskPayload{}, err
		}
	}
	return payload, nil
}

func decodeTupleMessage(msg *Message) (RainstormTupleMessage, error) {
	var tm RainstormTupleMessage
	switch data := msg.Data.(type) {
	case string:
		if err := json.Unmarshal([]byte(data), &tm); err != nil {
			return tm, err
		}
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return tm, err
		}
		if err := json.Unmarshal(b, &tm); err != nil {
			return tm, err
		}
	}
	return tm, nil
}

func decodeMetrics(msg *Message) (RainstormMetrics, error) {
	var m RainstormMetrics
	switch data := msg.Data.(type) {
	case string:
		if err := json.Unmarshal([]byte(data), &m); err != nil {
			return m, err
		}
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return m, err
		}
		if err := json.Unmarshal(b, &m); err != nil {
			return m, err
		}
	}
	return m, nil
}

var (
	tupleCounterMu sync.Mutex
	tupleCounter   int64
)

func newTupleID() string {
	tupleCounterMu.Lock()
	defer tupleCounterMu.Unlock()
	tupleCounter++
	return fmt.Sprintf("%s-%d-%d", NODE_ID, time.Now().UnixNano(), tupleCounter)
}

func KillTaskByStageIndex(jobID string, stage, index int) error {
	tid := TaskID{
		JobID: jobID,
		Stage: stage,
		Index: index,
	}

	payload, ok := lookupTaskPayload(tid)
	if !ok {
		return fmt.Errorf("KillTask: no payload found for task %+v", tid)
	}

	if payload.OpExe == "aggregateByKeys" {
		return fmt.Errorf("KillTask: refusing to kill stateful aggregate task %+v (OpExe=%s)", tid, payload.OpExe)
	}

	if stage == 0 {
		return fmt.Errorf("KillTask: refusing to kill stage 0 task %+v (source-adjacent). For Test 2, kill a later filter/transform stage.", tid)
	}

	taskRegistry.mu.RLock()
	info, ok := taskRegistry.tasks[tid]
	taskRegistry.mu.RUnlock()
	if !ok {
		return fmt.Errorf("KillTask: no assignment found for task %+v", tid)
	}

	member, exists := GetMember(info.nodeID)
	if !exists {
		return fmt.Errorf("KillTask: node %s not in membership list", info.nodeID)
	}

	stopMsg := Message{
		Kind: RS_STOP_TASK,
		Data: payload,
	}

	if err := SendMessage(member, stopMsg); err != nil {
		return fmt.Errorf("KillTask: failed to send RS_STOP_TASK to %s for %+v: %w",
			info.nodeID, tid, err)
	}

	nodeLogger.Log("INFO", "KillTask: sent RS_STOP_TASK for task %+v to node %s",
		tid, info.nodeID)

	time.Sleep(500 * time.Millisecond)

	restartMsg := Message{
		Kind: RS_START_TASK,
		Data: payload,
	}

	if err := SendMessage(member, restartMsg); err != nil {
		return fmt.Errorf("KillTask: failed to send RS_START_TASK to %s for %+v: %w",
			info.nodeID, tid, err)
	}

	nodeLogger.Log("INFO", "KillTask: sent RS_START_TASK (restart) for task %+v to node %s",
		tid, info.nodeID)

	return nil
}
