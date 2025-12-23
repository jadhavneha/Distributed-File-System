package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var rebalanceMu sync.Mutex
var rebalanceActive bool

func DesiredReplicaSet(fileName string, rf int) []string {
	filePos := ComputeRingPosition(fileName)

	primary := GetPrimaryReplicaNode(fileName)
	if primary == "" {
		return nil
	}

	out := make([]string, 0, rf)
	seen := make(map[string]struct{}, rf)

	out = append(out, primary)
	seen[primary] = struct{}{}

	for _, s := range GetSuccessorNodes(filePos) {
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		out = append(out, s)
		seen[s] = struct{}{}
		if len(out) == rf {
			break
		}
	}
	return out
}

func RebalanceTick(replicationFactor int) {
	rebalanceMu.Lock()
	if rebalanceActive {
		rebalanceMu.Unlock()
		return
	}
	rebalanceActive = true
	rebalanceMu.Unlock()

	atomic.StoreUint32(&isMeasuringRebalance, 1)
	atomic.StoreUint64(&rebalanceBandwidth, 0)

	defer func() {
		atomic.StoreUint32(&isMeasuringRebalance, 0)
		rebalanceMu.Lock()
		rebalanceActive = false
		rebalanceMu.Unlock()
	}()

	localNames := GetFilesOnNode()

	for _, fname := range localNames {
		targets := DesiredReplicaSet(fname, replicationFactor)

		shouldKeep := false
		for _, t := range targets {
			if t == NODE_ID {
				shouldKeep = true
				break
			}
		}

		primary := GetPrimaryReplicaNode(fname)
		if primary == "" {
			continue
		}

		fileStateMutex.Lock()
		if info, ok := fileInfoMap[fname]; ok && info != nil {
			if primary != NODE_ID && info.IsPrimary {
				info.IsPrimary = false
			}
		}
		fileStateMutex.Unlock()

		has, _ := remoteHasFile(primary, fname)
		if has {
			if !shouldKeep {
				deleteLocalFile(fname)
				fmt.Printf("[rebalance] removed local copy of %s (primary %s already had it)\n", fname, primary)
			}
			continue
		}

		if pushFileToPrimary(fname, primary) {
			if ok, _ := remoteHasFile(primary, fname); ok {
				if !shouldKeep {
					deleteLocalFile(fname)
					fmt.Printf("[rebalance] pushed %s -> %s and removed local copy (not in replica set)\n", fname, primary)
				} else {
					fmt.Printf("[rebalance] pushed %s -> %s; kept local copy (still in replica set)\n", fname, primary)
				}
			} else {
				fmt.Printf("[rebalance] push of %s to %s not visible yet; keeping local copy\n", primary, fname)
			}
		} else {
			fmt.Printf("[rebalance] primary %s missing %s; push failed; keeping local copy\n", primary, fname)
		}
	}
	for _, fname := range localNames {
		if GetPrimaryReplicaNode(fname) != NODE_ID {
			continue
		}

		targets := DesiredReplicaSet(fname, replicationFactor)

		for _, targetID := range targets {
			if targetID == NODE_ID {
				continue
			}

			has, _ := remoteHasFileByNodeID(targetID, fname)
			if has {
				continue
			}

			fmt.Printf("[rebalance] %s is a new replica for %s, pushing file...\n", targetID, fname)
			if !pushReplicaToNode(fname, targetID) {
				fmt.Printf("[rebalance] ...failed to push %s to %s\n", fname, targetID)
			}
		}
	}

	myPos := ComputeRingPosition(NODE_ID)
	succs := GetSuccessorNodes(myPos)
	if len(succs) > 0 {
		src := succs[0]
		names, err := listFilesHostedBy(src)
		if err == nil {
			for _, fname := range names {
				if GetPrimaryReplicaNode(fname) == NODE_ID && localBlockCount(fname) == 0 {
					get := GetMessage{Name: fname, Requester: NODE_ID}
					member, ok := GetMember(src)
					if !ok {
						continue
					}
					_ = SendMessage(member, Message{Kind: GETFILE, Data: string(MustMarshal(get))})
				}
			}
		} else {
			fmt.Printf("[rebalance] skip pull from successor %s: %v\n", src, err)
		}
	}

	if IsLeaderNode {
		restartFailedRainstormTasks()
	}
}

func deleteLocalFile(fname string) {
	fileStateMutex.Lock()
	delete(fileInfoMap, fname)
	delete(fileBlockMap, fname)
	delete(tempFileInfoMap, fname)
	delete(tempFileBlockMap, fname)
	delete(blockSeenMap, fname)
	fileStateMutex.Unlock()

	dir := STORAGE_LOCATION + fname
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("[rebalance] WARN: failed to delete disk dir %s: %v\n", dir, err)
	}
}

func pushFileToPrimary(fname, primary string) bool {
	fileStateMutex.RLock()
	info, ok := fileInfoMap[fname]
	if !ok || info == nil {
		fileStateMutex.RUnlock()
		return false
	}
	blocks := make([]*FileBlock, len(fileBlockMap[fname]))
	copy(blocks, fileBlockMap[fname])
	fileStateMutex.RUnlock()

	fi := *info
	fi.IsPrimary = true
	fi.Replicated = false
	createMsg := Message{Kind: CREATE, Data: string(MustMarshal(fi))}

	member, ok := GetMember(primary)
	if !ok {
		return false
	}

	if err := SendMessage(member, createMsg); err != nil {
		return false
	}

	for _, b := range blocks {
		if b == nil {
			continue
		}
		blk := *b
		blk.Replicated = false
		appendMsg := Message{Kind: APPEND, Data: string(MustMarshal(blk))}
		if err := SendMessage(member, appendMsg); err != nil {
			return false
		}
	}

	return true
}

func listFilesHostedBy(nodeID string) ([]string, error) {
	ip := GetIPFromID(nodeID)
	if ip == "" {
		return nil, fmt.Errorf("unknown nodeID %q", nodeID)
	}
	addr := net.JoinHostPort(ip, fmt.Sprint(SERVER_PORT))

	conn, err := net.DialTimeout("tcp", addr, 1500*time.Millisecond)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	req := Message{Kind: FILES}
	reqEnc, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(reqEnc); err != nil {
		return nil, err
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil || len(line) == 0 {
		if err == nil {
			return nil, fmt.Errorf("empty FILES response")
		}
		return nil, err
	}

	var resp Message
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		return nil, err
	}
	if resp.Kind != FILES {
		return nil, fmt.Errorf("unexpected FILES reply kind=%v", resp.Kind)
	}

	var names []string
	switch v := resp.Data.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &names); err != nil {
			return nil, err
		}
	case []byte:
		if err := json.Unmarshal(v, &names); err != nil {
			return nil, err
		}
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &names); err != nil {
			return nil, err
		}
	}
	return names, nil
}

func pushReplicaToNode(fname, targetNodeID string) bool {
	fileStateMutex.RLock()
	info, ok := fileInfoMap[fname]
	if !ok || info == nil {
		fileStateMutex.RUnlock()
		return false
	}
	blocks := make([]*FileBlock, len(fileBlockMap[fname]))
	copy(blocks, fileBlockMap[fname])
	fileStateMutex.RUnlock()

	fi := *info
	fi.IsPrimary = false
	fi.Replicated = true
	createMsg := Message{Kind: CREATE, Data: string(MustMarshal(fi))}

	member, ok := GetMember(targetNodeID)
	if !ok {
		return false
	}

	if err := SendMessage(member, createMsg); err != nil {
		return false
	}

	for _, b := range blocks {
		if b == nil {
			continue
		}
		blk := *b
		blk.Replicated = true
		appendMsg := Message{Kind: APPEND, Data: string(MustMarshal(blk))}
		if err := SendMessage(member, appendMsg); err != nil {
			return false
		}
	}
	return true
}

func restartFailedRainstormTasks() {
	taskRegistry.mu.RLock()
	assignments := make(map[TaskID]taskInfo)
	for tid, info := range taskRegistry.tasks {
		assignments[tid] = info
	}
	taskRegistry.mu.RUnlock()

	activeNodes := make(map[string]bool)
	for _, member := range GetSortedRingMembers() {
		activeNodes[member.Id] = true
	}

	for tid, info := range assignments {
		if _, isActive := activeNodes[info.nodeID]; !isActive {
			nodeLogger.Log("INFO", "RainStorm: Detected failed task %+v on node %s. Restarting...", tid, info.nodeID)

			payload, ok := lookupTaskPayload(tid)
			if !ok {
				nodeLogger.Log("ERROR", "RainStorm: Cannot restart task %+v: original payload not found.", tid)
				continue
			}

			members := GetSortedRingMembers()
			if len(members) == 0 {
				nodeLogger.Log("ERROR", "RainStorm: No available workers to restart task %+v", tid)
				continue
			}

			newNodeID := ""
			for _, m := range members {
				if m.Id != info.nodeID {
					newNodeID = m.Id
					break
				}
			}
			if newNodeID == "" {
				nodeLogger.Log("ERROR", "RainStorm: No alternative active workers to restart task %+v", tid)
				continue
			}

			payload.WorkerNodeID = newNodeID
			registerTaskAssignment(tid, newNodeID, payload)

			memberInfo, exists := GetMember(newNodeID)
			if !exists {
				nodeLogger.Log("ERROR", "RainStorm: Failed to get member info for new node %s", newNodeID)
				continue
			}

			logFile := taskLogLocalPath(payload.TaskID.JobID, payload.TaskID.Stage, payload.TaskID.Index)
			nodeLogger.Log("INFO",
				"RainStorm: restarting task %s on VM=%s pid=%d op=%s log=%s",
				tid.String(), newNodeID, os.Getpid(), payload.OpExe, logFile)

			msg := Message{Kind: RS_START_TASK, Data: payload}
			if err := SendMessage(memberInfo, msg); err != nil {
				nodeLogger.Log("ERROR", "RainStorm: Failed to send RS_START_TASK to %s for %+v: %v", newNodeID, tid, err)
				continue
			}

			nodeLogger.Log("INFO", "RainStorm: Re-assigned task %+v to node %s", tid, newNodeID)
		}
	}
}
