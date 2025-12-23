package main

import (
	"fmt"
	"sort"
	"sync"
)

var membershipInfo = make(map[string]MemberInfo)
var membershipMutex = sync.RWMutex{}

type RingMemberInfo struct {
	Id           string
	RingPosition int
}

func AddMember(nodeId string, logger *Logger) error {
	ipAddr := GetIPFromID(nodeId)

	membershipMutex.Lock()
	defer membershipMutex.Unlock()

	if _, leaderExists := membershipInfo[NODE_ID]; !leaderExists {
		membershipInfo[NODE_ID] = MemberInfo{
			Host:         GetIPFromID(NODE_ID),
			Failed:       false,
			RingPosition: RING_POSITION,
		}
		logger.Log("INFO", "Leader %s added to membership", NODE_ID)
	}

	if nodeId == NODE_ID {
		logger.Log("INFO", "Attempting to add leader again: %s, skipped.", nodeId)
		return nil
	}

	if _, exists := membershipInfo[nodeId]; exists {
		logger.Log("INFO", "Member %s already in membership, skipping.", nodeId)
		return nil
	}

	for existingID, info := range membershipInfo {
		if existingID != nodeId && info.Host == ipAddr {
			logger.Log("INFO",
				"Removing stale nodeID %s for host %s before adding %s",
				existingID, ipAddr, nodeId,
			)
			delete(membershipInfo, existingID)
		}
	}

	logger.Log("INFO", "Adding new member: %s (%s)", nodeId, ipAddr)
	membershipInfo[nodeId] = MemberInfo{
		Host:         ipAddr,
		Failed:       false,
		RingPosition: ComputeRingPosition(nodeId),
	}

	rebuildRingCache()

	go RebalanceTick(NUM_REPLICAS)

	logger.Log("INFO", "JOIN NODE: %s", nodeId)
	return nil
}

// AddMemberFromInfo adds a member using an existing MemberInfo struct.
func AddMemberFromInfo(nodeId string, member *MemberInfo, logger *Logger) {
	membershipMutex.Lock()
	defer membershipMutex.Unlock()

	membershipInfo[nodeId] = *member
	rebuildRingCache()
	logger.Log("INFO", "Restored member from info: %s", nodeId)
}

// GetAllMembers returns a copy of all current members.
func GetAllMembers() map[string]MemberInfo {
	membershipMutex.RLock()
	defer membershipMutex.RUnlock()

	out := make(map[string]MemberInfo, len(membershipInfo))
	for k, v := range membershipInfo {
		out[k] = v
	}
	return out
}

// GetMember returns information for a specific node.
func GetMember(nodeId string) (MemberInfo, bool) {
	membershipMutex.RLock()
	defer membershipMutex.RUnlock()

	member, exists := membershipInfo[nodeId]
	return member, exists
}

// RemoveMember marks a member as failed and deletes it from the membership list.
func RemoveMember(nodeId string, logger *Logger) {
	membershipMutex.Lock()

	if _, exists := membershipInfo[nodeId]; !exists {
		membershipMutex.Unlock()
		return
	}

	delete(membershipInfo, nodeId)
	rebuildRingCache()

	membershipMutex.Unlock()

	logger.Log("INFO", "DELETE NODE: %s", nodeId)
	fmt.Printf("DELETE NODE: %s\n", nodeId)

	go RebalanceTick(NUM_REPLICAS)
}

// PrintMembership prints the current membership list.
func PrintMembership() {
	membershipMutex.RLock()
	defer membershipMutex.RUnlock()

	fmt.Println("=== MEMBERSHIP LIST ===")
	ids := make([]string, 0, len(membershipInfo))
	for id := range membershipInfo {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		info := membershipInfo[id]
		fmt.Printf("NODE ID: %s | VM: %-15s | RING POSITION: %d | FAILED: %v\n",
			id, info.Host, info.RingPosition, info.Failed)
	}
	fmt.Println("==========================================")
}

func GetNodeIDFromIP(ip string) (string, bool) {
	membershipMutex.RLock()
	defer membershipMutex.RUnlock()

	for id, info := range membershipInfo {
		if info.Host == ip {
			return id, true
		}
	}
	return "", false
}

// getRingMembersUnsafe assumes the membershipMutex is already held for reading or writing.
func getRingMembersUnsafe() []RingMemberInfo {
	members := make([]RingMemberInfo, 0, len(membershipInfo))
	for id, info := range membershipInfo {
		members = append(members, RingMemberInfo{id, info.RingPosition})
	}
	return members
}
