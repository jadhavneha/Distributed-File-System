package main

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

func ComputeRingPosition(id string) int {
	hasher := fnv.New64a()
	hasher.Write([]byte(id))
	return int(hasher.Sum64() % RING_NUM_POINTS)
}

var (
	ringCacheMu   sync.RWMutex
	sortedMembers []RingMemberInfo
	nextOccupied  []int
	nextNodeIdx   []int
)

func rebuildRingCache() {
	members := getRingMembersUnsafe()

	sort.Slice(members, func(i, j int) bool {
		return members[i].RingPosition < members[j].RingPosition
	})

	n := len(members)
	no := make([]int, RING_NUM_POINTS)
	for i := range no {
		no[i] = -1
	}
	nni := make([]int, n)

	occupied := make([]int, RING_NUM_POINTS)
	for i := range occupied {
		occupied[i] = -1
	}
	for idx, m := range members {
		if m.RingPosition >= 0 && m.RingPosition < RING_NUM_POINTS {
			occupied[m.RingPosition] = idx
		}
	}

	last := -1
	for p := RING_NUM_POINTS - 1; p >= 0; p-- {
		if occupied[p] != -1 {
			last = occupied[p]
			no[p] = last
		} else {
			no[p] = last
		}
	}
	wrap := -1
	for p := 0; p < RING_NUM_POINTS; p++ {
		if occupied[p] != -1 {
			wrap = occupied[p]
			break
		}
	}
	if wrap != -1 {
		for p := RING_NUM_POINTS - 1; p >= 0 && no[p] == -1; p-- {
			no[p] = wrap
		}
	}

	for i := 0; i < n; i++ {
		nni[i] = (i + 1) % n
	}

	ringCacheMu.Lock()
	sortedMembers = members
	nextOccupied = no
	nextNodeIdx = nni
	ringCacheMu.Unlock()
}

func GetSortedRingMembers() []RingMemberInfo {
	ringCacheMu.RLock()
	defer ringCacheMu.RUnlock()
	out := make([]RingMemberInfo, len(sortedMembers))
	copy(out, sortedMembers)
	return out
}

func GetSuccessorNodes(position int) []string {
	ringCacheMu.RLock()
	defer ringCacheMu.RUnlock()

	if len(sortedMembers) < 2 {
		return nil
	}

	if position < 0 || position >= RING_NUM_POINTS {
		position = ((position % RING_NUM_POINTS) + RING_NUM_POINTS) % RING_NUM_POINTS
	}

	startIdx := -1
	for i, member := range sortedMembers {
		if member.RingPosition >= position {
			startIdx = i
			break
		}
	}
	if startIdx == -1 {
		startIdx = 0
	}

	successors := make([]string, 0, NUM_REPLICAS-1)
	seen := make(map[string]struct{})

	for i := 1; i < len(sortedMembers) && len(successors) < NUM_REPLICAS-1; i++ {
		currentIdx := (startIdx + i) % len(sortedMembers)
		memberID := sortedMembers[currentIdx].Id

		if _, exists := seen[memberID]; !exists {
			successors = append(successors, memberID)
			seen[memberID] = struct{}{}
		}
	}

	return successors
}

func GetPrimaryReplicaNode(filename string) string {
	pos := ComputeRingPosition(filename)
	ringCacheMu.RLock()
	defer ringCacheMu.RUnlock()

	if len(sortedMembers) == 0 {
		return ""
	}
	idx := nextOccupied[pos]
	if idx == -1 {
		return sortedMembers[0].Id
	}
	return sortedMembers[idx].Id
}

func PrintRingState() {
	members := GetSortedRingMembers()
	for i, m := range members {
		selfMarker := ""
		if m.Id == NODE_ID {
			selfMarker = "<- self"
		}
		fmt.Printf("%d | Node: %s | RingPos: %d %s\n", i, m.Id, m.RingPosition, selfMarker)
	}
}
