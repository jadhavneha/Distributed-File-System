package main

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func addNodeToRing(isLeader bool, leaderHost string, logger *Logger) error {
	if !isLeader {
		fmt.Printf("Attempting to connect to leader at %s:%d\n", leaderHost, SERVER_PORT)
		localIP, err := GetLocalIP()
		if err != nil {
			fmt.Println("introduceNode error:", err)
			return fmt.Errorf("unable to join the group: %w", err)
		}
		members, err := introduceNode(leaderHost, localIP)
		fmt.Println("Received membership list from leader:", members)
		if err != nil {
			fmt.Println("could not get local IP:", err)
			return fmt.Errorf("could not get local IP: %w", err)
		}
		fmt.Println("Local IP for initialization:", localIP)
		nodeID := InitializeMembership(members, localIP, logger)
		NODE_ID = nodeID
		fmt.Println("Node initialized with ID:", nodeID)
		helloMsg := NewMessage(HELLO, nodeID)
		piggybackBuffer.AddPiggybackMessage(*helloMsg)
		fmt.Println("Added piggyback HELLO message for:", nodeID)
	} else {
		fmt.Println("Starting as leader node.")
		NODE_ID = ConstructNodeID(leaderHost)
		RING_POSITION = ComputeRingPosition(NODE_ID)
		fmt.Println("Leader Node ID:", NODE_ID)
		fmt.Println("Leader Ring Position:", RING_POSITION)
	}
	fmt.Println("Joined the group as:", NODE_ID)
	return nil
}

func introduceNode(leaderHost string, localIP string) (map[string]MemberInfo, error) {
	endpoint := GetServerEndpoint(leaderHost)
	fmt.Printf("[DEBUG] Attempting TCP connection to leader at %s\n", endpoint)
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		fmt.Printf("[ERROR] Failed to connect to leader %s: %v\n", leaderHost, err)
		return nil, fmt.Errorf("failed to connect to leader %s: %w", leaderHost, err)
	}
	fmt.Println("[DEBUG] Connected to leader successfully.")
	defer conn.Close()
	joinMsg := NewJoinMessage(localIP)
	joinMsgEnc, err := joinMsg.Encode()
	if err != nil {
		fmt.Printf("[ERROR] Failed to encode join message: %v\n", err)
		return nil, fmt.Errorf("failed to encode join message: %w", err)
	}
	fmt.Println("[DEBUG] Encoded join message.")
	nWritten, err := conn.Write(joinMsgEnc)

	if err != nil {
		fmt.Printf("[ERROR] Failed to send join request: %v\n", err)
		return nil, fmt.Errorf("failed to send join request: %w", err)
	}
	fmt.Printf("[DEBUG] Sent join request (%d bytes).\n", nWritten)
	buffer := make([]byte, 4096)
	n, err := conn.Read(buffer)
	conn.Close()
	if err != nil {
		fmt.Printf("[ERROR] Failed to read join response: %v\n", err)
		return nil, fmt.Errorf("failed to read join response: %w", err)
	}
	fmt.Printf("[DEBUG] Read %d bytes as join response.\n", n)
	ackMsgs, err := DecodeAckMessage(buffer[:n])
	if err != nil {
		fmt.Printf("[ERROR] Failed to decode ACK wrapper: %v\n", err)
		return nil, fmt.Errorf("failed to decode ACK wrapper: %w", err)
	}
	fmt.Println("[DEBUG] Decoded ACK message(s).")
	if len(ackMsgs) == 0 {
		fmt.Println("[ERROR] No response message found in ACK.")
		return nil, fmt.Errorf("no response message found in ACK")
	}
	joinResponseMsg := ackMsgs[0]
	fmt.Println("[DEBUG] First ACK message extracted for join response.")
	members, err := ParseJoinResponse(joinResponseMsg)
	if err != nil {
		fmt.Printf("[ERROR] Invalid join response: %v\n", err)
		return nil, fmt.Errorf("invalid join response: %w", err)
	}
	fmt.Printf("[DEBUG] Parsed join response: %+v\n", members)
	return members, nil
}

func ParseJoinResponse(msg Message) (map[string]MemberInfo, error) {
	if msg.Kind != JOIN {
		return nil, fmt.Errorf("unexpected message kind: %s", msg.Kind.String())
	}
	members := make(map[string]MemberInfo)
	data, err := json.Marshal(msg.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal join data: %w", err)
	}
	if err := json.Unmarshal(data, &members); err != nil {
		return nil, fmt.Errorf("failed to parse members list: %w", err)
	}
	return members, nil
}

func InitializeMembership(members map[string]MemberInfo, localIP string, logger *Logger) string {
	type chosenInfo struct {
		id   string
		info MemberInfo
		ts   int64
	}

	byHost := make(map[string]chosenInfo)
	var nodeID string
	var newestSelfTS int64 = -1

	for id, info := range members {
		ip := GetIPFromID(id)

		var ts int64
		if at := strings.LastIndex(id, "@"); at > 0 {
			if parsed, err := strconv.ParseInt(id[at+1:], 10, 64); err == nil {
				ts = parsed
			}
		}

		if cur, ok := byHost[ip]; !ok || ts > cur.ts {
			byHost[ip] = chosenInfo{
				id: id,
				info: MemberInfo{
					Host:         ip,
					Failed:       info.Failed,
					RingPosition: info.RingPosition,
				},
				ts: ts,
			}
		}

		if ip == localIP && ts > newestSelfTS {
			newestSelfTS = ts
			nodeID = id
		}
	}

	for _, ch := range byHost {
		AddMemberFromInfo(ch.id, &ch.info, logger)

		if ch.info.Host == localIP && ch.id == nodeID {
			RING_POSITION = ch.info.RingPosition
		}
	}

	return nodeID
}

func IntroduceNodeToGroup(req string, ipAddr string, logger *Logger) (Message, error) {
	nodeID := ConstructNodeID(ipAddr)
	logger.Log("INFO", "Introducing node %s (%s) to the group", nodeID, ipAddr)

	if err := AddMember(nodeID, logger); err != nil {
		return Message{}, fmt.Errorf("failed to add new member: %w", err)
	}

	membershipList := GetAllMembers()

	membershipList[NODE_ID] = MemberInfo{
		Host:         LOCAL_IP,
		Failed:       false,
		RingPosition: RING_POSITION,
	}

	enc, err := json.Marshal(membershipList)
	if err != nil {
		return Message{}, fmt.Errorf("failed to encode membership list: %w", err)
	}

	resp := NewMessage(JOIN, json.RawMessage(enc))

	return *resp, nil
}
