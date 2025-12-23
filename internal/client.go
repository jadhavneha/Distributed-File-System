package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"
)

var clientLogger = NewLogger("client")

var isMeasuringReplication uint32
var reReplicationBandwidth uint64
var isMeasuringRebalance uint32
var rebalanceBandwidth uint64

func startClient(clientServerChan chan int) {
	_, _ = <-clientServerChan, <-clientServerChan

	for {
		members := GetAllMembers()

		var memberIDs []string
		for id := range members {
			memberIDs = append(memberIDs, id)
		}
		Shuffle(memberIDs)

		for _, nodeID := range memberIDs {
			go PingMember(nodeID)
			time.Sleep(PING_INTERVAL_MILLISECONDS * time.Millisecond)
		}
	}
}

// PingMember sends a PING message to a node and handles ACKs or failures.
func PingMember(nodeID string) {

	if nodeID == NODE_ID {
		return
	}

	member, ok := GetMember(nodeID)
	if !ok || member.Failed {
		return
	}

	conn, err := net.Dial("tcp", GetServerEndpoint(member.Host))
	if err != nil {
		clientLogger.Log("ERROR", "Node %s connection failed: %v", nodeID, err)
		go DeleteMemberAndReReplicate(nodeID, clientLogger)
		go OnNodeFailure(nodeID)
		piggybackBuffer.AddPiggybackMessage(Message{Kind: FAIL, Data: nodeID})
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			clientLogger.Log("WARN", "Failed to close connection to %s: %v", nodeID, err)
		}
	}()

	msgs := piggybackBuffer.GetUnexpired()
	data, err := json.Marshal(msgs)

	pingMsg := Message{Kind: PING, Data: string(data)}

	pingMsgEnc, err := json.Marshal(pingMsg)
	if err != nil {
		//clientLogger.Log("ERROR", "Encoding PING message failed: %v", err)
		return
	}

	if _, err := conn.Write(pingMsgEnc); err != nil {
		//clientLogger.Log("ERROR", "Failed to send PING to %s: %v", nodeID, err)
		return
	}

	buffer := make([]byte, 8192)
	if err := conn.SetReadDeadline(time.Now().Add(TIMEOUT_DETECTION_MILLISECONDS * time.Millisecond)); err != nil {
		//clientLogger.Log("WARN", "Failed to set read deadline for %s: %v", nodeID, err)
	}

	mLen, err := conn.Read(buffer)
	if err != nil {
		//clientLogger.Log("ERROR", "Connection to node [%s] failed: %v", nodeID, err)
		//clientLogger.Log("INFO", "Marked %s as failed", nodeID)
		fmt.Printf("Marked %s as failed\n", nodeID)

		go DeleteMemberAndReReplicate(nodeID, clientLogger)
		go OnNodeFailure(nodeID)
		piggybackBuffer.AddPiggybackMessage(Message{Kind: FAIL, Data: nodeID})
		return
	}

	ackMsgs, err := DecodeAckMessage(buffer[:mLen])
	if err != nil {
		//clientLogger.Log("ERROR", "Decoding ACK message failed for %s: %v", nodeID, err)
		return
	}

	for _, subMsg := range ackMsgs {
		switch subMsg.Kind {
		case HELLO:
			if err := ProcessHelloMessage(clientLogger, subMsg); err != nil {
				//clientLogger.Log("ERROR", "Error processing HELLO message from %s: %v", nodeID, err)
			}
		case LEAVE, FAIL:
			if err := ProcessFailOrLeaveMessage(clientLogger, subMsg); err != nil {
				//clientLogger.Log("ERROR", "Error processing FAIL/LEAVE message from %s: %v", nodeID, err)
			}
		default:
			log.Printf("Unknown message kind %d received in ACK from %s", subMsg.Kind, nodeID)
		}
	}
}

// ExitGroup sends a LEAVE message to all nodes before exiting.
func ExitGroup() {
	fmt.Printf("Exiting node %s\n", NODE_ID)

	leaveMsg := NewLeaveMessage(NODE_ID)

	leaveEnc, err := json.Marshal(leaveMsg)
	if err != nil {
		//clientLogger.Log("ERROR", "Encoding LEAVE message failed: %v", err)
		return
	}

	members := GetAllMembers()
	for nodeID, memberInfo := range members {
		if nodeID == NODE_ID {
			continue
		}
		conn, err := net.Dial("tcp", GetServerEndpoint(memberInfo.Host))
		if err != nil {
			//clientLogger.Log("WARN", "Could not connect to %s to send LEAVE: %v", nodeID, err)

			continue
		}

		if _, err := conn.Write(leaveEnc); err != nil {
			//clientLogger.Log("WARN", "Failed to send LEAVE to %s: %v", nodeID, err)
		}
		if err := conn.Close(); err != nil {
			//clientLogger.Log("WARN", "Failed to close connection to %s: %v", nodeID, err)
		}
	}

	os.Exit(0)
}

// SendReplicationMessages sends a list of files to replicate to another node.
func SendReplicationMessages(nodeID string, files []*FileInfo, ch chan error) {
	fmt.Printf("Replicating %d file(s) to node %s\n", len(files), nodeID)

	encodedFiles, err := json.Marshal(files)
	if err != nil {
		ch <- err
		return
	}

	msg := Message{Kind: REPLICATE, Data: string(encodedFiles)}
	msgEnc, err := json.Marshal(msg)
	if err != nil {
		ch <- err
		return
	}

	member, ok := GetMember(nodeID)
	if !ok {
		ch <- fmt.Errorf("could not find member %s", nodeID)
		return
	}

	conn, err := net.Dial("tcp", GetServerEndpoint(member.Host))
	if err != nil {
		ch <- err
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			ch <- err
		}
	}()

	if _, err := conn.Write(msgEnc); err != nil {
		ch <- err
		return
	}

	buffer := make([]byte, 8192)
	if _, err := conn.Read(buffer); err != nil {
		ch <- err
		return
	}

	ch <- nil
}

// SendAnyReplicationMessage sends an arbitrary replication-related message.
func SendAnyReplicationMessage(member MemberInfo, msg Message, ch chan error) {

	if member.Host == "" {
		ch <- fmt.Errorf("invalid member (empty host)")
		return
	}

	conn, err := net.Dial("tcp", GetServerEndpoint(member.Host))
	if err != nil {
		ch <- err
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			ch <- err
		}
	}()

	msgEnc, err := json.Marshal(msg)
	if err != nil {
		ch <- err
		return
	}

	n, err := conn.Write(msgEnc)
	if err != nil {
		ch <- err
		return
	}

	if atomic.LoadUint32(&isMeasuringReplication) == 1 {
		atomic.AddUint64(&reReplicationBandwidth, uint64(n))
	}

	buffer := make([]byte, 8192)
	if _, err := conn.Read(buffer); err != nil {
		ch <- err
		return
	}

	ch <- nil
}

func SendMessageToMember(member MemberInfo, msg Message) error {
	msgEnc, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	conn, err := net.Dial("tcp", GetServerEndpoint(member.Host))
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			//clientLogger.Log("WARN", "Failed to close connection to %s: %v", member.Host, err)
		}
	}()

	n, err := conn.Write(msgEnc)
	if err != nil {
		return err
	}

	if atomic.LoadUint32(&isMeasuringRebalance) == 1 {
		atomic.AddUint64(&rebalanceBandwidth, uint64(n))
	}

	return nil
}

// SendMessage sends a message and waits for any reply bytes (ignored).
func SendMessage(member MemberInfo, msg Message) error {
	return SendMessageToMember(member, msg)
}

// SendMessageGetReply sends a message and decodes a reply message.
func SendMessageGetReply(member MemberInfo, msg Message) (Message, error) {
	var resp Message

	msgEnc, err := json.Marshal(msg)
	if err != nil {
		return resp, err
	}

	conn, err := net.Dial("tcp", GetServerEndpoint(member.Host))
	if err != nil {
		return resp, err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			//clientLogger.Log("WARN", "Failed to close connection to %s: %v", member.Host, err)
		}
	}()

	if _, err := conn.Write(msgEnc); err != nil {
		return resp, err
	}

	buffer := make([]byte, 8192)
	mLen, err := conn.Read(buffer)
	if err != nil {
		return resp, err
	}

	err = json.Unmarshal(buffer[:mLen], &resp)
	return resp, err
}
