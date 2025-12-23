package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", fmt.Errorf("failed to list network interfaces: %w", err)
	}

	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		ipv4 := ipnet.IP.To4()
		if ipv4 != nil && !ipv4.IsUnspecified() && !ipv4.IsLinkLocalUnicast() {
			return ipv4.String(), nil
		}
	}

	return "", errors.New("no valid local IPv4 address found")
}

func GetIPFromID(id string) string {
	if i := strings.IndexByte(id, '@'); i > 0 {
		return id[:i]
	}
	return id
}

func ConstructNodeID(ip string) string {
	return fmt.Sprintf("%s@%d", ip, time.Now().UnixNano())
}

func GetServerEndpoint(host string) string {
	return fmt.Sprintf("%s:%d", host, SERVER_PORT)
}

func DecodeAckMessage(messageEnc []byte) ([]Message, error) {
	var wrapper Message
	if err := json.Unmarshal(messageEnc, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to decode outer message: %w", err)
	}

	if wrapper.Kind != ACK {
		return nil, fmt.Errorf("expected ACK message, got %s", wrapper.Kind.String())
	}

	var messages []Message
	dataStr, ok := wrapper.Data.(string)
	if !ok {
		return messages, nil
	}

	if dataStr == "" {
		return messages, nil
	}

	if err := json.Unmarshal([]byte(dataStr), &messages); err != nil {
		return nil, fmt.Errorf("failed to decode ACK submessages from string: %w", err)
	}
	return messages, nil
}

func Shuffle(slice []string) {
	n := len(slice)
	for i := n - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func DeleteMemberAndReReplicate(nodeID string, logger *Logger) {
	logger.Log("INFO", "Starting re-replication process due to node removal: %s", nodeID)

	RemoveMember(nodeID, logger)

	updatedPrimaryFiles := UpdatePrimaryFiles()
	logger.Log("INFO", "Primary replicas updated after removal of %s", nodeID)

	go func(files []*FileInfo) {
		logger.Log("INFO", "Initiating background re-replication for %d files", len(files))

		atomic.StoreUint32(&isMeasuringReplication, 1)
		atomic.StoreUint64(&reReplicationBandwidth, 0)
		startTime := time.Now()

		ReplicateFilesWrapper(files)

		duration := time.Since(startTime)
		atomic.StoreUint32(&isMeasuringReplication, 0)
		totalBytes := atomic.LoadUint64(&reReplicationBandwidth)

		logger.Log("EXPERIMENT", "Re-replication for node %s complete. Duration: %v, Bandwidth: %d bytes", nodeID, duration, totalBytes)
		logger.Log("INFO", "Re-replication process completed for node %s", nodeID)

		go func(d time.Duration, b uint64, failedNode string) {
			payload := fmt.Sprintf(
				"FailedNode: %s\nDuration: %v\nBandwidth: %d bytes",
				failedNode, d, b,
			)

			collectorURL := "http://" + LEADER_SERVER_HOST + ":9999/report"

			resp, err := http.Post(collectorURL, "text/plain", strings.NewReader(payload))
			if err != nil {
				logger.Log("ERROR", "Failed to send metrics to collector: %v", err)
				return
			}
			resp.Body.Close()
			logger.Log("INFO", "Successfully reported metrics to collector.")

		}(duration, totalBytes, nodeID)

	}(updatedPrimaryFiles)
}

func encodeWithType(kind MessageType, messages Message) ([]byte, error) {
	messagesEnc, err := json.Marshal(messages)
	if err != nil {
		return nil, fmt.Errorf("failed to encode message list: %w", err)
	}

	wrapped := Message{Kind: kind, Data: string(messagesEnc)}
	out, err := json.Marshal(wrapped)
	if err != nil {
		return nil, fmt.Errorf("failed to encode %s wrapper: %w", kind.String(), err)
	}

	return out, nil
}
