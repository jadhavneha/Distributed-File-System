package main

import (
	"encoding/json"
	"fmt"
)

type MessageType int32

const (
	PING MessageType = iota
	ACK
	JOIN
	CREATE
	APPEND
	CHECK
	REPLICATE
	FILES
	GETFILE
	TEMP_CREATE
	TEMP_APPEND
	DELETE
	MERGE
	LEAVE
	FAIL
	HELLO
	HAS_FILE
	HAS_FILE_REPLY

	RS_START_TASK
	RS_STOP_TASK
	RS_TUPLE
	RS_METRICS
	RS_UPDATE_TASK_COUNT
)

func (m MessageType) String() string {
	names := []string{
		"PING",
		"ACK",
		"JOIN",
		"CREATE",
		"APPEND",
		"CHECK",
		"REPLICATE",
		"FILES",
		"GETFILE",
		"TEMP_CREATE",
		"TEMP_APPEND",
		"DELETE",
		"MERGE",
		"LEAVE",
		"FAIL",
		"HELLO",
		"HAS_FILE",
		"HAS_FILE_REPLY",
		"RS_START_TASK",
		"RS_STOP_TASK",
		"RS_TUPLE",
		"RS_METRICS",
		"RS_UPDATE_TASK_COUNT",
	}
	if int(m) < len(names) {
		return names[m]
	}
	return fmt.Sprintf("UNKNOWN(%d)", m)
}

type MemberInfo struct {
	Failed       bool   `json:"failed"`
	Host         string `json:"host"`
	RingPosition int    `json:"ring_position"`
}

type Message struct {
	Kind MessageType `json:"kind"`
	Data any         `json:"data,omitempty"`
}

func (m *Message) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func DecodeMessage(data []byte) (*Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	return &msg, err
}

func NewMessage(kind MessageType, data any) *Message {
	return &Message{Kind: kind, Data: data}
}

type PiggybackMessage struct {
	Message Message `json:"message"`
	TTL     int     `json:"ttl"`
}

func (p *PiggybackMessage) Encode() ([]byte, error) {
	return json.Marshal(p)
}

func NewJoinMessage(host string) *Message {
	data := map[string]string{
		"Host": host,
	}
	return NewMessage(JOIN, data)
}

func NewHasFileRequest(hdfsName string) *Message {
	return NewMessage(HAS_FILE, struct {
		Name string `json:"Name"`
	}{Name: hdfsName})
}

func NewHasFileReply(present bool, code int) *Message {
	return NewMessage(HAS_FILE_REPLY, struct {
		Present bool `json:"present"`
		Code    int  `json:"code"`
	}{Present: present, Code: code})
}

func NewLeaveMessage(nodeID string) *Message {
	return NewMessage(LEAVE, nodeID)
}
