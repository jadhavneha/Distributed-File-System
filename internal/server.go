package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

func startServer(logger *Logger, clientServerChan chan int, isLeader bool) {
	server, err := net.Listen("tcp", fmt.Sprintf(":%d", SERVER_PORT))
	if err != nil {
		log.Fatalf("Couldn't start server: %s", err.Error())
	}

	clientServerChan <- 1

	for {
		conn, err := server.Accept()
		if err != nil {
			logger.Log("ERROR", "Error accepting: %v", err)
			continue
		}

		go func(conn net.Conn, isLeader bool) {
			defer func() { _ = conn.Close() }()
			remoteAddress := strings.Split(conn.RemoteAddr().String(), ":")[0]

			var message Message
			decoder := json.NewDecoder(conn)
			if err := decoder.Decode(&message); err != nil {
				if err == io.EOF {
					fmt.Println("[LEADER SERVER] Connection closed by client before message received")
					return
				}
				fmt.Println("[LEADER SERVER] Invalid JSON received:", err)
				logger.Log("ERROR", "Invalid JSON received: %v", err)
				return
			}

			var messagesToPiggyback []Message
			switch message.Kind {
			case PING:
				messagesToPiggyback = piggybackBuffer.GetUnexpired()
				var messages []Message
				if message.Data != nil {
					if dataStr, ok := message.Data.(string); ok && dataStr != "" {
						if err := json.Unmarshal([]byte(dataStr), &messages); err != nil {
						} else {
							for _, subMessage := range messages {
								switch subMessage.Kind {
								case HELLO:
									go func(msg Message) {
										if err := ProcessHelloMessage(logger, msg); err != nil {
											logger.Log("ERROR", "ProcessHelloMessage error: %v", err)
										}
									}(subMessage)
								case LEAVE, FAIL:
									go func(msg Message) {
										if err := ProcessFailOrLeaveMessage(logger, msg); err != nil {
											logger.Log("ERROR", "ProcessFailOrLeaveMessage error: %v", err)
										}
									}(subMessage)
								default:
									logger.Log("ERROR", "Unexpected submessage kind in PING: %v", subMessage.Kind)
								}
							}
						}
					}
				}
			case JOIN:
				fmt.Println("[LEADER SERVER] Processing JOIN request from", remoteAddress)
				logger.Log("INFO", "Processing JOIN request from %s", remoteAddress)
				responseMessage, err := ProcessJoinMessage(logger, message, remoteAddress, isLeader)
				if err != nil {
					logger.Log("ERROR", "Failed to process join message: %v", err)
					messagesToPiggyback = []Message{}
				} else {
					messagesToPiggyback = []Message{responseMessage}
				}
			case LEAVE:
				messagesToPiggyback = piggybackBuffer.GetUnexpired()
				if err := ProcessFailOrLeaveMessage(logger, message); err != nil {
					logger.Log("ERROR", "ProcessFailOrLeaveMessage error: %v", err)
				}
			case REPLICATE:
				if err := ProcessReplicateMessage(logger, message); err != nil {
					logger.Log("ERROR", "ProcessReplicateMessage error: %v", err)
				}
			case CREATE:
				if err := ProcessCreateMessage(message, false); err != nil {
					logger.Log("ERROR", "ProcessCreateMessage error: %v", err)
				}
			case TEMP_CREATE:
				if err := ProcessCreateMessage(message, true); err != nil {
					logger.Log("ERROR", "ProcessCreateMessage (temp) error: %v", err)
				}
			case APPEND:
				if err := ProcessAppendMessage(message, false); err != nil {
					logger.Log("ERROR", "ProcessAppendMessage error: %v", err)
				}
			case TEMP_APPEND:
				if err := ProcessAppendMessage(message, true); err != nil {
					logger.Log("ERROR", "ProcessAppendMessage (temp) error: %v", err)
				}
			case CHECK:
				if err := ProcessCheckMessage(logger, message, conn, remoteAddress); err != nil {
					logger.Log("ERROR", "%v", err)
				}
				return
			case FILES:
				if err := ProcessFilesMessage(logger, message, conn, remoteAddress); err != nil {
					logger.Log("ERROR", "%v", err)
				}
				return
			case GETFILE:
				if err := ProcessGetFileMessage(logger, message, conn, remoteAddress); err != nil {
					logger.Log("ERROR", "%v", err)
				}
				return
			case MERGE:
				if err := ProcessMergeMessage(logger, message, conn, remoteAddress); err != nil {
					logger.Log("ERROR", "%v", err)
				}
				return
			case DELETE:
				if err := ProcessDeleteMessage(logger, message); err != nil {
					logger.Log("ERROR", "ProcessDeleteMessage error: %v", err)
				}
			case HAS_FILE:

				var req struct{ Name string }

				switch data := message.Data.(type) {
				case string:
					if err := json.Unmarshal([]byte(data), &req); err != nil {
						_ = sendReply(conn, Message{Kind: HAS_FILE_REPLY, Data: map[string]any{"present": false, "code": -1}})
						return
					}
				case map[string]any:
					if v, ok := data["Name"].(string); ok {
						req.Name = v
					}
				default:
					_ = sendReply(conn, Message{Kind: HAS_FILE_REPLY, Data: map[string]any{"present": false, "code": -1}})
					return
				}

				if req.Name == "" {
					_ = sendReply(conn, Message{Kind: HAS_FILE_REPLY, Data: map[string]any{"present": false, "code": -1}})
					return
				}

				present := localBlockCount(req.Name) > 0

				if !present {
					fileStateMutex.RLock()
					_, inMainMap := fileInfoMap[req.Name]
					_, inTempMap := tempFileInfoMap[req.Name]
					fileStateMutex.RUnlock()
					if inMainMap || inTempMap {
						present = true
					}
				}

				if !present {
					dir := STORAGE_LOCATION + req.Name
					if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
						if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
							present = true
						}
						if entries, err := os.ReadDir(dir); err == nil {
							for _, e := range entries {
								if e.IsDir() {
									continue
								}
								name := e.Name()
								if len(name) > 0 && name[0] != '.' {
									present = true
									break
								}
							}
						}
					} else {
						tdir := dir + "_temp"
						if fi, err := os.Stat(tdir); err == nil && fi.IsDir() {
							if entries, err := os.ReadDir(tdir); err == nil && len(entries) > 0 {
								present = true
							}
						}
					}
				}
				code := 0
				if present {
					code = 1
				}

				_ = sendReply(conn, Message{
					Kind: HAS_FILE_REPLY,
					Data: map[string]any{
						"present": present,
						"code":    code,
					},
				})
				return
			case RS_START_TASK:
				HandleRainstormStartTask(&message)
			case RS_STOP_TASK:
				HandleRainstormStopTask(&message)
			case RS_TUPLE:
				HandleRainstormTuple(&message)
			case RS_METRICS:
				HandleRainstormMetrics(&message)
			case RS_UPDATE_TASK_COUNT:
				HandleUpdateTaskCount(&message)
			default:
				logger.Log("ERROR", "Unexpected message kind: %v", message.Kind)
				fmt.Printf("[LEADER SERVER] Unexpected message kind: %v\n", message.Kind)
				return
			}

			data, err := json.Marshal(messagesToPiggyback)
			if err != nil {
				fmt.Println("[LEADER SERVER] Failed to marshal piggyback messages:", err)
				logger.Log("ERROR", "Failed to marshal piggyback messages: %v", err)
				return
			}
			ackMessage := Message{
				Kind: ACK,
				Data: string(data),
			}
			ackResponse, err := json.Marshal(ackMessage)
			if err != nil {
				fmt.Println("[LEADER SERVER] Failed to marshal ACK response:", err)
				logger.Log("ERROR", "Failed to generate response.")
				return
			}
			_, err = conn.Write(ackResponse)
			if err != nil {
				fmt.Println("[LEADER SERVER] Failed to write ack response:", err)
				logger.Log("ERROR", "Failed to write ack response: %v", err)
			}
		}(conn, isLeader)
	}
}

func ProcessCheckMessage(logger *Logger, message Message, conn net.Conn, _ string) error {
	var name string
	if m, ok := message.Data.(map[string]any); ok {
		if v, ok := m["Name"].(string); ok {
			name = v
		} else if v, ok := m["name"].(string); ok {
			name = v
		}
	}
	cnt := localBlockCount(name)

	resp := map[string]any{"has": cnt > 0, "blocks": cnt}
	inner := Message{Kind: CHECK, Data: resp}

	enc, err := encodeWithType(ACK, inner)
	if err != nil {
		logger.Log("ERROR", "encodeWithType(ACK, inner) failed: %v", err)
		return err
	}
	_, err = conn.Write(enc)
	return err
}

func ProcessJoinMessage(logger *Logger, message Message, addr string, isLeader bool) (Message, error) {

	if !isLeader {
		return Message{}, fmt.Errorf("unexpected JOIN message received for non Introducer node")
	}
	host := addr
	var payload map[string]string
	if raw, ok := message.Data.([]byte); ok {
		_ = json.Unmarshal(raw, &payload)
	} else {
		if str, ok := message.Data.(string); ok {
			_ = json.Unmarshal([]byte(str), &payload)
		}
	}
	if h := strings.TrimSpace(payload["Host"]); h != "" {
		host = h
	}

	joinResponse, err := IntroduceNodeToGroup("", host, logger)
	if err != nil {
		return Message{}, err
	}

	return joinResponse, nil
}

func ProcessHelloMessage(logger *Logger, message Message) error {

	nodeId := message.Data.(string)

	_, ok := GetMember(nodeId)

	if ok {
		logger.Log("INFO", fmt.Sprintf("Node %s already exists in membership info, Skipping HELLO \n", nodeId))
		return nil
	}

	if nodeId == NODE_ID {
		logger.Log("INFO", fmt.Sprintf("Received self hello message for ID: %s Skip \n", nodeId))
		return nil
	}

	err := AddMember(nodeId, logger)
	if err != nil {
		return err
	}

	return nil
}

func ProcessFailOrLeaveMessage(logger *Logger, message Message) error {

	nodeId := message.Data.(string)

	if nodeId == NODE_ID {
		fmt.Println("Received self failure message.")
		os.Exit(0)
	}

	_, ok := GetMember(nodeId)

	if ok {
		DeleteMemberAndReReplicate(nodeId, logger)
		go OnNodeFailure(nodeId)
		return nil
	}

	return nil
}

func ProcessReplicateMessage(logger *Logger, message Message) error {

	encodedFiles, ok := message.Data.(string)
	if !ok {
		return fmt.Errorf("invalid data type for REPLICATE message")
	}

	var files []FileInfo
	err := json.Unmarshal([]byte(encodedFiles), &files)
	if err != nil {
		return err
	}

	for _, file := range files {
		file.IsPrimary = false
	}

	return nil
}

func ProcessGetFileMessage(logger *Logger, message Message, conn net.Conn, remoteAddress string) error {
	fmt.Printf("[NODE %s] Start GETFILE: Request from %s\n", NODE_ID, remoteAddress)
	encodedGetFileRequest, ok := message.Data.(string)
	if !ok {
		return fmt.Errorf("invalid data type for GETFILE message")
	}

	var getFileStruct GetMessage
	if err := json.Unmarshal([]byte(encodedGetFileRequest), &getFileStruct); err != nil {
		return err
	}

	if GetPrimaryReplicaNode(getFileStruct.Name) != NODE_ID {
		primary := GetPrimaryReplicaNode(getFileStruct.Name)
		if primary == "" {
			return fmt.Errorf("GETFILE: no primary for %q", getFileStruct.Name)
		}
		member, ok := GetMember(primary)
		if !ok {
			return fmt.Errorf("GETFILE: could not find primary member %s", primary)
		}
		return SendMessage(member, message)
	}

	fi, ok := fileInfoMap[getFileStruct.Name]
	if !ok || fi == nil {
		logger.Log("INFO", "Requested file not found locally: %s", getFileStruct.Name)
	}

	if !ok || fi == nil {
		return nil
	}

	requesterMember, ok := GetMember(getFileStruct.Requester)
	if !ok {
		return fmt.Errorf("GETFILE: could not find requester member %s", getFileStruct.Requester)
	}

	encodedFileInfo, err := json.Marshal(fi)
	if err != nil {
		return err
	}
	createMessage := Message{Kind: TEMP_CREATE, Data: string(encodedFileInfo)}
	if err := SendMessage(requesterMember, createMessage); err != nil {
		return err
	}

	for _, eachhdfsfileblock := range fileBlockMap[getFileStruct.Name] {
		encodedFileBlock, err := json.Marshal(eachhdfsfileblock)
		if err != nil {
			return err
		}
		appendMessage := Message{Kind: TEMP_APPEND, Data: string(encodedFileBlock)}
		if err := SendMessage(requesterMember, appendMessage); err != nil {
			return err
		}
	}
	fmt.Printf("[NODE %s] End GETFILE: %s (All blocks sent to %s)\n", NODE_ID, getFileStruct.Name, getFileStruct.Requester)
	return nil
}

func ProcessFilesMessage(logger *Logger, message Message, conn net.Conn, remoteAddress string) error {

	filenames := GetFilesOnNode()

	encodedFilenames, err := json.Marshal(filenames)
	if err != nil {
		return err
	}

	filesResponse := Message{Kind: FILES, Data: string(encodedFilenames)}
	encodedFilesResponse, err := json.Marshal(filesResponse)
	if err != nil {
		return err
	}

	if _, err := conn.Write(encodedFilesResponse); err != nil {
		logger.Log("ERROR", "conn.Write error in ProcessFilesMessage: %v", err)
	}
	if err := conn.Close(); err != nil {
		logger.Log("ERROR", "conn.Close error in ProcessFilesMessage: %v", err)
	}

	return nil
}

func ProcessDeleteMessage(logger *Logger, message Message) error {

	hdfsfilename := message.Data.(string)
	fileStateMutex.Lock()
	delete(fileInfoMap, hdfsfilename)
	delete(fileBlockMap, hdfsfilename)
	fileStateMutex.Unlock()
	return nil
}

func ProcessMergeMessage(logger *Logger, message Message, conn net.Conn, remoteAddress string) error {

	hdfsFileName, ok := message.Data.(string)
	if !ok {
		return fmt.Errorf("invalid data type for MERGE message")
	}
	if err := MergeHDFSFileOptimized(hdfsFileName); err != nil {
		logger.Log("ERROR", "MergeHDFSFileOptimized error in ProcessMergeMessage: %v", err)
	}

	return nil
}

func sendReply(conn net.Conn, msg Message) error {
	bw := bufio.NewWriter(conn)
	b, err := msg.Encode()
	if err != nil {
		return err
	}
	if _, err := bw.Write(append(b, '\n')); err != nil {
		return err
	}
	return bw.Flush()
}
