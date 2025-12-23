package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Logger handles thread-safe logging to a file
type Logger struct {
	mu     sync.Mutex
	lg     *log.Logger
	fn     string
	writer *bufio.Writer
}

func NewLogger(nodeID string) *Logger {
	os.MkdirAll("internal/logs", 0o755)
	fn := filepath.Join("internal/logs", fmt.Sprintf("%s.log", sanitize(nodeID)))

	f, err := os.OpenFile(fn, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		panic(fmt.Sprintf("failed to open log file: %v", err))
	}

	writer := bufio.NewWriter(f)

	return &Logger{
		lg:     log.New(writer, "", 0),
		fn:     fn,
		writer: writer,
	}
}

func sanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' || r == '@' || r == '.' {
			out = append(out, r)
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}

func (l *Logger) Log(tag string, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := time.Now().Format(time.RFC3339Nano)
	l.lg.Printf("[%s] %s "+format, append([]any{tag, ts}, args...)...)
	l.writer.Flush()
}

func (l *Logger) Path() string { return l.fn }

func PrintMessage(logger *Logger, direction string, message Message, nodeId string) {
	currentTime := time.Now().Format(time.RFC3339Nano)

	logger.mu.Lock()
	defer logger.mu.Unlock()

	switch message.Kind {
	case PING, ACK:
		fmt.Fprintf(logger.writer, "[%s] [%s] %s message (%s):\n",
			currentTime, direction, message.Kind.String(), nodeId)

		var messages []Message
		dataBytes, err := json.Marshal(message.Data)
		if err != nil {
			fmt.Fprintf(logger.writer, "Failed to marshal submessages: %v\n", err)
			return
		}
		if err := json.Unmarshal(dataBytes, &messages); err != nil {
			fmt.Fprintf(logger.writer, "Failed to unmarshal submessages: %v\n", err)
			return
		}

		fmt.Fprintf(logger.writer, "Submessages vvvvv\n")
		for _, subMessage := range messages {
			PrintMessage(logger, direction, subMessage, nodeId)
		}
		fmt.Fprintf(logger.writer, "Submessages ^^^^^\n")

	case JOIN, LEAVE, FAIL, HELLO, CREATE, APPEND, CHECK, FILES, GETFILE, MERGE, DELETE:
		fmt.Fprintf(logger.writer, "[%s] [%s] [%s] %s ",
			currentTime, direction, nodeId, message.Kind.String())

	default:
		fmt.Fprintf(logger.writer, "[%s] [%s] ********Unknown message type: %d**********\n",
			currentTime, direction, message.Kind)
	}

	fmt.Fprintf(logger.writer, "---------\n")
	logger.writer.Flush()
}
