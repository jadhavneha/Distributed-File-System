package main

import (
	"fmt"
	"sync"
)

const MaxPiggybackCount = 5

var piggybackBuffer = NewPiggybackMessages()

type PiggybackMessages struct {
	mu       sync.RWMutex
	messages []PiggybackMessage
}

// NewPiggybackMessages initializes a new piggyback manager.
func NewPiggybackMessages() *PiggybackMessages {
	return &PiggybackMessages{
		messages: make([]PiggybackMessage, 0),
	}
}

// Add inserts a new piggyback message with the default TTL.
func (p *PiggybackMessages) AddPiggybackMessage(msg Message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.messages = append(p.messages, PiggybackMessage{
		Message: msg,
		TTL:     PIGGYBACKTTL,
	})
}

func PrintPiggybackMessages() {
	fmt.Println("=== Piggyback Messages ===")
	piggybackBuffer.Print()
	fmt.Println("==========================")
}

// GetUnexpired returns up to MaxPiggybackCount active messages and decrements their TTLs.
func (p *PiggybackMessages) GetUnexpired() []Message {
	p.mu.Lock()
	defer p.mu.Unlock()

	var result []Message
	var active []PiggybackMessage
	count := 0

	for _, pb := range p.messages {
		if pb.TTL > 0 {
			if count < MaxPiggybackCount {
				result = append(result, pb.Message)
				pb.TTL--
				count++
			}
			if pb.TTL > 0 {
				active = append(active, pb)
			}
		}
	}

	p.messages = active
	return result
}

func (p *PiggybackMessages) Print() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.messages) == 0 {
		fmt.Println("[Piggyback] No messages")
		return
	}

	fmt.Println("[Piggyback] Current messages:")
	for i, msg := range p.messages {
		fmt.Printf("  #%d: %s (TTL=%d)\n", i, msg.Message.Kind.String(), msg.TTL)
	}
}