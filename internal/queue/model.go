package queue

import "time"

type Message struct {
	ID    string `json:"id"`
	JobID string `json:"job_id"`
}

type PendingMessage struct {
	Message
	Idle     time.Duration
	Consumer string
}
