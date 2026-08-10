package queue

type Message struct {
	ID    string `json:"id"`
	JobID string `json:"job_id"`
}
