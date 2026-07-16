package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/arjun118/fileupload/internal/jobs"
	"github.com/redis/go-redis/v9"
)

type Queue interface {
	Publish(ctx context.Context, job jobs.TranscodeJob) error
	Consume(ctx context.Context) (*jobs.TranscodeJob, error)
}

type RedisQueue struct {
	client    *redis.Client
	queueName string
}

func NewRedisQueue(client *redis.Client, queueName string) *RedisQueue {
	return &RedisQueue{
		client:    client,
		queueName: queueName,
	}
}

func (rq *RedisQueue) Publish(ctx context.Context, job jobs.TranscodeJob) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshall job: %w", err)
	}
	err = rq.client.LPush(ctx, rq.queueName, payload).Err()
	if err != nil {
		return fmt.Errorf("failed to push to queue: %w", err)
	}
	return nil
}

func (rq *RedisQueue) Consume(ctx context.Context) (*jobs.TranscodeJob, error) {
	result, err := rq.client.BLPop(ctx, 0, rq.queueName).Result()
	if err != nil {
		return nil, err
	}

	var job jobs.TranscodeJob
	if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}
	return &job, nil
}
