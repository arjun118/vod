package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue interface {
	Publish(ctx context.Context, jobID string) (string, error)
	Consume(ctx context.Context, limit int) ([]Message, error)
	Ack(ctx context.Context, id string) error
}

type RedisQueue struct {
	client       *redis.Client
	streamName   string
	groupName    string
	consumerName string
}

func (rq *RedisQueue) Init(ctx context.Context) error {
	err := rq.client.XGroupCreateMkStream(
		ctx,
		rq.streamName,
		rq.groupName,
		"$",
	).Err()

	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create consumer group: %w", err)
	}

	return nil
}

func NewRedisQueue(client *redis.Client, streamName string, groupName string, consumerName string) *RedisQueue {
	return &RedisQueue{
		client:       client,
		streamName:   streamName,
		groupName:    groupName,
		consumerName: consumerName,
	}
}

func (rq *RedisQueue) Publish(ctx context.Context, jobID string) (string, error) {
	args := &redis.XAddArgs{
		Stream: rq.streamName,
		MaxLen: 100_000,
		Approx: true,
		Values: map[string]any{
			"job_id": jobID,
		},
	}

	id, err := rq.client.XAdd(ctx, args).Result()
	if err != nil {
		return "", fmt.Errorf("publish job: %w", err)
	}

	return id, nil
}
func (rq *RedisQueue) Consume(ctx context.Context, limit int) ([]Message, error) {
	// XREADGROUP GROUP transcoders reader-1 BLOCK 0 COUNT 10 STREAMS video_jobs >
	args := &redis.XReadGroupArgs{
		Group:    rq.groupName,
		Consumer: rq.consumerName,
		Streams:  []string{rq.streamName, ">"},
		Count:    int64(limit),
		Block:    5 * time.Second,
	}

	streams, err := rq.client.XReadGroup(ctx, args).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("consume jobs: %w", err)
	}

	messages := make([]Message, 0)

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			v, ok := msg.Values["job_id"]
			if !ok {
				continue
			}

			jobID, ok := v.(string)
			if !ok {
				continue
			}
			messages = append(messages, Message{
				ID:    msg.ID,
				JobID: jobID,
			})
		}
	}

	return messages, nil
}

func (rq *RedisQueue) Ack(ctx context.Context, id string) error {
	err := rq.client.XAck(ctx, rq.streamName, rq.groupName, id).Err()
	if err != nil {
		return fmt.Errorf("ack job %s: %w", id, err)
	}
	return nil
}
