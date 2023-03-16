package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type RateLimiter struct {
	client      *redis.Client
	ctx         context.Context
	windowSize  int64
	maxRequests int
}

func NewRateLimiter(client *redis.Client, windowSize int64, maxRequests int) *RateLimiter {
	return &RateLimiter{
		client:      client,
		ctx:         context.Background(),
		windowSize:  windowSize,
		maxRequests: maxRequests,
	}
}

func (rl *RateLimiter) getCurrentWindow() int64 {
	return time.Now().Unix() / rl.windowSize * rl.windowSize
}

func (rl *RateLimiter) allowRequest(userID string) (bool, error) {
	currentWindow := rl.getCurrentWindow()
	previousWindow := currentWindow - rl.windowSize

	keyCurrent := fmt.Sprintf("%s:%d", userID, currentWindow)
	keyPrevious := fmt.Sprintf("%s:%d", userID, previousWindow)

	pipe := rl.client.TxPipeline()
	incr := pipe.Incr(rl.ctx, keyCurrent)
	pipe.Expire(rl.ctx, keyCurrent, time.Duration(2*rl.windowSize)*time.Second)
	pipe.SetNX(rl.ctx, keyPrevious, 0, time.Duration(2*rl.windowSize)*time.Second)

	if _, err := pipe.Exec(rl.ctx); err != nil {
		return false, err
	}

	countCurrent, err := incr.Result()
	if err != nil {
		return false, err
	}

	// Retrieve both the current and previous window keys with MGet
	counts, err := rl.client.MGet(rl.ctx, keyCurrent, keyPrevious).Result()
	if err != nil {
		return false, err
	}

	countPrevious := 0
	if counts[1] != nil {
		countPrevious, _ = strconv.Atoi(counts[1].(string))
	}

	previousWeight := float64(currentWindow-time.Now().Unix()) / float64(rl.windowSize)
	currentWeight := 1.0 - previousWeight

	rate := previousWeight*float64(countPrevious) + currentWeight*float64(countCurrent)

	if currentWindow > previousWindow+rl.windowSize {
		rl.client.Set(rl.ctx, keyPrevious, countCurrent, time.Duration(2*rl.windowSize)*time.Second)
	}

	return rate <= float64(rl.maxRequests), nil
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	rl := NewRateLimiter(rdb, 10, 5) // 5 requests per 10 seconds

	for i := 0; i < 7; i++ {
		allowed, err := rl.allowRequest("user1")
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else if allowed {
			fmt.Printf("Request %d allowed\n", i+1)
		} else {
			fmt.Printf("Request %d denied\n", i+1)
		}
		time.Sleep(2 * time.Second)
	}
}
