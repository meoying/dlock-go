package rlock

import (
	"context"
	"github.com/meoying/dlock-go"
	"github.com/redis/go-redis/v9"
	"time"
)

type Client struct {
	rdb redis.Cmdable
}

func NewClient(rdb redis.Cmdable) *Client {
	return &Client{rdb: rdb}
}

func (c *Client) NewLock(ctx context.Context, key string, expiration time.Duration) (dlock.Lock, error) {
	return NewLock(c.rdb, key, expiration), nil
}
