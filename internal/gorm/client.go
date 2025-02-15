package glock

import (
	"context"
	"github.com/meoying/dlock"
	"gorm.io/gorm"
	"time"
)

type Client struct {
	db *gorm.DB
}

func NewClient(db *gorm.DB) *Client {
	return &Client{db: db}
}

func (c *Client) NewLock(ctx context.Context, key string, expiration time.Duration) (dlock.Lock, error) {
	return NewLock(c.db, key, expiration), nil
}

func (c *Client) InitTable() error {
	return c.db.AutoMigrate(&DistributedLock{})
}
