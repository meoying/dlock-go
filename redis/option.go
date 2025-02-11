package redis

import (
	"github.com/ecodeclub/ekit/bean/option"
	"github.com/ecodeclub/ekit/retry"
	rlock "github.com/meoying/dlock/internal/redis"
	"time"
)

func WithLockTimeout(timeout time.Duration) option.Option[rlock.Lock] {
	return rlock.WithLockTimeout(timeout)
}

func WithLockRetryStrategy(strategy retry.Strategy) option.Option[rlock.Lock] {
	return rlock.WithLockRetryStrategy(strategy)
}
