package etcd_lock

import (
	"github.com/ecodeclub/ekit/retry"
	"github.com/meoying/dlock-go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
	"time"
)

var _ dlock.Client = (*etcdClient)(nil)

type etcdClient struct {
	client *clientv3.Client
}

// NewEtcdClient 创建 etcd 客户端
func NewEtcdClient(c *clientv3.Client) dlock.Client {
	return &etcdClient{client: c}
}

// NewLock 初始化锁实例
func (c *etcdClient) NewLock(_ context.Context, key string, expiration time.Duration) (dlock.Lock, error) {
	strategy, _ := retry.NewExponentialBackoffRetryStrategy(time.Millisecond*100, time.Second, 10)
	lt := time.Millisecond * 200

	lock := etcdLock{
		client:     c.client,
		key:        key,
		expiration: expiration,

		lockRetry:   strategy,
		lockTimeout: lt,
	}

	return &lock, nil
}
