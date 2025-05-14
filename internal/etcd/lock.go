package etcd_lock

import (
	"errors"
	"fmt"
	"github.com/ecodeclub/ekit/retry"
	"github.com/meoying/dlock-go"
	"github.com/meoying/dlock-go/internal/pkg"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
	"strconv"
	"time"
)

var _ dlock.Lock = (*etcdLock)(nil)

// etcdLock 结构体
type etcdLock struct {
	client     *clientv3.Client
	leaseID    clientv3.LeaseID
	key        string
	expiration time.Duration
	_          pkg.NoCopy

	lockRetry   retry.Strategy
	lockTimeout time.Duration
}

// Lock 加锁，成功后才能进行 Unlock
func (l *etcdLock) Lock(ctx context.Context) (err error) {
	isLocked := false
	c1, cancel1 := context.WithTimeout(ctx, l.lockTimeout)
	defer cancel1()
	// 这里可以引入重试机制
	leaseResp, err := l.client.Grant(c1, int64(l.expiration.Seconds()))
	if err != nil {
		return fmt.Errorf("创建租约失败: %w", err)
	}
	defer func() {
		c2, cancel2 := context.WithTimeout(context.Background(), l.lockTimeout)
		defer cancel2()
		if !isLocked {
			_, err1 := l.client.Revoke(c2, leaseResp.ID)
			if err1 != nil {
				err = fmt.Errorf("%w ,且解除租约失败 %w", err, err1)
			}
		} else {
			_, err1 := l.client.KeepAliveOnce(c2, leaseResp.ID)
			if err1 != nil {
				// 这里失败了，一般是重试时间太长了，导致租约过期了，让用户重新加锁吧
				err = fmt.Errorf("%w ,租约已到期，请重新加锁 %w", err, err1)
			}
		}
	}()

	sid := strconv.FormatInt(int64(leaseResp.ID), 10)
	return retry.Retry(ctx, l.lockRetry, func() (err error) {
		withTimeoutCtx, cancel := context.WithTimeout(ctx, l.lockTimeout)
		defer cancel()

		// 如果用户另一个协程加锁后释放锁，这个地方还是能进去
		// 但是逻辑上锁已经释放了，可以重新加锁
		txn := l.client.Txn(withTimeoutCtx).
			If(clientv3.Compare(clientv3.CreateRevision(l.key), "=", 0)).
			Then(clientv3.OpPut(l.key, sid, clientv3.WithLease(leaseResp.ID)))

		txnResp, err := txn.Commit()
		if err != nil {
			return fmt.Errorf("etcd加锁失败: %w", err)
		}

		if !txnResp.Succeeded {
			return dlock.ErrLocked
		}

		l.leaseID = leaseResp.ID

		isLocked = true
		return nil
	})
}

// Unlock 释放锁 只能保证当前 leaseID 释放对应的key
func (l *etcdLock) Unlock(ctx context.Context) error {
	withTimeoutCtx, cancel := context.WithTimeout(ctx, l.lockTimeout)
	defer cancel()
	_, err := l.client.Revoke(withTimeoutCtx, l.leaseID)
	if err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return dlock.ErrLockNotHold
		}
		return err
	}
	return nil
}

func (l *etcdLock) Refresh(ctx context.Context) error {
	withTimeoutCtx, cancel := context.WithTimeout(ctx, l.lockTimeout)
	defer cancel()
	_, err := l.client.KeepAliveOnce(withTimeoutCtx, l.leaseID)
	if err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return dlock.ErrLockNotHold
		}
		return err
	}
	return nil
}
