package etcd_lock

import (
	"errors"
	"fmt"
	"github.com/ecodeclub/ekit/retry"
	"github.com/meoying/dlock-go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
	"io"
	"strconv"
	"time"
)

var _ dlock.Lock = (*etcdLock)(nil)

// etcdLock 结构体
type etcdLockv1 struct {
	client     *clientv3.Client
	leaseID    clientv3.LeaseID
	key        string
	expiration time.Duration
	cf         context.CancelFunc

	lockRetry   retry.Strategy
	lockTimeout time.Duration
}

func (l *etcdLockv1) Lock(ctx context.Context) (err error) {
	isLocked := false
	c1, cancel1 := context.WithTimeout(ctx, l.lockTimeout)
	defer cancel1()
	// 这里可以引入重试机制
	leaseResp, err := l.client.Grant(c1, int64(l.expiration.Seconds()))
	if err != nil {
		return fmt.Errorf("创建租约失败: %w", err)
	}
	var cf1 context.CancelFunc
	defer func() {
		c2, cancel2 := context.WithTimeout(context.Background(), l.lockTimeout)
		defer cancel2()
		if !isLocked {
			defer cf1()
			_, err1 := l.client.Revoke(c2, leaseResp.ID)
			if err1 != nil {
				err = fmt.Errorf("%w ,且解除租约失败 %w", err, err1)
			}
		}
	}()

	go func() {
		cctx, cancelFunc := context.WithCancel(ctx)
		cf1 = cancelFunc
		defer cancelFunc()
		ch, err := l.client.KeepAlive(cctx, leaseResp.ID)
		if err != nil {
			return
		}
		for {
			select {
			case <-cctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				continue
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
		l.cf = cf1
		isLocked = true
		return nil
	})
}

// Unlock 释放锁 只能保证当前 leaseID 释放对应的key，没办法保证用户 Lock 发生err后，直接 Unlock 会不会接触当前leaseID的key
func (l *etcdLockv1) Unlock(ctx context.Context) error {
	defer l.cf()
	withTimeoutCtx, cancel := context.WithTimeout(ctx, l.lockTimeout)
	defer cancel()
	_, err := l.client.Revoke(withTimeoutCtx, l.leaseID)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("连接已断开: %w", err)
		}
		return dlock.ErrLockNotHold
	}
	return nil
}

func (l *etcdLockv1) Refresh(ctx context.Context) error {

	withTimeoutCtx, cancel := context.WithTimeout(ctx, l.lockTimeout)
	defer cancel()
	_, err := l.client.KeepAliveOnce(withTimeoutCtx, l.leaseID)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("连接已断开: %w", err)
		}
		return dlock.ErrLockNotHold
	}
	return nil
}
