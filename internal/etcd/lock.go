package etcd_lock

import (
	"errors"
	"fmt"
	"github.com/ecodeclub/ekit/retry"
	"github.com/meoying/dlock-go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
	"strconv"
	"time"
)

var _ dlock.Lock = (*etcdLock)(nil)

// noCopy 是一个特殊的类型，用于防止结构体被复制
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// etcdLock 结构体
type etcdLock struct {
	client     *clientv3.Client
	leaseID    clientv3.LeaseID
	key        string
	expiration time.Duration
	_          noCopy

	lockRetry   retry.Strategy
	lockTimeout time.Duration
}

func (l *etcdLock) Lock(ctx context.Context) (err error) {
	isLocked := false
	c1, cancel1 := context.WithTimeout(ctx, l.lockTimeout)
	defer cancel1()
	// 这里可以引入重试机制
	leaseResp, err := l.client.Grant(c1, int64(l.expiration.Seconds()))
	if err != nil {
		return fmt.Errorf("创建租约失败: %v", err)
	}
	defer func() {
		c2, cancel2 := context.WithTimeout(context.Background(), l.lockTimeout)
		defer cancel2()
		if !isLocked {
			_, err1 := l.client.Revoke(c2, leaseResp.ID)
			if err1 != nil {
				err = fmt.Errorf("%v ,且解除租约失败 %v", err, err1)
			}
		} else {
			_, _ = l.client.KeepAliveOnce(c2, leaseResp.ID)
		}
	}()
	cl := make(chan struct{})
	defer close(cl)
	go func() {
		ch, err := l.client.KeepAlive(ctx, leaseResp.ID)
		if err != nil {
			return
		}
		for {
			select {
			case <-cl:
				return
			case <-ctx.Done():
				return
			case <-ch:
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
			return fmt.Errorf("etcd加锁失败: %v", err)
		}

		if !txnResp.Succeeded {
			return dlock.ErrLocked
		}

		l.leaseID = leaseResp.ID
		isLocked = true
		return nil
	})
}

func (l *etcdLock) Unlock(ctx context.Context) error {

	// 这里其实没什么必要，因为etcd解除租约后会直接删除key
	//sid := strconv.FormatInt(int64(l.leaseID), 10)
	//withTimeoutCtx, cancel := context.WithTimeout(ctx, l.lockTimeout)
	//defer cancel()
	//txn := l.client.Txn(withTimeoutCtx).
	//	If(clientv3.Compare(clientv3.CreateRevision(l.key), "!=", 0),
	//		clientv3.Compare(clientv3.Value(l.key), "=", sid)).
	//	Then(clientv3.OpDelete(l.key))
	//
	//txnResp, err := txn.Commit()
	//if err != nil {
	//	return fmt.Errorf("释放锁失败: %v", err)
	//}
	//
	//if !txnResp.Succeeded {
	//	return dlock.ErrLockNotHold
	//}
	withTimeoutCtx, cancel := context.WithTimeout(ctx, l.lockTimeout)
	defer cancel()
	_, err := l.client.Revoke(withTimeoutCtx, l.leaseID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return dlock.ErrLockNotHold
	}

	return nil
}

func (l *etcdLock) Refresh(ctx context.Context) error {
	withTimeoutCtx, cancel := context.WithTimeout(ctx, l.lockTimeout)
	defer cancel()
	_, err := l.client.KeepAliveOnce(withTimeoutCtx, l.leaseID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return dlock.ErrLockNotHold
	}
	return nil
}
