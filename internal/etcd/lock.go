package etcd_lock

import (
	"fmt"
	"github.com/ecodeclub/ekit/retry"
	"github.com/meoying/dlock-go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
	"strconv"
	"time"
)

var _ dlock.Lock = (*etcdLock)(nil)

type etcdLock struct {
	client     *clientv3.Client
	leaseID    clientv3.LeaseID
	key        string
	expiration time.Duration
	isLocked   bool // 锁状态标记

	// 可以开放成公有
	lockRetry   retry.Strategy
	lockTimeout time.Duration
}

func (l *etcdLock) Lock(ctx context.Context) (err error) {

	return retry.Retry(ctx, l.lockRetry, func() (err error) {
		lctx, cancel := context.WithTimeout(ctx, l.lockTimeout)
		defer cancel()
		leaseResp, err := l.client.Grant(lctx, int64(l.expiration.Seconds()))
		if err != nil {
			return fmt.Errorf("创建租约失败: %v", err)
		}
		defer func() {
			if !l.isLocked {
				_, err1 := l.client.Revoke(lctx, leaseResp.ID)
				if err1 != nil {
					err = fmt.Errorf("%v ,且解除租约失败 %v", err, err1)
				}
			}
		}()

		sid := strconv.FormatInt(int64(leaseResp.ID), 10)

		txn := l.client.Txn(lctx).
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
		l.isLocked = true
		return nil
	})
}

func (l *etcdLock) Unlock(ctx context.Context) error {
	if !l.isLocked {
		return nil
	}

	// 这里其实没什么必要，因为etcd解除租约后会直接删除key
	sid := strconv.FormatInt(int64(l.leaseID), 10)

	txn := l.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(l.key), "!=", 0),
			clientv3.Compare(clientv3.Value(l.key), "=", sid)).
		Then(clientv3.OpDelete(l.key))

	txnResp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("释放锁失败: %v", err)
	}

	if !txnResp.Succeeded {
		return dlock.ErrLockNotHold
	}
	l.isLocked = false
	_, err = l.client.Revoke(ctx, l.leaseID)
	if err != nil {
		return fmt.Errorf("释放租约失败: %v", err)
	}

	return nil
}

func (l *etcdLock) Refresh(ctx context.Context) error {
	if !l.isLocked {
		return dlock.ErrLockNotHold
	}

	_, err := l.client.KeepAliveOnce(ctx, l.leaseID)
	if err != nil {
		return dlock.ErrLockNotHold
	}
	return nil
}
