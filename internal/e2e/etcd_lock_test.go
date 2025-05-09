package e2e

import (
	"context"
	etcd_lock "github.com/meoying/dlock-go/internal/etcd"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"

	"testing"
)

func TestEtcdLock(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	require.NoError(t, err)
	conn := cli.ActiveConnection()
	require.NotNil(t, conn)

	//err := rdb.Ping(context.Background()).Err()
	//require.NoError(t, err)
	suite.Run(t, &EtcdLockTestSuite{
		LockTestSuite: &LockTestSuite{
			client: etcd_lock.NewEtcdClient(cli),
		},
	})
}

type EtcdLockTestSuite struct {
	*LockTestSuite
	c *clientv3.Client
}

func (s *EtcdLockTestSuite) SetupSuite() {
	if s.c != nil {
		_, _ = s.c.Delete(context.Background(), "", clientv3.WithPrefix())
	}
}

func (s *EtcdLockTestSuite) TearDownTest() {
	if s.c != nil {
		_, _ = s.c.Delete(context.Background(), "", clientv3.WithPrefix())
	}
}
