package e2e

import (
	"context"
	"errors"
	"github.com/ecodeclub/ekit/retry"
	"github.com/meoying/dlock/internal/errs"
	glock "github.com/meoying/dlock/internal/gorm"
	rlock "github.com/meoying/dlock/internal/redis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

type LockTestSuite struct {
	suite.Suite
	db  *gorm.DB
	rdb redis.Cmdable
}

func (s *LockTestSuite) SetupSuite() {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/dlock?charset=utf8mb4&collation=utf8mb4_general_ci&parseTime=True&loc=Local&timeout=1s&readTimeout=3s&writeTimeout=3s"))
	require.NoError(s.T(), err)
	s.db = db
	err = s.db.AutoMigrate(&glock.DistributedLock{})
	require.NoError(s.T(), err)

	s.rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	// 确保测试的目标 Redis 已经启动成功了
	for s.rdb.Ping(context.Background()).Err() != nil {

	}
}

func (s *LockTestSuite) TearDownTest() {
	err := s.db.Exec("TRUNCATE TABLE distributed_locks").Error
	require.NoError(s.T(), err)

	err = s.rdb.FlushDB(context.Background()).Err()
	require.NoError(s.T(), err)
}

func (s *LockTestSuite) TestLock() {
	testCases := []struct {
		name       string
		key        string
		expiration time.Duration
		before     func(t *testing.T)
		after      func(t *testing.T)
		wantErr    error
	}{
		{
			name:       "加锁成功",
			key:        "locked-key",
			expiration: time.Minute,
			before: func(t *testing.T) {

			},
			after: func(t *testing.T) {
				res, err := s.rdb.Del(context.Background(), "locked-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock glock.DistributedLock
				err = s.db.WithContext(ctx).Where("`key` = ?", "locked-key").
					First(&lock).Error
				assert.NoError(s.T(), err)
				assert.True(t, lock.Utime > 0)
				assert.True(t, lock.Ctime > 0)
				assert.Equal(t, "locked-key", lock.Key)
				assert.True(t, len(lock.Value) > 0)
				assert.Equal(t, glock.StatusLocked, lock.Status)
				// 预期先前是没有锁的，所以版本号应该是 1
				assert.Equal(t, int64(1), lock.Version)
				assert.True(s.T(), lock.Expiration > time.Now().UnixMilli())
			},
		},
		{
			name:       "加锁失败，别人持有锁",
			key:        "failed-key",
			expiration: time.Minute,
			before: func(t *testing.T) {
				res, err := s.rdb.Set(context.Background(), "failed-key", "123", time.Minute).Result()
				require.NoError(t, err)
				require.Equal(t, "OK", res)

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				now := time.Now().UnixMilli()
				err = s.db.WithContext(ctx).Create(&glock.DistributedLock{
					Key:        "failed-key",
					Utime:      now,
					Ctime:      now,
					Status:     glock.StatusLocked,
					Expiration: time.Now().Add(time.Minute).UnixMilli(),
					Value:      "123",
					Version:    12,
				}).Error
				require.NoError(s.T(), err)
			},
			after: func(t *testing.T) {
				res, err := s.rdb.Get(context.Background(), "failed-key").Result()
				require.NoError(t, err)
				require.Equal(t, "123", res)
				delRes, err := s.rdb.Del(context.Background(), "failed-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), delRes)

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock glock.DistributedLock
				err = s.db.WithContext(ctx).Where("`key` = ?", "failed-key").First(&lock).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), glock.StatusLocked, lock.Status)
				assert.Equal(s.T(), int64(12), lock.Version)
			},
			wantErr: errs.ErrLocked,
		},
		{
			name:       "加锁成功，原持有人崩溃",
			key:        "retry-key",
			expiration: time.Minute,
			before: func(t *testing.T) {
				res, err := s.rdb.Set(context.Background(), "retry-key", "123", time.Second).Result()
				require.NoError(t, err)
				require.Equal(t, "OK", res)

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				err = s.db.WithContext(ctx).Create(&glock.DistributedLock{
					Key:        "retry-key",
					Utime:      123,
					Ctime:      123,
					Status:     glock.StatusLocked,
					Expiration: time.Now().Add(time.Second).UnixMilli(),
					Value:      "abc",
					Version:    12,
				}).Error
				require.NoError(s.T(), err)

				// 睡够1秒钟，让锁过期
				time.Sleep(time.Second)
			},
			after: func(t *testing.T) {
				res, err := s.rdb.Del(context.Background(), "retry-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock glock.DistributedLock
				err = s.db.WithContext(ctx).Where("`key` = ?", "retry-key").First(&lock).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), glock.StatusLocked, lock.Status)
				assert.True(s.T(), lock.Utime > 123)
				assert.Equal(s.T(), int64(123), lock.Ctime)
				assert.Equal(s.T(), int64(13), lock.Version)
				assert.True(s.T(), lock.Expiration > time.Now().UnixMilli())
			},
		},
	}
	s.T().Run(glock.ModeCASFirst, func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tc.before(t)

				redisLock := rlock.NewLock(s.rdb, tc.key, tc.expiration)
				err := redisLock.Lock(context.Background())
				assert.True(t, errors.Is(err, tc.wantErr))

				// 这里默认加的锁是 cas 模式
				gormLock := glock.NewLock(s.db, tc.key, tc.expiration)
				err = gormLock.Lock(context.Background())
				assert.True(t, errors.Is(err, tc.wantErr))

				tc.after(t)
			})
		}
	})

	s.TearDownTest()

	s.T().Run(glock.ModeInsertFirst, func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tc.before(t)

				redisLock := rlock.NewLock(s.rdb, tc.key, tc.expiration)
				err := redisLock.Lock(context.Background())
				assert.True(t, errors.Is(err, tc.wantErr))

				gormLock := glock.NewLock(s.db, tc.key, tc.expiration)
				gormLock.Mode = glock.ModeInsertFirst
				err = gormLock.Lock(context.Background())
				assert.True(t, errors.Is(err, tc.wantErr))

				tc.after(t)
			})
		}
	})
}

func (s *LockTestSuite) TestUnLock() {
	testCases := []struct {
		name   string
		key    string
		before func(t *testing.T) (*glock.Lock, *rlock.Lock)
		after  func(t *testing.T)

		wantErr error
	}{
		{
			name: "解锁成功",
			key:  "unlock-key1",
			before: func(t *testing.T) (*glock.Lock, *rlock.Lock) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()

				// 模拟加锁成功
				gl := glock.NewLock(s.db, "unlock-key1", time.Minute)
				err := gl.Lock(ctx)
				require.NoError(t, err)

				rl := rlock.NewLock(s.rdb, "unlock-key1", time.Minute)
				err = rl.Lock(ctx)
				assert.NoError(t, err)
				return gl, rl
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()

				var lock glock.DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "unlock-key1").First(&lock).Error
				require.NoError(t, err)
				assert.Equal(s.T(), glock.StatusUnlocked, lock.Status)

				res, err := s.rdb.Exists(ctx, "unlock-key1").Result()
				require.NoError(t, err)
				// unlock 成功，key 不存在
				require.Equal(t, int64(0), res)
			},
		},
		{
			name: "解锁失败-不是自己的锁",
			key:  "unlock_key2",
			before: func(t *testing.T) (*glock.Lock, *rlock.Lock) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cancel()
				// 模拟加锁成功
				gl := glock.NewLock(s.db, "unlock_key2", time.Minute)
				err := gl.Lock(ctx)
				require.NoError(t, err)

				// 修改value
				res := s.db.WithContext(ctx).Model(&glock.DistributedLock{}).
					Where("`key` = ?", "unlock_key2").Updates(map[string]any{
					"value": 123,
				})
				require.Equal(t, int64(1), res.RowsAffected)
				require.NoError(t, res.Error)

				rl := rlock.NewLock(s.rdb, "unlock_key2", time.Minute)
				err = rl.Lock(ctx)
				require.NoError(t, err)

				// 修改value
				err = s.rdb.Set(ctx, "unlock_key2", "123", time.Minute).Err()
				require.NoError(t, err)
				return gl, rl
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()

				var lock glock.DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "unlock_key2").First(&lock).Error
				require.NoError(t, err)
				assert.Equal(s.T(), glock.StatusLocked, lock.Status)

				res, err := s.rdb.Exists(ctx, "unlock_key2").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)
			},

			wantErr: errs.ErrLockNotHold,
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			gl, rl := tc.before(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err := gl.Unlock(ctx)
			assert.ErrorIs(t, err, tc.wantErr)
			err = rl.Unlock(ctx)
			assert.ErrorIs(t, err, tc.wantErr)
			tc.after(t)
		})
	}
}

func (s *LockTestSuite) TestRefresh() {
	testCases := []struct {
		name   string
		key    string
		before func(t *testing.T) (*glock.Lock, *rlock.Lock)
		after  func(t *testing.T)

		wantErr error
	}{
		{
			name: "刷新成功",
			key:  "refresh_key1",
			before: func(t *testing.T) (*glock.Lock, *rlock.Lock) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cancel()

				// 模拟加锁成功
				gl := glock.NewLock(s.db, "refresh_key1", time.Minute)
				err := gl.Lock(ctx)
				require.NoError(t, err)

				rl := rlock.NewLock(s.rdb, "refresh_key1", time.Minute)
				err = rl.Lock(ctx)
				assert.NoError(t, err)
				return gl, rl
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cancel()

				var lock glock.DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "refresh_key1").First(&lock).Error
				require.NoError(t, err)
				assert.Equal(s.T(), glock.StatusLocked, lock.Status)
				assert.True(s.T(), lock.Expiration > time.Now().UnixMilli())

				res, err := s.rdb.TTL(context.Background(), "refresh_key1").Result()
				require.NoError(t, err)
				// 刷新完过期时间
				assert.True(t, res.Seconds() > 50)
			},
		},
		{
			name: "刷新失败-不是自己的锁",
			key:  "refresh_key2",
			before: func(t *testing.T) (*glock.Lock, *rlock.Lock) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cancel()
				// 模拟加锁成功
				gl := glock.NewLock(s.db, "refresh_key2", time.Minute)
				err := gl.Lock(ctx)
				require.NoError(t, err)

				// 修改value
				res := s.db.WithContext(ctx).Model(&glock.DistributedLock{}).
					Where("`key` = ?", "refresh_key2").Updates(map[string]any{
					"value": 123,
				})
				require.Equal(t, int64(1), res.RowsAffected)
				require.NoError(t, res.Error)

				rl := rlock.NewLock(s.rdb, "refresh_key2", time.Minute)
				err = rl.Lock(ctx)
				require.NoError(t, err)

				// 修改value
				err = s.rdb.Set(ctx, "refresh_key2", "123", time.Minute).Err()
				require.NoError(t, err)
				return gl, rl
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()

				var lock glock.DistributedLock
				// 确定是别人的锁
				err := s.db.WithContext(ctx).Where("`key` = ? AND value = ?", "refresh_key2", "123").First(&lock).Error
				require.NoError(t, err)
				assert.Equal(s.T(), glock.StatusLocked, lock.Status)

				res, err := s.rdb.Get(context.Background(), "refresh_key2").Result()
				require.NoError(t, err)
				require.Equal(t, "123", res)
			},
			wantErr: errs.ErrLockNotHold,
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			gl, rl := tc.before(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err := gl.Refresh(ctx)
			assert.ErrorIs(t, err, tc.wantErr)
			err = rl.Refresh(ctx)
			assert.ErrorIs(t, err, tc.wantErr)
			tc.after(t)
		})
	}
}

// TestGORMLock gorm 独有的特性测试
func (s *LockTestSuite) TestGORMLock() {
	testCases := []struct {
		name string
		key  string
		// 用来测试的锁
		lock       func(t *testing.T) *glock.Lock
		expiration time.Duration
		before     func(t *testing.T)
		after      func(t *testing.T)

		wantErr error
	}{
		{
			name: "insert 加锁成功，而后是 cas 尝试加锁失败，最后成功",
			key:  "gorm_key1",
			lock: func(t *testing.T) *glock.Lock {
				gl := glock.NewLock(s.db, "gorm_key1", time.Minute)
				gl.Mode = glock.ModeCASFirst
				return gl
			},
			before: func(t *testing.T) {
				// 用 insert 模式加锁成功，5s 后过期
				gl := glock.NewLock(s.db, "gorm_key1", time.Second*5)
				gl.Mode = glock.ModeInsertFirst
				gl.LockRetry, _ = retry.NewFixedIntervalRetryStrategy(time.Second, 10)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
				defer cancel()
				err := gl.Lock(ctx)
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 如果 cas 成功，那么 version 会从 1 变为 2
				var res glock.DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ? AND version = ?", "gorm_key1", 2).First(&res).Error
				require.NoError(t, err)
				assert.True(s.T(), res.Expiration > time.Now().UnixMilli())
				assert.True(s.T(), res.Status == glock.StatusLocked)
			},
		},
		{
			name: "cas 尝试加锁成功，而后是 insert 加锁失败，最后成功",
			key:  "gorm_key2",
			lock: func(t *testing.T) *glock.Lock {
				gl := glock.NewLock(s.db, "gorm_key2", time.Minute)
				gl.Mode = glock.ModeInsertFirst
				return gl
			},
			before: func(t *testing.T) {
				// 用 cas 模式加锁成功，5s 后过期
				gl := glock.NewLock(s.db, "gorm_key2", time.Second*5)
				gl.Mode = glock.ModeCASFirst
				gl.LockRetry, _ = retry.NewFixedIntervalRetryStrategy(time.Second, 10)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
				defer cancel()
				err := gl.Lock(ctx)
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 如果 insert 成功，那么 version 会从 1 变为 2
				var res glock.DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ? AND version = ?", "gorm_key2", 2).First(&res).Error
				require.NoError(t, err)
				assert.True(s.T(), res.Expiration > time.Now().UnixMilli())
				assert.True(s.T(), res.Status == glock.StatusLocked)
			},
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			tc.before(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
			defer cancel()
			lock := tc.lock(t)
			err := lock.Lock(ctx)
			assert.NoError(t, err)
		})
	}
}

func TestLockTestSuite(t *testing.T) {
	suite.Run(t, new(LockTestSuite))
}
