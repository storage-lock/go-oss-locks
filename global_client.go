package oss_locks

import (
	"context"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	oss_storage "github.com/storage-lock/go-oss-storage"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	storage_lock_factory "github.com/storage-lock/go-storage-lock-factory"
)

var ossClientBeanFactory = storage_lock_factory.NewStorageLockFactoryBeanFactory[*oss.Bucket, *oss.Bucket]()

// newOSSStorage 指向 oss_storage.NewOSSStorage，包级变量以便单测注入构造失败，
// 覆盖 GetOrInit initFunc 的错误分支。client/endpoint 双入口共用。
var newOSSStorage = func(ctx context.Context, options *oss_storage.OSSStorageOptions) (storage.Storage, error) {
	return oss_storage.NewOSSStorage(ctx, options)
}

// bucketConnectionManager 包装已有的 oss.Bucket 为 ConnectionManager
type bucketConnectionManager struct {
	bucket *oss.Bucket
}

func (m *bucketConnectionManager) Name() string {
	return "bucket-connection-manager"
}

func (m *bucketConnectionManager) Take(ctx context.Context) (*oss.Bucket, error) {
	return m.bucket, nil
}

func (m *bucketConnectionManager) Return(ctx context.Context, connection *oss.Bucket) error {
	return nil
}

func (m *bucketConnectionManager) Shutdown(ctx context.Context) error {
	return nil
}

// NewOSSLockByClient 从已有 OSS Bucket 创建分布式锁
func NewOSSLockByClient(ctx context.Context, bucket *oss.Bucket, lockId string) (*storage_lock.StorageLock, error) {
	factory, err := GetOSSLockFactoryByClient(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return factory.CreateLock(lockId)
}

// GetOSSLockFactoryByClient 获取基于 OSS Bucket 的锁工厂（单例模式）
func GetOSSLockFactoryByClient(ctx context.Context, bucket *oss.Bucket) (*storage_lock_factory.StorageLockFactory[*oss.Bucket], error) {
	return ossClientBeanFactory.GetOrInit(ctx, bucket, func(ctx context.Context) (*storage_lock_factory.StorageLockFactory[*oss.Bucket], error) {
		cm := &bucketConnectionManager{bucket: bucket}
		options := oss_storage.NewOSSStorageOptions().
			SetConnectionManager(cm).
			SetBucketName(bucket.BucketName).
			SetTimeProvider(&oss_storage.LocalTimeProvider{})
		storage, err := newOSSStorage(ctx, options)
		if err != nil {
			return nil, err
		}
		return storage_lock_factory.NewStorageLockFactory[*oss.Bucket](storage, nil), nil
	})
}
