package oss_locks

import (
	"context"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	oss_storage "github.com/storage-lock/go-oss-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	storage_lock_factory "github.com/storage-lock/go-storage-lock-factory"
)

// EndpointConfig OSS 端点配置
type EndpointConfig struct {
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
	BucketName      string
}

var ossEndpointBeanFactory = storage_lock_factory.NewStorageLockFactoryBeanFactory[string, *oss.Bucket]()

// NewOSSLockByEndpoint 从端点参数创建分布式锁
func NewOSSLockByEndpoint(ctx context.Context, config *EndpointConfig, lockId string) (*storage_lock.StorageLock, error) {
	factory, err := GetOSSLockFactoryByEndpoint(ctx, config)
	if err != nil {
		return nil, err
	}
	return factory.CreateLock(lockId)
}

// GetOSSLockFactoryByEndpoint 获取基于端点的锁工厂（单例模式）
func GetOSSLockFactoryByEndpoint(ctx context.Context, config *EndpointConfig) (*storage_lock_factory.StorageLockFactory[*oss.Bucket], error) {
	cacheKey := config.Endpoint + "/" + config.BucketName

	return ossEndpointBeanFactory.GetOrInit(ctx, cacheKey, func(ctx context.Context) (*storage_lock_factory.StorageLockFactory[*oss.Bucket], error) {
		cm := oss_storage.NewOSSConnectionManagerFromEndpoint(config.Endpoint, config.AccessKeyID, config.AccessKeySecret, config.BucketName)
		options := oss_storage.NewOSSStorageOptions().
			SetConnectionManager(cm).
			SetBucketName(config.BucketName).
			SetTimeProvider(&oss_storage.LocalTimeProvider{})
		// newOSSStorage 定义在 global_client.go，双入口共用以便单测统一注入
		storage, err := newOSSStorage(ctx, options)
		if err != nil {
			return nil, err
		}
		return storage_lock_factory.NewStorageLockFactory[*oss.Bucket](storage, nil), nil
	})
}
