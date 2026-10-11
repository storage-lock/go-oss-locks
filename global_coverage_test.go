package oss_locks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	oss_storage "github.com/storage-lock/go-oss-storage"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

// fakeStorage 内存假存储：满足 CAS+可靠时间+原子删除，走通锁创建与加解锁，不依赖真实 OSS。
type fakeStorage struct {
	data map[string]string
	vers map[string]storage.Version
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{data: map[string]string{}, vers: map[string]storage.Version{}}
}

func (f *fakeStorage) GetName() string { return "fake-oss-storage" }
func (f *fakeStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{storage.CapabilityCAS, storage.CapabilityReliableTime, storage.CapabilityAtomicDelete}
}
func (f *fakeStorage) Init(ctx context.Context) error  { return nil }
func (f *fakeStorage) Close(ctx context.Context) error { return nil }
func (f *fakeStorage) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}
func (f *fakeStorage) Get(ctx context.Context, lockId string) (string, error) {
	v, ok := f.data[lockId]
	if !ok {
		return "", storage_lock.ErrLockNotFound
	}
	return v, nil
}
func (f *fakeStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, info *storage.LockInformation) error {
	if _, ok := f.data[lockId]; ok {
		return storage_lock.ErrLockAlreadyExists
	}
	f.data[lockId] = info.ToJsonString()
	f.vers[lockId] = version
	return nil
}
func (f *fakeStorage) UpdateWithVersion(ctx context.Context, lockId string, expected, newVersion storage.Version, info *storage.LockInformation) error {
	v, ok := f.vers[lockId]
	if !ok {
		return storage_lock.ErrVersionMiss
	}
	if v != expected {
		return storage_lock.ErrVersionMiss
	}
	f.data[lockId] = info.ToJsonString()
	f.vers[lockId] = newVersion
	return nil
}
func (f *fakeStorage) DeleteWithVersion(ctx context.Context, lockId string, expected storage.Version, info *storage.LockInformation) error {
	v, ok := f.vers[lockId]
	if !ok {
		return storage_lock.ErrLockNotFound
	}
	if v != expected {
		return storage_lock.ErrVersionMiss
	}
	delete(f.data, lockId)
	delete(f.vers, lockId)
	return nil
}
func (f *fakeStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	infos := make([]*storage.LockInformation, 0, len(f.data))
	for _, s := range f.data {
		info, err := storage.LockInformationFromJsonString(s)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return go_iterator.FromSlice(infos), nil
}

func withFakeOSSStorage() func() {
	old := newOSSStorage
	newOSSStorage = func(ctx context.Context, options *oss_storage.OSSStorageOptions) (storage.Storage, error) {
		return newFakeStorage(), nil
	}
	return func() { newOSSStorage = old }
}

// testOSSBucket 本地构造 oss.Bucket（oss.New + Bucket 均为本地校验，不联网）。
func testOSSBucket(t *testing.T, bucketName string) *oss.Bucket {
	t.Helper()
	client, err := oss.New("https://oss-cn-hangzhou.aliyuncs.com", "test-ak", "test-sk")
	assert.Nil(t, err)
	bucket, err := client.Bucket(bucketName)
	assert.Nil(t, err)
	return bucket
}

func TestBucketConnectionManager(t *testing.T) {
	bucket := testOSSBucket(t, "test-bucket")
	m := &bucketConnectionManager{bucket: bucket}
	assert.Equal(t, "bucket-connection-manager", m.Name())

	ctx := context.Background()
	got, err := m.Take(ctx)
	assert.Nil(t, err)
	assert.Equal(t, bucket, got)
	assert.Nil(t, m.Return(ctx, got))
	assert.Nil(t, m.Shutdown(ctx))
}

func TestGetOSSLockFactoryByClient(t *testing.T) {
	restore := withFakeOSSStorage()
	defer restore()

	ctx := context.Background()
	bucket := testOSSBucket(t, "test-bucket")

	factory, err := GetOSSLockFactoryByClient(ctx, bucket)
	assert.Nil(t, err)
	assert.NotNil(t, factory)

	// 缓存命中
	factory2, err := GetOSSLockFactoryByClient(ctx, bucket)
	assert.Nil(t, err)
	assert.Equal(t, factory, factory2)

	// 端到端加解锁
	lock, err := NewOSSLockByClient(ctx, bucket, "test-oss-lock")
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.Nil(t, lock.Lock(ctx, "owner-1"))
	assert.Nil(t, lock.UnLock(ctx, "owner-1"))
}

func TestGetOSSLockFactoryByEndpoint(t *testing.T) {
	restore := withFakeOSSStorage()
	defer restore()

	ctx := context.Background()
	config := &EndpointConfig{
		Endpoint:        "https://oss-cn-hangzhou.aliyuncs.com",
		AccessKeyID:     "test-ak",
		AccessKeySecret: "test-sk",
		BucketName:      "test-bucket",
	}

	factory, err := GetOSSLockFactoryByEndpoint(ctx, config)
	assert.Nil(t, err)
	assert.NotNil(t, factory)

	// 缓存命中
	factory2, err := GetOSSLockFactoryByEndpoint(ctx, config)
	assert.Nil(t, err)
	assert.Equal(t, factory, factory2)

	lock, err := NewOSSLockByEndpoint(ctx, config, "test-oss-endpoint-lock")
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.Nil(t, lock.Lock(ctx, "owner-1"))
	assert.Nil(t, lock.UnLock(ctx, "owner-1"))
}

func TestGetOSSLockFactoryByClient_RealStorage(t *testing.T) {
	// 走真实 newOSSStorage 成功路径：本地构造不联网（BucketName 已从 bucket 取）。
	ctx := context.Background()
	bucket := testOSSBucket(t, "real-bucket")

	factory, err := GetOSSLockFactoryByClient(ctx, bucket)
	assert.Nil(t, err)
	assert.NotNil(t, factory)
}

func TestGetOSSLockFactoryByEndpoint_RealStorage(t *testing.T) {
	// 走真实 newOSSStorage 成功路径：endpoint 入口本地构造不探活。
	ctx := context.Background()
	config := &EndpointConfig{
		Endpoint:        "https://oss-cn-shanghai.aliyuncs.com",
		AccessKeyID:     "test-ak",
		AccessKeySecret: "test-sk",
		BucketName:      "real-bucket",
	}

	factory, err := GetOSSLockFactoryByEndpoint(ctx, config)
	assert.Nil(t, err)
	assert.NotNil(t, factory)
}

func TestGetOSSLockFactoryByClient_RealStorageError(t *testing.T) {
	// 走真实 newOSSStorage 失败路径：BucketName 为空触发 Check 报错。
	ctx := context.Background()
	emptyBucket := &oss.Bucket{}

	_, err := GetOSSLockFactoryByClient(ctx, emptyBucket)
	assert.NotNil(t, err)
	_, err = NewOSSLockByClient(ctx, emptyBucket, "x")
	assert.NotNil(t, err)
}

func TestGetOSSLockFactoryByEndpoint_RealStorageError(t *testing.T) {
	// 走真实 newOSSStorage 失败路径：BucketName 为空触发 Check 报错。
	ctx := context.Background()
	config := &EndpointConfig{
		Endpoint:        "https://oss-cn-beijing.aliyuncs.com",
		AccessKeyID:     "test-ak",
		AccessKeySecret: "test-sk",
		BucketName:      "",
	}

	_, err := GetOSSLockFactoryByEndpoint(ctx, config)
	assert.NotNil(t, err)
	_, err = NewOSSLockByEndpoint(ctx, config, "x")
	assert.NotNil(t, err)
}

func TestOSSLock_StorageError(t *testing.T) {
	old := newOSSStorage
	newOSSStorage = func(ctx context.Context, options *oss_storage.OSSStorageOptions) (storage.Storage, error) {
		return nil, errors.New("boom")
	}
	defer func() { newOSSStorage = old }()

	ctx := context.Background()

	// 独立 bucket 指针，避免命中成功用例的缓存（bean factory 按指针缓存）
	errBucket := testOSSBucket(t, "boom-bucket")
	_, err := GetOSSLockFactoryByClient(ctx, errBucket)
	assert.NotNil(t, err)
	_, err = NewOSSLockByClient(ctx, errBucket, "x")
	assert.NotNil(t, err)

	// 独立 endpoint key，避免命中成功用例的缓存
	errConfig := &EndpointConfig{
		Endpoint:        "https://oss-cn-boom.aliyuncs.com",
		AccessKeyID:     "boom",
		AccessKeySecret: "boom",
		BucketName:      "boom-bucket",
	}
	_, err = GetOSSLockFactoryByEndpoint(ctx, errConfig)
	assert.NotNil(t, err)
	_, err = NewOSSLockByEndpoint(ctx, errConfig, "x")
	assert.NotNil(t, err)
}
