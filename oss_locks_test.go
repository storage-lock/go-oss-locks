package oss_locks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlobalOSSLockFactory_InitiallyInitialized(t *testing.T) {
	assert.NotNil(t, ossClientBeanFactory)
}
