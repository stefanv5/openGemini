/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package immutable

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockLeaseFile implements PoolFile for testing ShardLeaseCache.
type mockLeaseFile struct {
	path string
}

func (m *mockLeaseFile) Path() string          { return m.path }
func (m *mockLeaseFile) FreeFileHandle() error { return nil }

func TestShardLeaseCache_AddAndExpire(t *testing.T) {
	var expiredFile PoolFile
	cache := NewShardLeaseCache(50*time.Millisecond, func(f PoolFile) {
		expiredFile = f
	})

	f := &mockLeaseFile{path: "shard1/file1.tssp"}
	cache.Add(f, time.Now().Add(50*time.Millisecond))

	// Not expired yet
	cache.CheckExpired()
	assert.Nil(t, expiredFile)

	// Now expired
	time.Sleep(80 * time.Millisecond)
	cache.CheckExpired()
	assert.Equal(t, "shard1/file1.tssp", expiredFile.Path())
}

func TestShardLeaseCache_RemoveCancelsLease(t *testing.T) {
	var didExpire bool
	cache := NewShardLeaseCache(100*time.Millisecond, func(f PoolFile) {
		didExpire = true
	})

	f := &mockLeaseFile{path: "f1"}
	cache.Add(f, time.Now().Add(100*time.Millisecond))

	// Remove before expiry
	cache.Remove(f.Path())

	time.Sleep(150 * time.Millisecond)
	cache.CheckExpired()

	assert.False(t, didExpire)
}

func TestShardLeaseCache_Stats(t *testing.T) {
	cache := NewShardLeaseCache(5*time.Second, func(f PoolFile) {})
	f1 := &mockLeaseFile{path: "f1"}
	f2 := &mockLeaseFile{path: "f2"}

	cache.Add(f1, time.Now().Add(5*time.Second))
	cache.Add(f2, time.Now().Add(5*time.Second))

	stats := cache.Stats()
	assert.Equal(t, 2, stats.Size)
}

func TestShardLeaseCache_DrainOnStop(t *testing.T) {
	var closed int32
	cache := NewShardLeaseCache(5*time.Second, func(f PoolFile) {
		atomic.AddInt32(&closed, 1)
	})

	f1 := &mockLeaseFile{path: "f1"}
	f2 := &mockLeaseFile{path: "f2"}
	cache.Add(f1, time.Now().Add(5*time.Second))
	cache.Add(f2, time.Now().Add(5*time.Second))

	cache.Stop()

	assert.Equal(t, int32(2), atomic.LoadInt32(&closed))
	assert.Equal(t, 0, cache.Stats().Size)
}
