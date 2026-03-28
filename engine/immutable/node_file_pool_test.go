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

	"github.com/stretchr/testify/assert"
)

// mockPoolFile implements PoolFile for testing NodeFilePool.
type mockPoolFile struct {
	path string
}

func (m *mockPoolFile) Path() string          { return m.path }
func (m *mockPoolFile) FreeFileHandle() error { return nil }

func TestNodeFilePool_PutAndGet(t *testing.T) {
	pool := NewNodeFilePool(3, nil)
	f1 := &mockPoolFile{path: "shard1/file1.tssp"}
	f2 := &mockPoolFile{path: "shard1/file2.tssp"}

	ok := pool.Put(f1)
	assert.True(t, ok)
	ok = pool.Put(f2)
	assert.True(t, ok)

	got, found := pool.Get("shard1/file1.tssp")
	assert.True(t, found)
	assert.Equal(t, f1, got)
}

func TestNodeFilePool_LRU_Eviction(t *testing.T) {
	var closeCalls int32
	pool := NewNodeFilePool(2, func(f PoolFile) error {
		atomic.AddInt32(&closeCalls, 1)
		return nil
	})

	f1 := &mockPoolFile{path: "f1"}
	f2 := &mockPoolFile{path: "f2"}
	f3 := &mockPoolFile{path: "f3"}

	pool.Put(f1)
	pool.Put(f2)
	pool.Put(f3) // f1 should be evicted

	_, found := pool.Get("f1")
	assert.False(t, found) // f1 was evicted
	assert.Equal(t, int32(1), atomic.LoadInt32(&closeCalls))
}

func TestNodeFilePool_DuplicatePut(t *testing.T) {
	pool := NewNodeFilePool(3, nil)
	f1 := &mockPoolFile{path: "f1"}

	pool.Put(f1)
	pool.Put(f1) // duplicate

	got, found := pool.Get("f1")
	assert.True(t, found)
	assert.Equal(t, f1, got)
	assert.Equal(t, 1, pool.Stats().Size) // only one entry
}

func TestNodeFilePool_Remove(t *testing.T) {
	var closeCalls int32
	pool := NewNodeFilePool(5, func(f PoolFile) error {
		atomic.AddInt32(&closeCalls, 1)
		return nil
	})
	f1 := &mockPoolFile{path: "f1"}
	pool.Put(f1)

	pool.Remove("f1")

	_, found := pool.Get("f1")
	assert.False(t, found)
	assert.Equal(t, 0, pool.Stats().Size)
	assert.Equal(t, int32(1), atomic.LoadInt32(&closeCalls)) // remove calls closeFn
}

func TestNodeFilePool_Stats(t *testing.T) {
	pool := NewNodeFilePool(10, nil)
	f1 := &mockPoolFile{path: "f1"}

	pool.Put(f1)
	pool.Get("f1")       // hit
	pool.Get("notexist")  // miss

	stats := pool.Stats()
	assert.Equal(t, 1, stats.Size)
	assert.Equal(t, 10, stats.MaxSize)
	assert.Equal(t, int64(1), stats.Hits)
	assert.Equal(t, int64(1), stats.Misses)
	assert.Equal(t, int64(0), stats.Evictions)
}
