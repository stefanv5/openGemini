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
	"container/list"
	"sync"
	"sync/atomic"
)

// PoolFile is the minimal interface that NodeFilePool requires.
// TSSPFile implements this interface.
type PoolFile interface {
	Path() string
	FreeFileHandle() error
}

// PoolStats holds NodeFilePool statistics.
type PoolStats struct {
	Size      int
	MaxSize   int
	Hits      int64
	Misses    int64
	Evictions int64
}

// NodeFilePool is a node-level global LRU pool for file handles.
// It bounds the total number of open file handles across all shards.
// When full, new Put() calls evict the LRU entry and replace it with the new file.
type NodeFilePool struct {
	maxHandles int
	handles    *list.List // LRU list, front = most recently used
	index      map[string]*list.Element
	mu         sync.Mutex
	closeFn    func(PoolFile) error
	stats      PoolStats
}

// NewNodeFilePool creates a new NodeFilePool with the given max handle limit
// and a close function to call when evicting a file.
func NewNodeFilePool(maxHandles int, closeFn func(PoolFile) error) *NodeFilePool {
	return &NodeFilePool{
		maxHandles: maxHandles,
		handles:    list.New(),
		index:      make(map[string]*list.Element),
		closeFn:    closeFn,
	}
}

// Put attempts to add a file to the pool. If the pool is full, it evicts
// the least recently used entry and adds the new file in its place.
// Returns true if the file was placed in the pool.
func (p *NodeFilePool) Put(file PoolFile) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := file.Path()

	// If already in pool, just promote to front.
	if elem, exists := p.index[key]; exists {
		p.handles.MoveToFront(elem)
		return true
	}

	// If at capacity, evict LRU entry.
	if p.handles.Len() >= p.maxHandles {
		p.evictOneLocked()
	}

	elem := p.handles.PushFront(file)
	p.index[key] = elem
	return true
}

// Get looks up a file by path. Returns (file, true) if found, (nil, false) if not.
// Found entries are moved to front (LRU promotion).
func (p *NodeFilePool) Get(key string) (PoolFile, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if elem, found := p.index[key]; found {
		p.handles.MoveToFront(elem)
		atomic.AddInt64(&p.stats.Hits, 1)
		return elem.Value.(PoolFile), true
	}
	atomic.AddInt64(&p.stats.Misses, 1)
	return nil, false
}

// Remove closes and removes a specific file from the pool.
func (p *NodeFilePool) Remove(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if elem, found := p.index[key]; found {
		delete(p.index, key)
		p.handles.Remove(elem)
		if p.closeFn != nil {
			p.closeFn(elem.Value.(PoolFile))
		}
	}
}

// Stats returns a copy of current pool statistics.
func (p *NodeFilePool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{
		Size:      p.handles.Len(),
		MaxSize:   p.maxHandles,
		Hits:      atomic.LoadInt64(&p.stats.Hits),
		Misses:    atomic.LoadInt64(&p.stats.Misses),
		Evictions: atomic.LoadInt64(&p.stats.Evictions),
	}
}

// evictOneLocked removes the least recently used entry and closes it.
// Caller must hold p.mu.
func (p *NodeFilePool) evictOneLocked() {
	if elem := p.handles.Back(); elem != nil {
		f := p.handles.Remove(elem).(PoolFile)
		key := f.Path()
		delete(p.index, key)
		atomic.AddInt64(&p.stats.Evictions, 1)
		if p.closeFn != nil {
			p.closeFn(f)
		}
	}
}

var nodeFilePool *NodeFilePool

// InitNodeFilePool initializes the global NodeFilePool singleton.
// Must be called once at startup before any query runs.
func InitNodeFilePool(maxHandles int) {
	nodeFilePool = NewNodeFilePool(maxHandles, func(f PoolFile) error {
		return f.FreeFileHandle()
	})
}

// GetNodeFilePool returns the global NodeFilePool singleton.
func GetNodeFilePool() *NodeFilePool {
	return nodeFilePool
}
