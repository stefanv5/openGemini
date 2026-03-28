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
	"sync"
	"time"
)

// LeaseEntry represents a file in the Lease reuse window.
type LeaseEntry struct {
	file   PoolFile
	expiry time.Time
}

// LeaseStats holds ShardLeaseCache statistics.
type LeaseStats struct {
	Size     int
	Active   int
	Expired  int64
	Cancelled int64
}

// ShardLeaseCache implements Layer 2 of the file handle manager.
// It holds files when their ref count reaches 0, giving them a "Lease"
// reuse window before they are returned to the NodeFilePool or closed.
// If the file is reused (ref++) during the Lease window, it is removed
// from the cache immediately.
type ShardLeaseCache struct {
	entries  map[string]*LeaseEntry
	mu       sync.Mutex
	duration time.Duration
	onExpired func(PoolFile)
	stopCh   chan struct{}
	ticker   *time.Ticker
}

// NewShardLeaseCache creates a new ShardLeaseCache.
// duration: the Lease reuse window (e.g., 30 seconds).
// onExpired: called when a Lease expires and the file should be
//   returned to the NodeFilePool or closed.
func NewShardLeaseCache(duration time.Duration, onExpired func(PoolFile)) *ShardLeaseCache {
	return &ShardLeaseCache{
		entries:   make(map[string]*LeaseEntry),
		duration:  duration,
		onExpired: onExpired,
		stopCh:    make(chan struct{}),
		ticker:    time.NewTicker(duration / 2), // scan twice per duration
	}
}

// Add places a file into the Lease cache with an absolute expiry time.
// Called when file ref reaches 0.
func (c *ShardLeaseCache) Add(file PoolFile, expiry time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[file.Path()] = &LeaseEntry{file: file, expiry: expiry}
}

// Remove cancels an in-flight Lease (called when file is reused via ref++).
func (c *ShardLeaseCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// CheckExpired scans entries and calls onExpired for any that have passed their expiry.
// Called by the background goroutine or for testing.
func (c *ShardLeaseCache) CheckExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for key, entry := range c.entries {
		if !entry.expiry.After(now) {
			delete(c.entries, key)
			go c.onExpired(entry.file)
		}
	}
}

// Start launches the background Lease expiry goroutine.
func (c *ShardLeaseCache) Start() {
	go func() {
		for {
			select {
			case <-c.stopCh:
				return
			case <-c.ticker.C:
				c.CheckExpired()
			}
		}
	}()
}

// Stop stops the background goroutine and drains remaining entries
// by calling onExpired for each.
func (c *ShardLeaseCache) Stop() {
	close(c.stopCh)
	c.ticker.Stop()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.entries {
		c.onExpired(entry.file)
	}
	c.entries = make(map[string]*LeaseEntry)
}

// Stats returns current cache statistics.
func (c *ShardLeaseCache) Stats() LeaseStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return LeaseStats{
		Size: len(c.entries),
	}
}
