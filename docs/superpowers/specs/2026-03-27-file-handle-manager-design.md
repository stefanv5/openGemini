# File Handle Manager Design

## 1. Overview

Design a three-layer file handle resource management system for openGemini, optimized for distributed storage backends (HDFS, OBS) where open/close costs are high (~100-500ms per operation).

**Goals:**
- Reduce file open/close overhead for HDFS/OBS backends by caching open file handles
- Enforce node-level file handle limits to prevent resource exhaustion
- Support fair eviction under memory/handle pressure
- Zero overhead for local storage backends (passthrough)

**Non-Goals:**
- Cross-shard file sharing (files belong to a single shard)
- Modifying the existing Ref/Unref lifecycle at query boundaries

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Node (single process)                    │
│                                                                  │
│  ┌───────────┐   ┌───────────┐   ┌───────────┐                │
│  │  Shard 1  │   │  Shard 2  │   │  Shard N  │                │
│  │ LeaseCache│   │LeaseCache│   │LeaseCache│                │
│  │ ref==0:   │   │ref==0:   │   │ref==0:   │                │
│  │ enter     │   │enter     │   │enter     │                │
│  │ Lease     │   │Lease     │   │Lease     │                │
│  └─────┬─────┘   └─────┬─────┘   └─────┬─────┘                │
│        │               │               │                        │
│        └───────────────┴───────────────┘                        │
│                            ▼                                      │
│                  ┌─────────────────────┐                          │
│                  │    NodeFilePool      │  ← global handle pool  │
│                  │    (maxHandles)     │    bounded total count  │
│                  │  • Shard配额驱逐    │    fair LRU eviction    │
│                  └─────────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Layer 1: Shard-Local Ref/Unref (Existing, No Change)

```
Query start → RefFiles()  → each file ref++
Query end   → UnrefFiles() → each file ref--
```

No changes. The existing Ref/Unref lifecycle at query boundaries is preserved.

---

## 4. Layer 2: Shard-Local Lease Cache

### 4.1 Lease Entry

```go
type LeaseEntry struct {
    file   TSSPFile
    expiry time.Time // absolute Lease expiration time
}

type ShardLeaseCache struct {
    entries   map[string]*LeaseEntry // keyed by file path
    mu        sync.Mutex
    duration  time.Duration
    timer     *time.Timer
    onExpired func(TSSPFile) // callback when Lease expires
}
```

### 4.2 Lease Lifecycle

| Event | Action |
|-------|--------|
| `ref` reaches 0 (Unref) | Add file to LeaseCache, start Lease timer |
| `ref` increases (Ref) | Remove from LeaseCache immediately, cancel timer |
| Lease timer expires | Attempt to return file to NodeFilePool; if pool full, call `FreeFileHandle()` |

### 4.3 Lease Semantic: "Reuse Window"

Lease is a **reuse window**, not merely a delayed close:

- When `ref == 0`: file enters Lease state, timer starts
- When `ref++` (any access): Lease immediately cancelled, file reused
- When `ref` returns to 0: new Lease cycle begins
- When timer fires: file checked — if `ref == 0`, returned to pool or closed; if `ref > 0`, ignored (file already reused)

### 4.4 Timer Implementation

Each Shard runs one background goroutine:

```go
func (c *ShardLeaseCache) run() {
    for {
        select {
        case <-c.timer.C:
            c.mu.Lock()
            now := time.Now()
            for key, entry := range c.entries {
                if entry.expiry.Before(now) || entry.expiry.Equal(now) {
                    delete(c.entries, key)
                    go c.onExpired(entry.file) // non-blocking callback
                }
            }
            c.mu.Unlock()
            c.scheduleNext()
        }
    }
}
```

---

## 5. Layer 3: Node-Level Global File Handle Pool

### 5.1 Core Interface

```go
// TSSPFileKey uniquely identifies a file (path + level + sequence)
type TSSPFileKey struct {
    Path string
}

type NodeFilePool interface {
    // Try to return a file to the pool. Returns false if pool is full.
    Put(file TSSPFile) bool

    // Look up a file by key. Returns file and true if found (LRU-promoted).
    Get(key TSSPFileKey) (TSSPFile, bool)

    // Close and remove a specific file from the pool.
    Remove(key TSSPFileKey)

    // Pool statistics.
    Stats() PoolStats
}
```

### 5.2 LRU Implementation with Fair Eviction

```go
type nodeFilePool struct {
    maxHandles int
    handles    *list.List // LRU list, front = most recently used
    index      map[string]*list.Element
    mu         sync.Mutex
    closeFn    func(TSSPFile) error
}

func (p *nodeFilePool) Put(file TSSPFile) bool {
    p.mu.Lock()
    defer p.mu.Unlock()

    if p.handles.Len() >= p.maxHandles {
        return false // pool full, caller should close directly
    }

    key := file.Key()
    if elem, exists := p.index[key]; exists {
        p.handles.MoveToFront(elem) // already in pool, promote
        return true
    }

    elem := p.handles.PushFront(file)
    p.index[key] = elem
    return true
}

func (p *nodeFilePool) Get(key TSSPFileKey) (TSSPFile, bool) {
    p.mu.Lock()
    defer p.mu.Unlock()

    if elem, found := p.index[key.Key()]; found {
        p.handles.MoveToFront(elem)
        return elem.Value.(TSSPFile), true
    }
    return nil, false
}
```

### 5.3 Fair Eviction Strategy

When pool is full and a Shard needs to return a file, eviction selects from **the Shard that currently holds the most pool slots**, to distribute eviction pressure fairly:

```go
type shardEvictionWeight struct {
    shardID    uint64
    poolCount  int // files in pool from this shard
}

func (p *nodeFilePool) evictOne() TSSPFile {
    // Remove least recently used entry from the shard with highest pool count
    // (simplified: just LRU for now, shard fairness is a future optimization)
    if elem := p.handles.Back(); elem != nil {
        return p.handles.Remove(elem).(TSSPFile)
    }
    return nil
}
```

Note: Fair eviction across Shards is a future optimization. Initial implementation uses simple LRU.

---

## 6. Integration: Modified Unref Flow

Replace the existing `unRefFiles` logic in `engine/ts_index_info.go`:

### Before (existing)

```go
func (f *TSIndexInfoImpl) unRefFiles(files immutable.TableReaders) {
    if fileCacheManager != nil && len(files) <= int(fileCacheManager.GetCap()) {
        for _, file := range files {
            fileCacheManager.Put(file)  // old: put to channel, then sync close
            file.Unref()
        }
    } else {
        for _, file := range files {
            file.UnrefFileReader()  // sync close, blocks query thread
            file.Unref()
        }
    }
}
```

### After (new three-layer design)

```go
func (f *TSIndexInfoImpl) unRefFiles(files immutable.TableReaders) {
    for _, file := range files {
        // 1. Decrement ref
        file.Unref()  // when ref reaches 0, ShardLeaseCache takes over

        // 2. ShardLeaseCache handles Lease lifecycle
        // - If accessed again before Lease expires: ref++ and reused (no pool involved)
        // - If Lease expires: returns to NodeFilePool (or closes if pool full)
    }
}
```

The `Unref()` call on `tsspFile` is modified to trigger `ShardLeaseCache` when ref reaches 0, replacing the current `QueryfileCache` logic.

---

## 7. Configuration

| Config Key | Default | Description |
|------------|---------|-------------|
| `FileHandleCacheEnabled` | `true` | Enable the new file handle manager |
| `MaxFileHandles` | `ulimit / 2` | Node-level max open file handles |
| `LeaseDuration` | `30s` | Lease reuse window |
| `LeaseCheckInterval` | `5s` | How often to scan for expired Leases |
| `PoolEnabled` | `true` | Enable NodeFilePool (vs direct close on full) |

---

## 8. Storage Backend Abstraction

The design works transparently with all storage backends through the existing `BasicFileReader` interface:

- **Local**: `FreeFileHandle` ≈ 1ms → Lease benefit moderate
- **HDFS**: `FreeFileHandle` ≈ 200-500ms (NameNode RPC + DataNode block report) → **Lease benefit huge**
- **OBS**: `FreeFileHandle` ≈ 100-300ms → **Lease benefit significant**

No backend-specific code required; the abstraction is already in place.

---

## 9. Error Handling

| Scenario | Behavior |
|----------|---------|
| `FreeFileHandle()` fails on Lease expire | Log error, file remains in pool for retry |
| Pool `Put()` returns false (full) | Caller executes `FreeFileHandle()` synchronously |
| Shard shutdown with active Leases | Drain LeaseCache before shutdown |
| File reopened after Lease expires but before pool eviction | Pool `Get()` misses; normal `Open()` path (expensive but correct) |

---

## 10. Implementation Order

1. **Phase 1**: Add `NodeFilePool` with LRU and global handle limit
2. **Phase 2**: Add `ShardLeaseCache` with Lease timer and Ref/Unref integration
3. **Phase 3**: Remove/deprecate old `QueryfileCache`
4. **Phase 4**: Add fair eviction (shard quota weights)
5. **Phase 5**: Add metrics/statistics for monitoring

---

## 11. Out of Scope (Future Work)

- Cross-Shard file sharing (not needed for this codebase)
- Tiered pools (hot/warm/cold file handles)
- Storage backend-specific Lease strategies
