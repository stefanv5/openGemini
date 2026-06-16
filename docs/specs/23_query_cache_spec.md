# Module 23: 查询缓存深度审计报告（庖丁解牛版）

> openGemini 有两层缓存：**结果缓存**（PromQL 查询结果）和**读缓存**（磁盘 Block/Page 级别）。

---

## 1. 两层缓存架构

```mermaid
graph TB
    subgraph "查询路径"
        Q["查询请求"] --> RC["结果缓存 (Result Cache)"]
        RC -->|"命中"| R["返回缓存结果"]
        RC -->|"未命中"| E["执行查询"]
        E --> BC["读缓存 (Block Cache)"]
        BC -->|"命中"| M["从内存读取 Block"]
        BC -->|"未命中"| D["从磁盘读取 Block"]
        M --> E
        D --> E
    end
```

---

## 2. 结果缓存（PromQL 层）

**代码位置**：`lib/resultcache/`, `lib/util/lifted/influx/httpd/results_cache.go`

### 2.1 缓存策略

```go
type ResultCache interface {
    Get(key string) ([]byte, bool)   // 查找
    Put(key string, value []byte)    // 写入
}
```

**缓存 Key**：`mst:db:rp:cmd:step:startBucket`

```go
func generateCacheKey(command *promql2influxql.PromCommand, mst string, interval time.Duration) string {
    currentInterval := command.Start.UnixNano() / int64(interval)
    return fmt.Sprintf("%s:%s:%s:%s:%s:%d",
        mst,
        command.Database,
        command.RetentionPolicy,
        command.Cmd,
        command.Step.String(),
        currentInterval,
    )
}
```

**具体例子**：

```
mst = "cpu"
db = "prom"
rp = "autogen"
cmd = "rate(cpu_usage[5m])"
step = "10s"
SplitQueriesByInterval = 5m
start = 1970-01-01 00:08:00

key = cpu:prom:autogen:rate(cpu_usage[5m]):10s:1
```

`startBucket` 不是原始 start 时间，而是 `command.Start.UnixNano() / SplitQueriesByInterval` 的整数桶号。这样同一时间桶内的相同 PromQL 请求会路由到同一个结果缓存 key。

**淘汰策略**：LRU（Least Recently Used）

**TTL 机制**：惰性检查 — Get 时检查是否过期，无后台清理

### 2.2 ResultsCache.Do 真实流程

```mermaid
sequenceDiagram
    participant H as Prom Handler
    participant RC as ResultsCache.Do
    participant Cache as ResultCache
    participant Exec as DoRequests
    participant Merge as MergeResponse

    H->>RC: Do(reqInfo, command, key)
    RC->>RC: shouldCache(request)
    alt no-store / 不允许缓存
        RC-->>H: nil, false, nil
    else start 太新
        RC->>RC: command.Start > now - MaxCacheFreshness
        RC-->>H: nil, false, nil
    else 可缓存
        RC->>Cache: get(key)
        alt 命中
            Cache-->>RC: []Extent
            RC->>RC: partition(command, extents)
            alt full hit
                RC->>Merge: MergeResponse(cached responses)
                RC-->>H: response, true, nil
            else partial hit
                alt len(missing commands) > MaxRequestCount(3)
                    RC->>Exec: DoRequests(original command)
                    Exec-->>RC: full response
                    RC-->>H: response, true, nil
                else len(missing commands) <= MaxRequestCount
                    RC->>Exec: DoRequests(missing commands)
                    Exec-->>RC: missing extents
                    RC->>Merge: cached + missing
                    RC->>RC: filterRecentExtents()
                    RC->>Cache: put(key, merged extents)
                    RC-->>H: response, true, nil
                end
            end
        else 未命中
            RC->>Exec: DoRequests(command)
            Exec-->>RC: extent
            RC->>RC: shouldCacheResponse()
            RC->>RC: filterRecentExtents()
            RC->>Cache: put(key, extents)
            RC-->>H: response, true, nil
        end
    end
```

**核心代码讲解**：

```go
func (rc *ResultsCache) Do(reqInfo *RequestInfo, command *promql2influxql.PromCommand,
    key string) (*promql2influxql.PromQueryResponse, bool, error) {
    if !shouldCache(reqInfo.r) {
        return nil, false, nil
    }

    maxCacheTime := model.Now().Add(-rc.MaxCacheFreshness).UnixNano()
    if command.Start.UnixNano() > maxCacheTime {
        return nil, false, nil
    }

    cached, ok := rc.get(key)
    if ok {
        response, extents, err = rc.handleHit(reqInfo, command, cached, maxCacheTime, &fullHit)
    } else {
        response, extents, err = rc.handleMiss(reqInfo, command, maxCacheTime)
    }
    if fullHit {
        return response, true, nil
    }

    extents = rc.filterRecentExtents(extents, command.Step.Nanoseconds(), rc.MaxCacheFreshness)
    if len(extents) > 0 {
        rc.put(key, extents)
    }
    return response, true, nil
}
```

**逐行解释**：
- `shouldCache(reqInfo.r)`：遵守 HTTP cache-control，遇到 no-store 等场景直接绕过缓存
- `maxCacheTime`：用 `MaxCacheFreshness` 避免缓存过新的数据
- `rc.get(key)`：读取并反序列化 `CachedResponse`，同时校验响应内的 key
- `handleHit()`：把已有 Extent 与请求范围做 `partition()`；缺口数量不超过 `MaxRequestCount(3)` 时才逐段补查并合并，超过时回退执行原始完整查询
- `handleMiss()`：完整执行一次查询，并把结果作为一个 Extent 返回
- `shouldCacheResponse()`：除新鲜度外，还会检查 PromQL `@` modifier 是否落在可缓存时间内，以及负 `offset` 是否会导致不安全缓存
- `filterRecentExtents()`：裁掉太新的区间、空响应和无效响应
- `rc.put(key, extents)`：序列化 `CachedResponse{Key, Extents}` 写入底层 `ResultCache`

### 2.3 部分命中

```mermaid
sequenceDiagram
    participant Q as 查询 (time=0~100)
    participant Cache as 结果缓存

    Q->>Cache: Get(key)
    Cache-->>Q: 命中 time=0~60 的结果

    alt 缺口数量 <= 3
        Q->>Q: 只查询 time=60~100
        Q->>Q: 合并缓存 + 新查询结果
        Q->>Cache: Put(key, merged_result)
    else 缺口数量 > 3
        Q->>Q: 回退执行原始完整查询
        Q-->>Q: 返回完整查询结果
    end
```

**通俗解释**：
如果你查询"最近 100 分钟的数据"，缓存中有"前 60 分钟"的结果，系统只查询"后 40 分钟"，然后合并。这样避免了重复计算。

---

## 3. 读缓存（Block/Page 层）

**代码位置**：`lib/readcache/`

### 3.1 结构

```go
type blockCache struct {
    blocks []mapLruCache  // 分片数组，减少锁竞争
}

type lruCache struct {
    currBuffer *buffer  // 当前代
    oldBuffer  *buffer  // 上一代
    bufferSize int
}
```

### 3.2 代际淘汰（Generational Eviction）

```mermaid
stateDiagram-v2
    [*] --> CurrBuffer: 新数据写入

    state CurrBuffer {
        [*] --> 写入: 新 Page
        写入 --> [*]: 容量满
    }

    CurrBuffer --> OldBuffer: refreshBuffer()
    note left of OldBuffer: currBuffer 超过一半容量时<br/>currBuffer → oldBuffer<br/>清空旧 oldBuffer

    OldBuffer --> [*]: 被清空
```

**通俗解释**：
读缓存就像"两代仓库"。新数据先进"当前仓库"，当前仓库满了，就把"当前仓库"变成"上一代仓库"，同时清空旧的"上一代仓库"。查询时先查"当前仓库"，没找到再查"上一代仓库"。

### 3.3 缓存 Key

```
格式：filePath&&offset 或 filePath&&blockId
示例：/data/db0/0/cpu_0001/00000001-0000-00000001.tssp&&1048576
```

### 3.4 配置

```
Meta Cache: 2GB 默认
Data Cache: 2GB 默认
```

---

## 4. 总结

| 缓存层 | 粒度 | 淘汰策略 | TTL |
|--------|------|---------|-----|
| 结果缓存 | 查询结果 | LRU | 惰性检查 |
| 读缓存 | Block/Page | 代际淘汰 | 无（按容量） |
| MetaIndex 缓存 | 文件元数据 | 代际淘汰 | 无（按容量） |

| 设计 | 效果 |
|------|------|
| 结果缓存部分命中 | 避免重复计算 |
| 256 分片读缓存 | 减少锁竞争 |
| 代际淘汰 | 简单高效，无链表维护 |
| 引用计数 Page | 安全并发访问 |
