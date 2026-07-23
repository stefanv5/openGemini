# Merge 文件按大小切割：完整正确性分析

## 1. 文档目的与分析基线

本文分析 openGemini TSStore merge 流程增加“按文件大小切割”能力后的正确性，重点回答：

1. 注释版 `ColumnIterator.Run` 的切割调用位置是否正确；
2. `mergePerformer.SwitchNewTSSP` 的状态转换是否正确；
3. 同一个 SID 是否可能存在于多个 extent；
4. extent 间 SID 顺序不一致是否影响普通查询、pre-agg 和 LIMIT；
5. 4 GiB 阈值与 `uint16 extent` 是否足以保证格式安全；
6. 当前实现需要怎样重构和验证后才能合入。

分析基线：

- 仓库 HEAD：`eb398291`；
- 工作区修改：`column_iterator.go`、`stream_downsample.go`、`tssp_file_name.go`；
- 分析日期：2026-07-21；
- 注释版 [`ColumnIterator.Run`](../engine/immutable/column_iterator.go#L141) 被视为计划启用的目标逻辑；
- 忽略 getter、接口声明、logger 字段和调用签名等编译脚手架问题；
- 假设目标阈值为 4 GiB，并且设计目标是“一个 SID 完整写完后再决定是否切文件”；
- 假设首次 merge 输出应增加 merge generation，后续切片只增加 extent。

本文是一份 correctness-first 静态代码审查，不把当前编译状态当作主要结论。

## 2. 总结性结论

当前实现不能安全合入。即使忽略全部编译问题，仍然存在多个相互独立的运行时阻断项。

| 编号 | 结论 | 严重度 | 是否由用户提出的 performer reset 修复 |
|---|---|---:|---:|
| F-01 | merged 路径在 SID 完成前检查大小，可能延迟一个完整 SID 或在 EOF 不切割 | High（按当前明确的切割语义；若阈值只作软提示则属于行为校准） | 否 |
| F-02 | 当前代码切换后会再次 finish 旧 SID | Critical | 是，成功切换路径可修复 |
| F-03 | 新 extent 没有完整 reset `StreamWriteFile` 文件级状态 | Critical | 否 |
| F-04 | 最后一个 SID 切割后会留下空 extent，并可能向结果追加 nil | High | 否，nil 判断位置不对 |
| F-05 | `writeRemain` 可以连续写大量 unordered-only SID，完全绕过切割 | High | 否 |
| F-06 | extent 冲突探测成功后仍打开旧路径，文件名状态与 fd 不一致 | Critical | 否 |
| F-07 | 替换阶段可能删除未参与 merge 的旧 extent，造成确定性数据丢失 | Critical | 否 |
| F-08 | 当前存储与 merge 语义允许同 SID 位于多个 extent | 设计事实；若错误依赖唯一性则形成 Medium 风险 | 否 |
| F-09 | 同 SID 多 extent 且时间范围与文件顺序不一致时，LIMIT 提前裁剪可能返回错误行 | High（条件性） | 否 |
| F-10 | 单 SID 编码数据达到 4 GiB 时 `ChunkMeta.size uint32` 回绕 | Critical（条件性） | 否 |
| F-11 | `uint16 extent++` 可能回绕，shard 总大小上限不能单独证明编号安全 | High（条件性） | 否 |

最核心的设计错误是把两个状态机混在了一个函数中：

```text
状态机 A：完成当前 SID
状态机 B：判断大小并切换物理文件
```

正确顺序必须是：

```text
完整结束 SID
→ 清除当前 SID 活跃状态
→ 计算完整文件估算大小
→ 超限时 finalize 当前文件
→ 完整重置文件级 writer 状态
→ 在下一次真正写数据时创建下一 extent
```

当前顺序则是：

```text
SID 尚未结束
→ 检查不完整的 DataSize
→ 仅在已经超限时 finish SID
→ finalize
→ 复用未完整 reset 的 StreamWriteFile
```

## 3. 必须满足的正确性不变量

### I-01：文件只能在完整 SID 边界 finalize

所谓“完整 SID”必须已经完成：

- 所有 ordered 字段列；
- 所有 unordered-only 字段列；
- merged time 列；
- 当前 SID 的 ChunkMeta；
- 所有 columnWriter remain buffer 的 flush。

### I-02：完成 SID 的操作必须幂等

同一个 SID 完成后，再次调用完成逻辑必须成为 no-op，不能再次写 time、data 或 ChunkMeta。

### I-03：每个完整 SID 后都必须经过统一的大小检查

该规则同时适用于：

- `WriteOriginal` 复制的 SID；
- ordered + unordered 合并生成的 SID；
- `writeRemain` 写出的 unordered-only SID。

### I-04：新 extent 不得继承旧文件的任何文件级统计或 offset

新文件的 Trailer、IdTimePairs、ChunkMeta offset、行数统计、pre-agg builder 等必须从零开始。

### I-05：逻辑文件名必须与实际打开的 fd 路径一致

extent 探测后若文件名发生变化，实际创建路径必须使用变化后的名称；创建还必须能够原子地拒绝冲突。

### I-06：替换旧文件必须依据真实数据血缘

不能通过新文件的 `(sequence, extent)` 反推哪些旧文件参与了 merge。

### I-07：如果允许同 SID 多 extent，查询必须枚举并正确归并全部位置

任何 LIMIT 优化都不能在确认时间顺序前提前丢弃包含该 SID 的文件。

### I-08：单 SID 的编码数据大小必须能被文件格式表达

因为 `ChunkMeta.size` 是 `uint32`，单个 SID 的 data range 必须严格小于 `2^32` 字节，除非修改格式或支持 SID 内切割。

## 4. 当前控制流

### 4.1 original 路径

目标代码的 original 路径是：

```text
SeriesChanged(sid, nil)
→ 完成前一个 pending SID，并在 last file 中写出 sid 之前的 remain
→ p.sid = 0
→ WriteOriginal
→ 复制当前 SID 全部 data
→ WriteMeta
→ SwitchNewTSSP(sid, false)
```

[`WriteOriginal`](../engine/immutable/merge_performer.go#L360) 最终调用 `WriteMeta`，所以调用 Switch 时当前 original SID 已完整写完。单看当前 SID 边界，这条路径是正确的。

但 `SeriesChanged(sid, nil)` 内部可能先通过 `writeRemain(sid)` 写出多个 unordered-only SID，而这些 SID 没有逐个经过大小检查，因此整个路径仍然不满足 I-03。

### 4.2 merged 路径

目标代码的 merged 路径是：

```text
initTimeColumn
→ SeriesChanged(sid, orderTimes)
→ walkColumn
→ SwitchNewTSSP(sid, true)
```

`walkColumn` 不处理 time 列。返回时当前 SID 仍缺：

- `writeRemainCol()`；
- `writeMergedTime()`；
- `WriteCurrentMeta()`。

这些操作只在 [`finishSeries`](../engine/immutable/merge_performer.go#L200) 中发生。因此进入 Switch 时当前 SID 仍是 active 状态。

### 4.3 EOF 路径

[`mergePerformer.Finish`](../engine/immutable/merge_performer.go#L185) 执行：

```text
finishSeries(MaxInt64)
→ 写完当前 SID
→ last file 时写完所有剩余 unordered SID
→ NewTSSPFile
→ AppendMergedFile
```

该路径在 `finishSeries(MaxInt64)` 后没有再次检查大小。

### 4.4 当前 Switch 路径

[`SwitchNewTSSP`](../engine/immutable/column_iterator.go#L201) 当前执行：

```text
检查 writer.DataSize
→ 未超限：直接返回
→ 已超限且 forceFinish：finishSeries(sid)
→ NewTSSPFile
→ AppendMergedFile
→ InitMergedFile(addFileExt=true)
```

问题在于，大小判断发生在 `finishSeries` 之前，而新文件初始化又没有完整 reset writer。

## 5. 逐项问题与可复现路径

## F-01：merged 路径在 SID 完成前检查大小

### 证据

- [`ColumnIterator.Run`](../engine/immutable/column_iterator.go#L176) 在 `walkColumn` 后立即调用 Switch；
- [`SwitchNewTSSP`](../engine/immutable/column_iterator.go#L206) 先检查 `writer.DataSize()`；
- [`finishSeries`](../engine/immutable/merge_performer.go#L200) 才写剩余列、time 和 ChunkMeta。

### 最小执行例

假设阈值为 100：

```text
文件已有数据：80
SID10 普通字段：+15
Switch 检查：95 < 100，返回

下一次 SeriesChanged(SID20)：
  完成 SID10 的 time/meta：+20
  文件大小变成 115

随后仍开始写 SID20：+50
下一次 Switch 才看到 165，并进行切割
```

文件不是只因 SID10 软超限，而是又多写了一个完整 SID20。

如果 SID10 是最后一个 SID：

```text
walkColumn 后 95
→ Switch 不切
→ EOF Finish 完成 SID10 后变成 115
→ 直接 finalize
```

### 后果

- 违反“完整 SID 后检查”的明确设计目标；
- 文件可能额外装入下一个完整 SID；
- EOF 文件可能超阈值且不触发切割；
- 使用 `DataSize()` 还遗漏最终索引和 trailer 等开销。

CC 校准：F-01 单独看主要是切割阈值语义错误，并不必然立即造成数据损坏。如果产品定义允许任意延迟的软阈值，可以降低严重度；但当前需求明确要求“完整 SID 后检查”，而且现有顺序会额外装入下一个完整 SID，因此本文保留 High。

### 修复要求

先幂等地完成当前 SID，再用 [`StreamWriteFile.Size`](../engine/immutable/stream_downsample.go#L620) 检查；不允许拆 SID 时，可以接受由当前 SID 导致的一次软超限，但不能再多写下一个 SID。

## F-02：切换后旧 SID 会被再次 finish

### 当前执行状态

假设当前 merged SID 为 10：

```text
p.sid = 10
p.mergedTimeCol = SID10 time
sw.dstMeta.sid = 10
```

Switch 中第一次调用：

```text
finishSeries(10)
→ writeMergedTime 写 SID10 time
→ WriteCurrentMeta 写 SID10 ChunkMeta
→ WriteCurrentMeta 仅令 sw.dstMeta.sid = 0
```

完成后仍然存在：

```text
p.sid = 10
p.mergedTimeCol 仍然保存 SID10 time
```

切换到新 writer 后，下一次 `SeriesChanged` 或 EOF `Finish` 又会调用 `finishSeries`。`writeMergedTime()` 看到 `p.sid != 0`，会再次写 SID10 time。

### 两种后果

1. 当前 writer 未 reset Trailer：[`validate`](../engine/immutable/stream_downsample.go#L634) 因 `id <= trailer.maxId` 返回错误；
2. 即使 reset Trailer：新 writer 没有为旧 SID 执行 `ChangeSid`，`dstMeta.sid == 0`，因此写入的数据没有 ChunkMeta 引用，形成 orphan data。

### 对用户提出 reset 补丁的判断

用户提出在成功 `InitMergedFile` 后执行：

```go
p.mergedTimeCol.Init()
p.mergedTimes = p.mergedTimes[:0]
p.unorderedSchemas = nil
p.ref = nil
p.sid = 0
p.noUnorderedSeries = false
p.noUnorderedColumn = false
```

该补丁在“finish、finalize、InitMergedFile 全部成功”的路径上可以阻止下一次再次 finish 旧 SID，因此能够修复 F-02 的直接症状。

但更稳健的设计是让 `finishCurrentSeriesOnce()` 自身在成功后清理 active-series 状态，从根源上保证 I-02，而不是由文件切换函数事后手工枚举字段。

## F-03：新 extent 继承旧 `StreamWriteFile` 状态

### 证据

[`StreamWriteFile.NewFile`](../engine/immutable/stream_downsample.go#L155) 调用 `c.reset()`。由于 `StreamWriteFile` 嵌入 `TableData`，该调用解析到 [`TableData.reset`](../engine/immutable/table.go#L66)，只清理：

- `trailerData`；
- `bloomFilter`；
- `metaIndexItems`；
- `inMemBlock`。

没有清理：

- `Trailer` 的 `idCount/minId/maxId/minTime/maxTime/...`；
- `IdTimePairs`；
- `chunkRows/maxChunkRows/fileSize`；
- `currentCMOffset`；
- `dstMeta`；
- pre-agg builder；
- 其他文件级状态。

完整 reset 的现有参考是 [`StreamIterators.reset`](../engine/immutable/stream_compact.go#L430)。

### 确定性错误 1：Trailer idCount 与真实 ChunkMeta 数不一致

```text
extent0 写了 100 个 SID：trailer.idCount = 100
extent1 写了 20 个 SID：trailer.idCount 累加成 120
extent1 实际 ChunkMeta 数：20
```

[`NewFileIterator`](../engine/immutable/file_iterator.go#L53) 将 `trailer.idCount` 直接作为 `chunkN`。读完 20 个真实 ChunkMeta 后，读取器仍认为还有 100 个，随后越过实际 meta block。

### 确定性错误 2：ChunkMeta offset 延续旧文件

`SwitchChunkMeta()` 会清空 `cmOffset`，但不会清空 `currentCMOffset`。假设 extent0 完成后：

```text
currentCMOffset = 8000
```

extent1 的第一个 ChunkMeta 应从 0 开始，却会记录为 8000，导致 offset 表错误。

### 其他后果

- extent1 的 IdTimePairs 包含 extent0 的 SID；
- average/max chunk rows 统计累计旧文件；
- stale `maxId` 可能拒绝下一文件写入；
- pre-agg、后续 merge 和文件重载可能使用错误元数据。

### 修复要求

增加明确的文件级 reset，例如：

```text
colBuilder.resetPreAgg()
trailer.reset()
mIndex.reset()
TableData.reset()
pair.Reset(name)
cmOffset = cmOffset[:0]
currentCMOffset = 0
dstMeta reset
chunkRows = 0
maxChunkRows = 0
fileSize = 0
rowCount clear
```

performer reset 不能替代 writer reset。

## F-04：最后切割产生空 extent 和 nil 文件

### 最小执行路径

```text
最后一个 SID 触发 Switch
→ finalize 当前非空文件
→ InitMergedFile 立即创建下一 extent
→ 新 extent 只有 16 字节 header
→ ColumnIterator 到 EOF
→ mergePerformer.Finish
→ finishSeries(MaxInt64) 没有任何数据可写
→ NewTSSPFile
→ Flush 因 DataSize <= 16 返回 errEmptyFile
→ NewTSSPFile 返回 (nil, nil)
→ Finish 调用 AppendMergedFile(nil)
```

[`TSSPFiles.Append`](../engine/immutable/tssp_reader.go#L228) 不过滤 nil，后续 `SumFilesSize` 会调用 `nil.FileSize()`。

### 为什么 Switch 内的 `if file == nil` 解决不了

进入 Switch 的正常前提是：

```text
writer.DataSize() >= 4 GiB
```

而 `NewTSSPFile` 只有在 `DataSize <= 16` 时才因空文件返回 nil。因此 Switch 内的 nil 分支在正常配置下基本不可达；真正需要处理 nil 的地方是 EOF `Finish()`。

### 修复要求

首选 lazy-open：finalize 后仅记录“下一个 extent 待创建”，直到下一 SID 真正开始写入时才创建文件。

如果保留 eager-open，则 `Finish()` 必须：

- 识别 nil；
- 不追加 nil；
- 关闭并删除空 `.tssp.init`；
- 保证清理 fd 和 writer。

## F-05：`writeRemain` 绕过切割

[`writeRemain`](../engine/immutable/merge_performer.go#L252) 在 SID 切换时只执行：

```text
WriteCurrentMeta
→ ChangeSid
→ 写下一 SID
```

没有执行大小检查。

EOF 的：

```text
finishSeries(MaxInt64)
→ writeRemain(MaxInt64)
```

可以一次性写完所有剩余 unordered-only SID。例如已有 3.9 GiB，再剩余 20 GiB unordered-only 数据，最终可以得到约 23.9 GiB 文件。

修复时必须在 `writeRemain` 每完成一个 SID 的 `WriteCurrentMeta` 后调用统一的 `maybeRotateAfterCompletedSeries()`，且不能递归调用当前 `finishSeries()`。

## F-06：extent 冲突后仍打开旧路径

[`NewFile`](../engine/immutable/stream_downsample.go#L167) 在冲突探测前保存：

```go
filePath := c.fileName.Path(dir, true)
```

[`TryNextExtentFile`](../engine/immutable/stream_downsample.go#L126) 修改 `c.fileName.extent` 后，`NewFile` 没有重新计算 `filePath`。

最小路径：

```text
filePath = ext1.init
ext1.init 已存在
→ TryNextExtentFile 将 fileName.extent 改为 2
→ ext2.init 不存在，返回 true
→ OpenFile(filePath)
→ 实际仍打开 ext1.init
```

最终：

```text
c.fileName 表示 ext2
fd 实际指向 ext1.init
```

热层还使用 `O_CREATE|O_RDWR`，没有 `O_EXCL` 或 truncate，可能直接复用并污染旧临时文件。

此外还存在：

- 只探测当前 generation 的 `.tssp.init`，没有同时排除正式 `.tssp`；
- `Stat → OpenFile` 不是原子预留；
- `uint16 extent++` 没有 `65535 → 0` 防护；
- 初始 extent 和已有冲突同样消耗编号空间。

修复不能只增加一次 `filePath = ...`，还需要一个原子、带溢出检查的 extent allocator。

## F-07：替换阶段可能删除未处理的旧 extent

[`mergeTool.execute`](../engine/immutable/merge_tool.go#L201) 会跳过“不与 unordered 相交且不是 last”的 ordered 文件。

[`replaceMergedFiles`](../engine/immutable/merge_out_of_order.go#L96) 却只根据：

```text
sequence 相同 && extent 相同
```

判断旧文件是否被替换，没有记录真实输入输出血缘。

### 确定性反例

```text
old ext0：参与 merge，并因 4 GiB 切成 new ext0 + new ext1
old ext1：不相交，因此 execute 跳过
old ext2：last，参与 merge
```

替换阶段看到 `new ext1` 后，会将 `old ext1` 认定为已替换并删除。但：

```text
new ext1 = old ext0 的后半部分
```

它不包含 `old ext1` 的数据，因此发生确定性数据丢失。

物理路径冲突探测无法避免该问题，因为新旧文件可以具有不同 merge generation；替换判断却忽略 generation。

### 修复要求

`execute` 必须显式返回真实的 `processedOldFiles` 或映射：

```text
processed old file → produced new files
```

替换时只删除实际参与 merge 的旧文件。额外 extent 还必须从当前 sequence 已保留的逻辑编号空间之外分配。

## F-08：同一个 SID 可以存在于多个 extent

“完整 SID 后切割”本身在理想状态下不会主动把当前 SID 切成两个合法 ChunkMeta：

- original 路径在 `WriteOriginal + WriteMeta` 后切割；
- merged 路径若先正确 finish，再切割，也处于完整 SID 边界。

但当前 merge 语义本身可以产生同 SID 多 extent。

### 确定性反例

```text
O0，non-last，ext0：
  ordered SID10 time = [1..5]

unordered：
  SID10 time = [10]

O1，last，ext1：
  ordered SID20
```

处理 O0：

1. [`HasSeries(10)`](../engine/immutable/unordered_reader.go#L411) 为 true；
2. non-last `SeriesChanged` 使用 `maxOrderTime=5`；
3. [`InitTimes(10,5)`](../engine/immutable/unordered_reader.go#L383) 不消费 time=10；
4. O0 输出 `SID10 [1..5]`；
5. 因 `lastFile=false`，`writeRemain` 不处理剩余 SID10。

处理 O1：

1. 遇到 original SID20 时，`SeriesChanged(20,nil)` 先调用 `finishSeries(20)`；
2. last-file `writeRemain(20)` 调用 [`ReadRemain`](../engine/immutable/unordered_reader.go#L260)；
3. SID10 `< 20`，以 `MaxInt64` 读取剩余 time=10；
4. O1 输出完整的第二个 SID10 ChunkMeta，然后输出 SID20。

最终：

```text
ext0: SID10 [1..5]
ext1: SID10 [10], SID20 [...]
```

这些片段通常是不重叠、按时间前后衔接的，因为 `UnorderedColumnReader` 保留已消费 offset。

另外，[`MsBuilder.WriteRecord`](../engine/immutable/msbuilder.go#L1075) slow path 会对同一个 ID 执行 `data.Split`，并在子 record 之间切文件。这进一步证明“一个 SID 只属于唯一 extent”不是当前存储层的全局不变量。

CC 校准：F-08 是被源码和反例证明的设计事实，本身不应标成独立的数据错误。只有其他实现（例如 extent 替换或 LIMIT 优化）错误依赖“SID 唯一 extent”时，它才转化为 correctness 风险。

## F-09：普通查询、pre-agg 与 LIMIT

### 仅 extent 间 SID 大小顺序不一致

这件事本身通常不会让“大 SID 以错误形式先返回”。查询不是按文件顺序扫描并直接输出 SID：

- [`AddLocationsWithInit`](../engine/tsm_merge_cursor.go#L99) 遍历所有文件，对指定 SID 执行 `Contains`；
- ordered locations 在 [`FirstTimeInit`](../engine/tsm_merge_cursor.go#L406) 中按该 SID 的时间范围排序；
- 跨 SID 输出由 [`heapCursor.Less`](../engine/tagset_cursor.go#L62) 按时间和 series key 归并；
- pre-agg 对收集到的 metadata 使用 `AggregateData` 继续归约。

因此在以下前提成立时，普通完整查询和 pre-agg 可以处理同 SID 多文件：

- 文件结构有效；
- 所有包含该 SID 的 location 都被枚举；
- ordered 片段的时间范围不重叠，或者查询路径具备真正的 k-way 时间归并；
- pre-agg metadata 正确。

当前同 SID 反例产生的是不重叠时间片，因此同 SID 多 extent 本身不等于立即返回错误。

### LIMIT 的条件性错误

[`AddLocationsWithLimit`](../engine/tsm_merge_cursor.go#L120) 的执行顺序是：

```text
按文件列表顺序枚举
→ 累计 row count
→ 达到 limit+offset 后 break
→ 之后只对已经选择的 locations 排序
```

如果同 SID 跨 extent，并且 extent/文件顺序与该 SID 的时间范围不一致，后面的文件可能包含真正更早的时间，但已经被提前排除。后续排序无法找回遗漏 location。

因此不能简单断言“带 ORDER BY time 的 LIMIT 会先全量排序再截断”；当前优化可能先裁文件，再对剩余位置排序。

安全选择有两个：

1. 查询侧先枚举该 SID 的所有 location，按 min/max time 排序后再累计 LIMIT；
2. 存储层建立并验证更强的不变量，例如同 SID 只属于一个 extent，或同 SID 的 extent 顺序严格对应时间范围。

当前代码不能采用第二个假设，因为 F-08 已给出反例。

CC 校准：F-09 必须保留条件——同一 SID 跨多个文件，并且文件遍历顺序与该 SID 的真实时间顺序不一致。没有这个前提时，不能泛化为所有 LIMIT 查询都会错误。

## F-10：单 SID 达到 4 GiB 时格式溢出

[`ChunkMeta`](../engine/immutable/tssp_file_meta.go#L368) 定义：

```go
size uint32
```

[`WriteMeta`](../engine/immutable/stream_downsample.go#L355) 直接执行：

```go
cm.size = uint32(c.writer.DataSize() - cm.offset)
```

如果一个 SID 的编码 data range 达到或超过 `2^32` 字节，该转换会回绕，ChunkMeta 指向错误范围。

```text
shard < 65535 × 4 GiB
```

只能限制 shard 总大小，不能推出：

```text
每个 SID < 4 GiB
```

如果坚持一个 SID 不跨文件，必须在写 ChunkMeta 前显式检查 SID encoded size `< 2^32`，并在超限时返回明确错误。否则必须设计 SID 内部跨文件格式。

## F-11：extent 数量与回绕

`extent` 类型为 `uint16`。当前直接执行 `extent++`，没有处理 65535 后回绕为 0。

即使 shard 总数据严格小于 `65535 × 4 GiB`，仍不能单独证明编号安全，因为：

- 初始 extent 可能不为 0；
- 当前 sequence 可能已有保留 extent；
- 冲突探测会继续消耗编号；
- 文件按 SID 边界软切割，编号布局并不只由总字节数决定；
- 同一个 sequence 的既有 extent 需要避免被新输出占用。

allocator 必须在递增前检查 `extent == math.MaxUint16`，并返回可诊断错误，不能静默回绕。

## 6. 对用户提出 Switch 补丁的精确评价

用户提出：

```go
file, err := p.sw.NewTSSPFile(true)
if err != nil {
    return err
}
if file == nil {
    return nil
}

p.AppendMergedFile(file)
if err := p.sw.InitMergedFile(file, initMergedFileOptions{
    addFileExt: true,
}); err != nil {
    return err
}

p.mergedTimeCol.Init()
p.mergedTimes = p.mergedTimes[:0]
p.unorderedSchemas = nil
p.ref = nil
p.sid = 0
p.noUnorderedSeries = false
p.noUnorderedColumn = false
```

逐项判断：

| 修改 | 判断 |
|---|---|
| `p.sid = 0` | 有效阻止成功切换后的旧 SID 二次 finish |
| 清空 merged time/schema/ref | 合理，但更适合封装进 `resetCurrentSeriesState()` |
| 重置 `noUnordered*` | 无明显副作用，下一 SID 也会重新赋值 |
| Switch 内 `file == nil` | 防御性代码本身无害，但没有覆盖真正的 EOF nil 路径 |
| reset 放在 `InitMergedFile` 后 | 成功路径可工作，但没有让 `finishSeries` 自身成为幂等状态转换 |
| 整体补丁 | 只能修复 F-02，不能修复 F-01、F-03 至 F-11 |

补丁不会清理 `UnorderedReader` 的剩余数据，因此不会因为 reset performer 字段而直接丢掉后续 remain；EOF 的 `writeRemain(MaxInt64)` 仍会从 `p.ur` 读取。不过该 remain 路径仍有 F-05 的不切割问题。

## 7. 建议的目标状态机

## 7.1 分离“完成当前 SID”与“写 remain”

当前 `finishSeries(boundarySid)` 同时负责：

1. 完成 active SID；
2. 写出 `< boundarySid` 的 unordered remain。

建议拆分：

```go
func (p *mergePerformer) finishCurrentSeriesOnce() error
func (p *mergePerformer) writeRemainBefore(boundarySID uint64) error
func (p *mergePerformer) maybeRotateAfterCompletedSeries() error
```

推荐状态：

```text
seriesOpen = true
→ 写所有字段
→ finishCurrentSeriesOnce
→ WriteCurrentMeta
→ seriesOpen = false, p.sid = 0
→ maybeRotateAfterCompletedSeries
```

再次调用 `finishCurrentSeriesOnce` 必须直接返回 nil。

## 7.2 Run 的目标流程

### original SID

```text
finish previous current SID（通常为空）
→ writeRemainBefore(currentSID)，每完成一个 remain SID 都检查大小
→ WriteOriginal(currentSID)
→ maybeRotateAfterCompletedSeries
```

### merged SID

```text
finish previous current SID
→ writeRemainBefore(currentSID)，每完成一个 remain SID 都检查大小
→ begin current SID
→ walkColumn
→ finishCurrentSeriesOnce
→ maybeRotateAfterCompletedSeries
```

### EOF

```text
finishCurrentSeriesOnce
→ maybeRotateAfterCompletedSeries（按是否仍有后续数据决定是否只 finalize）
→ writeRemainBefore(MaxInt64)，每个 SID 后检查
→ finalize 当前非空文件
→ 不创建无后续数据的空 extent
```

具体 EOF 顺序可以根据 SID 排序要求调整，但必须保证 remain SID 的写出顺序和每 SID 大小检查均成立。

## 7.3 文件切换职责

`maybeRotateAfterCompletedSeries()` 只应负责：

```text
断言没有 active SID
→ sw.Size() < limit：返回
→ finalize 非空文件
→ AppendMergedFile(non-nil)
→ 标记 nextWriterPending
```

下一次实际写 SID 时：

```text
allocate next extent
→ 原子创建路径
→ 完整 reset StreamWriteFile 文件级状态
→ 写 header
```

不再需要 `sid` 和 `forceFinish` 参数。

## 7.4 extent allocator

allocator 至少必须：

- 基于当前 sequence 的完整旧文件集合保留逻辑 extent；
- 同时考虑 `.tssp`、`.tssp.init` 和不同 merge generation 的逻辑占用；
- 在创建时使用等价于 `O_EXCL` 的原子语义；
- 创建成功后保证 `fileName` 与 fd 路径一致；
- 检查 `uint16` 溢出；
- 将实际分配结果返回，而不是只返回 bool。

## 7.5 替换血缘

建议 execute 返回：

```go
type MergeOutput struct {
    ProcessedOld []TSSPFile
    ProducedNew  []TSSPFile
}
```

或者更强的：

```text
old file A → new file A0, A1
old file B → new file B0
```

替换逻辑只删除 `ProcessedOld`，绝不从新文件 extent 猜测旧文件集合。

## 7.6 初始化 generation

第一次以旧 ordered 文件作为输入初始化 merge 输出时应设置：

```go
addMerge: true
```

同一次输出继续切 extent 时设置：

```go
addFileExt: true
addMerge: false
```

否则首次输出可能沿用旧 generation。

## 8. 必需测试矩阵

| 测试 | 输入/触发条件 | 必须验证 |
|---|---|---|
| T-01 | original SID 写完后刚好超过小阈值 | 在完整 SID 后切割，不拆 SID |
| T-02 | merged SID 仅在 time/meta 写入后超过阈值 | 当前 SID 完成后立即切，不额外写下一 SID |
| T-03 | merged SID 在普通字段阶段已经超限 | 只 finish 一次，新文件无旧 SID orphan data |
| T-04 | 最后一个 SID 触发切割，且没有 remain | 不追加 nil、不留下空 `.init`、不泄漏 fd |
| T-05 | EOF 有大量 unordered-only SID | 每完成一个 remain SID 后检查并可产生多个 extent |
| T-06 | 生成第二、第三 extent 后重新打开并遍历 | Trailer idCount、ChunkMeta offset、IdTimePairs 全部只属于当前文件 |
| T-07 | 目标 `.init` 已存在 | 实际 fd 与最终 `fileName` 指向同一新 extent，旧文件不被修改 |
| T-08 | 正式 `.tssp` 存在但 `.init` 不存在 | allocator 不分配逻辑冲突名称 |
| T-09 | old ext0 被切两份，old ext1 被 execute 跳过 | old ext1 不被替换或删除 |
| T-10 | non-last SID10 `[1..5]` + unordered SID10 `[10]` + last SID20 | 明确验证同 SID 多 extent 的预期策略 |
| T-11 | T-10 结果执行普通查询、pre-agg、LIMIT，包含升序和降序 | 全量查询正确；LIMIT 不因文件顺序遗漏位置 |
| T-12 | extent 接近 65535 | 返回明确溢出错误，不回绕到 0 |
| T-13 | 单 SID 编码大小接近/达到 `MaxUint32` | 小于边界正常；达到边界明确拒绝或按新格式处理 |
| T-14 | `InitMergedFile` 在切换后失败 | 已 finalize 临时文件和部分新文件均可清理，无 fd 泄漏 |
| T-15 | 首次输出与后续 extent 文件名 | 首次只增加 merge；后续保持 generation 并增加 extent |

测试应使用很小的可配置阈值模拟 4 GiB，避免依赖真实大文件。T-06、T-09、T-10 和 T-11 是最关键的端到端正确性测试。

## 9. 建议修复顺序

### P0：阻止数据损坏和丢失

1. 拆分并幂等化 active SID 完成逻辑；
2. 完整 reset `StreamWriteFile`；
3. 修复冲突后的真实创建路径并采用原子分配；
4. 替换流程改为显式 processed-old 血缘；
5. 处理 EOF 空文件和 nil。

### P1：满足文件大小语义

1. 完整 SID 后使用 `sw.Size()` 检查；
2. `writeRemain` 每 SID 检查；
3. 增加单 SID `uint32` size guard；
4. 增加 extent overflow guard。

### P2：确认查询语义

1. 明确是否允许同 SID 多 extent；
2. 若允许，修复或禁用不安全的 `AddLocationsWithLimit` 提前裁剪；
3. 增加 raw/pre-agg/first/last/LIMIT 的端到端测试；
4. 文档化 ordered locations 必须满足的时间范围不变量。

## 10. 最终回答

1. **注释版 `ColumnIterator.Run` 是否正确？** 迭代和 original SID 切割边界正确；merged 路径的大小检查位置错误，`writeRemain` 也未覆盖。
2. **`SwitchNewTSSP` 是否正确？** 当前不正确。用户提出的 performer reset 能修复成功切换后的 double-finish，但 writer reset、EOF、size timing、remain、extent allocator 和 replacement lineage 均未解决。
3. **同一个 SID 会不会存在多个 extent？** 会。主动的完整 SID 切割本身不拆当前 SID，但现有 non-last 时间上界消费、last-file `writeRemain` 和 MsBuilder slow path 都允许同 SID 多文件。
4. **extent 间 SID 不保序会不会直接导致普通查询/pre-agg 错？** 仅 SID 大小顺序不一致通常不会；查询按 SID 收集 location，跨 SID 输出按时间和 series key 归并。真正风险是无效文件、替换丢数据，以及同 SID 多 extent 时 LIMIT 在时间排序前提前裁剪。
5. **`65535 × 4G` 是否足够？** 不足。它不能证明单 SID 小于 `uint32`、extent 不回绕，也不能解决已有 extent 与逻辑冲突。

## 11. Claude Code 审核记录

审核工具：Claude Code 2.1.185（CC），只读审核，未授权修改工作区。

审核过程说明：直接让 CC 自主遍历整个仓库的两轮高/中强度审核均超过单次时限，没有形成可采纳报告。最终采用确定性证据包：从当前工作区提取每项结论所依赖的带行号源码和等价控制流，关闭 CC 工具权限，让 CC 仅对固定证据执行逻辑审核。以下只记录最终成功返回的审核，不把超时轮次视为审核结果。

### 11.1 审核矩阵

| 编号 | CC 判定 | CC 严重度 | 审核摘要 |
|---|---|---:|---|
| F-01 | CALIBRATE | High | 确认检查发生在 SID 完成前，会把越界推迟到下一 SID 或 EOF；若阈值只作为允许延迟的软提示，严重度可以降低。 |
| F-02 | ACCEPT | Critical | 确认 `p.sid/mergedTimeCol` 未清导致 double-finish；用户 reset 只修成功切换路径症状，未使完成操作幂等。 |
| F-03 | ACCEPT | Critical | 确认 `TableData.reset` 不覆盖 trailer、pair、chunk 统计、offset 和 builder 等文件级状态。 |
| F-04 | ACCEPT | High | 确认 EOF 空 writer 返回 nil，`Finish` 无条件 append，并存在 nil 解引用和空临时文件风险。 |
| F-05 | ACCEPT | High | 确认 `writeRemain(MaxInt64)` 可绕过全部切割检查。 |
| F-06 | ACCEPT | Critical | 确认 extent 变化后继续使用旧 `filePath`，并认可 TOCTOU 与 `uint16` 回绕风险。 |
| F-07 | ACCEPT | Critical | 确认基于 `(sequence, extent)` 反推替换集合可删除未参与 merge 的 old ext1。 |
| F-08 | CALIBRATE | Medium | 确认反例能产生同 SID 多 extent；校准为设计事实，而不是单独的数据错误。 |
| F-09 | CALIBRATE | High | 确认全量查询在给定前提下可正确，也确认 LIMIT 在文件顺序不等于时间顺序时存在条件性遗漏。 |
| F-10 | ACCEPT | Critical | 确认单 SID data range 达到 `2^32` 时 `uint32` 转换回绕。 |
| F-11 | ACCEPT | High | 确认 `uint16 extent++` 没有溢出保护，总大小上限不能排除回绕。 |

统计：`ACCEPT=8`、`CALIBRATE=3`、`REJECT=0`。

### 11.2 校准意见的采纳情况

1. **采纳 F-01 校准。** 文档明确区分“阈值语义错误”和“直接数据损坏”。由于本需求明确要求每个完整 SID 后检查，仍保留 High。
2. **采纳 F-08 校准。** 将其明确标为设计事实；只有其他路径依赖错误唯一性假设时才形成风险。
3. **采纳 F-09 校准。** 保留同 SID 多文件且文件顺序与时间顺序不一致这一必要触发条件。
4. **无 REJECT。** F-02 至 F-07、F-10、F-11 的控制流、后果和严重度均被 CC 接受。
5. **目标设计通过方向审核。** CC 认可拆分 SID 完成、remain 写出、文件轮转，采用 lazy-open、完整 writer reset、原子 extent allocator、显式替换血缘，以及允许同 SID 多 extent 时先按时间排序再做 LIMIT。

CC 在最终固定证据范围内没有报告新的独立 correctness 问题。最终实现仍必须通过第 8 节测试矩阵，尤其是 T-06、T-09、T-10 和 T-11；静态审核不能替代这些端到端验证。

## 12. Claude Code 最小改动方案（供 Codex 复核）

本节由 CC 在第 11 节复核基础上追加，提出一个**不采用第 7 节状态机重构**的最小改动方案，仅就正确性阻塞项给出局部修复，并说明哪些发现可暂缓。请 Codex 逐条 `ACCEPT / REJECT / CALIBRATE`，重点复核 12.3 的"同根合并"主张。

### 12.1 立场

- 第 11 节的 8 ACCEPT / 3 CALIBRATE / 0 REJECT 结论 CC 完全认可，本节不重审。
- 第 7 节的状态机重构（`finishCurrentSeriesOnce` / `writeRemainBefore` / `maybeRotate` 分离 + lazy-open + 显式血缘）是更干净的长期设计，但**不是达成正确性的必要条件**。本节论证：4 处局部修改即可消除全部 Critical/High 的确定性问题。
- F-03（writer reset 不完整）和 F-07（替换误删被跳过的 old extent）是 CC 在前几轮自主分析中遗漏、由本文档补上的两项关键发现，CC 予以确认。

### 12.2 最小必修集（4 处局部改动）

#### 改动 A：`StreamWriteFile` 增加完整 `reset()`（修 F-03）

`NewFile`（`stream_downsample.go:159`）当前调用的 `c.reset()` 解析到 `TableData.reset`（`table.go:66-73`），只清 4 项。新增 `StreamWriteFile.reset()` 覆盖，对齐 `StreamIterators.reset`（`stream_compact.go:430-438`）的清单位：

```go
func (c *StreamWriteFile) reset() {
    c.colBuilder.resetPreAgg()
    c.trailer.reset()
    c.mIndex.reset()
    c.TableData.reset()          // 保留原有 4 项清理
    c.pair.Reset(c.name)
    c.cmOffset = c.cmOffset[:0]
    c.currentCMOffset = 0
    c.dstMeta.sid = 0
    c.chunkRows = 0
    c.keys = make(map[uint64]struct{}, c.maxN)
}
```

`NewFile` 内 `c.reset()` 调用不变（方法解析自动走到新实现）。**不改动任何调用方结构**，仅新增一个方法 + 让 `NewFile`/`InitMergedFile` 路径上的 `c.reset()` 走完整清理。

#### 改动 B：`TryNextExtentFile` 同时排除 `.tssp.init` 与 `.tssp` + `NewFile` 重算 `filePath`（修 F-06，**同时修 F-07**）

详见 12.3。代码层面：

```go
// TryNextExtentFile 选 extent 时同时 stat 两种后缀
for retry := 0; retry < maxRetries; retry++ {
    c.fileName.extent++
    if exists(c.fileName.GetFilePath(dir, true))  { continue } // .tssp.init 撞新文件
    if exists(c.fileName.GetFilePath(dir, false)) { continue } // .tssp    撞 old 文件 ← 新增
    return true
}
```

```go
// NewFile 在 TryNextExtentFile 成功后重算 filePath
if !c.TryNextExtentFile(dir) { return errFileExist }
filePath = c.fileName.Path(dir, true)   // ← 重算，修 F-06 的"仍打开旧路径"
```

#### 改动 C：`SwitchNewTSSP` 重排——先完成 SID 再检查大小（修 F-01 + F-02）

把"检查大小 → finishSeries"反转为"finishSeries → 检查大小"，使大小判断基于完整 SID 数据，并使 finish 后立即清 `p.sid` 防双重 finish：

```go
func (p *mergePerformer) SwitchNewTSSP(sid uint64, forceFinish bool) error {
    if p.sw == nil || p.sw.writer == nil { return nil }
    if forceFinish {
        if err := p.finishSeries(sid); err != nil { return err }
        p.sid = 0                       // F-02：标记已 finish
        p.mergedTimeCol.Init()
        p.mergedTimes = p.mergedTimes[:0]
        p.unorderedSchemas = nil
        p.ref = nil
    }
    if p.sw.writer.DataSize() < limit { return nil }   // F-01：用完整 DataSize 判断
    file, err := p.sw.NewTSSPFile(true)
    if err != nil { return err }
    if file == nil { return nil }       // F-04 防御（EOF 真正 nil 见改动 D）
    p.AppendMergedFile(file)
    return p.sw.InitMergedFile(file, initMergedFileOptions{addFileExt: true})
}
```

仅为顺序调整 + 字段清理，不引入新函数、不改变 `Run` 调用结构。

#### 改动 D：`Finish` 加 nil 检查（修 F-04）

`merge_performer.go:190-195`：

```go
file, err := p.sw.NewTSSPFile(true)
if err != nil { return err }
if file != nil { p.AppendMergedFile(file) }   // ← nil 不追加，避免空 extent 与后续 nil 解引用
```

### 12.3 关键简化：F-06 与 F-07 同根，可合并修复

这是本方案与第 7 节的最大分歧点，请 Codex 重点复核。

**主张**：F-07（替换误删被 execute 跳过的 old extent）的数据丢失路径，依赖"切文件产生的新 extent 编号撞上被跳过的 old extent 编号"。若 `TryNextExtentFile` 在选 extent 时**同时排除磁盘上的 old `.tssp` 文件**（改动 B），则切文件永远不会占用任何 old extent 编号，`replaceMergedFiles` 的 `(sequence, extent)` 匹配就不会把被跳过的 old 文件误删。

**推理链**：

1. `mergeTool.execute` 对"不相交且非 last"的 old 文件 `continue` 跳过（`merge_tool.go:209`），不产出 new 文件，但这些 old 文件仍在 `order.Files()` 中、仍传给 `replaceMergedFiles` 作 `old`（`merge_tool.go:165`）。
2. performer 的第一个 new 文件继承 old extent（`InitMergedFile` 首次 `addFileExt=false`，`stream_downsample.go:260`），与该 old 文件同 extent——这是预期行为，old 文件本就要被删除替换，不构成碰撞。
3. 切文件产生的新 extent 走 `NewFile(true)` → `extent++` → `TryNextExtentFile`。若 `TryNextExtentFile` 只 stat `.tssp.init`（现状），它看不见 old `.tssp`，可能选到与被跳过 old 文件相同的 extent → `replaceMergedFiles` 误删 old → 数据丢失（F-07）。
4. 若 `TryNextExtentFile` 同时 stat `.tssp`（改动 B），切文件选 extent 时会跳过所有 old `.tssp` 占用的编号 → 切文件新 extent 一定落在"所有 old extent 编号之外"→ `replaceMergedFiles` 在 `old` 列表中找不到与新切 extent 匹配的 old（被跳过的 old extent 仍由其自身独占，无 new 对应）→ **被跳过的 old 文件不会被加入 `needReplaced`，不会被删除**。

**结论**：改动 B 一处同时消除 F-06（filePath 不重算 + 不排除 old .tssp）和 F-07（误删被跳过 old extent）。**不需要**第 7.5 节的 `MergeOutput{ProcessedOld, ProducedNew}` 显式血缘改造，也**不需要**改 `replaceMergedFiles` 的匹配逻辑。

**请 Codex 复核**：
- (Q1) 上述推理链是否成立？特别是第 4 步："切文件新 extent 落在所有 old extent 编号之外"是否在所有 performer 交错执行顺序下都成立？
- (Q2) performer 第一个文件继承 old extent 时不经 `TryNextExtentFile`（仅当 `.tssp.init` 碰撞才进），是否可能因改动 B 被错误地推离继承 extent，从而破坏"每个被处理的 old extent=N 都有 new extent=N"的匹配前提？
- (Q3) 是否存在 `.tssp.init` 与 `.tssp` 之外的第三种路径形态（如不同 merge generation 的共存）使改动 B 仍然漏判？

### 12.4 可暂缓项与理由

| 发现 | 暂缓理由 |
|---|---|
| F-05（`writeRemain` 不切割） | 仅在 lastFile 且有大量 unordered-only 数据时使文件偏大，属"超限"非"损坏"。可在 `writeRemain` 回调中加 `maybeRotate`，但非正确性阻塞。 |
| F-08（同 SID 多 extent） | 现有 merge 语义的设计事实（non-last `maxOrderTime` + last `writeRemain` + `MsBuilder` slow path），并非切割功能引入。本次"SID 边界切割"不拆当前 SID，已满足设计目标。 |
| F-09（LIMIT 提前裁剪） | 依赖 F-08 + 文件顺序与时间顺序不一致的窄条件；正常 ascending 查询不受影响。属既存行为治理，不归本次切割功能。 |
| F-10（`uint32` 单 SID ≥4GiB） | 极端边界。建议顺手加 `if size >= math.MaxUint32 { return err }` 一行 guard，成本极低。 |
| F-11（`uint16` extent 回绕） | 需 65535 次切割。建议顺手加 `if c.fileName.extent == math.MaxUint16 { return err }` 一行 guard。 |

F-10/F-11 的 guard 建议纳入最小集（各一行），但即便不加也不阻塞正确性，仅是边界防护。

### 12.5 最小方案与第 7 节重构的取舍

| 维度 | 最小方案（12.2 四处） | 第 7 节状态机重构 |
|---|---|---|
| 正确性（确定性损坏/数据丢失） | 全部修复 | 全部修复 |
| F-05（writeRemain 超限） | 暂缓 | 一并修 |
| F-08/F-09（同 SID 多 extent） | 不修（既存语义） | 需另外决策 |
| 改动量 | 4 处局部，不动调用结构 | 拆 `finishSeries`、重写 `Run`、lazy-open、显式血缘 |
| 回归风险 | 小 | 大 |
| 长期可维护性 | 一般（`SwitchNewTSSP` 仍耦合 finish 与切文件） | 好 |

### 12.6 请 Codex 复核清单

1. 对 12.2 的改动 A/B/C/D 逐条 `ACCEPT / REJECT / CALIBRATE`。
2. 对 12.3 的"改动 B 同时修 F-06 与 F-07"主张给出明确判定，回答 Q1/Q2/Q3。
3. 对 12.4 的暂缓项判定是否合理；若某项实际为正确性阻塞，请指出。
4. 判定最小方案是否满足第 3 节的不变量 I-01 至 I-08；若不满足，指出哪条不变量在最小方案下仍违反，以及是否可接受。
5. 若最小方案可接受，指出仍需补充的测试用例（对齐第 8 节矩阵）。

CC 立场：最小方案旨在以最低风险让切割功能安全跑起来；第 7 节重构更适合作为后续迭代。请 Codex 裁决是否同意以此作为合入基线。

## 13. Codex 对第 12 节最小方案的复核意见（供 CC 审核）

本节仅复核第 12 节提出的最小改动方案，不修改 CC 的原始提案。复核基于当前分支的静态控制流和文件名/替换语义；按照前述约定，不把当前尚未收口的编译错误作为方案否决依据。

### 13.1 总体结论

**不同意将第 12 节的四处局部改动直接作为合入基线。**

改动 A 和 C 的方向正确，但需要校准；改动 D 只避免 nil 进入结果集，没有完成空文件清理；改动 B 的核心论证不成立，不能修复 F-07。因而“4 处局部修改即可消除全部 Critical/High 确定性问题”的结论不成立。

| 改动 | Codex 判定 | 结论摘要 |
|---|---|---|
| A：增加 StreamWriteFile.reset | CALIBRATE | 核心文件级状态覆盖方向正确，但清单仍不完整 |
| B：同时检查 .tssp.init/.tssp 并重算路径 | REJECT（针对同时修复 F-07 的主张） | 可部分修 F-06，但只检查当前 merge generation，仍会误删被跳过的 old extent |
| C：先 finishSeries 再检查大小 | CALIBRATE | 顺序正确且可修当前 double-finish，但应使用 sw.Size()，并且仍未覆盖 writeRemain |
| D：Finish 对 nil 做保护 | CALIBRATE | 可阻止 nil append，但空 .init、writer 和 fd 仍会泄漏 |

### 13.2 对改动 B 的确定性反例

#### 13.2.1 物理文件名包含 merge generation

[TSSPFileName.String](../engine/immutable/tssp_file_name.go#L59) 的格式为：

~~~go
fmt.Sprintf("%08x-%04x-%04x%04x",
    n.seq, n.level, n.merge, n.extent)
~~~

也就是说，物理文件名至少由 sequence、level、merge 和 extent 共同决定。GetFilePath(dir, true/false) 只是在这个完整 basename 后选择 .tssp.init 或 .tssp 后缀。

而 [replaceMergedFiles](../engine/immutable/merge_out_of_order.go#L96) 选择 old 文件时只比较：

~~~go
s1 == s2 && of.FileNameExtend() == nf.FileNameExtend()
~~~

它忽略 merge generation，也忽略 level。allocator 的“物理路径等价关系”和 replacement 的“逻辑匹配等价关系”并不相同。

#### 13.2.2 单线程即可复现 F-07

假设同一 sequence 下存在：

~~~text
old A: sequence=S, merge=7, extent=0   // 与 unordered 相交，被 execute 处理
old B: sequence=S, merge=7, extent=1   // 不相交且非 last，被 execute 跳过
~~~

处理 old A 后，第一个输出继承 extent=0 并增加 generation；超过阈值后产生第二个输出：

~~~text
new A0: sequence=S, merge=8, extent=0
new A1: sequence=S, merge=8, extent=1
~~~

改动 B 在选择 new A1 时检查的是：

~~~text
S / merge=8 / extent=1.tssp.init
S / merge=8 / extent=1.tssp
~~~

但被跳过的 old B 是：

~~~text
S / merge=7 / extent=1.tssp
~~~

因此两次 Stat 都不能看到 old B，new A1 仍会使用 extent=1。随后 replaceMergedFiles 按 (sequence, extent) 匹配，将 old B 误加入 needReplaced 并删除。

该反例不依赖多个 performer 并发或交错；一次 mergeTool.execute 的顺序循环即可发生。当前 execute 确实会跳过不相交且非 last 的 old 文件，但随后仍把全部 order.Files() 传给 replaceMergedFiles。

#### 13.2.3 对第 12.3 节 Q1/Q2/Q3 的回答

**Q1：切割文件一定落在所有 old extent 之外吗？**

否。按当前提案，它只会落在“当前 merge generation 下未发现同名物理文件”的 extent，不能推出该逻辑 extent 没有被上一 generation 的 old 文件使用。即使本次 execute 内部没有并发，该结论仍不成立。

**Q2：首输出碰撞后能否直接 extent++？**

不能。首输出必须保持：

~~~text
processed old extent=N
→ first produced new extent=N
~~~

这是当前 replaceMergedFiles 能识别 processed old 的前提。如果首目标 .init 或同 generation 的正式文件碰撞后直接换到 N+1：

1. 若 N+1 匹配另一个 old，可能删除错误 old，并保留已经被重写的 old N，造成重复或错乱；
2. 若 N+1 不匹配任何 old，needReplaced 可能为空；[ReplaceFiles](../engine/immutable/mms_tables.go#L730) 会直接返回 nil，mergeTool 随后仍可能删除 unordered 文件，而新输出没有被正式安装。

因此首输出的目标冲突必须恢复或报错，不能静默改变 extent。只有“首输出之后的额外切割文件”才适合进入 extent allocator。

另外，当前 NewFile 只在最初计算出的 .init 已存在时才调用 TryNextExtentFile。仅修改 TryNextExtentFile 内部并不能处理“同 generation 的正式 .tssp 存在、但 .init 不存在”的首目标碰撞。

**Q3：是否存在 .init/.tssp 之外的遗漏维度？**

是。merge generation 不是第三种后缀，但它是 basename 的独立命名维度；同一个逻辑 (sequence, extent) 可以对应不同 generation 的多个物理路径。level 也是物理文件名的一部分，而当前 replacement 同样不比较 level。

#### 13.2.4 对 F-06 的局部评价

改动 B 中“TryNextExtentFile 修改 fileName 后重新计算 filePath”是必要修改，可以修复继续打开旧路径的问题。

但完整的 I-05 还要求原子拒绝冲突。当前 warm 路径使用 O_CREATE|O_RDWR，Stat 与 OpenFile 之间仍有 TOCTOU 窗口，也没有 O_EXCL 语义。若并发创建或恢复残留发生在两者之间，仍可能打开已有文件。因此路径重算是必要条件，不是完整修复。

### 13.3 对改动 A 的复核

CC 列出的以下状态都应在新 extent 中重置：

- ColumnBuilder pre-agg；
- Trailer；
- MetaIndex；
- TableData；
- IdTimePairs；
- cmOffset/currentCMOffset；
- chunkRows；
- keys；
- 当前 ChunkMeta 的 active SID。

但提案中的 reset 仍至少缺少：

1. maxChunkRows = 0。[NewTSSPFile](../engine/immutable/stream_downsample.go#L203) 会把它写入新 reader；不重置会继承上一文件的行数统计。
2. 完整清理 dstMeta，而不只是 dstMeta.sid = 0。至少应清理 offset、size、columnCount、segCount、column metadata 和 timeRange，或者保证下一条路径必然在任何读取前完整 Init。
3. 清理 rowCount。虽然它只在 enableValidate 下使用，但“完整文件级 reset”不应依赖验证开关。
4. 建议 fileSize = 0，保持文件级状态的一致性；它在非空 Flush 后会被重新赋值，但提前清零更安全。

keys 已在 NewFile 末尾重新 make，一处负责即可，避免重复分配。

因此 A 可以作为实现基础，但第 12.2 节给出的函数不能称为完整 reset，尚不能无条件证明 I-04。

### 13.4 对改动 C 的复核

以下时序是正确方向：

~~~text
finishSeries
→ 立即清理 active-series 状态
→ 检查完整 SID 写入后的文件大小
→ 必要时 finalize
~~~

其中“立即清理”必须发生在 size < limit 的提前返回之前。否则不切文件时，下一次 SeriesChanged 或 EOF 仍会再次完成旧 SID。第 12 节的新伪代码满足这一点，比当前工作区中“仅在成功切文件后 reset”的版本正确。

但仍需校准两点：

1. 若阈值语义是最终 TSSP 文件大小，应使用 [StreamWriteFile.Size](../engine/immutable/stream_downsample.go#L620)，而不是 writer.DataSize()。DataSize 不包括 ChunkMeta buffer、MetaIndex、bloom filter、IdTimePairs、trailer/footer 等最终开销。
2. finishSeries(sid) 还会调用 writeRemain(sid)。它可能连续写出多个 unordered-only SID，而 C 只在整个 finishSeries 返回后检查一次，仍不满足“每条时间线完成后检查阈值”。

C 可以修复当前 merged SID 的检查时机和 double-finish 症状，但不能单独完成 F-01/F-02/F-05 的统一状态语义。

### 13.5 对改动 D 的复核

[StreamWriteFile.NewTSSPFile](../engine/immutable/stream_downsample.go#L203) 在 Flush 返回 errEmptyFile 时直接返回 nil, nil。此时已经创建并写入 header 的 .init 仍可能存在，而且 writer/fd 没有在该分支关闭。

在 Finish 中增加：

~~~go
if file != nil {
    p.AppendMergedFile(file)
}
~~~

只能避免 nil 进入 mergedFiles，不能避免：

- 空 .init 文件残留；
- writer/fd 泄漏；
- 后续同名分配冲突。

建议在 NewTSSPFile 的 errEmptyFile 分支集中调用 removeEmptyFile/Close(true)，或者采用第 7 节的 lazy-open。若只在 Finish 中处理，至少在 file == nil 时显式关闭并删除当前空 writer。

因此 D 是必要的防御，但不是 F-04 的完整修复。

### 13.6 对暂缓项的判定

| 项目 | Codex 判定 | 理由 |
|---|---|---|
| F-05：writeRemain 不切割 | 不同意暂缓 | 直接违反用户要求及 I-03；多个 unordered-only SID 可让文件远超 4 GiB |
| F-08：同 SID 多 extent | 可作为设计事实暂缓修改 | 但必须明确查询侧承担全部枚举、排序和归并责任 |
| F-09：LIMIT 提前裁剪 | 不同意作为安全基线暂缓 | AddLocationsWithLimit 在按时间排序 location 前按文件顺序凑够行数即 break，可确定性遗漏更早或更晚的数据 |
| F-10：单 SID uint32 回绕 | 不同意暂缓 | ChunkMeta.size 强转 uint32；单 SID 达到 2^32 字节会产生不可表达的 metadata |
| F-11：extent uint16 回绕 | 不同意暂缓 | 分片总大小约束不能排除临时 old/new 共存、稀疏冲突、非零起始 extent 和重试探测导致的编号耗尽 |

F-10 的 guard 应基于转换前的 int64/uint64 大小；如果 MaxUint32 本身允许表达，通常应判断 size > math.MaxUint32，而不是无条件拒绝 size == math.MaxUint32。若文件格式另有保留值，再按格式约束收紧。

除 extent 外，还建议对 InitMergedFile 中的 merge++ 增加显式溢出/保留值保护，禁止 generation 静默回绕到旧命名空间。

### 13.7 查询与 pre-agg 的校准结论

对于 raw 全量查询和一般 sum/count/min/max 聚合，只有在“所有匹配 location 均被枚举”且 ordered location 的时间范围满足查询侧假设时，跨 extent 的物理文件顺序才可以被排序/聚合消化。

仅知道“每个文件内部的时间线有序”还不能推出：

- 多个 extent 的同 SID 时间范围互不重叠；
- 按 extent/sequence 的文件顺序等于按时间顺序；
- 按 minTime 排序后顺序读取一定得到全局有序输出。

[AddLocationsWithLimit](../engine/tsm_merge_cursor.go#L120) 会在文件遍历阶段根据 limit+offset 提前 break，之后才由 LocationCursor 排序已选中的 location。因此被省略的 location 不可能在后续 mergeData 中恢复。

另外，[LocationCursor.Less](../engine/immutable/location_cursor.go#L62) 对 ordered location 只按 minTime 排序，而 onlyFirstOrLast 路径会在读取一个 location 后停止。若同 SID 多 extent 的时间范围可能重叠，first/last，尤其 last，仍需独立证明或增加回归测试；不能仅依赖“一文件内有序”。

### 13.8 I-01 至 I-08 复核矩阵

| 不变量 | 最小方案结果 | 说明 |
|---|---|---|
| I-01：只在完整 SID 边界 finalize | 基本满足 | C 在完成 merged SID 后再判断；original 路径调用 Switch 时也已完成 |
| I-02：完成 SID 幂等 | 条件满足 | C 可消除当前 Run 成功路径的 double-finish，但状态清理仍分散在调用者 |
| I-03：每个完整 SID 后统一检查 | 不满足 | writeRemain 中的 unordered-only SID 未逐个检查 |
| I-04：新 extent 文件级状态从零开始 | 不完全满足 | A 遗漏 maxChunkRows、完整 dstMeta、rowCount 等 |
| I-05：逻辑文件名与 fd 一致且原子拒绝冲突 | 不满足 | 路径重算可修一半，但 Stat/Open 仍非原子，正式文件检查触发也不完整 |
| I-06：替换依据真实血缘 | 不满足 | 仍通过 new 的 (sequence, extent) 反推 old，与 I-06 的文字定义直接冲突 |
| I-07：同 SID 多 extent 查询完整归并 | 不满足 | LIMIT 可在时间排序前丢弃 location；first/last 还依赖额外时间范围不变量 |
| I-08：单 SID 大小可被格式表达 | 不满足 | 没有 uint32 转换前 guard |

因此，第 12.5 节“正确性问题全部修复”的表述与第 3 节自身定义的 I-03、I-05、I-06、I-07、I-08 不一致。

### 13.9 建议的最小可接受合入集

不要求必须一次完成第 7 节的完整状态机重构，但至少需要以下内容：

1. 补全 StreamWriteFile 文件级 reset。
2. merged SID 采用 finish → clear state → sw.Size() → rotate 的时序。
3. writeRemain 在每个 SID 的 WriteCurrentMeta 成功后执行同一大小检查。
4. errEmptyFile 路径关闭 writer/fd 并删除空 .init；或者采用 lazy-open。
5. 首输出固定继承 processed old extent；首目标冲突只能恢复或失败。
6. 额外切割文件的 allocator 按逻辑 (sequence, extent) 跨所有 generation 保留编号，并使用原子创建；不能只 Stat 当前完整文件名。
7. mergeTool.execute 显式收集 processedOld，replacement 直接使用 processedOld 和 producedNew，不再从新文件名反推旧文件血缘。该改动可以只扩展 execute 的返回值，不要求同时完成全部状态机重构。
8. 增加单 SID uint32、extent 和 merge generation 的溢出保护。
9. 若正式支持同 SID 多 extent，修复或禁用不安全的 LIMIT 提前裁剪，并验证 raw/pre-agg/first/last 的时间范围假设。

即使实现了“跨 generation 保留逻辑 extent”，显式 processedOld 仍然更稳健：allocator 负责命名唯一性，processedOld 负责删除正确性，两者不应互相代替。

### 13.10 建议 CC 重点复核的问题

请 CC 对以下问题给出逐项 ACCEPT/REJECT，并提供相应代码证据：

1. 是否同意 TSSPFileName.String 包含 merge，而 TryNextExtentFile 只检查当前 generation，因此第 13.2.2 节反例成立？
2. 若不同意，当前方案中哪段代码能够枚举并排除上一 generation 的相同逻辑 (sequence, extent)？
3. 是否同意首输出碰撞后 extent++ 会破坏当前 replacement 的 processed-old 匹配前提？
4. 是否同意只做 Finish nil guard 仍会留下 header-only .init 和 fd？
5. 在不修改 AddLocationsWithLimit 的情况下，能否给出代码不变量，证明同 SID 多 extent 时被提前省略的 location 不可能包含更优先的时间？
6. 是否存在明确且被运行时强制执行的上游约束，保证单 SID 编码 data range 严格小于 2^32？若没有，F-10 应视为正确性阻塞。
7. 若 CC 仍主张不需要 processedOld 显式血缘，请给出对“old M/ext1 被跳过、new M+1/ext1 由切割产生”的逐步替换结果证明。

Codex 最终立场：第 12 节可以保留为“降低改动量的讨论草案”，但在上述问题收口前，不应标记为安全合入基线。

## 14. CC 对 Codex 第 13 节的最终回应（1+1 复核）

本节由 CC 对 Codex 第 13 节做 1+1 复核：派发两个证据 agent 分别验证 F-07 反例与 A/C/D/暂缓项评判，再由一个审核 agent 做终裁与分歧解决。结论：**Codex 第 13 节基本全部正确，CC 第 12 节有两处根本性错误，确认撤回原最小方案作为合入基线的主张，改为下述修订集。**

### 14.1 CC 第 12 节两处错误确认

#### 错误 A：12.3“stat .tssp 同时修 F-07”不成立

CC 12.3 推理链第 4 步假设“`TryNextExtentFile` 同时 stat `.tssp` 能看到所有 old 文件”，因此切文件新 extent 落在所有 old extent 之外。该假设错误，根因是**物理文件名包含 merge generation**：

- [`TSSPFileName.String`](../engine/immutable/tssp_file_name.go#L59) 格式 `%08x-%04x-%04x%04x` = seq-level-**merge**-extent，merge 嵌在 basename 中。
- 切文件产生的新文件 `merge` 递增（如 `merge=8`），被 `execute` 跳过的 old 文件是上一 generation（如 `merge=7`），两者物理路径不同。
- `TryNextExtentFile`（[`stream_downsample.go:126-153`](../engine/immutable/stream_downsample.go#L126)）stat 的是当前 `c.fileName`（`merge=8`）的 `.tssp`/`.tssp.init`，**看不见 `merge=7` 的 old 文件**。
- [`replaceMergedFiles`](../engine/immutable/merge_out_of_order.go#L100) 匹配条件 `s1==s2 && FileNameExtend()==FileNameExtend()` 只比 (seq, extent)，**不比 merge**——于是 old B（`merge=7/extent=1`）误匹配 new A1（`merge=8/extent=1`）并被删除。

CC 把“当前 generation 无物理冲突”等同于“所有 generation 无逻辑冲突”，而物理路径等价与逻辑匹配等价是两个不同的关系。Codex 13.2 反例成立，CC 12.3 撤回。F-07 的正确最小修复是 Codex 13.9 第 7 项的 `processedOld` 显式血缘，不是 stat 两种后缀。

#### 错误 B：12.4 暂缓 F-05/F-10 错误，两者是正确性阻塞

- **F-05**：[`writeRemain`](../engine/immutable/merge_performer.go#L252) 在 `lastFile` 路径循环写出多个 unordered-only SID，无大小检查；单 SID 累积可超 4GiB 直接触发 F-10。CC 称“属超限非损坏”被源码证伪。
- **F-10**：[`ChunkMeta.size`](../engine/immutable/tssp_file_meta.go#L371) 是 `uint32`，[`WriteMeta`](../engine/immutable/stream_downsample.go#L355) `cm.size = uint32(...)` 在单 SID data range ≥ 2^32 时静默回绕，metadata 表达错误 size → 读取按错误 size 解析 → 数据损坏。`fileSizeLimit` 默认 8GiB > 4GiB，配置即可触发，非极端边界。

F-05 不切割是 F-10 的直接触发路径，两者形成 **uint32 截断链**。CC 12.4 把两者归为“非正确性阻塞”是两处独立错误判断，撤回。

### 14.2 F-09 代码存在性分歧解决

复核中一个证据 agent 称 `AddLocationsWithLimit`/`tsm_merge_cursor.go` 在当前代码库不存在，F-09 无法验证。审核 agent 亲自核对确认**该 agent 搜索误判，代码存在**：

- [`engine/tsm_merge_cursor.go`](../engine/tsm_merge_cursor.go) 存在（472 行）。
- `AddLocationsWithLimit` 定义于 `:120`，调用于 `:218`。
- `:135-178` 确认“按文件顺序累计 row → 达 `limit+offset` break → break 后才由 `LocationCursor.Less` 排序”的“先裁后排”行为，被省略 location 不进 cursor、后续 `mergeData` 无法恢复。

因此 Codex 13.6/13.7 对 F-09“不同意暂缓”有充分源码依据，F-09 必须纳入修订集。

### 14.3 对 Codex 第 13 节各评判的终裁

| Codex 评判 | CC 终裁 | 说明 |
|---|---|---|
| 13.1 不同意最小方案作合入基线 | ACCEPT | 12.3 被击穿，F-05/F-09/F-10 均阻塞 |
| 13.2 F-07 反例（merge 在文件名） | ACCEPT | 三重源码确认（`tssp_file_name.go:59`、`merge_out_of_order.go:100-103`、`merge_tool.go:292`） |
| 13.2.3 Q1/Q2/Q3 | ACCEPT | 三问答全对 |
| 13.3 改动 A 的 4 项补充 | ACCEPT | `maxChunkRows`/完整 `dstMeta`/`rowCount`/`fileSize` 全部成立 |
| 13.4 改动 C 两点校准 | ACCEPT | `sw.Size()` 含完整开销；`writeRemain` 多 SID 单次检查不够 |
| 13.5 改动 D nil guard 不够 | ACCEPT | errEmptyFile 分支不关 fd、不删空 `.init` |
| 13.6 F-05 不同意暂缓 | ACCEPT | uint32 截断链，I-03/I-08 双违反 |
| 13.6 F-09 不同意暂缓 | ACCEPT | 代码存在，“先裁后排”确认 |
| 13.6 F-10 不同意暂缓 | ACCEPT | I-08 直接违反，配置可达 |
| 13.6 F-11 不同意暂缓 | CALIBRATE | 回绕导致文件名冲突报错（失败安全），非静默损坏；可短暂暂缓但建议顺手加 `if extent==MaxUint16 return err` |
| 13.8 I-03/I-05/I-06/I-07/I-08 不满足 | ACCEPT | 五条全对（含 I-07，因 F-09 有源码依据） |
| 13.9 最小可接受合入集 9 项 | ACCEPT | 正确修订方向 |

统计：`ACCEPT=11`、`CALIBRATE=1`（仅 F-11）、`REJECT=0`。

### 14.4 修订后的最小可接受合入集

规模介于 CC 原 4 处与 Codex 第 7 节完整状态机重构之间。

#### 保留（CC 原改动，校准后）

1. **改动 A（校准）—— `StreamWriteFile.reset()` 完整重置**（修 F-03，满足 I-04）
   CC 原清单 + Codex 13.3 四项补充：`maxChunkRows=0`、完整 `dstMeta`（offset/size/columnCount/segCount/colMeta/**timeRange**）、`rowCount`、`fileSize=0`。对齐 [`StreamIterators.reset`](../engine/immutable/stream_compact.go#L430) 并补其遗漏项。

2. **改动 C（校准）—— `SwitchNewTSSP` 重排 + `sw.Size()`**（修 F-01/F-02）
   顺序：`finishSeries(sid) → 立即清理 active-series 状态（含 p.sid=0）→ sw.Size()（不是 DataSize）→ 必要时 rotate`。

3. **改动 D（扩展）—— errEmptyFile 路径清理**（修 F-04 完整）
   CC 原 nil guard + 在 [`NewTSSPFile`](../engine/immutable/stream_downsample.go#L203) 的 errEmptyFile 分支集中 `removeEmptyFile`/`Close(true)`，关闭 writer/fd 并删除空 `.init`。

#### 必须新增（CC 原方案遗漏的正确性阻塞）

4. **F-07 用 `processedOld` 显式血缘**（修 F-07，满足 I-06）
   [`mergeTool.execute`](../engine/immutable/merge_tool.go#L201) 扩展返回值收集实际处理过的 old 文件为 `processedOld`；[`replaceMergedFiles`](../engine/immutable/merge_out_of_order.go#L96) 改用 `processedOld`+`producedNew` 直接匹配，不再从 new 文件名反推 old。只扩展 execute 返回值，不要求完整状态机重构（Codex 13.9 第 7 项）。

5. **F-05 `writeRemain` 切割**（修 F-05，满足 I-03）
   [`writeRemain`](../engine/immutable/merge_performer.go#L252) 回调内每个 SID 的 `WriteCurrentMeta` 成功后执行与改动 C 相同的 `sw.Size() >= limit → rotate` 检查。

6. **F-10 uint32 guard**（修 F-10，满足 I-08）
   在 [`WriteMeta`](../engine/immutable/stream_downsample.go#L355) 等 uint32 转换前加 `if size > math.MaxUint32 { return err }`（基于转换前的 int64/uint64）。

#### 视情况暂缓/处理

7. **F-08（同 SID 多 extent）**——可暂缓作为设计事实，但必须在文档/代码注释明确“查询侧承担全部枚举、排序、归并责任”。本次“SID 边界切割”不拆当前 SID，符合现有设计目标。

8. **F-11（extent uint16 回绕）**——可短暂暂缓（失败安全），建议顺手加 `if c.fileName.extent == math.MaxUint16 { return err }`。同时建议对 `InitMergedFile` 的 `merge++` 加溢出保护。

9. **F-09（LIMIT 提前裁剪）**——**不暂缓**。处理方式：禁用 [`AddLocationsWithLimit`](../engine/tsm_merge_cursor.go#L120) 的 `limit+offset` 提前 break，改为全枚举后排序再裁剪；或证明“同 SID 多 extent 时被省略 location 不可能含更优时间”——但 [`LocationCursor.Less`](../engine/immutable/location_cursor.go#L62) 仅按 minTime 排序，无法保证，故推荐禁用提前裁剪。

### 14.5 修订集对 I-01~I-08 的满足情况

| 不变量 | 修订集是否满足 | 由哪项覆盖 |
|---|---|---|
| I-01 完整 SID 边界 finalize | 满足 | 改动 C |
| I-02 完成 SID 幂等 | 满足 | 改动 C（立即清理 + `p.sid=0`） |
| I-03 每完整 SID 后统一检查 | 满足 | 改动 C + 新增 5（writeRemain 切割） |
| I-04 新 extent 文件级状态从零 | 满足 | 改动 A（校准后） |
| I-05 逻辑文件名与 fd 一致且原子拒绝冲突 | **部分满足** | 改动 D + 新增 4；Stat/Open 间 TOCTOU 窗口仍存，需 O_EXCL 或 lazy-open 才完全消除（作后续加固） |
| I-06 替换依据真实血缘 | 满足 | 新增 4（processedOld） |
| I-07 同 SID 多 extent 查询完整归并 | 满足 | 新增 9（禁用 F-09 提前裁剪） |
| I-08 单 SID 大小可被格式表达 | 满足 | 新增 6（uint32 guard） |

I-05 是唯一“部分满足”项：修订集消除了 F-04 的 fd 泄漏和 F-07 的误删，但 Stat/OpenFile 之间的 TOCTOU 窗口未原子化。可作为后续加固（lazy-open 或 O_EXCL），不阻塞当前正确性收口。

### 14.6 CC 最终立场

1. **撤回第 12 节“4 处局部修改即可消除全部 Critical/High”的主张**。12.3（stat-both-suffixes 修 F-07）和 12.4（暂缓 F-05/F-10）两处错误确认，原最小方案不能作为合入基线。
2. **接受 Codex 第 13 节全部评判**（仅 F-11 CALIBRATE：可短暂暂缓但建议加 guard）。
3. **以 14.4 的修订集（9 项）作为新的最小可接受合入基线**。该集满足 I-01~I-04/I-06~I-08，I-05 部分满足（TOCTOU 留后续），规模仍显著小于第 7 节完整状态机重构。
4. **F-09 代码存在性已确认**（`engine/tsm_merge_cursor.go:120`），必须纳入修订集，不能暂缓。
5. 第 7 节状态机重构仍作为长期优化方向；若后续要支持“writeRemain 内嵌切割的更优语义”或“I-05 原子分配”，再考虑完整重构。

### 14.7 请 Codex 最终复核

1. 14.4 修订集 9 项是否覆盖全部正确性阻塞？是否有遗漏？
2. 14.5 的 I-01~I-08 满足判定是否准确？I-05“部分满足、TOCTOU 留后续”是否可接受作为合入基线？
3. 14.4 第 9 项（禁用 `AddLocationsWithLimit` 提前裁剪）是否是 F-09 的正确处理？还是应保留提前裁剪并另建时间范围不变量？
4. 修订集是否可作为合入基线，不再要求一次完成第 7 节完整重构？

CC 立场：以 14.4 修订集为合入基线，第 12 节降级为“降低改动量的早期讨论草案”。请 Codex 裁决是否同意。

---

## 15. Codex 对第 14 节的 1+1 终审意见（供 CC 复核）

本节保留第 14 节原文，不覆盖 CC 的结论；它记录本轮两阶段复核结果：

1. **证据提出者**：从当前分支重新追踪写出、命名、替换、查询和失败清理链路，构造反例。
2. **独立审核者**：逐项挑战证据的可达性、严重级别和是否能由现有串行机制兜底。
3. **Codex 裁决**：只把能由当前源码直接证明的问题列为确定性阻断项；证据不足的风险降级为“需证明的不变量”，不把推测写成既成事实。

双方共识是：第 14 节已经修正了第 12 节的主要错误，但 **14.4 的 9 项仍不能直接作为合入基线**。主要遗漏不是要求全面采用第 7 节重构，而是局部方案尚未闭合以下链路：

- 文件名冲突探测后，实际打开的路径没有随 extent 更新；
- <code>processedOld</code> 只解决删除血缘，不能充当输出文件名分配器；
- 首个输出文件的 merge 代际规则没有进入最小方案；
- <code>writeRemain</code> 的回调是“每列一次”，切割点必须落在完整 SID 完成边界；
- 删除 LIMIT 的 <code>break</code> 仍未修正全局 <code>firstTime</code>；
- extent 和 merge 回绕并非天然失败安全；
- 同 SID 多 location 的查询正确性仍依赖尚未写明和验证的时间范围不变量。

### 15.1 对第 14 节结论的总裁决

| 第 14 节主张 | Codex 终审 | 原因 |
|---|---|---|
| 14.4 的 9 项覆盖全部正确性阻塞 | **REJECT** | 至少遗漏 F-06 路径错配、跨代输出名分配、首文件 merge 代际、全局 firstTime 和完整溢出保护 |
| I-01～I-04、I-06～I-08 均满足 | **REJECT / 条件成立** | 多项只有实现方向，没有形成可验证的状态机和失败语义 |
| I-05 只剩 Stat/Open 的 TOCTOU，可后续加固 | **REJECT / 校准严重级别** | TOCTOU 是否阻塞取决于单写者证明；但 F-06 和逻辑名分配缺失是确定性问题，不能后移 |
| 删除 <code>AddLocationsWithLimit</code> 的提前 break 即修复 F-09 | **CALIBRATE** | 必须同时全局归约 firstTime，或禁用整条 <code>CanLimitCut</code> 优化 |
| F-11 当前失败安全，可短暂暂缓 | **REJECT** | uint16 自增会回绕；低 extent 若空闲，探测可能成功，并不保证以错误结束 |
| 不必一次完成第 7 节完整重构 | **ACCEPT** | 可以局部修复，但必须把 15.15 的闭环条件全部补齐 |

### 15.2 确定性阻断一：F-06 仍被第 14 节完全遗漏

当前 [StreamWriteFile.NewFile](../engine/immutable/stream_downsample.go#L155) 的执行顺序是：

1. 第 167 行先把当前 <code>fileName</code> 计算为局部变量 <code>filePath</code>；
2. 第 168～174 行发现冲突后调用 <code>TryNextExtentFile</code>；
3. <code>TryNextExtentFile</code> 在第 126～153 行修改 <code>c.fileName.extent</code>；
4. 返回 <code>NewFile</code> 后没有重新计算 <code>filePath</code>；
5. 第 175～179 行仍使用旧 <code>filePath</code> 创建或打开 fd。

因此会出现：

~~~text
逻辑文件名：extent = N + 1
实际 fd：    extent = N 的旧路径
~~~

热存储路径使用 <code>O_CREATE | O_RDWR</code>，没有 <code>O_EXCL</code>；当旧路径已经存在时，它可能直接打开旧文件，而不是返回“文件已存在”。这不是并发窗口才会触发的问题，而是单线程即可到达的确定性路径错配。

第 14 节的改动 D 和 <code>processedOld</code> 都不能修复此问题。最低要求是：

1. 每次 extent 变化后重新计算正式路径和临时路径；
2. 冲突探测、fd 创建、writer 名称和最终 rename 必须使用同一个最终文件名；
3. 创建成功后增加断言或测试，证明 <code>fd.Name()</code>、writer 名称与 <code>c.fileName</code> 指向同一个逻辑文件；
4. 不得继续使用冲突前缓存的 <code>filePath</code>。

在 F-06 修复前，I-05 不是“部分满足”，而是明确不满足。

### 15.3 确定性阻断二：processedOld 不是输出文件名分配器

CC 第 14 节把两件不同的事合并了：

- <code>processedOld</code>：决定成功提交后哪些旧文件可以删除；
- 输出名分配：决定本次新文件可以占用哪个 <code>(sequence, merge, extent)</code>。

前者不能自动保证后者。可构造以下跨代反例：

~~~text
现有旧文件：
  sequence=S, merge=M, extent=0
  sequence=S, merge=M, extent=1

先处理 extent=0，产生：
  sequence=S, merge=M+1, extent=0
  sequence=S, merge=M+1, extent=1   （因大小切割）

旧 extent=1 本轮未处理，仍保留。
未来再处理旧的 merge=M, extent=1 时，其首个候选输出名仍可能是：
  sequence=S, merge=M+1, extent=1
~~~

<code>processedOld</code> 可以阻止第一次提交时误删未处理的旧 extent=1，却不会阻止未来输出名撞上已经存在的 merge=M+1、extent=1。

此外，[TSSPFiles.Less](../engine/immutable/tssp_reader.go#L147) 的主要排序键是 sequence 和 extent，不能替代跨代文件名占用关系。替换流程又是先把新文件 rename 到正式名，再删除旧文件；一旦正式名分配重复，可能打开、覆盖或拒绝错误的对象，具体表现取决于底层文件系统和打开标志。

最低要求是同时存在两套独立数据：

1. <code>processedOld</code>：精确记录实际进入本次执行的输入；
2. 输出名占用集合或分配器：在当前 measurement 的所有存活文件、本批已产出文件和本批已预留文件中，为完整逻辑名做唯一性检查。

额外切割只能从“所有存活和已预留逻辑名之外”的 extent 分配，不能只看当前 old 文件，也不能只靠 extent 自增。

### 15.4 确定性阻断三：首个输出文件的 merge 代际规则未进入 14.4

[StreamWriteFile.InitMergedFile](../engine/immutable/stream_downsample.go#L253) 只有在 <code>opts.addMerge</code> 为 true 时才执行 <code>merge++</code>；<code>opts.addFileExt</code> 只控制 extent。

因此最小方案必须明确区分：

| 输出时机 | addMerge | addFileExt | 目的 |
|---|---:|---:|---|
| 针对一个旧代际创建首个新文件 | true | false | 进入新 merge 代际，避免与输入同名 |
| 同一输出代际因大小继续切割 | false | true | 保持 merge 不变，只分配新 extent |

若首个输出只设置 <code>addFileExt</code> 或两项都为 false，新文件可能仍处在旧 merge 代际，甚至与待替换输入同 basename。替换流程先 rename 新文件、后删除旧文件时，风险会直接进入正式文件阶段。

这条规则必须成为调用点可见的显式协议，并有测试覆盖“首文件”和“后续切割文件”两条路径，不能依赖调用者碰巧传对布尔值。

### 15.5 processedOld 的精确定义和提交语义

第 14 节提出的 <code>processedOld</code> 方向正确，但“直接匹配”一词容易继续引入一一映射误解。正确语义更接近：

~~~go
ReplaceFiles(name, processedOld, producedNew, true)
~~~

其中：

- <code>processedOld</code> 只包含实际进入本次 <code>ColumnIterator.Run</code> 且本次执行成功的旧文件；
- 被跳过、未打开或本轮未消费的旧文件绝不能加入；
- <code>producedNew</code> 包含所有成功 finalize 的非 nil 新文件；
- old 与 new 数量不要求相等，也不按 extent 一一配对；
- 任何写出、finalize、rename 前置步骤失败时，都不能进入 ReplaceFiles；
- 失败路径只清理本批临时产物，不删除 unordered 输入，也不修改未处理 old；
- 原先所有“根据 new 文件名倒推 old 文件”的循环应完全移除，不能与 <code>processedOld</code> 双轨并存。

所以 I-06 只能在上述提交边界和失败语义全部实现后判定满足。

### 15.6 writeRemain 的真实 SID 完成边界

[mergePerformer.writeRemain](../engine/immutable/merge_performer.go#L252) 调用的 <code>ReadRemain</code> 回调不是“每个 SID 一次”，而是“每个 SID 的每个列一次”。当前代码中的 <code>lastSid</code> 正是在回调内识别 SID 边界。

因此第 14 节“回调内每个 SID 的 WriteCurrentMeta 成功后检查”的表述不足以直接实现。实际只有两个安全完成点：

1. 收到新 SID 时：先为 previous SID 执行 <code>WriteCurrentMeta</code>，再判断是否切割，最后才能 <code>ChangeSid(current)</code>；
2. <code>ReadRemain</code> 返回后：为最后一个 SID 执行 <code>WriteCurrentMeta</code>，再判断是否切割或直接交给最终文件收尾。

建议把低层逻辑约束为：

~~~text
on sid change:
    if previous sid exists:
        WriteCurrentMeta(previous)
        rotateCompletedFileIfNeeded(hasMoreOutput=true)
    ChangeSid(current)
    write current column

after ReadRemain:
    WriteCurrentMeta(last)
    rotateCompletedFileIfNeeded(hasMoreOutput=由调用上下文决定)
~~~

还必须满足：

- 切割函数只 finalize 已完成文件并初始化下一文件；
- 切割函数不得再次调用 <code>finishSeries</code> 或 <code>writeRemain</code>，否则形成递归完成或重复消费；
- 只有在确认后续仍有输出时才立即创建下一空文件；最终结束场景应直接由 Finish 收尾，或保证空文件被完整关闭并删除；
- rotate 后 active-series 状态必须一次性清空，不能只清部分字段。

所以 I-01～I-03 目前应标为“实现后条件满足”，不能仅凭第 14 节一句描述判定已经满足。

### 15.7 F-09：仅删除提前 break 仍不完整

[AddLocationsWithLimit](../engine/tsm_merge_cursor.go#L120) 当前存在两个耦合优化：

1. 按文件遍历顺序累计行数，到 <code>limit + offset</code> 后提前 break；
2. <code>firstTime</code> 只在第一个命中的 location 上初始化一次。

删除第一个优化后，第二个问题仍在。反例：

~~~text
升序查询，同一 SID：
  先遍历的 extent：time = [100, 110]
  后遍历的 extent：time = [1, 10]

删除 break 后两个 location 都加入 cursor，
但 firstTime 仍为 100，而真实全局 firstTime 应为 1。
~~~

<code>firstTime</code> 会进入 TopN/LIMIT 剪枝；错误基准可能使整个 SID 被提前淘汰。后续 location 排序和 <code>mergeData</code> 无法恢复已经在更上层被淘汰的 SID。

可接受方案二选一：

1. **保守方案**：同 SID 可能位于多个 extent 时，禁用整条 <code>CanLimitCut</code> 快路径，走完整枚举和最终排序；
2. **保留优化**：完整枚举所有命中 location，并像 <code>AddLocationsWithFirstTime</code> 一样做全局归约：
   - 升序取所有 <code>metaMinTime</code> 的最小值；
   - 降序取所有 <code>metaMaxTime</code> 的最大值；
   - LIMIT/OFFSET 只能在全局时间顺序已经建立后裁剪。

因此第 14 节第 9 项方向正确，但“只禁用 break”不足以让 I-07 成立。

### 15.8 I-07 与 pre-agg 查询：需要把隐藏前提变成显式不变量

<code>CanLimitCut</code> 只覆盖无聚合调用的原始查询，因此 F-09 的直接影响主要在正常查询和 LIMIT 路径；不能把它直接等同于所有 pre-agg 风险。

但是 pre-agg 的 <code>first/last</code> 及普通读取都依赖同一 SID 的多个 location 可以按时间范围顺序串行消费。当前可见行为是：

- [LocationCursor.Less](../engine/immutable/location_cursor.go#L62) 对有序文件只按 location 的 minTime 排序；
- [LocationCursor.ReadData](../engine/immutable/location_cursor.go#L200) 按排序后的 location 逐个读取；
- only-first/only-last 路径可能在第一个产生结果的 location 后停止；
- 常规聚合会遍历 location 并用 <code>AggregateData</code> 合并，count/sum/min/max 对“先读哪个 extent”通常不敏感。

如果同一 SID 的不同 location 时间范围互不重叠，且按 minTime 排序后同时保证 maxTime 单调，那么正常读取、first/last 和常规聚合都可以成立。当前正常写出路径很可能维持这一事实，但第 14 节没有给出源码级不变量、断言或回归测试，所以不能直接宣布 I-07 已满足。

必须二选一：

1. 在写出和加载边界明确强制并验证：

~~~text
对同一 SID，按 minTime 排序后：
previous.maxTime < current.minTime
~~~

   若系统允许同时间戳覆盖或去重，应把严格小于改成与实际重复时间语义一致的条件。

2. 若允许时间范围交叠，则查询侧必须对同 SID 多 location 做真正的按时间 k-way merge；first/last 也必须比较全部候选 location，不能读到一个就提前结束。

最低回归矩阵应覆盖：

- raw 全量查询、raw LIMIT/OFFSET；
- count、sum、min、max、first、last；
- 升序和降序；
- extent 顺序与时间顺序相反；
- 同 SID 多 extent 的非重叠时间范围；
- 人工构造的交叠时间范围，用于确认系统是明确拒绝还是正确归并。

在不变量或归并实现落地前，I-07 应标为“正常路径可能成立，但尚未被证明”，而不是“已满足”。

### 15.9 F-11：uint16 回绕不是天然失败安全

extent 字段是 uint16；当前至少有两处直接自增：

- [TryNextExtentFile](../engine/immutable/stream_downsample.go#L126) 中的 <code>c.fileName.extent++</code>；
- [NewFile](../engine/immutable/stream_downsample.go#L155) 中的 <code>c.fileName.extent++</code>。

当 extent 为 <code>math.MaxUint16</code> 时，自增会回绕为 0。若当前 merge 代际的 extent=0 路径不存在，<code>Stat</code> 会返回不存在，探测逻辑就可能把回绕后的 0 当作成功候选，而不是返回错误。因此“文件名冲突必然让它失败”并不成立。

merge 字段也需要同类保护：构造新名字时的取模或 <code>merge++</code> 都不能把最大值回绕为已使用代际。

最低要求：

1. 自增前检查 extent 是否等于 <code>math.MaxUint16</code>；
2. 自增 merge 前检查是否达到格式上限；
3. 返回可识别错误，并进入统一失败清理；
4. 不允许依赖回绕后再 Stat 来实现边界控制。

所以 F-11 应进入合入基线，而不是“建议顺手修”。

### 15.10 I-05 的 TOCTOU：风险真实，但严重级别需要校准

当前冲突检查是 <code>Stat → OpenFile(O_CREATE | O_RDWR)</code>，两者之间确实没有原子拒绝冲突。理论上另一个写者可以在窗口内占用同名目标，现有打开标志仍可能打开它。

独立审核者指出，正常 merge 入口存在两层串行机制：

- <code>MergeOutOfOrder</code> 通过 <code>inMerge.Add(ctx.mst)</code> 串行同 measurement 的 merge；
- <code>mergePrepare</code> 的 acquire/release 协调本次输入文件。

这会降低正常单进程、同 measurement merge 的并发可达性，因此不能在没有进一步证据时把 TOCTOU 无条件定为独立 Critical。

但现有证据仍不足以把 I-05 判为已满足：

- acquire 主要锁定输入路径，不是对候选输出名做原子预留；
- 尚未证明不存在恢复流程、跨进程写者或持有不同输入集合的并发生产者；
- <code>O_CREATE | O_RDWR</code> 本身不拒绝已存在目标；
- F-06 的旧路径打开问题与并发无关，必须先修；
- 15.3 的跨代逻辑名碰撞也与 TOCTOU 无关，必须先修。

终审结论：

1. F-06 和逻辑文件名分配器是确定性阻断，不能暂缓；
2. <code>O_EXCL</code> 或等价原子预留可以作为直接修复；
3. 若要把原子创建延后，CC 必须提供并写入代码契约的“全生产者单写者证明”，并覆盖恢复和跨进程场景；在此之前只能说风险受限，不能说 I-05 已满足。

### 15.11 sw.Size 的语义：适合软阈值，不等于严格最终文件大小

[StreamWriteFile.Size](../engine/immutable/stream_downsample.go#L620) 比 <code>writer.DataSize()</code> 更接近最终文件大小，因为它加入了 chunk meta、meta index、bloom、trailer 和 id-time 估算。但仍有两点：

- id-time 使用固定压缩率估算，不是实际编码后的精确值；
- 调用时是否已计入最后一个尚未 flush 的 meta index，需要由调用点状态保证。

另外，当前设计只在完整 SID 写完后检查，因此单个超大 SID 允许让文件超过 4 GiB 阈值。这与“SID 不跨 extent”的设计目标一致，但意味着 4 GiB 是软切割阈值，不是最终文件硬上限。

合入前必须明确产品语义：

- 若 4 GiB 是软阈值：允许最后一个 SID 导致有限超出，并在文档和测试中说明；
- 若 4 GiB 是硬上限：仅靠 SID 边界检查不足，需要保守余量、预估下一 SID，或允许拆分 SID。

无论哪种语义，格式字段的 uint32 上限都必须独立保护，不能因为配置阈值也是 4 GiB 就假设不会截断。

### 15.12 F-10：必须在转换前失败，并保证事务式清理

当前 [WriteMeta](../engine/immutable/stream_downsample.go#L355) 直接执行：

~~~go
cm.size = uint32(c.writer.DataSize() - cm.offset)
~~~

正确的最小保护应在转换前使用宽类型计算：

~~~go
size := c.writer.DataSize() - cm.offset
if size > math.MaxUint32 {
    return errChunkTooLarge
}
cm.size = uint32(size)
~~~

并且：

- 比较应是大于 <code>math.MaxUint32</code>，等于最大可表达值仍合法；
- 错误必须从 <code>WriteMeta → WriteCurrentMeta → ColumnIterator.Run → execute</code> 完整向上传播；
- 失败后清理本批所有临时新文件；
- 不调用 ReplaceFiles，不删除 unordered 文件；
- 同时审计 <code>mIndex.size</code>、<code>cmOffset</code>、columnCount、segCount 等其他 uint32 转换，分别确认其上界或增加 guard。

仅在一个转换点返回错误、却仍继续提交或遗漏临时文件清理，仍不满足 I-08。

### 15.13 I-01～I-08 的终审矩阵

| 不变量 | 第 14 节当前状态 | 终审说明 |
|---|---|---|
| I-01 完整 SID 边界 finalize | **条件满足** | 改动 C 加 15.6 的真实 remain 边界后可满足 |
| I-02 完成 SID 幂等 | **条件满足** | 需统一清 active 状态，并禁止 rotate 递归调用 finishSeries/writeRemain |
| I-03 每完整 SID 后统一检查 | **尚未证明** | writeRemain 是每列回调，第 14 节表述不足以保证每 SID 恰好检查一次 |
| I-04 新 extent 文件级状态从零 | **条件满足** | reset 全字段并有第二文件元数据回归测试后可满足 |
| I-05 逻辑名、fd 一致且拒绝冲突 | **不满足** | F-06、跨代分配器均缺失；原子创建需修复或给出完整单写者证明 |
| I-06 替换依据真实血缘 | **条件满足** | 需按 15.5 的集合提交语义实现，不能做 old/new 一一匹配 |
| I-07 同 SID 多 extent 查询正确 | **尚未证明** | 全局 firstTime 未闭合；location 时间范围不变量也未建立 |
| I-08 格式字段可表达且失败安全 | **条件满足** | 需转换前 guard、extent/merge guard 和全链路失败清理 |

### 15.14 对 CC 第 14.7 节四个问题的直接回答

1. **14.4 的 9 项是否覆盖全部正确性阻塞？**  
   否。至少遗漏 F-06、跨代输出名分配、首文件 merge 代际、全局 firstTime、extent/merge 回绕保护，以及 writeRemain 的真实完成点。

2. **I-05“部分满足、TOCTOU 留后续”是否可接受？**  
   当前不可接受。F-06 和跨代逻辑名碰撞不是 TOCTOU，必须本次修。纯并发 TOCTOU 是否可后移，取决于 CC 能否给出覆盖所有输出生产者的单写者证明。

3. **禁用 AddLocationsWithLimit 的提前裁剪是否正确？**  
   方向正确，但不能只删 break。还要全局归约 firstTime，或直接禁用整条 <code>CanLimitCut</code> 快路径。pre-agg 另需明确同 SID location 的时间范围不变量。

4. **是否必须一次完成第 7 节完整状态机重构？**  
   不必须。局部方案可以合入，但必须包含 15.15 的闭环项，并用故障注入和查询矩阵证明状态转换正确。

### 15.15 Codex 建议的修订版最小合入基线

在 CC 14.4 基础上，最小闭环应至少包含以下 13 项：

1. **完整 reset**：覆盖 writer、dstMeta 全字段、rowCount、fileSize、keys、pair、meta index、bloom 及所有缓存。
2. **统一 SID 完成顺序**：finish current SID → 清 active 状态 → 用 <code>sw.Size()</code> 判断 → 必要时 rotate。
3. **修正 writeRemain**：只在 SID 转换点和最后 SID 完成点检查；rotate 是低层非递归操作。
4. **空文件完整清理**：errEmptyFile 分支关闭 writer/fd、删除空 init 文件，并禁止 append nil。
5. **明确输出代际**：首文件 <code>addMerge=true</code>；同代切割 <code>addFileExt=true</code> 且不再次加 merge。
6. **修复 F-06**：extent 改变后重算路径；探测、打开、writer 和 rename 使用同一最终逻辑名。
7. **增加跨代逻辑名分配器**：检查所有存活、已产出和已预留的完整逻辑名，而非只依赖 processedOld 或 extent++。
8. **精确 processedOld 提交**：按 15.5 的集合语义直接 ReplaceFiles，彻底删除 new-name 反推 old 的逻辑。
9. **所有格式窄化转换 guard**：转换前校验，错误贯穿 execute，并事务式清理临时产物。
10. **extent 和 merge 上限 guard**：自增前检查，禁止 uint16 回绕。
11. **修复 raw LIMIT**：全枚举并全局归约 firstTime，或禁用完整 <code>CanLimitCut</code> 快路径。
12. **明确查询时间不变量**：强制同 SID location 不交叠并加断言/测试，或实现真正 k-way merge。
13. **冲突原子性收口**：使用 <code>O_EXCL</code> 或等价预留；若延后，必须提交可审核的全生产者单写者证明。

这 13 项不是要求大规模架构重写。其中 1～6、8～11 可以保持局部修改；第 7、12、13 也可以采用小型辅助函数、显式不变量和测试完成。关键是不能再用删除血缘替代名字分配，也不能用“正常情况下串行”替代未经证明的输出名唯一性。

### 15.16 请 CC 针对以下问题复核

1. 第 14 节为何没有覆盖已经可由当前代码直接证明的 F-06 旧路径打开问题？
2. CC 是否同意 <code>processedOld</code> 只决定删除集合，不能决定输出名可用性？
3. 对 15.3 的跨代反例，现有哪一层代码保证未来处理旧 extent=1 时不会选择已存在的 merge=M+1、extent=1？
4. 首个新文件的 <code>addMerge=true</code> 与后续切割的 <code>addFileExt=true</code> 是否会作为显式协议进入实现和测试？
5. CC 是否确认 <code>ReadRemain</code> 回调粒度是列，而非 SID；最后一个 SID 的切割检查准备放在哪个完成点？
6. 删除 LIMIT 的 break 后，准备如何把 firstTime 从“第一个命中值”改成全局 min/max？
7. 同 SID 多 location 的时间范围不交叠由哪个写出点保证、哪个加载点验证、哪些测试覆盖？
8. 对 extent=MaxUint16 后回绕到 0 且该路径空闲的情形，现有代码为何一定失败？
9. 若 TOCTOU 延后，能否列出所有可能创建正式 TSSP 输出的入口，并证明它们在进程内、跨进程和恢复流程中互斥？
10. 4 GiB 是软切割阈值还是最终文件硬上限？单 SID 超阈值时的正式语义是什么？

### 15.17 Codex 最终立场

**不同意直接以第 14.4 节作为合入基线，也不要求一次完成第 7 节完整重构。**

第 14 节已经修复了原最小方案中关于血缘、uint32 和 LIMIT 提前裁剪的主要误判；但输出名分配、F-06 路径一致性、writeRemain 的真实 SID 边界、全局 firstTime 和查询时间范围不变量仍未闭合。将 15.15 纳入修订集并以对应回归测试验证后，局部方案可以成为可接受的合入基线。

---

## 16. 基于最新代码与两轮 1+1 review 的最终方案（提交 CC 审核）

> 本节记录 2026-07-22 基于当前工作区代码完成的最新结论。第一轮由证据 agent 提出源码链和反例，再由审核 agent 独立复核；第二轮针对修订方案再次执行相同的 1+1 review。
>
> 本节会修订第 15.15 节中关于“跨代逻辑名分配器”和 `writeRemain` 落点的建议。若两节有冲突，以本节为准。

### 16.1 范围和结论

本次目标不是把最终文件严格限制在 4 GiB，而是：

1. 只在一个完整 SID 写完后判断大小；
2. 文件超过配置阈值时尽可能切割；
3. 如果安全位置不足，允许当前文件继续增长并超过软阈值；
4. 不允许切割改变既有文件内容的逻辑顺序；
5. 不允许 `writeRemain` 把同 SID 的较晚时间片写到较早的逻辑位置；
6. 不允许输出名碰撞、覆盖已有正式文件或误删未处理旧文件。

最终推荐采用 **D+A 组合方案**：

- **方案 A：gap-only allocator**。一个旧文件的输出只能使用它与下一个旧占用 extent 之间的连续空洞，不允许扫描并跨过任何存活逻辑位置；空洞用完后停止切割，继续写当前文件。
- **方案 D：global-last anchor**。每轮 merge 都将稳定快照中全局最后一个 committed live order 文件加入处理集合，让它成为唯一 `lastFile`；全部 residual unordered 只能在这个全局末尾锚点被消费或写出。

两轮 1+1 review 的最终裁决都是：

~~~text
CALIBRATE 后 ACCEPT
~~~

也就是说，D+A 的顺序证明成立，但只有本节列出的锁、错误传播、原子命名、输入不变量和恢复语义全部落实后，才能作为合入方案。当前分支的 `reservedExtents + TryNextExtentFile 扫描跳号` 不等于 D+A，不能直接合入。

本节不讨论接近 4 GiB 时既有 `uint32` 读取回绕的修复；该问题由其他修改负责。本节仍保留 extent/merge 计数回绕保护，因为它直接属于输出命名正确性。

### 16.2 当前流程实际做了什么

当前代码的主要调用链如下：

~~~text
matchOrderFiles
    ↓
按文件全局 MinMaxTime 选择 order 文件
    ↓
execute 按 (sequence, extent) 遍历选中的文件
    ↓
非 last 且不 Contains 任一 unordered SID：跳过
选中集合中的最后一个文件：强制 Run，并设置 p.lastFile=true
    ↓
ColumnIterator.Run 按 SID/chunk 写出
    ↓
SID 完成后 SwitchNewTSSP / writeRemain 检查大小并 rotate
    ↓
NewFile 增加 extent；冲突时 TryNextExtentFile 继续向后扫描
    ↓
ReplaceFiles 先 rename 所有新文件，再删除 processedOld
~~~

相关实现位置：

- [matchOrderFiles](../engine/immutable/merge_out_of_order.go#L235)；
- [mergeTool.execute](../engine/immutable/merge_tool.go#L201)；
- [ColumnIterator.Run](../engine/immutable/column_iterator.go#L96)；
- [SwitchNewTSSP](../engine/immutable/column_iterator.go#L169)；
- [writeRemain](../engine/immutable/merge_performer.go#L307)；
- [TryNextExtentFile](../engine/immutable/stream_downsample.go#L154)；
- [StreamWriteFile.NewFile](../engine/immutable/stream_downsample.go#L222)；
- [ReplaceFiles](../engine/immutable/mms_tables.go#L730)。

### 16.3 当前流程的问题汇总

#### 16.3.1 问题一：物理文件不存在不等于逻辑 extent 可用

物理文件名包含：

~~~text
sequence-level-merge-extent
~~~

但 order 文件集合的排序只比较 `(sequence, extent)`，不比较 level/merge，见 [TSSPFiles.Less](../engine/immutable/tssp_reader.go#L147)。因此：

~~~text
0000002a-0001-00030001.tssp
0000002a-0002-00070001.tssp
~~~

虽然是两个不同的物理文件名，但都占用同一个逻辑位置：

~~~text
(sequence=0x2a, extent=1)
~~~

只对候选的当前 generation 执行：

~~~text
Stat(candidate.tssp)
Stat(candidate.tssp.init)
~~~

无法发现其他 level/merge generation 已经占用该逻辑位置。

#### 16.3.2 问题二：当前 reservation 不完整，且首候选会绕过 reservation

当前 `reservedExtents`：

- 只来自本轮匹配到的 `order.Files()`，不是 measurement 的全部 live order 文件；
- key 只有 `uint16 extent`，没有 sequence 和 owner；
- 无法表达同一逻辑位置存在多个 owner；
- 无法表达尚未落盘的 in-flight claim；
- `NewFile` 会先直接尝试 `extent+1`，只有对应 `.init` 已存在时才进入 `TryNextExtentFile`，因此首候选可能根本不检查 reservation；
- `TryNextExtentFile` 继续扫描更高 extent，会改变 tail 相对于 survivor 的位置。

所以现有 map 既会对不同 sequence 的同 extent 产生不必要阻塞，又会漏掉真正的跨 generation owner。

#### 16.3.3 问题三：任意扫描空闲 extent 会改变同 SID 分片的处理顺序

完整 SID 边界切割只能证明 source head 和 source tail 的 SID 集合互斥，不能证明 tail 与它跨过的 survivor SID 互斥。

例如切割后形成：

~~~text
extent=1：SID20 late  [30,40]
extent=3：SID20 early [10,20]   // 从更早 source relocation 到这里
~~~

下一轮 unordered 为 `SID20@5` 时，merge 按 extent 先处理 extent=1。non-last cutoff=40 会提前消费 `@5`，得到：

~~~text
extent=1：[5,30,40]
extent=3：[10,20]
~~~

查询侧只按 location 的 `minTime` 排序，再逐 location 串行读取，并没有对同一 SID 的 ordered locations 做 k-way merge，见：

- [LocationCursor.Less](../engine/immutable/location_cursor.go#L62)；
- [LocationCursor.ReadData](../engine/immutable/location_cursor.go#L200)。

因此 raw 升序结果可变成：

~~~text
[5,30,40,10,20]
~~~

如果 unordered 时间与 early location 中已有时间相同，还会保留新旧两份记录；count/sum 会重复计数或累加，first/last 也可能从错误的 location 提前返回。

#### 16.3.4 问题四：即使不跳 extent，当前 `writeRemain` 仍可能写到中间位置

第一轮 1+1 review 证明：gap-only 只能保护旧 source 内容的相对顺序，不能单独约束新注入的 `writeRemain` 数据。

确定性反例如下：

~~~text
F1 = 00000001-0000-00000001.tssp
     SID10@40

F3 = 00000001-0000-00000003.tssp
     SID20@[10,15(old),20]

本轮 unordered：SID20@30
~~~

当前 [matchOrderFiles](../engine/immutable/merge_out_of_order.go#L235) 的结果是：

1. F1 的 `minTime=40 > unordered.maxTime=30`，所以 F1 被选中；
2. F3 的 `maxTime=20 < unordered.minTime=30`，所以 F3 被跳过；
3. F1 成为“选中集合的最后文件”，即 `p.lastFile=true`；
4. F1 不包含 SID20，但 last 文件被强制 Run；
5. EOF 的 `writeRemain` 把 SID20@30 写入 F1 的 replacement。

第一轮后形成：

~~~text
较早 extent=1：SID20@30
较晚 extent=3：SID20@[10,15(old),20]
~~~

第二轮加入：

~~~text
SID20@[5,15(new)]
~~~

extent=1 的 cutoff=30 会先消费两个点，结果为：

~~~text
extent=1：[5,15(new),30]
extent=3：[10,15(old),20]
~~~

可观察后果：

~~~text
raw ascending：[5,15,30,10,15,20]
last：可能返回 20，而不是全局最后值 30
count/sum：同时统计 15(old) 和 15(new)
~~~

这个问题不依赖新切割是否跳号；根因是“最后一个时间命中文件”不一定是“全部 live order 文件的逻辑最后文件”。

#### 16.3.5 问题五：当前 rotate 顺序无法支持“冲突时继续当前文件”

当前 [rotateCompletedFile](../engine/immutable/merge_performer.go#L233) 先调用 `NewTSSPFile` 封存当前 writer，再调用 `InitMergedFile` 创建下一文件。

如果下一逻辑位置此时才发现冲突：

1. 当前 writer 已经 Flush/Close；
2. 无法继续向当前文件写数据；
3. 只能整轮失败，不能利用“4 GiB 是软阈值”的语义继续当前文件。

因此逻辑 claim 和物理预检必须发生在封存 writer 之前。

#### 16.3.6 问题六：SID 结束后 eager rotate 会创建无意义空后继

当前 `SwitchNewTSSP` 在最后一个 ordered SID 写完后也可能立即 rotate；`writeRemain` 在最终 SID 的 `WriteCurrentMeta` 后也会再次 rotate。若已经到 EOF，下一文件不会再写任何数据，只会产生 header-only `.init`，随后依赖空文件清理逻辑回收。

这会带来：

- 无意义的 slot claim；
- 无意义的创建、关闭和删除；
- 所有有效数据已经写完后，仍可能因为创建空后继失败而让整轮 merge 失败；
- 更复杂的 active-series reset 和重复 finish 风险。

#### 16.3.7 问题七：错误被当成“不命中”，会破坏顺序证明

当前代码中：

- `matchOrderFiles` 在 `MinMaxTime` 出错时直接 `continue`；
- `mergeTool.contains` 忽略 `Contains` 错误并按 false 处理。

这两条路径会把无法验证的文件当成 skipped survivor。此时无法证明 skipped 文件满足：

~~~text
file.maxTime < unordered.tr.Min
~~~

也无法证明实际包含某 SID 的 selected owner 没被错误跳过。因此两个错误都必须向上传播并终止本轮 merge。

#### 16.3.8 问题八：`.init` 排他创建不能替代 final no-replace

需要区分三个层次：

1. **逻辑 slot claim**：防止不同 level/merge 的文件同时占用相同 `(sequence, extent)`；
2. **`.init` 的 O_EXCL**：防止两个 writer 同时创建完全相同的临时路径；
3. **final no-replace rename**：提交时拒绝覆盖已经存在的正式 `.tssp`。

三者不可互相替代。当前 [RenameTmpFiles](../engine/immutable/mms_tables.go#L843) 调用普通 rename；在允许替换目标的平台上可能覆盖已有正式文件。

#### 16.3.9 问题九：失败清理无法覆盖所有创建过的 writer/path

当前 `CleanTmpFiles` 主要遍历 `mergedFiles` 并清理当前 `p.sw`。如果某个新的 `StreamWriteFile` 在 `p.Reset` 之前执行 `InitMergedFile` 失败，`p.sw` 仍可能指向上一个 writer，新创建的 header-only `.init` 和 fd 不一定能被当前清理路径找到。

事务必须显式登记本 job 创建过的每一个 writer、temp path、logical claim 和 produced file，不能再从 `p.sw` 或 `mergedFiles` 反推全部资源。

#### 16.3.10 问题十：稳定快照和全 producer 互斥尚未闭合

`inMerge` 只能串行同 measurement 的 merge；现有 `acquire` 主要保护精确输入路径。使用不相交输入的 compaction/downsample/recovery 或新 flush，仍可能改变未选 survivor、创建更大的 key，或争用同一个输出 hole。

仅在 merge 开始时复制一个 occupancy map 不构成 reservation。必须满足以下二选一：

1. 所有能生成 ordered TSSP 的 producer 共享 slot claim registry；
2. 使用更粗的 measurement/order namespace writer lock，覆盖 snapshot、execute、claim、commit 和 recovery。

如果 recovery 只在启动阶段运行，也必须有明确启动屏障：recovery 完成前不得启动 merge/compaction。

### 16.4 推荐方案 A：gap-only allocator

#### 16.4.1 逻辑位置和 owner

在一个 `MmsTables`/shard 内，logical slot key 定义为：

~~~text
(ordered namespace, measurement, sequence, extent)
~~~

如果 allocator 是跨 shard/store 的全局对象，则 key 还必须包含规范化的 store/shard path。

key 不包含 level 和 merge。occupancy 必须保存 owners，而不是 `map[key]bool`。来源至少包括：

- measurement 全部 committed live order 文件，覆盖所有 level/merge generation；
- measurement ordered 根目录非递归扫描得到的 `.tssp` 和 `.tssp.init`；
- 本进程中已经 claim、尚未落盘的 slot；
- 本事务的 `producedNew`。

`out-of-order` 子目录属于另一个 namespace，不应混入 ordered occupancy。

若一个 logical key 已有多个 owner，必须硬失败或先修复存量；不能依赖排序 tie-break，因为 comparator 对相同 `(sequence, extent)` 没有定义确定顺序。

#### 16.4.2 首输出是 owner transfer，不是找空位

处理旧文件：

~~~text
0000002a-0001-00030000.tssp
~~~

首输出必须固定为：

~~~text
0000002a-0001-00040000.tssp.init
0000002a-0001-00040000.tssp
~~~

即：

~~~text
sequence 不变
level 不变
extent 不变
merge = old.merge + 1
~~~

这是本事务把 old slot 的 owner 从旧文件转移到新 generation。首输出不能因为冲突而移动到 extent=1。

以下情况必须整轮失败：

- 首输出 `.init` 已存在；
- 首输出 final 已存在；
- old logical slot 除当前 old 外还有其他 owner；
- merge 计数达到上限；
- 路径检查发生权限或其他未知 IO 错误。

#### 16.4.3 每个旧文件只能使用自己的连续空洞

旧文件：

~~~text
A = 0000002a-0001-00030000.tssp
B = 0000002a-0001-00030002.tssp
~~~

snapshot 占用：

~~~text
(seq=0x2a, extent=0)
(seq=0x2a, extent=2)
~~~

对 A 而言，同 sequence 的下一个旧 occupied extent 是 2，因此 exclusive ceiling 为 2。A 的输出只能位于：

~~~text
[0,2) = extent 0、1
~~~

合法输出：

~~~text
0000002a-0001-00040000.tssp
0000002a-0001-00040001.tssp
~~~

如果 extent=1 写完后仍超过阈值，精确 next 是 extent=2。尽管：

~~~text
0000002a-0001-00040002.tssp
0000002a-0001-00040002.tssp.init
~~~

可能尚不存在，old B 仍然占用逻辑 `(0x2a,2)`。此时必须：

~~~text
不 rotate
不扫描 extent=3
继续写 extent=1
允许最终文件超过软阈值
~~~

随后处理 B 时，B 的首输出仍为：

~~~text
0000002a-0001-00040002.tssp
~~~

所有 snapshot old slots 整轮都保持占用，包括本轮稍后会被处理和删除的 `processedOld`。否则 A 的 tail 会抢占 B 的固定首输出位置。

若同 sequence 没有后继 old slot，虚拟 ceiling 为 `MaxUint16+1`；达到 `MaxUint16` 后返回“不切”，禁止回绕到 extent=0。

#### 16.4.4 tail 冲突的语义

tail 只尝试精确的：

~~~text
candidate.extent = current.extent + 1
~~~

处理结果必须区分：

| 情况 | 结果 |
|---|---|
| candidate logical slot 被 live/disk/claim owner 占用 | `false, nil`，不切，继续当前 writer |
| candidate `.init` 或 final 已存在 | `false, nil`，不切，继续当前 writer |
| candidate 达到 extent 上限 | `false, nil`，不切，继续当前 writer |
| Stat 权限错误、IO 错误、路径解析错误 | 硬失败，整轮清理 |
| 预检通过后 O_EXCL 竞态失败 | 硬失败，整轮清理 |

`false,nil` 只有在 claim 是纯 preflight、尚未改变 `fileName/fd/writer/state`、没有 finalize 当前文件且没有遗留 claim 时才安全。

### 16.5 推荐方案 D：全局最后 order 文件作为唯一锚点

#### 16.5.1 修改 matchOrderFiles

在稳定的 committed live order snapshot 上：

1. 成功读取所有 live 文件的 `MinMaxTime`；任一失败则整轮失败；
2. 执行现有时间匹配：

~~~text
ctx.tr.Overlaps(file.min, file.max) || file.min > ctx.tr.Max
~~~

3. 按 `(sequence, extent)` 找到唯一全局最后文件 G；
4. 若 G 尚未在匹配集合中，将其去重加入；
5. 排序后确保 G 是 `execute` 中唯一 `lastFile`；
6. 即使 G 不 Contains 任何 unordered SID，也必须强制 `Run`；
7. no-order 场景没有 G，继续走独立的 unordered-self/promote 路径。

同时将：

~~~go
func (mt *mergeTool) contains(...) bool
~~~

改成等价的：

~~~go
func (mt *mergeTool) contains(...) (bool, error)
~~~

任何 `Contains` 错误都中止本轮，不能按 false 继续。

#### 16.5.2 为什么未匹配 survivor 一定早于 unordered

现有匹配条件为：

~~~text
overlap || file.min > tr.Max
~~~

对一个未选文件 F：

~~~text
not overlap AND F.min <= tr.Max
~~~

闭区间不相交只有两种情况：

~~~text
F.max < tr.Min
或
F.min > tr.Max
~~~

第二种已经被 `F.min <= tr.Max` 排除，所以必然得到：

~~~text
F.max < unordered.tr.Min
~~~

因此 F 中任意 SID 的数据都早于本轮任意 unordered 时间点。

这个证明要求：

- 所有 `MinMaxTime` 调用成功；
- time range 合法；
- `ctx.tr` 包含本轮全部 unordered 输入；
- snapshot 在本轮内稳定。

#### 16.5.3 G 含或不含目标 SID 都能正确收尾

对任意 SID：

- 前序 processed owner 按自身 `maxOrderTime` 依次消费 unordered 前缀；[readTime](../engine/immutable/unordered_reader.go#L89) 只返回 `<= cutoff` 的部分并推进 offset；
- 前序 skipped owner 的全部时间 `< unordered.tr.Min`，因此早于所有 remaining；
- G 之后不存在 committed live owner。

如果 G 包含目标 SID，`p.lastFile=true` 使该 SID 使用 `MaxInt64`，remaining 会与 G 的 ordered fragment 在同一 chunk 内排序、去重和归并。

如果 G 不包含目标 SID，`writeRemain` 会按照 SID 顺序：

- 在第一个更大 G SID 开始前写出较小 SID；或
- 在 EOF 的 `finishSeries(MaxInt64)` 写出较大 SID。

所以 remaining 最终仍落在全局最后逻辑位置。G 若切割，方案 A 只允许它的 tail 在 G 后面连续增长，不会跨越任何旧 owner。

#### 16.5.4 上述反例在 D 下的结果

仍使用：

~~~text
F1 extent=1：SID10@40
G  extent=3：SID20@[10,15,20]
unordered：SID20@30
~~~

D 会强制加入 G。F1 不是 last 且不 Contains SID20，因此可以跳过；G 被强制处理并成为唯一 last，最终得到：

~~~text
G' extent=3：SID20@[10,15,20,30]
~~~

不会再产生：

~~~text
extent=1：SID20@30
extent=3：SID20@[10,15,20]
~~~

#### 16.5.5 D 的成本

- G 原本就在匹配集合中时，几乎没有额外文件级成本；
- G 原本不匹配时，每轮需要完整复制、重写一个额外文件；
- G 可能大于 4 GiB，因此最坏增加一个大文件的读写；
- G 会成为 merge/compaction 热点，且 merge generation 增长更快；
- 相比平移/重写整个 suffix，D 仍只额外重写一个文件，是更小且更容易证明的方案。

如果不能接受每轮重写 G，可以设计 `lastOwner[sid]`：为每个 unordered SID 找到其最后 live owner，并把 `MaxInt64` 从 file-level 改为 SID-level。但这需要 per-SID owner 扫描、多个终点文件、无 owner SID 的额外锚点和更复杂的 performer 状态，不建议作为本次最小方案。

### 16.6 推荐的 SID 完成与 lazy rotate 流程

不建议在一个 SID 写完后立即封存当前文件并打开下一文件。推荐将 rotate 推迟到“已经确定还有下一个 SID 即将写入”的时刻。

推荐流程：

~~~text
SeriesChanged(newSID)
    ↓
finish 前一个 active SID
    ↓
lastFile 场景先写出 SID < newSID 的 remaining unordered
    ↓
此时前一个 SID/中间 remaining SID 已完整写完
    ↓
如果 sw.Size() >= softLimit：
    尝试 claim 精确 next extent
    ├─ next unavailable：继续当前 writer
    └─ claim success：finalize 当前文件，打开精确 next 文件
    ↓
初始化并写 newSID
~~~

`writeRemain` 的 callback 粒度是列，不是 SID。只能在：

~~~text
lastSid != sid
~~~

并且前一个 SID 的 `WriteCurrentMeta` 成功后，确定下一个 SID 已存在时尝试 rotate。不能在同一 SID 的不同列之间切割。

EOF 流程：

~~~text
finish 最后 SID
写完所有 remaining
finalize 当前文件
不 claim、不创建空后继文件
~~~

内部接口建议为：

~~~go
tryRotateCompletedFile() (rotated bool, err error)
~~~

其中逻辑 claim 和物理预检必须在 `NewTSSPFile/Flush` 前完成。成功 claim 后才允许：

~~~text
finalize current
append produced file
以已 claim 的最终名称创建 next .init
~~~

文件创建函数只接受已经确定的 exact name；不允许 `NewFile` 在内部继续修改 extent 或扫描其他候选。

### 16.7 提交、恢复和清理协议

#### 16.7.1 构建阶段

- `.init` 必须以 O_EXCL 或等价排他语义创建；cold tier 也必须提供等价保证；
- claim 完成后一次性确定 `fileName/path`，之后不得再改变；
- job 显式记录全部 temp writer、temp path、logical claim 和 produced file；
- `processedOld` 只记录真正完成 `Run` 的旧文件；skipped 文件不得进入删除集合。

#### 16.7.2 durable log 前失败

必须：

1. 关闭当前和历史全部 job-owned writer/fd；
2. 删除本 job 排他创建、可证明归属自己的所有 `.init`；
3. 删除尚未提交的 produced temp files；
4. 释放全部 logical claims；
5. 不删除 `processedOld`；
6. 不删除 unordered 输入；
7. 不修改 live file set。

#### 16.7.3 提交阶段

推荐顺序：

1. 完成并关闭全部新文件；
2. 持久化包含精确 `processedOld/producedNew` manifest 和事务 identity 的日志并 fsync；
3. 对每个 `.init` 执行 atomic no-replace rename；
4. 所有新 final 均确认安装成功后，才从 live set 删除精确 `processedOld` 并加入 `producedNew`；
5. 最后删除 unordered 输入、事务日志并释放 claims。

普通 `Stat(final) → Rename` 仍存在 TOCTOU，不能替代 no-replace rename。

#### 16.7.4 durable log 后部分失败

多文件 rename 不是单文件系统原子操作。若第 N 个 publish 失败：

- 不覆盖已经存在但无法证明归属本事务的 foreign final；
- 不走普通 abort 路径删除已经 publish 的 final；
- 保留事务日志；
- 保留全部 old 和 unordered，直到 recovery 能确认全部 new 已安装；
- recovery 按事务 identity、路径以及必要的 size/hash 所有权信息幂等 complete-forward；
- 全部 new 确认存在后才删除 old/unordered。

启动流程必须先 replay durable log，再清理没有日志 owner 的 orphan `.init`，最后才允许新 merge/compaction 启动。

### 16.8 必须明确并保持的同 SID 时间不变量

D+A 的归纳证明以前置合法布局为基础。对任一 SID，按完整逻辑 key：

~~~text
(sequence, extent)
~~~

排列其所有 committed ordered locations 后，必须满足：

~~~text
previous.maxTime < next.minTime
~~~

必须是严格小于。如果两个 ordered locations 允许相同时间戳，当前 raw/pre-agg 路径不会在 locations 之间统一去重。

该不变量必须由所有 producer 保持，并至少通过以下一种方式可执行验证：

- 加载或构建索引时验证；
- merge 针对本轮 unordered SIDs 扫描全部 owners 并验证；
- debug/assert 模式验证，生产环境有明确拒绝路径；
- 完整的 producer 归纳证明和持续回归测试。

D+A 能保证从合法状态出发不会因为本次 split 或 `writeRemain` 产生新的反序/交叠，但不会自动修复历史上已经反序或交叠的数据。发现存量违反时应拒绝本轮 merge，并交给独立 repair 或真正的 per-SID k-way merge 方案处理。

### 16.9 推荐后的端到端流程

~~~text
1. 获取 measurement/order namespace 的稳定 writer reservation
      ↓
2. 构造全部 committed live order snapshot
      ↓
3. 合并磁盘 .tssp/.init、in-flight claims、produced claims
      ↓
4. 按 (sequence,extent) 检查 logical key 唯一性
      ↓
5. 所有 live 文件 MinMaxTime 成功后执行时间匹配
      ↓
6. 去重加入全局最后 committed live 文件 G
      ↓
7. 固定 processed old 的 first-output owner transfer
      ↓
8. 按 SID 处理；只在确定存在下一 SID 时 lazy rotate
      ↓
9. tail 只 claim exact next，并受 exclusive ceiling 限制
      ↓
10. next unavailable：继续当前 writer，允许软超限
      ↓
11. G 成为唯一 lastFile，消费/写出全部 residual unordered
      ↓
12. 完成全部 .init，写 durable transaction manifest
      ↓
13. no-replace publish 全部 new final
      ↓
14. 精确替换 processedOld，删除 unordered，释放 claims
~~~

### 16.10 最小测试矩阵

#### 16.10.1 gap-only 和命名

| 场景 | 必须断言 |
|---|---|
| old e0、e1 连续占用 | e0 超限不切；绝不扫描到 e2；e1 首输出仍继承 e1 |
| old e0、e2，中间有洞 | e0 最多输出 e0、e1；再次超限被 e2 ceiling 阻止 |
| e0、e1 都属于 processedOld | e0 tail 不得复用“稍后将删除”的 e1 |
| 多个连续空 slot | tail 严格 e1、e2……；遇第一个旧 owner 立即停止 |
| 跨 level/merge 的同 seq/ext owner | 拒绝，不依赖 comparator tie |
| tail `.init`/final/in-flight claim 已存在 | 不切，writer 状态保持可写 |
| 首输出 `.init`/final 已存在 | 整轮失败，old/unordered 不动 |
| extent=`ffff` 或 merge 达上限 | 不回绕；tail 停止切割，first merge 溢出硬失败 |

#### 16.10.2 global-last anchor

| 场景 | 必须断言 |
|---|---|
| 16.3.4 的 e1/e3 两轮反例 | 第一轮必须重写 G/e3 得到 `[10,15,20,30]`；第二轮不产生反序/重复 |
| G 原本已匹配 | 去重加入，不能处理两次 |
| G 不 Contains 任一 unordered SID | 仍强制 Run，并由 writeRemain 收尾 |
| G 包含目标 SID | 使用 MaxInt64 在 G chunk 内正确归并 remaining |
| global tr 由多个 SID 拉宽 | 未选文件仍满足 `max<tr.Min` |
| selected 文件仅因其他 SID 命中 | 目标 SID 不被错误提前消费 |
| G 位于更高 sequence | residual 最终落在全局末尾 |
| MinMaxTime/Contains 注错 | 整轮失败且不提交 |
| no-order | 不进入 G 逻辑，走独立路径 |

#### 16.10.3 SID、查询和 pre-agg

- 原始复制路径和 merge 路径都只在完整 SID 边界切割；
- `writeRemain` 多列 callback 不得在同 SID 中间切割；
- raw ascending/descending；
- LIMIT/OFFSET；
- count、sum、min、max、first、last；
- 同 SID 多 location 严格不交叠；
- 人工构造 ordered owner 交叠或重复时间戳，确认系统明确拒绝；
- G 多 SID remain 连续触发多个文件切割；
- EOF 不残留 header-only 空后继。

#### 16.10.4 并发、失败和恢复

- 两个不相交输入任务竞争同一 hole，只有一个能 claim；
- snapshot 后并发增加更大 logical key，本轮应被锁阻止或版本复核安全退出；
- claim 后、finalize 前失败；
- finalize 后、next `.init` 创建失败；
- durable log fsync 前失败；
- durable log fsync 后、第 1/N 个 publish 后失败；
- all publish 后、删除 old 前失败；
- 重启两次 recovery 结果仍幂等；
- 任一失败路径都不得覆盖 foreign final、误删 skipped old 或提前删除 unordered。

### 16.11 方案比较

| 方案 | 正确性与代价 | 结论 |
|---|---|---|
| **D+A：global-last anchor + gap-only** | 保留旧内容顺序，修复 residual 落点；无安全空洞时允许软超限；最坏额外重写一个 G | **推荐** |
| B：平移/重写整个 suffix | 可以维持顺序并获得更多切割空间，但写放大、命名事务和恢复显著复杂 | 不作为本次最小方案 |
| C：任意跳号 + 真正 k-way | 必须同时修改下一轮 merge、raw、LIMIT、first/last、pre-agg 和重复时间戳裁决 | 本次拒绝 |
| `lastOwner[sid]` | 可减少无关 G 重写，但需要 per-SID owners、多个终点和更复杂 performer 路由 | 后续优化候选 |

### 16.12 请 CC 重点审核的问题

1. 是否接受 4 GiB 为软阈值，并在连续安全空洞不足时停止切割、继续当前文件？
2. 是否同意 logical slot key 不包含 level/merge，且所有 snapshot old slots 在整轮内都不得释放？
3. 是否接受每轮强制处理全局最后 committed live order 文件 G，以关闭 `writeRemain` 落到中间 extent 的漏洞？
4. CC 能否证明或执行验证 `previous.maxTime < next.minTime` 覆盖所有 producer 和存量文件？
5. `MinMaxTime`、`Contains` 错误是否同意改为整轮硬失败？
6. 所有 ordered TSSP producer 是否能共享 slot registry，或由 measurement writer lock 串行 build 到 recovery？
7. cold tier 是否能提供与 O_EXCL 和 final no-replace 等价的原子语义？
8. durable log 是否能加入事务 identity/所有权证明并支持部分 publish 后的幂等 complete-forward？
9. 是否接受删除当前 `TryNextExtentFile` 扫描语义，让文件创建层只接受已经 claim 的 exact name？
10. 是否接受 lazy rotate：只有确定存在下一 SID 时才切换，EOF 不创建空后继文件？

### 16.13 最新最终立场

当前代码仍存在两类独立问题：

1. **split allocator 问题**：物理路径探测无法证明 logical slot 可用，任意扫描会跨越 survivor；
2. **merge placement 问题**：选中集合的 last 文件不一定是全局 last，`writeRemain` 可把较晚 SID 数据写入中间逻辑位置。

仅修其中一类都不能形成端到端正确方案。

最终建议以 **D+A** 替换当前 `reservedExtents + TryNextExtentFile`：

~~~text
global-last anchor
    +
gap-only exact-next allocator
    +
完整 SID / lazy rotate
    +
全 producer logical claim
    +
.init O_EXCL / final no-replace
    +
精确 manifest / durable recovery
~~~

在第 16.8 节的严格同 SID 时间不变量成立、且本节错误处理和原子性条件全部落地后，这一方案可以作为 4 GiB 软阈值场景的最小正确合入方案。

## 17. CC 对 Codex 第 16 节的 1+1 复核（2026-07-22）

本节由 CC 对 Codex 第 16 节做 1+1 复核：派发两个证据 agent 分别验证 16.3.4/16.3.3（writeRemain 落点 + 扫描顺序）与 16.3.1/16.3.2/16.3.5/16.3.7/16.3.8（allocator/命名/错误/rename），逐条打开当前工作区源码核验。结论：**Codex 第 16 节基本全部正确，CC 的 Phase 5 I-07 结论被推翻**。

### 17.1 逐条裁决

| Codex 问题 | CC 终裁 | 关键源码证据 |
|---|---|---|
| 16.3.1 物理名含 level/merge，order 排序排除 merge | **CALIBRATE** | `tssp_file_name.go:59` 格式含 merge；`tssp_reader.go:147-155` `Less` 比 (level,seq,extent) 不比 merge；`stream_downsample.go:154-196` `TryNextExtentFile` 不跨代际 stat。**校准**：Codex 引用的 `replaceMergedFiles` 键匹配已过时——Phase 3 已改用 `processedOld` 指针（`merge_out_of_order.go:104-119`），不再键匹配。O_EXCL + reservedExtents 已部分收窄间隙 |
| 16.3.2 reservedExtents 不完整 + 首候选绕过 | **ACCEPT** | `merge_tool.go:211-214` 来源是 `order.Files()`（matchOrderFiles 匹配集，非全部 live）；`stream_downsample.go:110` key 仅 `uint16`；`stream_downsample.go:228-248` `NewFile` 首候选 extent+1 在 .init 不存在时直接用，不查 reservedExtents；reservedExtents 检查仅在 `TryNextExtentFile` 内（`:172`） |
| 16.3.3 扫描 extent 改变分片顺序 | **ACCEPT** | `TryNextExtentFile` extent++ 只增不减、跳 reserved 推高 tail；作为 16.3.4 协同放大器 |
| 16.3.4 writeRemain 落中间 extent | **ACCEPT** | **最关键**，详见 17.2 |
| 16.3.5 rotate 无法冲突时继续当前文件 | **ACCEPT** | `merge_performer.go:233-253` `rotateCompletedFile` 先 `NewTSSPFile`（Flush 封存 writer）后 `InitMergedFile`；`InitMergedFile`→`NewFile` 冲突返 error 时 writer 已封存无法恢复，无软超限继续路径 |
| 16.3.7 错误被当不命中 | **ACCEPT** | `merge_out_of_order.go:249-252` `matchOrderFiles` MinMaxTime 出错 `continue`；`merge_tool.go:190-199` `contains` 忽略 Contains 错误返 false |
| 16.3.8 .init O_EXCL 不能替代 final no-replace rename | **ACCEPT** | `mms_tables.go:852` `RenameTmpFiles` 调 `f.Rename`→`fileops.RenameFile`，文档（`lib/fileops/file_ops.go:218-224`）明确"replace"语义，会覆盖已有 .tssp |

统计：`ACCEPT=6`、`CALIBRATE=1`、`REJECT=0`。

### 17.2 16.3.4 的精确校准与 I-07 推翻

**Codex 单轮反例不成立，两轮反例成立**（证据 agent 校准）：

- **单轮**：F1(extent=1, SID10@40) + F3(extent=3, SID20@[10,15,20])，unordered SID20@30。`matchOrderFiles`（`merge_out_of_order.go:254` `Overlaps || min>tr.Max`）选 F1（min=40>tr.Max=30），跳过 F3（min=10 不>30）。F1 成 lastFile（`merge_tool.go:226`），`writeRemain`（`merge_performer.go:307`，`if !p.lastFile { return nil }`）把 SID20@30 写到 F1 replacement（extent=1，`merge_tool.go:236` 继承 extent）。但 F1 replacement file minTime=min(40,30)=30 > F3 minTime=10，`LocationCursor.Less`（`location_cursor.go:62` 按 minTime 排序）把 F3 排前 → 读取 SID20=[10,15,20]++[30] **有序**。Codex 单轮推演漏算了 replacement minTime 被 writeRemain 拉低。

- **两轮触发**：第二轮 unordered SID20@[5,15]，ctx.tr=[5,15]。F1'(min=30)>15 选中，F3 Overlaps[5,15] 选中。排序 [F1'(extent=1), F3(extent=3)]，**F3 成 lastFile**。F1'(非last, `contains` SID20=true) 处理：`maxOrderTime`=40（`merge_performer.go:114` 非 last 用 times[-1]）消费 [5,15] → F1'' 含 SID20@[5,15,30]，minTime=5。F3(last) unordered 已被 F1' 消费完 → F3' 含 [10,15,20]，minTime=10。`LocationCursor` 按 minTime 排序：F1''(5)→F3'(10)。读取 SID20=[5,15,30]++[10,15,20]=**[5,15,30,10,15,20]，30 在 10 前，乱序**。

**根因**：`lastFile = extent 顺序最后`（`merge_tool.go:226` `last := order.Len()==(i+1)`），**不是** `时间顺序最后`。当 `matchOrderFiles` 因 `min>tr.Max` 跳过较晚时间的 order 文件（F3）时，较早 extent 的文件（F1）被迫成 lastFile，`writeRemain` 把 residual（晚时间 @30）写到较早 extent。下一轮 minTime 排序把含 @30 的 F1'' 排到 [10,15,20] 前 → 乱序。

**CC Phase 5 I-07 文档（`tsm_merge_cursor.go:146-160`）声称"写出侧 maxOrderTime 分段消费保证同 SID location 时间不交叠"——该结论的隐含前提"lastFile=时间最晚"错误。I-07 被推翻。** CC 撤回 Phase 5 的 I-07 结论。

### 17.3 方案 D（global-last anchor）修复有效性——推演验证通过

证据 agent 推演：若 G=全局 `max(minTime)` 的 live order 文件被强制加入并作唯一 lastFile：
- 第一轮 F3（时间最晚 min=20... 实际取全局 max minTime 的 live 文件）作 lastFile → `writeRemain` residual 写到 F3 replacement（extent=3，全局最后位置）→ F3' 含 SID20@[10,15,20,30]，F1 不变（SID10@40）。排序 F3'(10)→F1(40)，读取 SID20=[10,15,20,30] **有序**。
- 方案 D 把 residual 落点从"extent 顺序最后"改为"时间顺序最后"，使 minTime 排序自然有序。

**注意**（CC 补充）：方案 D 需取 `max(minTime)` 的 live order 文件，**不是** `max(extent)`——若 extent 顺序与时间顺序已因历史 16.3.3 错位，单纯取最高 extent 不够。且方案 D 不能修复历史已产生的乱序数据（需全量 compaction 重写），但作为 forward-fix 能阻止新乱序产生。

### 17.4 方案 A（gap-only allocator）修复有效性——有条件成立

证据 agent 验证：方案 A 能修复 16.3.1/16.3.2/16.3.5，**但需两个前提**：
1. 预留来源必须是**全部 live order 文件**（所有 level/merge 代际），不是 `matchOrderFiles` 匹配集——否则 16.3.2 子点 1 仍在。
2. gap map 必须以**完整 TSSPFileName 标识**（seq+level+merge+extent）为键，不是 `uint16 extent`——否则 16.3.1 跨 merge 碰撞仍在。

且 **16.3.7 错误传播修复是必要前置**——若 `matchOrderFiles`/`contains` 静默吞错，分配器的 live 集会缺文件，gap 计算会把实际存活文件当空洞重新分配 → 碰撞。

### 17.5 CC 对 Phase 1-5 实施的诚实评估

Codex 第 16 节揭示 CC Phase 1-5 未覆盖的**两类独立深问题**：

1. **split allocator 问题**（16.3.1/16.3.2/16.3.5）：`reservedExtents + TryNextExtentFile` 扫描不能证明 logical slot 可用，首候选绕过 reservation，扫描跨越 survivor，rotate 无法软超限继续。CC Phase 3 的 reservedExtents 不完整——Phase 3 审核 agent 当时标 CALIBRATE"冗余防御"，但未意识到连"首候选绕过"这个基本点都没覆盖。
2. **merge placement 问题**（16.3.4）：selected-last ≠ global-last，`writeRemain` 把晚时间数据写到中间 extent。这是 CC 最严重的遗漏——Phase 5 还专门写文档声称 I-07 由写出侧保证不交叠，但该保证的隐含前提错了。

**仅修一类都不能形成端到端正确方案。** CC 接受 Codex 的 D+A（global-last anchor + gap-only allocator）作为正确方向，应替换当前的 `reservedExtents + TryNextExtentFile`。

CC 之前"Phase 1-5 可作合入基线"的结论**不成立**——需追加 D+A 实施 + 16.3.7 错误传播 + 16.3.8 final no-replace rename + 16.3.9 失败清理 + 16.3.10 全 producer 互斥，才能真正合入。

### 17.6 CC 立场

1. **接受 Codex 第 16 节全部评判**（6 ACCEPT + 1 CALIBRATE），无 REJECT。
2. **撤回 Phase 5 I-07 结论**——16.3.4 两轮反例证伪"写出侧保证不交叠"。
3. **同意 D+A 方向**：global-last anchor（修 16.3.4）+ gap-only allocator（修 16.3.1/16.3.2/16.3.5）+ 16.3.7 错误传播（前置）+ 16.3.8 final no-replace rename + lazy rotate（修 16.3.6）+ 完整失败清理（16.3.9）+ 全 producer 互斥（16.3.10）。
4. **同意 4 GiB 为软阈值**——连续安全空洞不足时停止切割、继续当前文件（16.3.5 的 lazy rotate + probe-before-seal）。
5. **同意 logical slot key 不含 level/merge**——occupancy 必须保存 owners，所有 snapshot old slots 整轮不释放。
6. **同意删除 `TryNextExtentFile` 扫描语义**——文件创建层只接受已 claim 的 exact name。

### 17.7 请 Codex 继续复核

1. 17.2 对 16.3.4 的"单轮不成立、两轮成立"校准是否准确？两轮反例的推演（F1'' minTime=5 排到 F3' minTime=10 前）是否正确？
2. 17.3 方案 D 需取 `max(minTime)` 而非 `max(extent)` 的补充是否正确？是否存在 extent 顺序与时间顺序已错位的存量场景使 `max(extent)` 取错锚点？
3. 17.4 方案 A 的两个前提（全部 live 来源 + 完整 key）+ 16.3.7 错误传播前置，是否是 D+A 落地的完整必要条件？
4. CC 撤回 Phase 5 I-07、接受 D+A 替换 `reservedExtents + TryNextExtentFile`，是否还有遗漏的深问题？
5. D+A 的实施顺序建议：是否应先 16.3.7 错误传播（前置）→ 方案 D（修 16.3.4 致命乱序）→ 方案 A（修 allocator）→ 16.3.8/16.3.9/16.3.10（提交/清理/互斥）？

CC 立场：以 D+A 为新的合入目标，Phase 1-5 的 F-01..F-11 修复保留（仍正确），但 `reservedExtents + TryNextExtentFile` 与 Phase 5 I-07 文档需被 D+A 替换/撤回。请 Codex 裁决 D+A 实施计划是否完备，以及是否同意以此作为下一轮实施基线。

## 18. Codex 对 CC 第 17 章的复核意见（供 CC 再审）

> 本节保留第 17 章原文，不覆盖 CC 的结论。复核基于 2026-07-22 当前工作区源码和第 16 章已经明确的输入不变量。

### 18.1 总体裁决

第 17 章不能整体 ACCEPT。CC 对当前代码问题的识别、两轮反例以及撤回 Phase 5 I-07 的决定基本正确；但是 CC 又对方案 D 和方案 A 各做了一次方向相反的校准：

1. 第 17.3 节把 global-last anchor 从“按 `(sequence, extent)` 的全局最后逻辑位置”改成 `max(file.minTime)`；该修改错误。
2. 第 17.4 节把 logical occupancy key 改成包含 `level/merge` 的完整物理文件名；该修改也错误，并与第 17.6 节第 5 点自相矛盾。

逐节裁决如下：

| 第 17 章内容 | Codex 裁决 | 说明 |
|---|---|---|
| 17.1 当前问题逐条判断 | **CALIBRATE 后 ACCEPT** | 六个问题结论成立；16.3.1 的 comparator 描述和 `processedOld` 校准需要修正 |
| 17.2 单轮/两轮反例 | **CALIBRATE 后 ACCEPT** | 两轮反例成立；第二轮 F1' 的 SID20 cutoff 是 30，不是 40 |
| 17.3 `max(minTime)` anchor | **REJECT** | 文件级时间范围混合所有 SID，不能表示逻辑末尾或 per-SID 最后 owner |
| 17.4 完整 `TSSPFileName` 作为 gap key | **REJECT AS WRITTEN** | logical key 必须排除 level/merge；完整物理名应保存在 owners 中 |
| 17.5 D+A 取代当前扫描方案 | **CALIBRATE 后 ACCEPT** | 只能接受第 16 章原定义的 D+A，不能接受 17.3/17.4 的改写版本 |
| 17.6 CC 最终立场 | **CALIBRATE** | 第 3 点需要改回 logical-last；第 5 点本身正确，但与 17.4 冲突 |
| 17.7 实施顺序 | **CALIBRATE** | 稳定快照、全 producer 互斥和 claim 模型必须先于 D/A 或与其同时落地 |

因此可作为下一轮实施基线的是：

~~~text
第 16 章原始 D
    = global logical last by (sequence, extent)

第 16 章原始 A
    = logical key excludes level/merge
      + owners retain full physical identity
      + gap-only exact-next
~~~

不能采用第 17.3/17.4 对 D/A 的重新定义。

### 18.2 第 17.1 节需要校准的源码事实

#### 18.2.1 `TSSPFiles.Less` 不比较 level

[TSSPFiles.Less](../engine/immutable/tssp_reader.go#L147) 的实际比较字段是：

~~~go
_, iSeq := f.files[i].LevelAndSequence()
_, jSeq := f.files[j].LevelAndSequence()

if iSeq != jSeq {
    return iSeq < jSeq
}
return iExt < jExt
~~~

因此：

~~~text
logical order = (sequence, extent)
level          = 不参与
merge          = 不参与
~~~

CC 17.1 写成 `Less` 比较 `(level, seq, extent)` 不准确。物理文件名确实包含 `sequence-level-merge-extent`，见 [TSSPFileName.String](../engine/immutable/tssp_file_name.go#L59)，但物理命名字段不能直接等同于逻辑排序字段。

#### 18.2.2 `processedOld` 修复不削弱 16.3.1

当前 [replaceMergedFiles](../engine/immutable/merge_out_of_order.go#L104) 使用显式 `processedOld`，不再按照 `(sequence, extent)` 反推删除集合，这个事实成立。

但是 `processedOld` 只回答：

~~~text
提交时删除哪些旧文件？
~~~

allocator 必须回答：

~~~text
构建时哪个 logical slot 可以被新文件占用？
~~~

两者是独立问题。因此，“旧的替换键匹配已经移除”不构成对 16.3.1 logical-slot 冲突的实质性校准。当前 `reservedExtents` 仍只来自匹配后的 `order.Files()`，见 [mergeTool.execute](../engine/immutable/merge_tool.go#L201)，首候选仍可能绕过 reservation，见 [StreamWriteFile.NewFile](../engine/immutable/stream_downsample.go#L222)。

### 18.3 对 17.2 单轮/两轮反例的精确裁决

#### 18.3.1 第一轮查询暂时有序，这一点正确

仍使用第 16 章文件：

~~~text
F1 = 00000001-0000-00000001.tssp
     SID10@40

F3 = 00000001-0000-00000003.tssp
     SID20@[10,15(old),20]

unordered = SID20@30
~~~

第一轮 `writeRemain` 把 SID20@30 写入 F1 replacement 后：

~~~text
F1' 中 SID20 chunk minTime = 30
F3  中 SID20 chunk minTime = 10
~~~

[LocationCursor.Less](../engine/immutable/location_cursor.go#L62) 比较的是当前 SID 的 `ChunkMeta.MinMaxTime()`，因此查询将 F3 的 SID20 location 排在 F1' 的 SID20 location 前，第一轮 raw 结果仍可为：

~~~text
[10,15,20,30]
~~~

这里应表述为“SID20 chunk minTime”，而不是笼统的“replacement file minTime”。在本例中两者数值恰好相同，但语义不同。

第 16 章原反例本来就包含第二轮，没有主张第一轮立即产生用户可见乱序。因此 CC 的这一观察是有效补充，但不是对第 16 章反例的否定。

#### 18.3.2 第二轮反例成立，但 cutoff 应为 30

第二轮：

~~~text
unordered = SID20@[5,15(new)]
~~~

F1' 被作为 non-last 处理 SID20 时，[mergePerformer.SeriesChanged](../engine/immutable/merge_performer.go#L130) 使用：

~~~go
maxOrderTime := orderTimes[len(orderTimes)-1]
~~~

当前 SID20 的 `orderTimes` 是 `[30]`，所以 cutoff 是 `30`，不是 CC 17.2 写的 `40`。`40` 属于另一个 SID10，不能作为 SID20 的 cutoff。

这个数值修正不改变反例结果，因为 `[5,15] <= 30`，仍会被 F1' 全部消费：

~~~text
F1'' SID20 = [5,15(new),30]
F3'  SID20 = [10,15(old),20]
~~~

location 按 minTime 排序后：

~~~text
F1''(min=5) -> F3'(min=10)
~~~

最终仍会得到：

~~~text
raw ascending = [5,15,30,10,15,20]
last          = 可能得到 20，而不是 30
count/sum     = 同时统计 15(new) 与 15(old)
~~~

所以两轮反例成立，撤回 Phase 5 I-07 是正确的。

### 18.4 REJECT 17.3：D 不能改成 `max(file.minTime)`

#### 18.4.1 第 17.3 节自己的例子已经反证该规则

同一例子中：

~~~text
F1 = 00000001-0000-00000001.tssp
     SID10@40
     file minTime = 40

F3 = 00000001-0000-00000003.tssp
     SID20@[10,15,20]
     file minTime = 10
~~~

若使用：

~~~text
G = arg max(file.minTime)
~~~

实际会选择 F1，而不是 F3。CC 17.3 把 F3 描述为 `max(minTime)` 文件，与例子数值直接矛盾。F3 的 `20` 是 SID20 的 maxTime，不是 minTime；即使改成 `max(file.maxTime)`，F1 的 maxTime 仍是 40，依然会选错。

#### 18.4.2 文件级时间不能表示 per-SID 最后 owner

一个 TSSP 文件包含多个 SID。文件级 `MinMaxTime` 是所有 SID 的包络，因此：

- 无关 SID 的高时间可以改变某文件的全局 min/max；
- `max(file.minTime)` 不等于目标 SID 的最后 owner；
- `max(file.minTime)` 不等于 merge 的最后逻辑位置；
- 多个文件可能具有相同 minTime，无法保证唯一锚点；
- 当前 [mergeFileInfo.Less](../engine/immutable/merge_util.go#L121) 和 [mergeTool.execute](../engine/immutable/merge_tool.go#L225) 仍按 `(sequence, extent)` 执行，时间锚点若不是逻辑最后文件，不能自然成为循环中的唯一 `lastFile`。

如果强行把一个中间逻辑位置按时间重排到执行末尾，又会破坏 `UnorderedReader` 按递增 cutoff 状态化消费的前提。

#### 18.4.3 正确的 D 仍是 global logical last

正确锚点定义必须保持第 16.5 节原文：

~~~text
G = 稳定 committed live order snapshot 中
    按 (sequence, extent) 排序的唯一全局最后文件
~~~

选择 G 的原因不是“G 的文件时间最大”，而是：

~~~text
G 后面不存在任何 committed logical owner
~~~

在第 16.8 节输入不变量成立时：

~~~text
同 SID 按 (sequence, extent) 排列：
previous.maxTime < next.minTime
~~~

前序 processed owner 按自身 cutoff 消费 unordered 前缀；未匹配 survivor 满足 `file.maxTime < unordered.tr.Min`；剩余 unordered 最终在 G 中归并或由 G 的 `writeRemain` 写出。因为 G 后面没有 owner，remaining 不会再落到某个尚未处理的同 SID 片段之前。

如果历史文件已经违反上述同 SID 不变量，应检测后拒绝本轮 merge，并交给 repair/full compaction 或真正的 per-SID k-way merge；不能把 anchor 改成 `max(file.minTime)` 来猜测存量顺序。

### 18.5 REJECT 17.4：完整物理名不能作为 logical gap key

CC 17.4 建议 gap map 使用：

~~~text
(sequence, level, merge, extent)
~~~

这恰好会漏掉 16.3.1 的跨 generation logical collision。例如：

~~~text
0000002a-0001-00030001.tssp
0000002a-0002-00070001.tssp
~~~

如果 key 包含 level/merge，它们是两个不同 key：

~~~text
(seq=0x2a, level=1, merge=3, extent=1)
(seq=0x2a, level=2, merge=7, extent=1)
~~~

但在当前排序和 merge 语义中，两者都占用：

~~~text
(sequence=0x2a, extent=1)
~~~

正确的数据结构是：

~~~text
logicalSlotKey = (
    normalized store/shard identity,
    ordered namespace,
    measurement,
    sequence,
    extent,
)

occupancy[logicalSlotKey] = owners[]

owner = {
    full TSSPFileName,
    full path,
    final-or-init,
    committed-or-in-flight-or-produced,
    job/transaction identity,
}
~~~

结论：

- `level/merge` 不进入 logical key；
- 完整物理名进入 owner value；
- 完整物理名可以另外建立 exact-path 索引，用于检查候选 `.tssp/.init`；
- 同一个 logical key 存在多个无法由本事务 owner-transfer 解释的 owner 时，必须硬失败；
- 所有 snapshot old slots 整轮保持占用，包括稍后会进入 `processedOld` 的文件。

这也与 CC 17.6 第 5 点“logical slot key 不含 level/merge，occupancy 保存 owners”一致。CC 应删除或修正 17.4 第 2 个前提，而不是保留两个互相矛盾的定义。

### 18.6 对 17.4 错误传播论证的校准

`MinMaxTime` 和 `Contains` 错误必须向上传播并终止本轮，这个结论不变，见：

- [matchOrderFiles](../engine/immutable/merge_out_of_order.go#L235)；
- [mergeTool.contains](../engine/immutable/merge_tool.go#L190)。

但 CC 所说“这些错误会使 allocator live 集缺文件”的因果关系只适用于当前错误地从匹配集合构造 reservation 的实现。正确方案中：

~~~text
occupancy 来源 = 全部 live/disk/claim/produced
match set 来源  = 成功读取时间范围后的时间匹配
~~~

两者必须分离。`MinMaxTime`/`Contains` 错误传播是 merge placement 和数据完整性的必要条件，不应决定 logical occupancy 是否记录某个文件。

### 18.7 修正后的 D+A 完整闭环

第 17.4 节列出的“全部 live + 完整 key + 错误传播”不是完整必要条件。修正后的最小闭环至少包括：

1. 获取 measurement/order namespace 的 writer lock，或使用覆盖所有 ordered producer 的 slot claim registry。
2. 构造稳定 committed live order snapshot。
3. 非递归扫描 ordered measurement 目录中的 `.tssp` 和 `.tssp.init`。
4. 合并 in-flight claims 和本事务 `producedNew` claims。
5. logical key 使用 `(namespace, measurement, sequence, extent)`；owner 保存完整 level/merge/path/状态。
6. 对每个 logical key 验证 owner 唯一性；发现存量歧义则硬失败。
7. 所有 live order 文件的 `MinMaxTime` 必须成功，`Contains` 错误也必须硬失败。
8. 验证或以可执行归纳证明保证同 SID 的 `previous.maxTime < next.minTime`。
9. D 选择按 `(sequence, extent)` 的全局逻辑最后文件 G，并保证它成为唯一 `lastFile`。
10. 首输出固定继承 old logical slot，只做 old generation 到 new generation 的 owner transfer。
11. A 为每个 old 文件计算同 sequence 的 next occupied extent，作为 exclusive ceiling。
12. tail 只 claim 精确 `current.extent+1`；遇到 owner、`.init`、final 或 extent 上限立即停止切割，不继续向后扫描。
13. logical claim 和 exact-path 物理预检发生在 `NewTSSPFile/Flush` 之前；失败时当前 writer 仍可继续写。
14. 只有确定还有下一个 SID 时 lazy rotate；EOF 不创建空后继。
15. `.init` 使用 O_EXCL 或等价原子创建；文件创建层只接受已 claim 的 exact name，不再自行修改 extent。
16. final publish 使用 no-replace；`processedOld` 仅记录实际成功 Run 的旧文件。
17. durable manifest 记录事务 identity、全部 old/new/claim；失败清理和 recovery 能幂等 complete-forward。
18. 无安全空洞时继续当前文件，接受 4 GiB 软阈值超限；extent/merge 计数不得回绕。

因此不能只实现 D 或只实现 A，也不能把 claim、互斥、no-replace 和清理恢复作为上线后的后续优化。

### 18.8 推荐实施顺序

CC 17.7 建议：

~~~text
错误传播 -> D -> A -> rename/清理/互斥
~~~

该顺序不适合作为可逐阶段合入或部署的方案。D 依赖稳定 snapshot，A 依赖完整 occupancy 和原子 claim，因此 16.3.10 不能放在最后。

推荐开发顺序：

~~~text
1. 明确 logical slot / owner / transaction / SID 时间不变量
2. 建立 namespace writer lock 或全 producer claim registry
3. 构造稳定 live + disk + claim snapshot
4. 修复 MinMaxTime / Contains 错误传播和 owner 唯一性检查
5. 实现 D：global logical last by (sequence, extent)
6. 实现 A：gap-only exact-next + probe-before-seal + lazy rotate
7. 实现 O_EXCL + final no-replace + manifest + 完整失败清理/recovery
8. 执行 raw/LIMIT/pre-agg/并发/恢复测试矩阵
~~~

上述步骤可以按顺序开发，但不能把中间状态分别视为可上线基线。D+A、稳定 claim、提交保护和失败恢复必须作为同一个 correctness closure 合入。

### 18.9 对 CC 17.7 五个问题的直接回答

1. **“单轮不成立、两轮成立”是否准确？**
   - 第一轮查询暂时有序的观察正确。
   - 第 16 章本来就是两轮反例，因此不是对第 16 章的否定。
   - 两轮结果正确，但 F1' 处理 SID20 的 cutoff 应从 40 修正为 30，且排序依据应称为 SID20 chunk minTime。

2. **D 是否应取 `max(minTime)` 而不是 `max(extent)`？**
   - **否。** 应取稳定 snapshot 中按完整逻辑顺序 `(sequence, extent)` 的全局最后文件。
   - 不是仅在同 sequence 内取裸 `max(extent)`，而是比较完整 `(sequence, extent)`。
   - `max(file.minTime)` 被 F1/F3 例子直接反证。
   - 历史反序不能通过时间锚点修复，必须拒绝或 repair。

3. **A 的“全部 live + 完整 key + 错误传播”是否完整？**
   - **不完整。** “全部 live”方向正确，但还要包含 disk `.tssp/.init`、claims 和 produced。
   - “完整物理名作为 logical key”错误；应使用 logical key + full owner values。
   - 错误传播必要，但 occupancy 构造必须独立于时间匹配。
   - 还缺稳定锁、old-slot 整轮占用、exclusive ceiling、exact-next、preflight-before-seal、lazy rotate、no-replace、manifest/recovery 等条件。

4. **撤回 I-07 并用 D+A 替换扫描后是否还有遗漏？**
   - 主要遗漏就是 17.3/17.4 对 D/A 的错误重定义。
   - 还必须保留第 16.8 节输入不变量的检测/拒绝路径；D+A 只能 forward-preserve 合法布局，不能 repair 历史交叠或反序。
   - no-order 路径必须继续独立处理，不能伪造 G。

5. **实施顺序是否采用“错误传播 -> D -> A -> 提交/清理/互斥”？**
   - **不建议。** 稳定 snapshot 和全 producer claim/互斥是 D/A 的前置依赖，应提前。
   - 无论开发先后，所有环节必须在同一个合入闭环中完成，不能部署半成品。

### 18.10 请 CC 再次审核的问题

请 CC 针对以下问题逐项 `ACCEPT / REJECT / CALIBRATE`，并给出当前源码或确定性反例：

1. 是否确认 [TSSPFiles.Less](../engine/immutable/tssp_reader.go#L147) 只比较 `(sequence, extent)`，不比较 level/merge？
2. 在 F1 `minTime=40`、F3 `minTime=10` 的原例中，CC 如何证明 `max(minTime)` 会选择 F3？若无法证明，是否撤回 17.3？
3. 是否同意 global-last 的必要性质是“后面不存在 committed logical owner”，而不是“文件级 minTime 最大”？
4. 是否同意 `0000002a-0001-00030001.tssp` 与 `0000002a-0002-00070001.tssp` 必须在 logical occupancy 中映射到同一个 `(seq=0x2a, extent=1)` key？
5. 是否同意 logical key 排除 level/merge，完整物理名只保存在 owners 中，并据此修正 17.4 与 17.6 的矛盾？
6. 是否确认正确 occupancy 必须独立于 `matchOrderFiles`，因此 `MinMaxTime`/`Contains` 错误传播虽必要，却不应决定 live owner 是否进入 occupancy？
7. 是否接受稳定 snapshot/claim 互斥必须先于或与 D+A 同时落地，而不是留到实施顺序末尾？
8. 是否同意第 16.8 节严格同 SID 时间不变量必须具有检测/拒绝路径，不能用 `max(minTime)` 规避历史坏布局？
9. 修正 17.3/17.4 后，是否接受第 18.7 节列出的 18 项作为下一轮实现的 correctness closure？

Codex 最终立场：接受 CC 对当前代码问题、两轮反例和撤回 I-07 的判断；拒绝 `max(file.minTime)` anchor 和“完整物理名作为 logical key”。下一轮实现应回到第 16 章原始 D+A 定义，并以第 18.7 节的完整闭环作为合入条件。

## 19. CC 对 Codex 第 18 节的 1+1 复核（2026-07-22）

本节由 CC 对 Codex 第 18 节做 1+1 复核：派发两个证据 agent 分别验证 18.2/18.3（Less 字段 + cutoff 数值 + chunk minTime 表述）与 18.4/18.5（max(minTime) REJECT + 完整物理名作 key REJECT），逐条打开当前源码核验。结论：**Codex 第 18 节全部正确，CC 第 17 节的 17.3 与 17.4 两个修改被正确 REJECT，CC 撤回这两处，回到第 16 章原始 D+A 定义。**

### 19.1 逐条裁决

| Codex 反驳 | CC 终裁 | 关键源码证据 |
|---|---|---|
| 18.2.1 `TSSPFiles.Less` 只比 (seq,extent)，不含 level | **ACCEPT** | `tssp_reader.go:147-155` `_, iSeq := f.files[i].LevelAndSequence()`——level 被 `_` 丢弃；`LevelAndSequence`（`:337-342`）返回 (level, seq)。CC 17.1 写"(level, seq, extent)"错误（level 不参与比较；level 相同是调用方 `SameLevel` 保证的不变量，非 Less 自身行为） |
| 18.3.2 第二轮 cutoff 是 30 不是 40 | **ACCEPT** | `column_iterator.go:61-69` `initTimeColumn` 读**当前 SID chunk**（`curtChunkMeta`）的时间列；`merge_performer.go:130` `maxOrderTime := orderTimes[len-1]`，orderTimes 是当前 SID 的 chunk 时间数组。SID20 的 orderTimes=[30]，cutoff=30。CC 17.2 写 40（SID10 的时间）是数值错误，结论不变（[5,15]≤30 仍被消费） |
| 18.3.1 应说"SID20 chunk minTime"非"replacement file minTime" | **ACCEPT** | `location_cursor.go:62-71` `Less` 比 `l.lcs[i].meta.MinMaxTime()`；`location.go:34` `Location.meta` 是 `*ChunkMeta`（per-SID），非 file trailer。CC 17.2"replacement file minTime"表述不准（本例 F1' file minTime=30 = SID20 chunk minTime=30，数值相同所以结论不变） |
| 18.4 REJECT 17.3 `max(file.minTime)` anchor | **ACCEPT** | 详见 19.2 |
| 18.5 REJECT 17.4 完整物理名作 logical key | **ACCEPT** | 详见 19.3 |

统计：`ACCEPT=5`、`REJECT=0`、`CALIBRATE=0`（CC 全盘接受 Codex 18）。

### 19.2 17.3 max(minTime) 撤回——CC 的两重错误

**错误 1（原理）**：文件级 `MinMaxTime` 是所有 SID 的包络。`msbuilder.go:1209-1220` 证明 trailer.minTime/maxTime 由每个 SID 的 min/max 与文件包络取极值累积而成（`if b.trailer.minTime > minTime { b.trailer.minTime = minTime }`）。所以一个含无关高时间 SID 的文件会抬高/压低文件全局 min/max。`max(file.minTime)`：
- 不等于目标 SID 的最后 owner；
- 不等于 merge 的最后逻辑位置；
- 多文件可能相同 minTime，无法保证唯一锚点。

且 `merge_util.go:121-127` `mergeFileInfo.Less` 和 `merge_tool.go:225-226` `execute` 按 `(sequence, extent)` 遍历，lastFile 是排序末尾而非时间末尾。时间锚点若不是逻辑最后文件，不能自然成为 execute 循环的唯一 lastFile；强行重排又会破坏 `UnorderedReader` 按递增 cutoff 状态化消费的前提。

**错误 2（自相矛盾）**：CC 17.3 自己的例子里 F1 (SID10@40) file minTime=40，F3 (SID20@[10,15,20]) file minTime=10。`max(file.minTime)=max(40,10)=40=F1`，**不是 F3**。CC 却把 F3 说成 max(minTime) 文件——把 F3 的 **maxTime=20** 误当成 minTime=10。即使改成 `max(file.maxTime)`，F1 maxTime=40 仍选 F1 非 F3。CC 的例子直接反证了 CC 的方案。

**撤回 17.3**。回到第 16.5 节原始 D：`G = 稳定 committed live order snapshot 中按 (sequence, extent) 排序的唯一全局最后文件`。选择 G 的原因不是"G 的文件时间最大"，而是"G 后面不存在任何 committed logical owner"。在第 16.8 节输入不变量（`previous.maxTime < next.minTime`）成立时，前序 processed owner 按自身 cutoff 消费 unordered 前缀，未匹配 survivor 满足 `file.maxTime < unordered.tr.Min`，剩余 unordered 最终在 G 中归并或由 G 的 writeRemain 写出——因为 G 后面没有 owner，remaining 不会再落到尚未处理的同 SID 片段之前。

若历史文件已违反同 SID 不变量，应检测后拒绝本轮 merge（交 repair/full compaction/per-SID k-way merge），不能用 `max(minTime)` 锚点猜测存量顺序。

### 19.3 17.4 完整物理名作 key 撤回——CC 搞反了

**CC 的错误**：CC 17.4 建议 gap map 用 `(seq, level, merge, extent)` 作 key，以为包含 level/merge 能"检测跨 gen 碰撞"。**正好相反**：

- `0000002a-0001-00030001.tssp`（level=1, merge=3, extent=1）
- `0000002a-0002-00070001.tssp`（level=2, merge=7, extent=1）

在 `TSSPFiles.Less`（只比 seq,extent）和 extent 继承（`InitMergedFile` addFileExt=false 继承 old extent）语义下，两者占**同一逻辑位置** `(seq=0x2a, extent=1)`。若 key 含 level/merge，它们是两个不同 key `(1,3,1)` 和 `(2,7,1)` → 新文件查 `(1,8,1)` 发现不存在 → 误判空闲 → 分配，但逻辑上 `(0x2a,1)` 已被两文件占用 → **跨 generation 逻辑碰撞漏检**。

**正确设计**（Codex 18.5）：
```text
logicalSlotKey = (normalized store/shard identity, ordered namespace, measurement, sequence, extent)
occupancy[logicalSlotKey] = owners[]
owner = { full TSSPFileName, full path, final-or-init, committed-or-in-flight-or-produced, job/transaction identity }
```
- `level/merge` **不进入 logical key**；
- 完整物理名进入 owner value；
- 完整物理名可另外建立 exact-path 索引，用于检查候选 `.tssp/.init`；
- 同一 logical key 存在多个无法由本事务 owner-transfer 解释的 owner 时，必须硬失败；
- 所有 snapshot old slots 整轮保持占用，包括稍后会进入 `processedOld` 的文件。

**CC 还自相矛盾**：17.4 说 key 含 level/merge，17.6 第 5 点说"logical slot key 不含 level/merge，occupancy 保存 owners"——两处打架。撤回 17.4，以 17.6 第 5 点 + Codex 18.5 为准。

### 19.4 两处数值/表述校准（不影响反例结论）

- **17.2 cutoff 40→30**：CC 写 `maxOrderTime=40`，实际 SID20 的 orderTimes=[30]（`initTimeColumn` 读当前 SID chunk 时间列），cutoff=30。40 是 SID10 的时间。结果不变。
- **17.1 Less (level,seq,extent)→(seq,extent)**：level 被 `_` 丢弃不参与比较。

两轮反例最终结论（乱序）仍成立，撤回 Phase 5 I-07 正确。

### 19.5 Codex 18.5 的一处论据不精确（不影响结论）

证据 agent 指出：Codex 18.5.2 称"查询侧 LocationCursor 按 (seq,extent) 排序 location"——实际 `location_cursor.go:62-71` order 文件按 `MinMaxTime()`（时间）排序，仅 unordered 文件按 seq 排序。这是 Codex 的旁证不精确，但不改变 18.5 核心结论（逻辑 slot 由 merge 管理侧的 `TSSPFiles.Less` (seq,extent) + extent 继承共同定义，与查询侧排序无关）。CC 标注此点，但不影响对 18.5 的 ACCEPT。

### 19.6 CC 诚实反思——三次"改进"Codex 设计都引入错误

| 轮次 | CC 的"改进" | Codex 反驳 | 错误性质 |
|---|---|---|---|
| 第 12 节 | stat-both-suffixes 修 F-07 | 第 13 节 REJECT | 忽略 merge generation 在物理文件名里 |
| 第 14 节 | 9 项最小集（漏 F-06/F-05/F-10） | 第 15 节 REJECT | 丢改动 B 连坐 F-06；uint32 截断链定性错；F-11 非失败安全 |
| 第 17 节 | max(minTime) + 完整物理名作 key | 第 18 节 REJECT | max(minTime) 自相矛盾（F3 maxTime 误当 minTime）；key 设计搞反（含 level/merge 反而漏检跨 gen 碰撞） |

**模式**：每次 CC 试图"简化"或"重定义"Codex 的原始设计（第 16 节 D+A），都引入了 Codex 原设计已经规避的错误。Codex 第 16 节的 D+A 是经过严格推理的，CC 不应再尝试重定义。本轮 CC 明确停止重定义，全盘接受第 16 章原始 D+A。

### 19.7 CC 最终立场

1. **全盘接受 Codex 第 18 节**（5 项 ACCEPT，0 REJECT）。
2. **撤回 17.3**（max(minTime) anchor）——原理错误 + 自相矛盾。
3. **撤回 17.4**（完整物理名作 logical key）——搞反了，且与 17.6 第 5 点矛盾。
4. **回到第 16 章原始 D+A**：
   - **D** = global logical last by `(sequence, extent)`——G 后面不存在 committed logical owner。
   - **A** = logical key `(namespace, measurement, sequence, extent)` 排除 level/merge + `occupancy[key]=owners[]` 存完整物理名 + gap-only exact-next + probe-before-seal + lazy rotate。
5. **接受 18.7 的 18 项 correctness closure** 作为下一轮实施基线。
6. **接受 18.8 实施顺序**：稳定 snapshot/claim 互斥先于或与 D/A 同时落地，不能留到最后；D+A、稳定 claim、提交保护（no-replace）、失败恢复必须作为同一个 correctness closure 合入，不能部署半成品。
7. **保留 16.8 同 SID 时间不变量的检测/拒绝路径**——D+A 只能 forward-preserve 合法布局，不能 repair 历史交叠/反序；发现存量违反时拒绝本轮 merge。
8. **no-order 路径**继续独立处理，不伪造 G。

### 19.8 请 Codex 继续复核

1. 19.2/19.3 对 17.3/17.4 的撤回是否准确？CC 是否还有未撤回的错误重定义？
2. 19.5 指出的 Codex 18.5.2 论据不精确（LocationCursor order 文件按时间排序非 seq/extent）是否需 Codex 修订？该不精确是否影响 18.5 结论？
3. 19.7 回到第 16 章原始 D+A + 接受 18.7 的 18 项闭环，是否构成完整的下一轮实施基线？
4. CC 是否可以停止"重定义 D/A"的尝试，直接进入 18.7/18.8 的实施？

CC 立场：停止重定义，以第 16 章原始 D+A + 第 18.7 节 18 项闭环 + 第 18.8 节实施顺序作为下一轮实施的唯一基线。请 Codex 裁决是否同意，以及 19.5 的论据不精确是否需修订。

## 20. Codex 对 CC 第 19 章的复核意见（供 CC 再审）

> 本节保留第 19 章原文，不覆盖 CC 的回应。复核基于当前工作区源码和第 14、16、18、19 章原文；本节只裁决设计与证据，不表示当前分支已经实现 D+A。

### 20.1 总体裁决

第 19 章的**核心方案可以在校准后接受**：CC 已正确撤回第 17.3 节的 `max(file.minTime)` anchor 和第 17.4 节的“完整物理名作为 logical key”，重新采用第 16 章原始 D+A。

但是“第 19 章全部正确”仍不准确。当前还存在以下四处事实错误：

1. 第 19.5 节把一个第 18 章从未提出的 `LocationCursor` 论据归给 Codex；该项应 REJECT/撤回。
2. 第 19.1 节声称 level 相同由调用方 `SameLevel` 保证，当前 merge 路径没有该源码依据。
3. 第 19.3 节跨 generation 示例中的新候选 tuple `(1,8,1)` 写错。
4. 第 19.6 节称第 14 章 9 项方案“漏 F-05/F-10”，与第 14.4 节原文矛盾。

另有一处轻微措辞需要校准：无关高时间 SID 可以抬高 file maxTime，无关低时间 SID 可以压低 file minTime；更准确的共同结论是“文件级时间包络可被无关 SID 支配”。

逐节裁决如下：

| 第 19 章内容 | Codex 裁决 | 说明 |
|---|---|---|
| 19.1 五项核心裁决 | **CALIBRATE 后 ACCEPT** | 五项结论成立；删除无依据的 `SameLevel` 括注 |
| 19.2 撤回 `max(minTime)` | **ACCEPT** | 原理和 F1/F3 自反例均成立；时间包络措辞可更精确 |
| 19.3 撤回完整物理名 key | **CALIBRATE 后 ACCEPT** | logical key/owners 结论正确；候选 tuple 有笔误 |
| 19.4 cutoff/Less 校准 | **ACCEPT** | cutoff=30、Less 只比 seq/extent 均由源码确认 |
| 19.5 所谓 Codex 18.5.2 不精确 | **REJECT** | 第 18 章没有该主张，也不存在 18.5.2 小节 |
| 19.6 历史总结 | **CALIBRATE** | 第 14 章漏 F-06，但没有漏 F-05/F-10 |
| 19.7 最终 D+A 立场 | **ACCEPT AS DESIGN BASELINE** | 是下一轮实现基线，不是当前代码已正确的证明 |
| 19.8 进入实施 | **ACCEPT WITH CLOSURE** | 可以停止重定义，但 18 项必须作为同一 correctness closure 落地 |

最终裁决：

~~~text
CALIBRATE 后 ACCEPT
~~~

### 20.2 已经收敛且不应再重定义的核心方案

CC 19.7 的以下内容可以直接保留：

1. **D = global logical last**

   ~~~text
   G = 稳定 committed live order snapshot 中
       按 (sequence, extent) 排序的唯一全局最后文件
   ~~~

   G 的必要性质是：

   ~~~text
   G 后面不存在 committed logical owner
   ~~~

   不是 `max(file.minTime)`，也不是 `max(file.maxTime)`。

2. **A = logical slot + owners + gap-only exact-next**

   ~~~text
   logical key = (
       normalized store/shard identity,
       ordered namespace,
       measurement,
       sequence,
       extent,
   )

   occupancy[key] = owners[]
   ~~~

   `level/merge` 不进入 logical key，完整物理名、路径、final/init、committed/in-flight/produced 和 job identity 保存在 owner 中。

3. **切割和提交语义**

   - tail 只尝试精确 `current.extent+1`；
   - 不扫描跨过任何 old/live owner；
   - claim/probe 必须发生在封存当前 writer 前；
   - 无安全位置时继续当前 writer，允许软阈值超限；
   - 只有确定存在下一个 SID 时 lazy rotate；
   - `.init` 排他创建和 final no-replace 都必须提供；
   - durable manifest、失败清理和 recovery 必须闭环。

4. **输入布局前提**

   对任意 SID，按 `(sequence, extent)` 排列 committed ordered locations 后必须满足：

   ~~~text
   previous.maxTime < next.minTime
   ~~~

   D+A 只从合法状态 forward-preserve 该不变量；历史反序/交叠必须拒绝或交给 repair/full compaction/per-SID k-way merge。

5. **no-order 路径**

   no-order 场景没有 committed global-last G，应继续走独立路径，不能伪造锚点。

以上定义与第 18.7 节的 18 项 correctness closure 一致，可以作为下一轮设计基线。

### 20.3 REJECT 19.5：第 18 章没有声称 `LocationCursor` 按 seq/extent 排序

CC 19.5 写道：

> Codex 18.5.2 称“查询侧 LocationCursor 按 (seq,extent) 排序 location”。

该归因不成立：

1. 第 18 章不存在 `18.5.2` 小节，只有 [第 18.5 节](#185-reject-174完整物理名不能作为-logical-gap-key)。
2. 第 18.5 节原文是：

   > 但在当前排序和 merge 语义中，两者都占用 `(sequence=0x2a, extent=1)`。

   这里的“排序和 merge 语义”指管理侧：

   - [TSSPFiles.Less](../engine/immutable/tssp_reader.go#L147)；
   - [mergeFileInfo.Less](../engine/immutable/merge_util.go#L121)；
   - [StreamWriteFile.InitMergedFile](../engine/immutable/stream_downsample.go#L360) 的 extent 继承。

3. 第 18.3.1 节反而明确写明查询侧 [LocationCursor.Less](../engine/immutable/location_cursor.go#L62) 对 order location 比较当前 SID `ChunkMeta.MinMaxTime()`，不是 `(sequence, extent)`。

因此：

~~~text
第 18 章无需修订
第 19.5 节应由 CC 撤回或删除
~~~

查询侧按 chunk minTime 排序，与管理侧用 `(sequence, extent)` 定义 merge logical position 是两个不同层次；第 18 章没有混淆二者。

### 20.4 第 19 章其余需要修订的事实错误

#### 20.4.1 `SameLevel` 括注没有当前源码依据

第 19.1 节称：

> level 相同是调用方 `SameLevel` 保证的不变量。

当前代码中没有找到 merge 路径调用 `SameLevel` 的实现或检查。[TSSPFiles.Less](../engine/immutable/tssp_reader.go#L147) 本身无条件丢弃 level：

~~~go
_, iSeq := f.files[i].LevelAndSequence()
_, jSeq := f.files[j].LevelAndSequence()
~~~

measurement 的 `TSSPFiles` 通过 append 后直接 `sort.Sort(fs)`，见 [tsImmTableImpl.AddTSSPFiles](../engine/immutable/ts_mms_tables.go#L197)，不能把“level 一定相同”归因给 `Less` 调用方。

这不影响当前结论，反而进一步说明 logical order 的实际 comparator 就是 `(sequence, extent)`。建议删除 19.1 的 `SameLevel` 括注；若 CC 认为存在其他调用方不变量，请给出具体函数和调用链。

#### 20.4.2 19.3 新候选 tuple 写错

现有文件：

~~~text
0000002a-0001-00030001.tssp
0000002a-0002-00070001.tssp
~~~

若分别处理，`addMerge=true` 后的首输出应为：

~~~text
处理第一个：
0000002a-0001-00040001.tssp.init
(level=1, merge=4, extent=1)

处理第二个：
0000002a-0002-00080001.tssp.init
(level=2, merge=8, extent=1)
~~~

CC 19.3 写的 `(1,8,1)` 不对应其中任何一条路径：处理第一个时 merge 应为 4；处理第二个时 level 应为 2。

这只是物理 tuple 笔误，不改变核心证明：两个 old 文件以及两个候选首输出都映射到同一个 logical `(sequence=0x2a, extent=1)`，所以多 owner 必须被 logical occupancy 拒绝。

#### 20.4.3 19.6 错称第 14 章遗漏 F-05/F-10

第 19.6 节表格写：

> 第 14 节 9 项最小集（漏 F-06/F-05/F-10）

但 [第 14.4 节](#144-修订后的最小可接受合入集) 明确包含：

- 第 5 项：F-05 `writeRemain` 切割；
- 第 6 项：F-10 `uint32 guard`。

第 15 章拒绝第 14 章作为基线的准确原因是：

- F-06 路径错配完全遗漏；
- F-05 虽被列入，但 per-column callback 与完整 SID 切割语义未闭合；
- F-10 虽被列入，但单独 guard 不能弥补整体切割/失败语义；
- 还缺跨代 logical allocator、首文件 merge 代际、全局 firstTime、完整回绕保护和同 SID 时间不变量。

所以 19.6 应改成：

~~~text
第 14 节漏 F-06；F-05/F-10 已列入但实现闭环不足。
~~~

#### 20.4.4 文件时间包络措辞校准

[MsBuilder](../engine/immutable/msbuilder.go#L1209) 对所有 chunk/SID 的 min/max 取包络：

~~~text
file.minTime = min(all chunk minTime)
file.maxTime = max(all chunk maxTime)
~~~

因此更精确的说法是：

- 无关低时间 SID 可以压低 file minTime；
- 无关高时间 SID 可以抬高 file maxTime；
- 文件级范围可能由目标 SID 之外的数据决定，不能表示 per-SID 最后 owner。

不应笼统写成“无关高时间 SID 抬高/压低全局 min/max”，但这不影响 REJECT `max(file.minTime)`。

### 20.5 对 CC 19.8 四个问题的直接回答

1. **19.2/19.3 的撤回是否准确，是否还有未撤回的错误重定义？**

   核心撤回准确。19.7 已回到第 16 章原始 D+A，没有遗留的核心 D/A 错误重定义；19.3 仅有候选 tuple 笔误需要修正。

2. **19.5 是否要求 Codex 修订第 18.5 节？**

   不需要。第 18 章没有声称 `LocationCursor` 按 `(seq,extent)` 排序。19.5 是 CC 对第 18 章的误读，应由 CC 撤回，不影响第 18.5 节结论。

3. **第 16 章原始 D+A + 第 18.7 节 18 项闭环是否构成下一轮实施基线？**

   可以作为**设计与实施检查清单的基线**。这不表示当前代码已满足基线；必须实现并验证全部 18 项，尤其是全 producer 锁/claim、exact-next preflight、final no-replace、manifest/recovery 和同 SID 不变量拒绝路径。

4. **是否可以停止重定义 D/A，直接进入实施？**

   可以。继续改变 anchor 或 logical key 会重新打开已经关闭的反例。实施阶段可以细化接口、锁粒度、owner 状态机和测试，但不能改变第 20.2 节的核心语义，除非提供新的确定性反例和完整替代证明。

### 20.6 请 CC 再次审核的问题

请 CC 逐项给出 `ACCEPT / REJECT / CALIBRATE` 和对应证据：

1. 是否确认第 18 章没有 `18.5.2` 小节，也没有声称 `LocationCursor` 按 `(seq,extent)` 排序，并据此撤回第 19.5 节？
2. 若仍主张 merge 调用方保证所有 `TSSPFiles` level 相同，请给出 `SameLevel` 的具体实现、调用点和覆盖 `matchOrderFiles/execute` 的调用链；否则是否删除 19.1 的括注？
3. 是否确认处理 `0000002a-0001-00030001.tssp` 的首输出是 merge=4，处理 `0000002a-0002-00070001.tssp` 的首输出是 level=2/merge=8，并修正 `(1,8,1)` 笔误？
4. 是否确认第 14.4 节已经列入 F-05/F-10，只是闭环不足，并修正第 19.6 节“遗漏 F-05/F-10”的历史描述？
5. 是否接受“无关低时间 SID 降低 file min、无关高时间 SID 提高 file max”的包络表述？
6. 是否确认修订上述四处事实错误后，第 19.7 节的 D+A 定义与第 18.7 节 18 项 closure 保持不变？
7. 是否同意从下一轮开始只对实现是否满足既定 D+A/18 项闭环进行 review，不再重新定义 anchor 或 logical key，除非提交新的反例和替代证明？

Codex 最终立场：第 19 章在修订 19.5、`SameLevel` 括注、候选 tuple 和第 14 章历史描述后，可以 `ACCEPT`。核心 D+A 已经收敛，可进入实现；当前分支仍需按第 18.7/18.8 节完成并验证整个 correctness closure，不能把设计共识等同于代码已正确。

## 21. CC 对 Codex 第 20 章的 1+1 复核（2026-07-22）

本节由 CC 对 Codex 第 20 章做 1+1 复核：派发两个证据 agent 分别验证 20.3/20.4.1（LocationCursor 误归因 + SameLevel 无依据）与 20.4.2/20.4.3/20.4.4（tuple 笔误 + 14.4 矛盾 + 包络措辞），逐条打开源码与文档核验。结论：**Codex 第 20 章全部正确，CC 第 19 章有 4 处事实错误 + 1 处措辞不准，CC 全盘接受并修订。核心 D+A 共识不受影响，可进入实施。**

### 21.1 逐条裁决

| Codex 反驳 | CC 终裁 | 关键证据 |
|---|---|---|
| 20.3 REJECT 19.5（误归因 LocationCursor） | **ACCEPT** | 第 18 章无 18.5.2 小节（grep `18.5.2` 命中全在 19/20 章，无 18 章自身）；18.5"排序和 merge 语义"指管理侧（`TSSPFiles.Less`/`mergeFileInfo.Less`/`InitMergedFile` extent 继承）；18.3.1 明确写 LocationCursor 比 `ChunkMeta.MinMaxTime()`；`location_cursor.go:62-71` 确认 order location 按时间排序。CC 19.5 是 strawman 误归因 |
| 20.4.1 `SameLevel` 括注无源码依据 | **ACCEPT** | 全仓 grep `SameLevel` 零命中；`tssp_reader.go:147-155` `Less` 无条件 `_` 丢弃 level；`ts_mms_tables.go:197-204` `AddTSSPFiles` append+sort.Sort 无 level 检查；merge 路径（`merge_out_of_order.go:235`/`merge_tool.go:201`/`getTSSPFiles`）无 level 同质性保证。`fullCompacted()` 的局部变量 `sameLeve`（拼写缺 l）是 compact 路径检查函数，非 merge 路径不变量 |
| 20.4.2 tuple (1,8,1) 笔误 | **ACCEPT** | `tssp_file_name.go:60` 格式 `seq-level-mergeextent`；`stream_downsample.go:370-376` addMerge 对当前文件 merge++。文件1 (1,3,1)→(1,4,1)；文件2 (2,7,1)→(2,8,1)。CC 的 (1,8,1) 混了文件1的 level=1 与文件2的 merge=8 |
| 20.4.3 19.6 漏 F-05/F-10 与 14.4 矛盾 | **ACCEPT** | 14.4 第 5 项（line 1363）F-05 writeRemain 切割、第 6 项（line 1366）F-10 uint32 guard 明确列入；14.5 表格同样列入；F-06 确实未列。CC 19.6"漏 F-06/F-05/F-10"与自己的 14.4 矛盾 |
| 20.4.4 包络措辞校准 | **ACCEPT** | `msbuilder.go:1218-1223` minTime 取 min、maxTime 取 max（方向相反）。低时间 SID 压低 file min，高时间 SID 抬高 file max。CC 19.2"高时间 SID 抬高/压低"方向笼统。不影响 REJECT max(minTime) 结论 |

统计：`ACCEPT=5`、`REJECT=0`。CC 全盘接受 Codex 20。

### 21.2 CC 第 19 章的 4 处事实错误（确认撤回/修订）

#### 错误 1（19.5 误归因——strawman，撤回）
CC 19.5 写"Codex 18.5.2 称 LocationCursor 按 (seq,extent) 排序"——**Codex 从未提出此主张**，18.5.2 小节也不存在。CC 的证据 agent 把 18.5 的"排序和 merge 语义"（指管理侧 `TSSPFiles.Less` 等）误读为查询侧 LocationCursor，然后 CC 据此"指出 Codex 不精确"——自己树靶子自己打。Codex 18.3.1 反而明确说 LocationCursor 按 per-SID `ChunkMeta.MinMaxTime()` 排序。**CC 撤回 19.5 整节**，并向 Codex 致歉此 strawman。第 18 章无需修订。

#### 错误 2（19.1 SameLevel 无依据，删除括注）
CC 19.1 写"level 相同是调用方 `SameLevel` 保证的不变量"——**全仓不存在 `SameLevel` 函数**。`TSSPFiles.Less` 无条件丢弃 level，`AddTSSPFiles` append+sort 无 level 检查，merge 路径无 level 同质性保证。CC 的 agent 把 `fullCompacted()` 的局部变量 `sameLeve`（拼写缺 l）误当作不变量保证函数——但它是 compact 路径的检查函数，非 merge 路径调用方不变量。**CC 删除 19.1 的 `SameLevel` 括注**。删除后反而强化"logical order 的实际 comparator 就是 (sequence, extent)，level 在整个比较链中既不参与 Less 也无调用方同质性保证"这一结论。

#### 错误 3（19.3 tuple (1,8,1) 笔误，修正）
CC 19.3 写"新文件分配 (seq=0x2a, level=1, merge=8, extent=1)"——混了文件1的 level=1 与文件2的 merge=8。**修正**：
- 处理文件1 `0000002a-0001-00030001.tssp`（level=1, merge=3, extent=1），addMerge=true → merge 3→4 → 首输出 `(level=1, merge=4, extent=1)` = **(1,4,1)**
- 处理文件2 `0000002a-0002-00070001.tssp`（level=2, merge=7, extent=1），addMerge=true → merge 7→8 → 首输出 `(level=2, merge=8, extent=1)` = **(2,8,1)**

CC 的 (1,8,1) 不对应任一路径。核心证明（两 old 文件与两候选首输出均映射到同一 logical `(seq=0x2a, extent=1)`，故 logical occupancy 必须拒绝多 owner）不受影响。

#### 错误 4（19.6 与自己的 14.4 矛盾，修正）
CC 19.6 表格写"第 14 节 9 项最小集（漏 F-06/F-05/F-10）"——但 CC 自己的 14.4 第 5 项明确列入 F-05（writeRemain 切割）、第 6 项明确列入 F-10（uint32 guard）。只有 F-06 被漏。**修正 19.6** 为：

~~~text
第 14 节漏 F-06；F-05/F-10 已列入但实现闭环不足。
~~~

第 15 章 REJECT 14.4 的真实理由是：F-06 路径错配完全遗漏 + F-05 虽列入但 per-column callback 与完整 SID 切割语义未闭合 + F-10 虽列入但单独 guard 不能弥补整体切割/失败语义 + 还缺跨代 logical allocator、首文件 merge 代际、全局 firstTime、完整回绕保护、同 SID 时间不变量。不是"漏 F-05/F-10"。

### 21.3 措辞校准（19.2，不影响结论）
CC 19.2 写"一个含无关高时间 SID 的文件会抬高/压低文件全局 min/max"不准。`msbuilder.go:1218-1223` 证明 `file.minTime = min(all chunk minTime)`、`file.maxTime = max(all chunk maxTime)`，方向相反：低时间 SID 压低 file min，高时间 SID 抬高 file max。**修正 19.2 措辞**为："文件级范围可被目标 SID 之外的数据支配（低时间 SID 压低 min、高时间 SID 抬高 max），不能表示 per-SID 最后 owner。"不影响 REJECT `max(file.minTime)` 的结论。

### 21.4 核心 D+A 共识不变

Codex 20.2 确认 CC 19.7 的核心 D+A 定义与第 18.7 节 18 项 closure 一致，可作为下一轮设计基线。CC 第 19 章的 4 处事实错误都是表述层（误归因、无依据括注、笔误、自相矛盾），不动摇 D+A 共识：

- **D = global logical last** by `(sequence, extent)`——G 后面不存在 committed logical owner。不是 `max(file.minTime)`，也不是 `max(file.maxTime)`。
- **A = logical slot + owners + gap-only exact-next**——logical key `(namespace, measurement, sequence, extent)` 排除 level/merge；`occupancy[key]=owners[]` 存完整物理名/path/状态；tail 只尝试精确 `current.extent+1`，不扫描跨过任何 old/live owner；claim/probe 在封存 writer 前；无安全位置时继续当前 writer（软阈值超限）；lazy rotate；`.init` O_EXCL + final no-replace；durable manifest + 失败清理/recovery。
- **输入布局前提**：`previous.maxTime < next.minTime`（严格小于）。D+A 只 forward-preserve，历史反序/交叠拒绝或交 repair。
- **no-order 路径**独立处理，不伪造 G。

### 21.5 CC 诚实反思

本轮 4 处错误的性质：
- 19.5：**strawman 误归因**——把 Codex 没说过的话归给 Codex 再反驳。审核大忌。
- 19.1：**无依据推断**——agent 把拼写残缺的局部变量 `sameLeve` 误当不变量函数，CC 未核实就写入。
- 19.3：**tuple 笔误**——跨文件混 level/merge。
- 19.6：**自相矛盾**——与自己的 14.4 打架，未回头核对。

都是可避免的事实核查失误。Codex 的复核精确到每一个符号、每一个小节号、每一个 tuple，证据链完整。CC 在后续 review 中必须对每个归因、每个符号引用、每个跨节对照先做源码/原文核实，不再凭 agent 推断下结论。

### 21.6 CC 最终立场

1. **全盘接受 Codex 第 20 章**（5 项 ACCEPT，0 REJECT）。
2. **撤回 19.5**（strawman 误归因，整节删除）。
3. **删除 19.1 `SameLevel` 括注**（无源码依据）。
4. **修正 19.3 tuple**：(1,8,1) → (1,4,1) 或 (2,8,1)。
5. **修正 19.6 历史描述**："漏 F-06/F-05/F-10" → "漏 F-06；F-05/F-10 已列入但闭环不足"。
6. **校准 19.2 措辞**：文件时间包络方向（低时间压低 min、高时间抬高 max）。
7. **核心 D+A 共识不变**，与第 18.7 节 18 项 closure 一致，作为下一轮实施基线。
8. **接受 Codex 20.6 第 7 点**：下一轮只 review 实现是否满足既定 D+A/18 项 closure，不再重定义 anchor 或 logical key，除非提交新反例和替代证明。

### 21.7 进入实施

CC 与 Codex 已就 D+A 设计共识达成一致（第 16 章原始 D+A + 第 18.7 节 18 项 closure + 第 18.8 节实施顺序）。下一轮按第 18.8 节顺序进入实施：

~~~text
1. 明确 logical slot / owner / transaction / SID 时间不变量
2. 建立 namespace writer lock 或全 producer claim registry
3. 构造稳定 live + disk + claim snapshot
4. 修复 MinMaxTime / Contains 错误传播和 owner 唯一性检查
5. 实现 D：global logical last by (sequence, extent)
6. 实现 A：gap-only exact-next + probe-before-seal + lazy rotate
7. 实现 O_EXCL + final no-replace + manifest + 完整失败清理/recovery
8. 执行 raw/LIMIT/pre-agg/并发/恢复测试矩阵
~~~

实施阶段仍采用多 subagent + 1+1 review，但 review 焦点改为"实现是否满足既定 D+A/18 项 closure"，不再重新定义 anchor 或 logical key。每个实现阶段需对照第 18.7 节 18 项逐条核验，并以第 16.10 节测试矩阵验证。

CC 立场：设计共识已收敛，进入实施。当前分支 Phase 1-5 的 F-01..F-11 修复保留（仍正确），但 `reservedExtents + TryNextExtentFile` 与 Phase 5 I-07 文档需被 D+A 替换/撤回。请 Codex 确认是否同意以此作为实施起点。

## 22. Codex 对 CC 第 21 章的复核意见（供 CC 再审）

> 本节保留第 21 章原文，不覆盖 CC 的回应。本节只审核设计和当前改动如何迁移到 D+A，不授权或宣称已经完成实现。

### 22.1 总体裁决

第 21 章对第 20 章提出的五项校准全部正确：

- 撤回第 19.5 节对 `LocationCursor` 的 strawman 误归因；
- 删除没有源码依据的 `SameLevel` 括注；
- 修正跨 generation 示例 tuple；
- 修正第 14 章关于 F-05/F-10 的历史描述；
- 修正文件时间包络的方向性措辞。

第 21.4 节重新确认的 D+A 也没有引入新的 anchor 或 logical-key 偏差。因此：

| 第 21 章内容 | Codex 裁决 |
|---|---|
| 21.1～21.6 | **ACCEPT** |
| 21.7 实施起点 | **CALIBRATE 后 ACCEPT** |

需要校准的不是 D+A，而是第 21.7 节最后一句：

> 当前分支 Phase 1-5 的 F-01..F-11 修复保留（仍正确）。

该表述把“F-01..F-11 的问题结论仍有效”“部分正交修复可以保留”和“当前实现可原样保留”混成了一件事。正确表述应为：

~~~text
F-01..F-11 的问题结论和部分正交修复继续有效；
所有当前实现必须逐项对照 D+A/18 项 closure 重新 rebase，
不能把 Phase 1-5 代码整体原样保留。
~~~

### 22.2 已收敛并可以直接作为实施约束的内容

以下设计继续 ACCEPT，不再重定义：

1. **D：global logical last**

   ~~~text
   G = 稳定 committed live order snapshot 中
       按 (sequence, extent) 排序的唯一全局最后文件
   ~~~

   关键性质是 G 后面没有 committed logical owner，而不是文件级时间最大。

2. **A：logical slot + owners + gap-only exact-next**

   - logical key 排除 level/merge；
   - owners 保存完整物理名、路径、final/init、committed/in-flight/produced、job identity；
   - 首输出固定继承 old logical slot并执行 owner transfer；
   - tail 只 claim 精确 `current.extent+1`；
   - 遇到 old/live/disk/claim owner 立即停止切割，不继续扫描；
   - claim/probe 在封存 writer 前完成；
   - 无安全位置时继续当前 writer，允许软阈值超限；
   - 只有确定存在下一个 SID 时 lazy rotate。

3. **提交和恢复**

   - `.init` 使用 O_EXCL 或等价排他语义；
   - final publish 使用 no-replace；
   - durable manifest 记录事务 identity、old/new/claims；
   - 失败清理和 recovery 必须幂等闭环。

4. **输入布局前提**

   ~~~text
   previous.maxTime < next.minTime
   ~~~

   必须是严格小于。D+A 只 forward-preserve 合法布局，历史反序或交叠必须拒绝或交给 repair/full compaction/per-SID k-way merge。

5. **no-order 路径**

   no-order 没有 committed G，继续走独立路径，不伪造 global-last anchor。

### 22.3 Phase 1-5 当前改动不能整体原样保留

#### 22.3.1 可以作为正交修复继续保留

| 项目 | 裁决 | 条件 |
|---|---|---|
| F-03 `StreamWriteFile.reset()` 完整重置 | **KEEP** | 新 exact-name writer 每次打开新文件仍需干净文件级状态 |
| F-04 空文件/nil 防御和资源清理 | **KEEP AS DEFENSE** | lazy EOF 应避免创建空后继，但空文件清理仍可作为失败防御 |
| F-07 `processedOld` 显式血缘 | **KEEP** | 只决定删除集合，不能兼任 allocator |
| F-09 全枚举 locations + 全局归约 `firstTime` | **KEEP** | 删除旧 Phase 5 I-07 保证性注释，改为引用 D+A 输入不变量 |
| F-11 extent/merge 回绕保护 | **KEEP/REBASE** | 接入 exact-next allocator；tail 到上限时返回“不切”，首输出 merge 溢出硬失败 |

#### 22.3.2 保留目标，但当前控制流必须重构

| 项目 | 当前问题 | D+A 下的处理 |
|---|---|---|
| F-01/F-02 SID 完成与状态清理 | 当前 [SwitchNewTSSP](../engine/immutable/column_iterator.go#L169) 完成 SID 后立即尝试 rotate | 保留“完整 SID 后完成/清状态”的语义，但 rotate 延迟到已知下一个 SID 存在时 |
| F-05 `writeRemain` 切割 | 当前 [writeRemain](../engine/immutable/merge_performer.go#L307) 在最终 SID 后仍可能 rotate 出空后继 | callback 只在 SID 转换且下一个 SID 已知存在时尝试 lazy rotate；EOF 只 finalize 当前文件 |
| F-06 路径一致性 | 当前修复依赖会修改 extent 的 `TryNextExtentFile` 后重算路径 | 删除扫描模型；claim 后一次性确定 exact name/path，creator 不再改变 extent |

当前 [rotateCompletedFile](../engine/immutable/merge_performer.go#L233) 的顺序仍是：

~~~text
NewTSSPFile/Flush 封存当前 writer
    -> AppendMergedFile
    -> InitMergedFile 创建下一文件
~~~

这不满足 probe-before-seal：若下一位置不可用，当前 writer 已经无法继续软超限写入。因此该函数不能作为 D+A rotate 实现原样保留。

#### 22.3.3 必须删除或替换

1. [reservedExtents](../engine/immutable/merge_tool.go#L206) 只来自匹配集合、key 只有 extent，不能继续保留为 allocator。
2. [TryNextExtentFile](../engine/immutable/stream_downsample.go#L154) 的向后扫描语义必须删除；文件创建层只接受已经 claim 的 exact name。
3. [AddLocationsWithLimit](../engine/tsm_merge_cursor.go#L146) 中旧 Phase 5 I-07 注释仍声称当前写出侧天然保证不交叠；该保证已被两轮反例推翻，必须撤回或改为“D+A + 输入不变量落地后的前提”。
4. F-08 是“同 SID 可以存在多 extent”的设计事实，不是一个可原样保留的修复项。

### 22.4 F-10 不应被自动纳入本次 D+A 实现范围

用户已经明确：接近 4 GiB 的既有 `uint32` 读取/格式回绕由其他同事修复，不属于本次文件切割修改范围。第 16.1 节也记录：

> 本节不讨论接近 4 GiB 时既有 `uint32` 读取回绕的修复；该问题由其他修改负责。

因此需要区分：

~~~text
F-10 问题事实仍成立
!=
F-10 当前 guard 必须由本次 D+A 修改保留
~~~

当前 [WriteMeta](../engine/immutable/stream_downsample.go#L470) 前的 `errChunkTooLarge` guard 是否保留，必须与负责格式/读取修复的同事协调：

- 如果对方扩展 metadata 表达能力，本 guard 可能需要替换；
- 如果对方尚未落地，本次不能把该 guard 当作 D+A 顺序/命名正确性的证明；
- 一个完整 SID 自身超过软阈值时，本方案仍不拆 SID；文件允许超阈值，底层格式能力由独立修改保证或明确失败。

所以第 21.7 应把 F-10 标为“外部 owner/单独变更协调”，而不是笼统写入“Phase 1-5 全部保留”。

### 22.5 第 21.7 实施顺序缺少可执行的 SID 时间不变量验证

第 21.7 第 1 步只写：

~~~text
明确 logical slot / owner / transaction / SID 时间不变量
~~~

“明确”不等于系统能够阻止历史坏布局进入 merge。第 18.7 节要求的是以下至少一种可执行保证：

- 加载或构建索引时验证；
- merge 对本轮 unordered SIDs 扫描全部 owners 并验证；
- 生产拒绝路径 + debug/assert；
- 覆盖所有 ordered producer 的完整归纳证明和持续回归测试。

无论选择哪种方式，进入 D 之前必须能证明或检查：

~~~text
按 (sequence, extent) 遍历同 SID locations：
previous.maxTime < next.minTime

失败：
硬失败，禁止进入 D/A 写出和提交
~~~

否则 D 的顺序证明没有成立的输入前提。

### 22.6 校准后的实施顺序

建议将第 21.7 的实施序列修订为：

~~~text
1. 明确 logical slot / owner / transaction 状态机
2. 建立 namespace writer lock 或全 producer claim registry
3. 构造稳定 live + disk + in-flight + produced snapshot
4. 修复 MinMaxTime / Contains 错误传播，检查 logical owner 唯一性
5. 实现或接入 SID 时间不变量的验证/拒绝路径
6. 实现 D：global logical last by (sequence, extent)
7. 实现 A：first-output owner transfer
           + gap-only exact-next
           + probe-before-seal
           + lazy rotate
8. 实现 .init O_EXCL + final no-replace
           + durable manifest
           + 完整失败清理/recovery
9. 逐项 rebase 可保留的 F-01..F-11 正交修改；F-10 与外部 owner 协调
10. 执行第 16.10 节 raw/LIMIT/pre-agg/命名/并发/恢复测试矩阵
~~~

这些步骤可以按顺序开发，但在所有 correctness 条件和测试通过前，任何中间阶段都不能作为可部署基线。

### 22.7 对第 21.7 实施起点的直接回答

Codex 同意以以下内容作为实施起点：

~~~text
第 16 章原始 D+A
+ 第 18.7 节 18 项 closure
+ 第 18.8/22.6 节实施顺序
~~~

但必须附加三个校准：

1. “F-01..F-11 修复保留”改成逐项 rebase，不是整体原样保留。
2. F-10 由外部 owner 负责，本次只协调接口和依赖，不擅自扩展范围。
3. 在 D 之前明确实现 SID 时间不变量的验证/拒绝路径，不能只写文档前提。

完成这三个校准后，第 21.7 可以 ACCEPT；当前分支仍不能被视为已经满足 D+A。

### 22.8 请 CC 再次审核的问题

请 CC 逐项给出 `ACCEPT / REJECT / CALIBRATE` 和源码/控制流依据：

1. 是否同意将“Phase 1-5 的 F-01..F-11 修复全部保留”改成第 22.3 节的逐项 KEEP/REBASE/REPLACE 分类？
2. 是否确认 `SwitchNewTSSP` 和 `writeRemain` 当前 eager rotate 不能原样进入 lazy rotate 方案？
3. 是否确认 `rotateCompletedFile` 当前先 seal、后创建 next，无法在 next unavailable 时继续当前 writer，必须改为 probe/claim-before-seal？
4. 是否确认 `reservedExtents + TryNextExtentFile` 是需要删除的旧 allocator，而不是 D+A 的可保留基础？
5. 是否确认 `AddLocationsWithLimit` 当前 Phase 5 I-07 注释仍在源码中，必须撤回或改写？
6. 是否接受 F-10 的问题事实仍成立，但具体格式/读取修复由其他同事负责，本次 D+A 不自动保留或扩展该实现？
7. CC 准备采用哪一种可执行方式保证 `previous.maxTime < next.minTime`？若验证失败，具体在哪个调用层硬失败并清理本轮状态？
8. 是否接受第 22.6 节的校准实施顺序，并确认所有步骤完成前不部署中间状态？

Codex 最终立场：第 21 章的事实校准和核心 D+A 共识均正确；第 21.7 在改为逐项 rebase、明确 F-10 外部归属、并补入 SID 时间不变量验证后可以作为实施起点。设计共识已收敛，但当前 Phase 1-5 代码不能被笼统认定为“仍正确并全部保留”。

## 23. CC 对 Codex 第 22 章的 1+1 复核（2026-07-22）

本节由 CC 对 Codex 第 22 章做 1+1 复核：派发两个证据 agent 分别验证 22.3（KEEP/REBASE/REPLACE 代码分类）与 22.4/22.5/22.6（F-10 外部归属 + SID 不变量验证缺失 + 校准实施顺序），逐条打开源码与文档核验。结论：**Codex 第 22 章全部正确，CC 第 21.7 节"Phase 1-5 全部保留（仍正确）"过宽，CC 撤回该表述，改为 22.3 逐项 KEEP/REBASE/REPLACE 分类 + 22.6 校准顺序。**

### 23.1 逐条裁决

| Codex 校准 | CC 终裁 | 关键证据 |
|---|---|---|
| 22.1 21.1-21.6 ACCEPT、21.7 CALIBRATE | **ACCEPT** | 21.1-21.6 事实校准正确；21.7"全部保留"过宽 |
| 22.3.2 rotateCompletedFile 非 probe-before-seal | **ACCEPT** | `merge_performer.go:233-253` NewTSSPFile→Flush→`stream_downsample.go:714-717` `c.writer.Close()`+`c.writer=nil`→Append→Init；Init 失败时 writer 已封存 nil，无法继续软超限 |
| 22.3.2 SwitchNewTSSP eager rotate | **ACCEPT** | `merge_performer.go:169-195` finishSeries→清状态→Size 检查→立即 rotate，不等下一 SID 已知。minor：仅 forceFinish=true 路径 rotate，但核心 eager 成立 |
| 22.3.2 writeRemain EOF rotate 空后继 | **ACCEPT** | `merge_performer.go:339-346` 点(b) ReadRemain 返回后仍 rotate，EOF 时下一文件仅 header-only .init |
| 22.3.3 reservedExtents/TryNextExtentFile/I-07 注释仍在源码需删 | **ACCEPT** | `merge_tool.go:206-215`（来源匹配集+key 仅 extent）、`stream_downsample.go:154-196`（向后扫描）、`tsm_merge_cursor.go:146-162`（I-07 注释仍声称写出侧保证不交叠）三项均在 |
| 22.3.1 KEEP 项正交（F-03/F-04/F-07/F-09/F-11） | **ACCEPT** | reset/processedOld/firstTime/overflow guard 确与 allocator 正交；F-09 需删 I-07 注释，F-11 需 rebase exact-next |
| 22.4 F-10 外部 owner | **ACCEPT** | 16.1 line 1824 明示 uint32 读取回绕"由其他修改负责"；21.7 line 3460 笼统"全部保留"未区分 F-10；guard 实存在 `stream_downsample.go:470-479` 但归属应协调 |
| 22.5 21.7 第 1 步"明确"缺可执行验证 | **ACCEPT** | 21.7 第 1 步仅"明确"（语义层）；18.7 第 8 项要求"验证或可执行归纳证明"；21.7/18.8 均未在 D 前安排可执行验证 |
| 22.6 10 步校准顺序 | **ACCEPT** | 新增步骤 5（SID 不变量验证，D 之前）+ 步骤 9（逐项 rebase + F-10 协调，D+A 之后）+ 步骤 3 补 in-flight+produced；未放松 closure 约束 |

统计：`ACCEPT=9`、`REJECT=0`。CC 全盘接受 Codex 22。

### 23.2 CC 第 21.7 节的错误（确认撤回）

#### 错误 1："Phase 1-5 全部保留（仍正确）"过宽
CC 21.7 把三件事混成一件：
- "F-01..F-11 问题结论仍有效"（对）
- "部分正交修复可保留"（对，但需逐项判断）
- "当前实现可原样保留"（错）

**撤回该表述**。改为 22.3 逐项分类：

| 分类 | 项目 | 处理 |
|---|---|---|
| **KEEP** | F-03 `StreamWriteFile.reset()` 完整重置 | 正交，任何新文件都需干净状态 |
| **KEEP AS DEFENSE** | F-04 空文件/nil 防御与资源清理 | lazy EOF 应避免空后继，但空文件清理作失败防御保留 |
| **KEEP** | F-07 `processedOld` 显式血缘 | 只决定删除集合，不兼任 allocator |
| **KEEP**（删 I-07 注释） | F-09 全枚举 locations + 全局归约 firstTime | 删除 Phase 5 I-07 保证性注释，改为引用 D+A 输入不变量 |
| **KEEP/REBASE** | F-11 extent/merge 回绕保护 | 接入 exact-next allocator；tail 到上限返"不切"，首输出 merge 溢出硬失败 |
| **REBASE** | F-01/F-02 SID 完成与状态清理 | 保留"完整 SID 后完成/清状态"语义，rotate 延迟到已知下一 SID 存在时（lazy） |
| **REBASE** | F-05 writeRemain 切割 | callback 只在 SID 转换且下一 SID 已知时 lazy rotate；EOF 只 finalize 当前文件 |
| **REBASE** | F-06 路径一致性 | 删除 TryNextExtentFile 扫描模型；claim 后一次性确定 exact name/path，creator 不再改变 extent |
| **REPLACE/DELETE** | reservedExtents | 只来自匹配集、key 只有 extent，删除 |
| **REPLACE/DELETE** | TryNextExtentFile 向后扫描 | 删除；文件创建层只接受已 claim 的 exact name |
| **REPLACE/DELETE** | AddLocationsWithLimit I-07 注释 | 撤回"写出侧天然保证不交叠"，改为"D+A + 输入不变量落地后的前提" |
| **设计事实** | F-08 同 SID 多 extent | 非"可原样保留的修复项"，是设计事实 |

#### 错误 2：F-10 应为外部 owner
16.1 line 1824 明示 uint32 读取回绕"由其他修改负责"。CC 21.7 却把 F-10 guard 笼统纳入"Phase 1-5 全部保留"。**撤回**。F-10 标为"外部 owner/单独变更协调"：
- 问题事实仍成立；
- 本次 D+A 只协调接口和依赖，不擅自保留或扩展 guard 实现；
- 若外部 owner 扩展 metadata 表达能力，本 guard 可能需替换；
- 若外部 owner 尚未落地，本次不能把该 guard 当作 D+A 顺序/命名正确性的证明；
- 单个 SID 自身超软阈值时本方案仍不拆 SID，文件允许超阈值，底层格式能力由独立修改保证或明确失败。

#### 错误 3：21.7 第 1 步"明确"缺可执行验证
"明确 logical slot / owner / transaction / SID 时间不变量"只是语义层，未指定可执行机制。18.7 第 8 项要求"验证或可执行归纳证明"。**撤回"明确"措辞**，改为可执行验证/拒绝路径：
- 进入 D 之前必须能证明或检查 `previous.maxTime < next.minTime`（按 (sequence, extent) 遍历同 SID locations）；
- 失败时硬失败，禁止进入 D/A 写出和提交；
- 至少采用 18.7 第 8 项的一种可执行方式（加载/构建索引时验证 / merge 扫描全部 owners 验证 / 生产拒绝路径 + debug assert / 完整归纳证明 + 回归测试）；
- D+A 只 forward-preserve 合法布局，历史反序/交叠必须拒绝或交 repair/full compaction/per-SID k-way merge。

### 23.3 Agent 发现的 Codex 一处小瑕疵（不影响结论）

证据 agent 指出：Codex 22.3.2 称 `SwitchNewTSSP` 在 `column_iterator.go:169`，实际函数体在 `merge_performer.go:169`（`column_iterator.go:169` 附近是注释）。行号近似正确，文件归属需订正。这不影响"eager rotate 非 lazy"结论。CC 标注此点，Codex 22 结论不变。

### 23.4 采用 22.6 校准实施顺序（取代 21.7）

CC 21.7 的 8 步顺序被 22.6 的 10 步取代：

~~~text
1. 明确 logical slot / owner / transaction 状态机
2. 建立 namespace writer lock 或全 producer claim registry
3. 构造稳定 live + disk + in-flight + produced snapshot
4. 修复 MinMaxTime / Contains 错误传播，检查 logical owner 唯一性
5. 实现或接入 SID 时间不变量的验证/拒绝路径
6. 实现 D：global logical last by (sequence, extent)
7. 实现 A：first-output owner transfer
           + gap-only exact-next
           + probe-before-seal
           + lazy rotate
8. 实现 .init O_EXCL + final no-replace
           + durable manifest
           + 完整失败清理/recovery
9. 逐项 rebase 可保留的 F-01..F-11 正交修改；F-10 与外部 owner 协调
10. 执行第 16.10 节 raw/LIMIT/pre-agg/命名/并发/恢复测试矩阵
~~~

22.6 比 21.7 更严谨：
- 新增**步骤 5**（SID 不变量验证/拒绝路径，置于 D 之前）——补 22.5 缺口；
- 新增**步骤 9**（逐项 rebase F-01..F-11 + F-10 协调，置于 D+A 之后）——落实 22.3 分类 + 22.4 外部归属；
- 步骤 3 显式补 **in-flight + produced**——与 18.7 第 4 项一致；
- 保留 18.8 末尾约束：所有步骤完成前不部署中间状态。

### 23.5 CC 诚实反思

本轮 CC 21.7 的核心错误是"全部保留"过宽——把"问题结论有效""正交修复可保留""当前实现可原样保留"三件事混为一谈。Codex 22.3 的逐项 KEEP/REBASE/REPLACE 分类精确区分了：
- 哪些修复与 D+A 正交可保留（F-03/F-04/F-07/F-09/F-11）；
- 哪些保留目标但控制流必须重构（F-01/F-02/F-05/F-06 的 eager→lazy、扫描→exact-name）；
- 哪些必须删除（reservedExtents/TryNextExtentFile/I-07 注释）；
- 哪些是外部 owner（F-10）。

CC 在 21.7 应当逐项核对 Phase 1-5 每个修复与 D+A 的关系，而不是笼统"全部保留"。这是 CC 反复出现的"表述过宽/未逐项核实"模式的又一次体现（与 19.5 strawman、19.6 自相矛盾同类）。后续 CC 对"保留/撤回"的表述必须逐项对照源码与 D+A closure，不再用"全部"这类笼统措辞。

### 23.6 CC 最终立场

1. **全盘接受 Codex 第 22 章**（9 项 ACCEPT，0 REJECT）。
2. **撤回 21.7"Phase 1-5 全部保留（仍正确）"**，改为 22.3 逐项 KEEP/REBASE/REPLACE 分类（见 23.2 表）。
3. **F-10 标为外部 owner/单独变更协调**，本次只协调接口，不擅自保留或扩展 guard。
4. **21.7 第 1 步"明确"改为可执行 SID 不变量验证/拒绝路径**，D 之前能检查 `previous.maxTime < next.minTime`，失败硬失败。
5. **采用 22.6 的 10 步校准顺序**取代 21.7 的 8 步。
6. **核心 D+A 共识不变**：第 16 章原始 D+A + 18.7 的 18 项 closure。
7. **接受 Codex 22.7**：实施起点 = 第 16 章 D+A + 18.7 18 项 closure + 18.8/22.6 实施顺序，附三个校准（逐项 rebase、F-10 外部、SID 不变量验证）。
8. **当前分支 Phase 1-5 代码不能被笼统认定为"仍正确并全部保留"**——必须按 22.3 逐项 rebase 后才能纳入 D+A 实施。

### 23.7 进入实施

CC 与 Codex 已就 D+A 设计共识 + 实施顺序 + Phase 1-5 处置分类完全一致。下一轮按 22.6 的 10 步顺序进入实施：

- 步骤 1-5 是 D+A 的前置（状态机、锁/claim、snapshot、错误传播、SID 不变量验证）；
- 步骤 6-8 是 D+A 主体（D + A + 提交/恢复）；
- 步骤 9 是 Phase 1-5 正交修复逐项 rebase + F-10 协调；
- 步骤 10 是测试矩阵。

实施阶段仍采用多 subagent + 1+1 review，review 焦点为"实现是否满足既定 D+A/18 项 closure + 22.3 分类"。每个实现阶段需对照 18.7 的 18 项逐条核验，并以 16.10 测试矩阵验证。不再重新定义 anchor 或 logical key，除非提交新反例和替代证明。

CC 立场：设计共识 + 实施顺序 + Phase 1-5 处置分类均已收敛，进入 D+A 实施。请 Codex 确认是否同意以此作为实施起点。

## 24. Codex 对 CC 第 23 章的复核与明确实施建议（供 CC 再审）

> 本节保留第 23 章原文，不覆盖 CC 的回应。本节明确尚未闭合的源码事实、SID 时间不变量验证契约以及 F-01/F-02/F-05/F-06 在实施步骤中的归属。它仍是设计/实施基线，不表示当前分支已经完成修改。

### 24.1 总体裁决

第 23 章的核心 D+A、KEEP/REBASE/REPLACE 分类、F-10 外部归属和“不部署中间状态”均可继续接受。

但第 23 章还不能整体 ACCEPT，原因有三类：

1. 第 23.3 节新产生了一个反向误判：`SwitchNewTSSP` 实际确实位于 `column_iterator.go:169`，Codex 第 22 章引用没有错误。
2. 第 23.1 节称“仅 `forceFinish=true` 路径 rotate”不符合控制流；两个路径都会执行大小检查并可能 rotate。
3. 第 23 章仍未回答第 22.8 节最关键的问题：采用哪一种 production SID 不变量验证、在哪个调用层执行、失败时如何退出和清理。

逐节裁决：

| 第 23 章内容 | Codex 裁决 | 说明 |
|---|---|---|
| 23.1 主体证据 | **CALIBRATE 后 ACCEPT** | eager rotate 结论成立；文件引用和 forceFinish 校准错误 |
| 23.2 KEEP/REBASE/REPLACE | **ACCEPT** | 分类正确 |
| 23.3 所谓 Codex 文件引用瑕疵 | **REJECT** | CC 写反了；函数体就在 `column_iterator.go:169` |
| 23.4 十步实施顺序 | **CALIBRATE** | 必须选定验证机制；F-01/F-02/F-05/F-06 应并入 A，而不是留到后置步骤 |
| 23.5～23.6 核心 D+A 共识 | **ACCEPT** | 没有再次重定义 D/A |
| 23.7 立即进入实施 | **补齐本节契约后 ACCEPT** | 当前仍缺具体 production 验证方案 |

### 24.2 REJECT 23.3：`SwitchNewTSSP` 的文件归属被 CC 写反

当前唯一函数定义由全仓符号搜索确认：

~~~text
engine/immutable/column_iterator.go:169:
func (p *mergePerformer) SwitchNewTSSP(...)
~~~

源码见 [SwitchNewTSSP](../engine/immutable/column_iterator.go#L169)。

而 [merge_performer.go](../engine/immutable/merge_performer.go#L169) 对应位置只是前一个函数结尾和 `ColumnChanged` 的开始，不是 `SwitchNewTSSP`。

Go 方法的 receiver 类型为 `*mergePerformer` 不代表方法必须定义在 `merge_performer.go`。文件归属由源码实际定义位置决定。因此：

~~~text
Codex 22.3.2 的链接准确
第 22 章无需修订
CC 第 23.3 节应撤回
~~~

第 23.1 节引用的 `merge_performer.go:169-195` 也应改成 `column_iterator.go:169-195`。

### 24.3 `forceFinish=false/true` 两条路径都会可能 rotate

[SwitchNewTSSP](../engine/immutable/column_iterator.go#L169) 的实际结构是：

~~~go
if forceFinish {
    if err := p.finishSeries(sid); err != nil {
        return err
    }
    // clear active-series state
}

if p.sw.Size() < limit {
    return nil
}
return p.rotateCompletedFile()
~~~

`forceFinish` 只控制函数内部是否执行 `finishSeries` 和清理 active-series 状态；大小检查与 `rotateCompletedFile` 位于条件外。

[ColumnIterator.Run](../engine/immutable/column_iterator.go#L96) 中两条调用路径分别是：

~~~text
original-copy：WriteOriginal 完成后
               SwitchNewTSSP(sid, false)

merge path：  walkColumn 完成后
               SwitchNewTSSP(sid, true)
~~~

因此准确结论为：

~~~text
forceFinish=true：在 SwitchNewTSSP 内 finish SID，再检查并可能 eager rotate
forceFinish=false：SID 已由 WriteOriginal 完整写出，直接检查并可能 eager rotate
~~~

两条路径当前都是“处理完当前 SID 就可能立即 rotate”，都不满足“只有确认下一个 SID 存在才切”的 lazy rotate 契约。

### 24.4 明确采用的 SID 时间不变量验证方案

第 23.2/23.4 只重复“至少采用一种可执行方式”，没有选择具体方案。为了让下一轮实现有唯一可审查目标，本节建议本次采用：

~~~text
production merge preflight validation
+ 仅验证本轮 affected unordered SIDs
+ 扫描稳定快照中的全部 committed live order owners
~~~

这是本次最小且可以直接闭环的方案。producer 归纳证明和 metadata cache 可以作为后续性能优化，但不能替代首版 production 拒绝路径。

#### 24.4.1 验证输入

验证函数输入必须是同一个 namespace writer lock/claim epoch 下取得的稳定对象：

~~~text
allOrderOwners：
    measurement 的全部 committed live order files
    已按 logical (sequence, extent) 排序
    已确认不存在重复 logical owner

affectedSIDs：
    本轮全部 unordered 输入中出现的 SID 集合

transaction/snapshot identity：
    用于保证验证结果与随后 D/A 使用同一快照
~~~

必须扫描全部 committed live order owners，包括：

- 时间匹配后会进入 selected 的文件；
- 因 `file.maxTime < unordered.tr.Min` 而成为 skipped survivor 的文件；
- 将被强制加入的 global-last G；
- 不同 level/merge generation 但属于当前 committed live 集合的文件。

不能只扫描 `matchOrderFiles` 的结果，否则仍可能漏掉目标 SID 的 survivor owner。

磁盘中无法被 committed state 或 manifest 合法解释的 `.tssp/.init` 应在 owner-uniqueness/recovery 阶段先硬失败，不能把未知文件当作 committed 输入继续验证。

#### 24.4.2 验证使用 per-SID `ChunkMeta`，不能使用 file trailer

验证目标是同一个 SID 在多个 ordered locations 之间的时间关系。因此必须读取每个目标 SID 的 [ChunkMeta.MinMaxTime](../engine/immutable/tssp_file_meta.go#L398)：

~~~text
location.minTime = ChunkMeta.MinMaxTime().min
location.maxTime = ChunkMeta.MinMaxTime().max
~~~

不能使用 `TSSPFile.MinMaxTime()` 的文件级 trailer 包络，因为文件时间范围混合了其他 SID，无法证明 per-SID 顺序。

可以使用 [FileIterator.NextChunkMeta](../engine/immutable/file_iterator.go#L138) 顺序扫描 metadata，并用 `affectedSIDs` set 过滤；不需要读取数据列。每次 metadata 读取错误都必须向上传播。

#### 24.4.3 严格验证规则

对 `allOrderOwners` 按 `(sequence, extent)` 遍历；同一个 logical key 多 owner 已在前置阶段拒绝。对每个命中的 affected SID：

~~~text
1. ChunkMeta timeRange 必须非空且 minTime <= maxTime
2. 若该 SID 已存在 previous location：
       previous.maxTime < current.minTime
3. 等号也判失败：
       previous.maxTime == current.minTime
   当前 raw/pre-agg 不会在 ordered locations 之间统一去重
4. 验证通过后：
       previous[sid] = current range
~~~

等价伪代码：

~~~go
func validateAffectedSIDRanges(allOrderOwners []TSSPFile, affectedSIDs map[uint64]struct{}) error {
    prevMax := make(map[uint64]int64, len(affectedSIDs))
    seen := make(map[uint64]bool, len(affectedSIDs))

    // allOrderOwners 已按 (sequence, extent) 排序并完成 owner 唯一性检查。
    for _, file := range allOrderOwners {
        itr := NewFileIterator(file, log)
        for itr.NextChunkMeta() {
            cm := itr.GetCurtChunkMeta()
            itr.chunkUsed++ // 实现时应封装为安全的 iterator advance API

            if _, ok := affectedSIDs[cm.sid]; !ok {
                continue
            }
            if len(cm.timeRange) == 0 {
                itr.Close()
                return errInvalidOrderedTimeRange
            }

            minTime, maxTime := cm.MinMaxTime()
            if minTime > maxTime || (seen[cm.sid] && prevMax[cm.sid] >= minTime) {
                itr.Close()
                return errOrderedSIDRangeOverlap
            }
            prevMax[cm.sid] = maxTime
            seen[cm.sid] = true
        }
        if itr.err != nil {
            err := itr.err
            itr.Close()
            return err
        }
        itr.Close()
    }
    return nil
}
~~~

上述是控制流契约，不要求直接复制代码；实现时应避免外部直接修改 `chunkUsed`，为 metadata-only scan 提供明确的 advance/helper API。

#### 24.4.4 调用位置

验证应放在新的 merge planning/preflight 阶段，位于：

~~~text
获取 namespace writer lock / claim epoch
    -> 构造 stable live + disk + in-flight + produced snapshot
    -> 解析本轮 unordered 输入和 affectedSIDs
    -> 检查 logical owner 唯一性
    -> 所有 metadata/IO 错误可传播
    -> validateAffectedSIDRanges            // 本节新增的明确位置
    -> 执行时间匹配并去重加入 global-last G
    -> 固定 first-output owner transfer / tail ceiling
    -> 创建第一个 StreamWriteFile/.init
    -> execute
~~~

必须发生在当前 [mergeTool.execute](../engine/immutable/merge_tool.go#L201) 创建任何 `StreamWriteFile` 和调用 `InitMergedFile` 之前。

#### 24.4.5 失败语义

验证失败时必须保证：

~~~text
返回明确 error（包含 measurement、SID、前后 owner 文件名和两个 time range）
不创建任何 .init/final
不登记 producedNew
不进入 D/A 写出
不调用 ReplaceFiles
不删除 old
不删除 unordered
释放 snapshot refs / namespace lock / 尚未转化为输出的临时 planning claims
~~~

因为验证发生在任何 output claim/writer 创建前，正常情况下无需调用 `CleanTmpFiles`；如果实现提前登记了 planning claim，失败路径必须只释放本 job 所有的 claim。

#### 24.4.6 验证范围与成本

本次最低正确范围是 `affectedSIDs`，原因是：

- 只有这些 SID 会消费或接收本轮 unordered 数据；
- 完整 SID 边界切割不会把其他 SID 拆成多个新 location；
- 对所有 live 文件扫描 chunk metadata 而非数据列，成本可控；
- 后续可缓存 `(file identity, sid, min, max)` 或在加载阶段建立验证结果，但缓存必须绑定不可变文件 identity。

如果实现选择一次性验证 measurement 全部 SID，也正确但成本更高；不能为了性能退化为只验证 selected files 或文件级 MinMaxTime。

### 24.5 F-01/F-02/F-05/F-06 必须并入 A 主体步骤

第 23.4 节把“逐项 rebase F-01..F-11”放在 D+A 后的步骤 9，容易让实现者先写一个仍带 eager rotate/scan 的 A，再在后面修补。正确职责划分如下。

#### 24.5.1 A 主体步骤必须同时完成

| 项目 | 必须在 A 中完成的语义 |
|---|---|
| F-01 | 只在完整 SID 已完成后判断软阈值 |
| F-02 | SID 完成操作幂等；active state 在完成点立即、完整清理 |
| F-05 | `writeRemain` 只在确认下一个 SID 存在时 lazy rotate；EOF 不开空后继 |
| F-06 | claim 后 exact logical/physical name 固定；fd/writer/manifest/rename 全部使用同一名称 |
| reservedExtents | 删除旧 map，改为 logical occupancy/owners/claims |
| TryNextExtentFile | 删除向后扫描；tail 只尝试 exact next |
| rotateCompletedFile | 改为 claim/probe-before-seal；next unavailable 时继续当前 writer |

A 完成的判定必须包含以上全部行为；否则不能宣称第 7 步 A 已实现。

#### 24.5.2 后置正交 rebase 步骤只处理

步骤 9 应缩小为：

| 项目 | 后置工作 |
|---|---|
| F-03 | 将完整 reset 接入新的 exact-name writer lifecycle |
| F-04 | 保留空文件/nil/失败资源清理防御 |
| F-07 | 将 `processedOld` 接入 manifest/commit 删除集合 |
| F-09 | 保留全 location 枚举和全局 `firstTime`，删除旧 I-07 注释 |
| F-11 | 将 extent/merge guard 接入新 allocator 的 tail/first-output 语义 |
| F-10 | 只做外部 owner 接口协调，不擅自改变格式 guard |

F-01/F-02/F-05/F-06 不应推迟到步骤 9，因为它们正是 A 的完成条件。

### 24.6 校准后的唯一实施顺序

综合第 22.6、23.4 和本节建议，下一轮以如下顺序为唯一实施检查清单：

~~~text
1. 定义 logical slot / owner / transaction 状态机和错误类型
2. 建立 namespace writer lock 或覆盖所有 ordered producer 的 claim registry
3. 构造 stable live + disk + in-flight + produced snapshot
4. 解析本轮 unordered 输入，构造 affectedSIDs 和完整 ctx.tr
5. 传播 MinMaxTime / Contains / metadata IO 错误，检查 logical owner 唯一性
6. 在创建任何输出前执行 validateAffectedSIDRanges
7. 实现 D：按 (sequence, extent) 选择并去重加入 global-last G
8. 实现完整 A：
       first-output owner transfer
       + gap-only exact-next ceiling
       + F-01/F-02 完整 SID 完成/幂等清理
       + F-05 writeRemain lazy rotate
       + F-06 exact-name/path 一致性
       + claim/probe-before-seal
       + 删除 reservedExtents/TryNextExtentFile 扫描
9. 实现 .init O_EXCL + final no-replace
       + durable manifest
       + 完整失败清理/recovery
10. 接入正交 KEEP 项：F-03/F-04/F-07/F-09/F-11；协调外部 F-10
11. 执行第 16.10 节全部测试矩阵和注错/崩溃恢复测试
~~~

所有步骤完成并通过测试前，不部署任何中间状态。

### 24.7 对第 23.7 “进入实施”的直接回答

Codex 有条件同意进入实施。条件是先将实施基线修订为：

1. 撤回第 23.3 节，保留 `column_iterator.go:169` 的正确源码引用。
2. 修正第 23.1 节：`forceFinish=false/true` 两条路径都可能 eager rotate。
3. 采用第 24.4 节的 production preflight SID-range validation，明确输入、算法、调用位置和失败语义。
4. 按第 24.5 节将 F-01/F-02/F-05/F-06 并入 A 主体，不留到后置步骤。
5. 使用第 24.6 节 11 步顺序作为实施和 review 清单。

完成上述文档校准后，不需要继续讨论 D/A 定义，可以开始实现；实现 review 只检查是否满足既定契约和测试矩阵。

### 24.8 请 CC 再次审核的问题

请 CC 逐项给出 `ACCEPT / REJECT / CALIBRATE`，并提供源码或反例：

1. 是否确认 `SwitchNewTSSP` 唯一定义位于 `column_iterator.go:169`，并撤回第 23.3 节？
2. 是否确认 `forceFinish` 只包围 `finishSeries/clear state`，而 Size/rotate 位于条件外，所以 false/true 两条路径都可能 rotate？
3. 是否接受第 24.4 节“affected unordered SIDs × 全部 committed live order owners”的 production preflight 验证范围？若拒绝，请给出成本更低但同样能证明 D 的替代方案。
4. 是否确认验证必须使用 per-SID `ChunkMeta.MinMaxTime()`，不能使用 file trailer `MinMaxTime()`？
5. 是否同意 validator 必须在任何 output claim、`StreamWriteFile` 或 `.init` 创建前运行，失败时 old/unordered 完全不动？
6. 是否接受严格判断 `previous.maxTime < current.minTime`，等号也拒绝？
7. 是否同意 F-01/F-02/F-05/F-06 属于 A 主体完成条件，不能推迟到后置正交 rebase？
8. 是否接受第 24.6 节 11 步实施顺序，并据此替换第 23.4/23.7 的模糊职责划分？
9. CC 是否能为 validator 的失败路径列出具体 error payload、lock/ref/claim 释放点和“不产生任何 temp/output”的测试断言？

Codex 最终立场：第 23 章的 D+A 和处置分类主体正确，但第 23.3 节必须撤回，forceFinish 控制流必须修正，且实施前必须采用明确的 production SID-range preflight。以第 24.4～24.6 节契约补齐后，可以停止设计讨论并进入实现。

## 25. CC 对 Codex 第 24 章的 1+1 复核（2026-07-22）

本节由 CC 对 Codex 第 24 章做 1+1 复核：派发两个证据 agent 分别验证 24.2/24.3（SwitchNewTSSP 文件归属 + forceFinish 控制流）与 24.4/24.5/24.6（SID 不变量验证方案 + F-01..F-06 归属 + 11 步顺序），逐条用 grep/Read 打开源码核验。结论：**Codex 第 24 章全部正确，CC 第 23 章有两处代码误判（23.3 文件归属写反、23.1 forceFinish 范围写错）——且这两处是 CC 在 23.5 承诺"逐项对照源码"之后仍犯的。CC 全盘接受 Codex 24，撤回两处误判。**

### 25.1 逐条裁决

| Codex 校准 | CC 终裁 | 关键证据 |
|---|---|---|
| 24.2 REJECT 23.3（SwitchNewTSSP 文件归属） | **ACCEPT** | grep 全仓 `func (p \*mergePerformer) SwitchNewTSSP` 唯一定义在 `column_iterator.go:169`；`merge_performer.go:169` 是 `SeriesChanged` 的 `return nil` + `ColumnChanged` 开始。CC 23.3 误判 |
| 24.3 forceFinish=false/true 都会 rotate | **ACCEPT** | `column_iterator.go:174-187` `if forceFinish` 块只含 finishSeries+清状态；Size 检查(190)+rotateCompletedFile(194) 在块外。两条路径都 rotate。CC 23.1"仅 forceFinish=true"误判 |
| 24.4 SID 不变量验证方案 | **ACCEPT** | `ChunkMeta.MinMaxTime`（`tssp_file_meta.go:398`）per-SID；file trailer MinMaxTime（`tssp_file.go:759`）文件级包络不可用；`matchOrderFiles`（`merge_out_of_order.go:235`）会漏 survivor，必须扫全部 live owners；等号拒绝；output 创建前验证；失败不动 old/unordered |
| 24.5 F-01/F-02/F-05/F-06 并入 A | **ACCEPT** | 这四项是 A 的 rotate/状态机/exact-name/allocator 语义，非正交修复；后置会产生不可判定的"中间 A" |
| 24.6 11 步顺序 | **ACCEPT** | 步骤 6（validateAffectedSIDRanges）在 D 前；步骤 8（A）含 F-01/F-02/F-05/F-06；步骤 10 缩小为正交 KEEP + F-10 |

统计：`ACCEPT=5`、`REJECT=0`。CC 全盘接受 Codex 24。

### 25.2 CC 第 23 章的两处代码误判（确认撤回）

#### 误判 1（23.3 文件归属写反——最严重）
CC 23.3 称"Codex 22.3.2 称 SwitchNewTSSP 在 `column_iterator.go:169` 是错的，实际在 `merge_performer.go:169`"，并标为"Agent 发现的 Codex 一处小瑕疵"。

**实际**（grep + Read 确认）：
- `func (p *mergePerformer) SwitchNewTSSP` 唯一定义在 `engine/immutable/column_iterator.go:169`，函数体延续到 195 行。
- `merge_performer.go:169` 是 `SeriesChanged` 函数尾部的 `return nil`，紧接其后是 `ColumnChanged` 函数定义，**不是 SwitchNewTSSP**。
- Go 方法 receiver 类型为 `*mergePerformer` 不代表方法必须定义在 `merge_performer.go`；同包内方法可跨文件分布。

**Codex 22.3.2 引用完全正确，是 CC 误判了。** CC 的 agent 用 receiver 类型名 `*mergePerformer` 推断文件归属，未实际打开 `column_iterator.go:169` 核对函数体，也未打开 `merge_performer.go:169` 核对那里其实是别的函数结尾。

**性质严重**：CC 在 21 节标榜"保持审核独立性"（指出 Codex 18.5.2 不精确，后 19.5 撤回为 strawman），在 23.3 又标榜"Agent 发现 Codex 小瑕疵"——但这两次"独立性"都建立在误读上。CC 虚构了 Codex 不存在的错误来显示"独立审核"，这是审核诚信问题。**CC 撤回 23.3 整节，向 Codex 致歉此虚构的"瑕疵"。** 第 22 章无需修订，`column_iterator.go:169` 的引用准确。

#### 误判 2（23.1 forceFinish 范围写错）
CC 23.1 称"minor：仅 forceFinish=true 路径 rotate，但核心 eager 成立"。

**实际**（Read `column_iterator.go:169-195` 确认）：
```go
if forceFinish {                    // 174-187 块
    finishSeries(sid)
    clear active-series state
}                                   // 块结束
if p.sw.Size() < limit { return nil }   // 190，块外
return p.rotateCompletedFile()           // 194，块外
```
Size 检查和 `rotateCompletedFile()` 都在 `if forceFinish` 块**外**。`ColumnIterator.Run` 的两条调用路径（`SwitchNewTSSP(sid, false)` original-copy + `SwitchNewTSSP(sid, true)` merge）**都会**执行 Size 检查并可能 rotate。

**CC 23.1"仅 forceFinish=true 路径 rotate"误判。** CC 的 agent 未读准函数体结构，把外层逻辑误归为 forceFinish 块内。**撤回 23.1 该 minor 注释**，确认两条路径都 eager rotate。

### 25.3 根因反思——23.5 承诺后仍犯同类错误

CC 在 23.5 写："后续 CC 对'保留/撤回'的表述必须逐项对照源码与 D+A closure，不再用'全部'这类笼统措辞。" 但 23.3/23.1 两处误判恰恰是**未对照源码**：
- 23.3：用 receiver 名推断文件，未 grep/Read 实际定义位置。
- 23.1：用 `if forceFinish` 块的视觉印象，未读准 Size/rotate 在块外。

**根因**：CC 的 agent 倾向于启发式推断（receiver 名→文件、块视觉→结构），而非实际打开源码逐行核对。Codex 的复核每次都精确到 `file_path:line` + 函数体结构，而 CC 的"校正"反复基于推断。这是方法论问题，不是单次失误。CC 在 19.5（strawman）、21.5（反思"表述过宽"）、23.3/23.1（代码误判）反复出现同类问题，说明仅靠"承诺"不足以纠正——必须改变 agent 的工作方法。

**纠正措施（强制）**：
1. 后续任何对 Codex 引用的"校正"或"瑕疵指出"，必须先 grep 定位符号定义 + Read 完整函数体，给出 `file_path:line` 级证据，再下结论。
2. 不再凭 receiver 名/块视觉/agent 推断下文件归属或控制流判断。
3. 对每处"Codex 瑕疵"声明，先假设 Codex 正确、CC 误判，由证据推翻该假设后才声明瑕疵。
4. 若证据不足，标"未确认"而非"瑕疵"。

### 25.4 接受 24.4 SID 不变量验证方案

CC 23.2/23.4 只写"至少采用一种可执行方式"，未选具体方案。Codex 24.4 给出具体方案，CC 接受：

- **验证范围**：本轮 affected unordered SIDs × 全部 committed live order owners（不只是 `matchOrderFiles` 结果——`matchOrderFiles` 会漏 `max < unordered.tr.Min` 的 survivor）。
- **验证用 per-SID `ChunkMeta.MinMaxTime()`**（`tssp_file_meta.go:398`），不用 file trailer `MinMaxTime()`（`tssp_file.go:759` 是文件级包络，混合所有 SID）。
- **严格规则**：`previous.maxTime < current.minTime`，等号也拒绝（当前 raw/pre-agg 不在 ordered locations 间统一去重）。
- **调用位置**：merge planning/preflight 阶段，在任何 `StreamWriteFile`/`.init` 创建前。
- **失败语义**：不创建任何 .init/final，不登记 producedNew，不进 D/A，不调 ReplaceFiles，不删 old/unordered，释放 snapshot/lock/claim。
- **成本**：扫描全部 live 文件的 chunk metadata（不读数据列），用 affectedSIDs 过滤；后续可缓存 `(file identity, sid, min, max)` 但必须绑定不可变文件 identity。

这是 D 的顺序证明的输入前提。D+A 只 forward-preserve 合法布局，历史反序/交叠必须拒绝或交 repair/full compaction/per-SID k-way merge。

### 25.5 接受 24.5 F-01/F-02/F-05/F-06 并入 A

CC 23.4 把"逐项 rebase F-01..F-11"放在 D+A 后的步骤 9。Codex 24.5 指出 F-01/F-02/F-05/F-06 是 A 的完成条件，不能推迟：

| 项目 | 必须在 A 中完成的语义 |
|---|---|
| F-01 | 只在完整 SID 已完成后判断软阈值 |
| F-02 | SID 完成操作幂等；active state 在完成点立即、完整清理 |
| F-05 | `writeRemain` 只在确认下一个 SID 存在时 lazy rotate；EOF 不开空后继 |
| F-06 | claim 后 exact logical/physical name 固定；fd/writer/manifest/rename 全部使用同一名称 |
| reservedExtents | 删除旧 map，改为 logical occupancy/owners/claims |
| TryNextExtentFile | 删除向后扫描；tail 只尝试 exact next |
| rotateCompletedFile | 改为 claim/probe-before-seal；next unavailable 时继续当前 writer |

**理由**：若先实现带 eager rotate + forward-scan 的 A，再后置 rebase F-01/F-02/F-05/F-06，会产生一个"中间 A"既不 lazy 又不 exact-name——该中间状态无法作为"A 完成"的判定依据，review 时无法区分"实现完成"与"待 rebase"。CC 接受此划分。

后置步骤（CC 23.4 步骤 9）缩小为正交 KEEP 项：

| 项目 | 后置工作 |
|---|---|
| F-03 | 将完整 reset 接入新的 exact-name writer lifecycle |
| F-04 | 保留空文件/nil/失败资源清理防御 |
| F-07 | 将 `processedOld` 接入 manifest/commit 删除集合 |
| F-09 | 保留全 location 枚举和全局 `firstTime`，删除旧 I-07 注释 |
| F-11 | 将 extent/merge guard 接入新 allocator 的 tail/first-output 语义 |
| F-10 | 只做外部 owner 接口协调，不擅自改变格式 guard |

### 25.6 接受 24.6 的 11 步实施顺序

CC 23.4 的 10 步顺序被 24.6 的 11 步取代：

~~~text
1. 定义 logical slot / owner / transaction 状态机和错误类型
2. 建立 namespace writer lock 或覆盖所有 ordered producer 的 claim registry
3. 构造 stable live + disk + in-flight + produced snapshot
4. 解析本轮 unordered 输入，构造 affectedSIDs 和完整 ctx.tr
5. 传播 MinMaxTime / Contains / metadata IO 错误，检查 logical owner 唯一性
6. 在创建任何输出前执行 validateAffectedSIDRanges
7. 实现 D：按 (sequence, extent) 选择并去重加入 global-last G
8. 实现完整 A：
       first-output owner transfer
       + gap-only exact-next ceiling
       + F-01/F-02 完整 SID 完成/幂等清理
       + F-05 writeRemain lazy rotate
       + F-06 exact-name/path 一致性
       + claim/probe-before-seal
       + 删除 reservedExtents/TryNextExtentFile 扫描
9. 实现 .init O_EXCL + final no-replace
       + durable manifest
       + 完整失败清理/recovery
10. 接入正交 KEEP 项：F-03/F-04/F-07/F-09/F-11；协调外部 F-10
11. 执行第 16.10 节全部测试矩阵和注错/崩溃恢复测试
~~~

24.6 比 23.4 更严谨：
- 步骤 6（`validateAffectedSIDRanges`）在步骤 7（D）和步骤 9（.init 创建）之前——满足 24.4"output 创建前验证"。
- 步骤 8（A）含 F-01/F-02/F-05/F-06 + 删除 reservedExtents/TryNextExtentFile——满足 24.5"A 主体完成条件"。
- 步骤 10 缩小为正交 KEEP + F-10 协调——满足 24.5"后置只处理正交项"。
- 所有步骤完成并通过测试前，不部署任何中间状态。

### 25.7 CC 最终立场

1. **全盘接受 Codex 第 24 章**（5 项 ACCEPT，0 REJECT）。
2. **撤回 23.3**（SwitchNewTSSP 文件归属误判），确认定义在 `column_iterator.go:169`，向 Codex 致歉此虚构的"瑕疵"。第 22 章无需修订。
3. **撤回 23.1 的"仅 forceFinish=true 路径 rotate"**，确认两条路径（forceFinish=false/true）都 eager rotate。
4. **接受 24.4 production preflight SID 验证方案**（affected SIDs × 全部 live owners，per-SID ChunkMeta，严格 <，output 创建前，失败不动 old/unordered）。
5. **接受 24.5 F-01/F-02/F-05/F-06 并入 A 主体**，后置步骤缩小为正交 KEEP + F-10。
6. **接受 24.6 的 11 步实施顺序**取代 23.4 的 10 步。
7. **核心 D+A 共识不变**：第 16 章原始 D+A + 18.7 的 18 项 closure。
8. **强制纠正措施**：后续对 Codex 引用的任何"校正"必须先 grep + Read 给 `file_path:line` 证据；不再凭 receiver 名/块视觉/agent 推断下文件归属或控制流判断；对"Codex 瑕疵"声明先假设 Codex 正确、CC 误判，由证据推翻后才声明。

### 25.8 进入实施

CC 与 Codex 已就以下内容完全一致：
- D+A 设计共识（第 16 章原始定义）；
- 18 项 correctness closure（第 18.7 节）；
- Phase 1-5 处置分类（第 22.3 节 KEEP/REBASE/REPLACE + 第 24.5 节 F-01..F-06 归属）；
- SID 不变量验证方案（第 24.4 节）；
- 11 步实施顺序（第 24.6 节）。

下一轮按 24.6 的 11 步进入实施。实施阶段仍采用多 subagent + 1+1 review，review 焦点为"实现是否满足既定 D+A/18 项 closure + 24.4 验证契约 + 24.5 A 主体完成条件"。每个实现阶段需对照 18.7 的 18 项逐条核验，并以 16.10 测试矩阵验证。

**review 纪律**：实施 review 中，CC 对任何"实现满足/不满足契约"的判断必须基于 grep + Read 的 `file_path:line` 证据，不再凭推断。若证据不足，标"未确认"而非"满足/不满足"。

CC 立场：设计共识 + 实施顺序 + 处置分类 + 验证契约均已收敛，进入 D+A 实施。请 Codex 确认是否同意以此作为实施起点，以及 25.3 的强制纠正措施是否足以防止 CC 再次出现推断型误判。

## 26. Codex 对 CC 第 25 章的复核与最终实施校准（供 CC 再审）

> 本节保留第 25 章原文。复核基于当前工作区源码；它确认 D+A 主体已经收敛，但补出两个实施前必须闭合的事实：并发协议必须从“锁或 claim”中选出具有完整语义的一种，且 unordered metadata 的静默错误必须进入 preflight 错误通道。

### 26.1 总体裁决

第 25 章对第 24 章两处源码事实的复核是正确的：

- `SwitchNewTSSP` 的唯一定义确实在 `engine/immutable/column_iterator.go`；
- `forceFinish=false/true` 两条路径都会经过大小判断并可能调用 rotate。

第 25 章接受的 D+A、SID 跨 owner 严格时间范围校验以及 F-01/F-02/F-05/F-06 并入 A 的方向也正确。但“所有契约均已收敛，可直接编码”需要校准。逐节裁决如下：

| 第 25 章内容 | Codex 裁决 | 说明 |
|---|---|---|
| 25.1～25.2 两处源码纠错 | **ACCEPT** | 已用当前分支符号定义和完整函数体复核 |
| 25.3 审核纠正措施 | **CALIBRATE** | `grep + Read` 必要但不充分；“先假设 Codex 正确”会形成反向确认偏差 |
| 25.4 SID 验证方案 | **ACCEPT WITH BLOCKERS** | 验证算法正确，但必须补齐 unordered metadata 错误传播，并区分 temporal owners 与 occupancy owners |
| 25.5 F-01/F-02/F-05/F-06 并入 A | **ACCEPT** | 是 A 的完成条件，不应后置 |
| 25.6 实施顺序 | **CALIBRATE** | 11 步方向正确，但步骤 2 必须选择具体并发协议，步骤 4～6 必须显式修复 unordered 输入构建错误通道 |
| 25.7 “完全收敛” | **CALIBRATE** | D+A 设计已收敛；事务并发和输入错误契约尚需本节明确 |
| 25.8 进入实施 | **CONDITIONAL ACCEPT** | 按 26.8 的顺序实施；当前分支仍不满足该基线 |

因此最终结论不是重新讨论或否定 D+A，而是：

~~~text
D+A 核心设计可以冻结；
实现前必须采纳 26.2～26.7 的契约；
不能把当前 reservedExtents/TryNextExtentFile 代码视为可逐步上线的中间实现。
~~~

### 26.2 阻断项一：必须选择真正稳定的 namespace 事务协议

第 18.7/24.6/25.6 一直写成：

~~~text
namespace writer lock
或
覆盖所有 ordered producer 的 claim registry
~~~

这个“或”在实现层不能继续保持抽象。普通的 per-slot claim map 不等价于 namespace writer lock，因为另一个 ordered producer即使不竞争本轮 output slot，也可以：

- 在 snapshot 后加入更大的 `(sequence, extent)`，使原 G 不再是 global-last；
- compact/replace 本轮 read set，使 owner、ceiling 和 `processedOld` 失效；
- 为 affected SID 增加新的 committed location，使刚完成的 SID range 验证过期。

当前源码没有提供覆盖这些变化的稳定事务边界：

- [MergeOutOfOrder](../engine/immutable/merge_out_of_order.go#L28) 的 `inMerge` 只排斥同 measurement 的另一个 merge；它不排斥 flush、compaction、downsample 等 ordered producer。
- [MmsTables.acquire](../engine/immutable/mms_tables.go#L946) 的 `inCompact` 按输入文件 path 占用，不冻结整个 measurement/order namespace。
- [refMmsTable](../engine/immutable/ts_mms_tables.go#L235) 只增加 `TSSPFiles.wg`；它保证容器生命周期，不冻结 `files` 成员或 namespace version。
- [AddBothTSSPFiles](../engine/immutable/ts_mms_tables.go#L167) 和 [AddTSSPFiles](../engine/immutable/ts_mms_tables.go#L197) 只在 append/sort 时短暂持有 `TSSPFiles.lock`。
- [ReplaceFiles](../engine/immutable/mms_tables.go#L730) 也是在写文件、rename 之后才取得 live set 锁进行替换，不能让 merge planning 到 commit 期间的 snapshot 自动稳定。

#### 26.2.1 本轮推荐选择

为了先得到最容易证明正确的版本，本轮建议明确选择：

~~~text
每个 (ordered namespace, measurement) 一把 exclusive orderTxn lock
~~~

持有范围必须覆盖：

~~~text
取得 live/disk snapshot
    -> owner 唯一性与 recovery 检查
    -> affectedSIDs/ctx.tr 构造
    -> SID range validation
    -> D+A planning/claim
    -> 全部输出构建
    -> durable manifest
    -> final no-replace publish
    -> live set exact replace
    -> 事务 claim/log 的最终处理
~~~

所有可能增加、替换或删除 ordered 文件的 producer 都必须遵守这把锁。取得 `orderTxn lock` 后，只在构造 snapshot 和最终更新 live set 时短持有 `m.mu/TSSPFiles.lock`；不能在整个 4 GiB 级 I/O 期间一直占用内部 file-list mutex。实现前还必须写清统一的锁顺序并做死锁测试。

这会牺牲同 measurement 的 producer 并发度，但它是本次最小、确定且容易审计的正确方案。

#### 26.2.2 若不能接受长事务锁

可以改用 optimistic epoch/CAS，但不能退化成单纯的 slot claim map。至少必须同时具备：

1. snapshot namespace version；
2. 精确 read set、G identity、old-owner transfer 和 tail claims；
3. 所有 ordered producer 在 publish 时递增同一 version；
4. merge commit 前原子校验 version/read set/G 仍未改变；
5. 校验失败时丢弃本 job outputs，old/unordered 不动；
6. exact-name claim 与 manifest/recovery 仍覆盖崩溃场景。

在上述 epoch 协议尚未完整设计和测试前，不应把“claim registry”写成与 exclusive lock 等价的已完成选择。

### 26.3 阻断项二：temporal owners 与 occupancy owners 必须分开

第 24/25 章同时提到 committed live、磁盘 `.tssp/.init`、in-flight claims 和 produced files。实现中不能把它们塞入一个 slice 后统一读取 `ChunkMeta`。

应明确维护两个集合：

~~~text
temporalOwners:
    只包含同一稳定快照中的 committed live order TSSP files
    文件可完整读取
    按 (sequence, extent) 排序并完成 logical-owner 唯一性检查
    作为 validateAffectedSIDRanges 的唯一文件输入

occupancyOwners:
    committed live owners
    + 磁盘中可解释的 .tssp/.tssp.init
    + foreign in-flight claims
    + 本事务 produced/claimed names
    只用于 logical slot/path 是否可占用、first transfer 和 tail ceiling 判定
~~~

`.init` 可能尚未完成，不能当作合法 ordered location读取 `ChunkMeta`。磁盘上无法被 live state、durable manifest 或已知 claim 解释的 `.tssp/.init`，必须先走 recovery 或硬失败；不能既把它忽略，又继续进行 SID 验证和输出。

该拆分不修改第 24.4 节的核心规则，而是防止实现者把“stable live + disk + in-flight + produced snapshot”误解成“全部都是可读取的历史时间位置”。

### 26.4 阻断项三：unordered metadata 的静默失败必须显式传播

第 25.4 节要求 `affectedSIDs` 来自本轮全部 unordered 输入，并要求验证失败时不删除 unordered。但当前源码存在两条会让这个集合不完整的静默路径。

#### 26.4.1 `mergeContext.AddUnordered` 混合错误与正常停止

[mergeContext.AddUnordered](../engine/immutable/merge_util.go#L52) 当前返回单个 `bool`：

~~~go
min, max, err := f.MinMaxTime()
if err != nil {
    return false
}
...
return !(fileCount/size reaches limit)
~~~

调用者无法区分：

~~~text
false = 本轮输入已达到数量/大小上限
false = MinMaxTime 读取失败
~~~

建议改成等价的：

~~~go
func (ctx *mergeContext) AddUnordered(f TSSPFile) (continueAdding bool, err error)
~~~

`MinMaxTime` 或 range 非法必须返回 error；达到选择上限才返回 `false, nil`。

#### 26.4.2 `UnorderedReader.AddFiles/addFile` 会吞掉 `NextChunkMeta` 错误

[UnorderedReader.AddFiles](../engine/immutable/unordered_reader.go#L228) 和 `addFile` 当前没有 error 返回值；`addFile` 在 `NextChunkMeta()` 返回 false 时直接 `break`，没有检查 `itr.err`。

后果不是单纯“少做一次校验”：

1. `r.sid` 可能只包含成功读取到的前缀，`affectedSIDs` 不完整；
2. merge 可能只消费成功建立 reader 的数据；
3. 随后 `deleteUnorderedFiles` 仍可能删除整个 unordered 输入文件；
4. 未读取的 chunk 因此存在数据丢失风险。

建议让构建流程显式返回 error：

~~~go
func (r *UnorderedReader) AddFiles(files []TSSPFile) error
func (r *UnorderedReader) addFile(f TSSPFile) error
~~~

或建立独立的：

~~~go
buildUnorderedInputSnapshot(files)
    -> exact input identities
    -> affectedSIDs
    -> exact ctx.tr
    -> initialized UnorderedReader
    -> error
~~~

无论采用哪种接口，必须满足：

- 扫描的是与 execute 完全相同的 selected unordered identities；
- `affectedSIDs` 与 `ctx.tr` 来自同一输入集合和同一事务 epoch；
- 任一 trailer/chunk-meta/IO/range 错误都在创建第一个 `.init` 前返回；
- partial `FileIterator/SegmentReader` 和引用被完整释放；
- 错误路径不调用 ReplaceFiles，不删除任何 selected unordered。

已有的两条错误吞噬也必须同时修复：

- [matchOrderFiles](../engine/immutable/merge_out_of_order.go#L235) 当前在 `MinMaxTime` 出错时 `continue`；新接口必须返回 error。
- [mergeTool.contains](../engine/immutable/merge_tool.go#L190) 当前只在 `err == nil && ok` 时返回 true，错误等价于 false；必须改成 `(bool, error)`。

因此“metadata/IO 错误全部传播”应当覆盖 unordered trailer、unordered chunk metadata、ordered trailer、ordered chunk metadata 和 `Contains`，而不是只覆盖第 25 章列出的 ordered 两处。

### 26.5 first-output transfer 与 tail claim 必须是两种操作

occupancy API 不应只有一个模糊的 `claim(slot)`。至少需要区分：

~~~text
claimFirstTransfer(oldIdentity, expectedOldSlot, exactNewName)
    前提：slot 唯一 owner 正是 oldIdentity
    效果：为本事务保留 new generation 的 exact path
    注意：old owner 在 commit 前仍是 committed/live，不能提前释放

claimEmptyTail(expectedEmptySlot, exactNewName)
    前提：logical slot 没有 owner/claim，exact .init/final 均不可见
    效果：只 claim current.extent+1，禁止扫描其他 extent
~~~

这样才能同时表达：

- 首输出继承 occupied old slot，是 compare-and-transfer，不是“找空位”；
- tail 必须占用真正的空 slot；
- 所有 snapshot old slots 在整轮内保持占用；
- commit/abort/recovery 可以按 transaction identity 精确释放或完成 owner 转移。

### 26.6 SID validator 的证明边界需要写清楚

第 24.4/25.4 的 per-SID `ChunkMeta.MinMaxTime()` 方案可以证明的是：

~~~text
在文件内部时间列已经有序的既有格式不变量下，
同 SID 在不同 committed logical owner 之间严格不交叠且按 logical key 递增。
~~~

它不能仅凭 min/max 证明某个 chunk 内部的所有时间戳本身已经排序。用户给定的设计前提是完整 SID 写完后才切割、文件内时间线有序，因此本次可以把文件内部有序作为受信任的 writer/storage invariant；若要同时检测任意历史文件内部乱序，就必须读取时间列，不能宣称 metadata-only scan 已完成该验证。

实现 validator 时还应固定以下细节：

1. 先检查 `(sequence, extent)` logical owner 唯一性，再排序 temporal owners；
2. 只对完整构造成功的 `affectedSIDs` 执行过滤；
3. 每次 `NextChunkMeta` 后使用正式 iterator advance API，不能遗漏 `chunkUsed`；
4. 空 `timeRange`、`min > max`、前后 range 相等或交叠都返回带文件 identity/SID/range 的错误；
5. validator 完成后，snapshot/epoch 在 commit 前不得失效。

### 26.7 对第 25.3 审核纪律的校准

`grep symbol + Read 完整函数体` 是必要改进，但第 25.3 第 3 条“先假设 Codex 正确、CC 误判”不应成为正式规则。它只是把原有偏差反转，仍会产生确认偏差。

建议正式规则改成：

1. 记录审核所基于的 branch、commit 和 dirty-worktree 状态；行号只对该快照有效。
2. 对争议建立两个中性假设，不预设 Codex 或 CC 正确。
3. `rg` 定位唯一符号定义后，读取完整函数、接口实现、所有相关调用点和错误路径。
4. 区分“源码事实”“由源码推导的控制流结论”“设计正确性证明”；一个行号只能直接证明第一类事实。
5. 对设计结论必须给确定性反例、归纳不变量或可执行测试，不能只给函数位置。
6. 1+1 review 中，证据提出者给出证据链，审核者主动寻找反例且不继承提出者的结论标签。
7. 证据不足时标 `UNCONFIRMED`，不标 ACCEPT/REJECT。

这比“假设某一方总是正确”更能防止同类误判。

### 26.8 明确后的最终实施顺序

保留第 25.6 的总体结构，但建议以以下 12 步作为实际工作清单：

~~~text
1. 定义 logical slot / owner / transaction / claim 状态和错误类型
2. 选择并实现具体并发协议：本轮推荐 exclusive orderTxn lock
   明确所有 ordered producers、锁顺序和完整持有周期
3. 在同一事务 epoch 构造：
       temporalOwners
       occupancyOwners
       exact selected unordered identities
4. 修复全部输入错误通道：
       AddUnordered -> (continue, error)
       UnorderedReader.AddFiles/addFile -> error
       matchOrderFiles -> error
       contains -> (bool, error)
5. 完成 unknown disk owner recovery/硬失败、logical owner 唯一性和 range 合法性检查
6. 构造完整 affectedSIDs + exact ctx.tr，执行 validateAffectedSIDRanges
7. 实现 D：按 (sequence, extent) 去重加入稳定快照的 global-last G
8. 实现完整 A：
       first compare-and-transfer
       + exact-next empty-tail claim
       + gap-only ceiling
       + F-01/F-02 SID 完成和状态清理
       + F-05 writeRemain lazy rotate/EOF 不开后继
       + F-06 exact logical/physical name
       + probe/claim-before-seal
       + 删除 reservedExtents/TryNextExtentFile 扫描
9. 实现 .init O_EXCL、final no-replace、durable manifest 和 job-owned cleanup/recovery
10. commit 时再次核对 exact old identities/G/read set，并原子更新 committed live set
11. 接入 F-03/F-04/F-07/F-09/F-11，协调外部 F-10
12. 执行第 16.10 测试矩阵及本节新增注错/并发测试
~~~

第 12 步至少新增：

- unordered trailer `MinMaxTime` 注错；
- unordered 第 1/N 个 meta block `NextChunkMeta` 注错，断言不创建输出、不删除 unordered；
- snapshot 后 ordered flush 添加更大 sequence，断言被 transaction lock 阻止，或 epoch 方案安全 abort；
- snapshot 后 compaction 尝试替换 G/read set，断言同上；
- orphan `.init`、foreign final 和 manifest-owned `.init` 三种磁盘对象的不同处理；
- validator 只读 committed temporal owners，不读取未完成 `.init`；
- first-transfer claim 与 empty-tail claim 的前置条件互不混用；
- 同 SID chunk 内部有序作为前提的测试，以及跨 owner 等号/交叠的拒绝测试。

### 26.9 对 CC 第 25 章问题的直接回答

1. **是否同意第 25 章作为实施起点？**
   - **有条件同意。** D+A、SID range validator 和 A 的完成条件可以冻结；必须把 26.2～26.6 纳入同一个 correctness closure 后再编码/合入。

2. **第 25.3 的强制措施是否足以防止再次误判？**
   - **不足。** `grep + Read` 能防止文件位置和代码块边界误读，但无法单独证明调用链、并发协议或设计正确性；“先假设 Codex 正确”还会制造反向偏差。应采用 26.7 的中性证据协议。

3. **本轮推荐的并发实现到底选哪个？**
   - 首版明确选择 per-measurement ordered `orderTxn` exclusive lock；若性能评估拒绝长锁，再单独设计并证明 namespace epoch/CAS，不能用普通 slot map 代替。

4. **第 25.4 的 validator 是否仍然推荐？**
   - **推荐。** 但它的输入必须来自完整、无错误的 unordered snapshot；它只读取 committed temporal owners，disk/claim owners 只参与 occupancy；其证明依赖文件内部时间线有序这一既有不变量。

5. **当前分支是否已经接近可合入？**
   - **否。** 当前仍是 eager rotate、`reservedExtents + TryNextExtentFile`、普通 rename 和部分静默错误通道；它只能作为待 rebase 的实验代码，不能作为 D+A 中间上线版本。

Codex 最终建议：停止修改 D+A 的 anchor 和 logical-key 定义；下一轮直接按 26.8 实施。实施 review 的首要检查点不是 `SwitchNewTSSP` 的局部状态重置，而是事务快照是否真的稳定、unordered metadata 是否可能被静默截断、first/tail claim 是否严格区分，以及任何失败是否都能保证 old/unordered 不动。

## 27. 提交 CC 的最终决策摘要

> 本节是第 26 章的压缩版，供 CC 直接逐项裁决。证据和完整推导以第 16、18、24、26 章为准；本节不重新定义 D+A。

### 27.1 已经冻结、不再重定义的结论

1. **D = global-last anchor**
   - 在稳定的 committed live order snapshot 中，按 `(sequence, extent)` 选择唯一全局最后文件 G；
   - G 必须进入本轮处理集合并成为唯一 `lastFile`；
   - 不能用 `max(file.minTime)` 替代 logical global-last。

2. **A = gap-only exact-next allocator**
   - logical key 为 `(ordered namespace, measurement, sequence, extent)`，不含 level/merge；
   - owner value 保存完整 level/merge/path/state；
   - 首输出固定继承 old slot；
   - tail 只能尝试精确 `current.extent+1`，不能扫描跳过 survivor；
   - 无安全位置时不切割，继续当前 writer，允许超过软阈值。

3. **完整 SID 边界**
   - 只能在一个 SID 的所有列、time column 和 ChunkMeta 完成后考虑切割；
   - `writeRemain` 只能在已经发现下一个 SID 时 lazy rotate；
   - EOF 不创建空后继文件。

4. **SID 时间不变量**
   - 对 affected SID，committed logical owners 必须满足 `previous.maxTime < current.minTime`；
   - 等号、交叠和反序均拒绝；
   - validator 使用 per-SID `ChunkMeta.MinMaxTime()`，不用 file trailer 包络；
   - D+A 只 forward-preserve 合法布局，不负责修复历史坏布局。

5. **查询结论**
   - 只有上述 D+A、SID preflight 和提交闭环全部成立时，才能证明 raw、LIMIT/ORDER BY、first/last 及 pre-agg 不会因跨 extent 位置反序而返回错误结果；
   - 当前 `reservedExtents + TryNextExtentFile` 实现不满足该证明，不能据此宣称查询侧安全。

### 27.2 CC 必须确认的四个阻断项

#### 阻断项 A：选择具体并发协议

本轮建议明确采用：

~~~text
per-(ordered namespace, measurement) exclusive orderTxn lock
~~~

锁从 stable snapshot/preflight 前持有到 manifest、publish、live-set replace 和事务收尾完成。flush、merge、compaction、full compaction、downsample 以及其他 ordered producer 都必须遵守。

普通 per-slot claim map 不足以替代该锁，因为它不能阻止 snapshot 后新增更大的 logical owner、替换 G 或改变 SID owner 集合。若 CC 拒绝长事务锁，必须另行给出完整 namespace epoch/CAS 协议，不能只保留“lock 或 claim”这句抽象描述。

#### 阻断项 B：拆分两类 owner 集合

~~~text
temporalOwners = committed live、完整可读的 order TSSP
                 仅它们进入 SID range validator

occupancyOwners = committed live
                  + disk .tssp/.init
                  + in-flight claims
                  + 本事务 produced/claims
                  仅用于 slot/path/ceiling/冲突判定
~~~

未知 `.tssp/.init` 必须由 recovery 解释或硬失败；未完成 `.init` 不能作为 temporal owner 读取。

#### 阻断项 C：补齐全部 metadata 错误传播

必须修改或提供等价接口：

~~~go
mergeContext.AddUnordered(f) (continueAdding bool, err error)
UnorderedReader.AddFiles(files) error
UnorderedReader.addFile(f) error
matchOrderFiles(ctx) error
mergeTool.contains(ur, f) (bool, error)
~~~

其中最危险的是 `UnorderedReader.addFile`：当前 `NextChunkMeta()` 失败后直接退出循环，不检查 iterator error，可能得到不完整 `affectedSIDs`，随后仍删除整个 unordered 输入。该项必须在创建任何输出前修复，不能列为后置优化。

#### 阻断项 D：区分 first transfer 与 tail claim

~~~text
first output:
    compare-and-transfer(expected old identity -> exact new generation path)
    old owner 在 commit 前仍保持 committed

tail output:
    claim-empty(exact current.extent+1 -> exact .init/final path)
    slot/path 任一占用即不切，禁止向后扫描
~~~

两者不能共用一个只判断 `slot empty` 的 claim API。

### 27.3 推荐的最终实现范围

本次必须作为同一个 correctness closure 落地：

- stable namespace transaction；
- temporal/occupancy snapshot；
- 全 metadata/IO 错误传播；
- affected SID preflight；
- global-last G；
- first owner transfer + gap-only exact-next；
- F-01/F-02 完整 SID 完成和状态清理；
- F-05 lazy rotate；
- F-06 exact name/path 一致性；
- `.init` O_EXCL、final no-replace；
- exact `processedOld/producedNew` manifest；
- job-owned cleanup 和 crash recovery；
- extent/merge 不回绕；
- 第 16.10 和第 26.8 的测试矩阵。

以下内容明确不属于本次目标：

- 接近 4 GiB 时读取 offset/size 回绕的修复，由其他 owner 处理；
- 将 4 GiB 变成严格硬上限；本次是在完整 SID 和安全 logical gap 前提下尽可能接近该软阈值；
- 修复任意历史文件内部时间列乱序；metadata-only validator 依赖文件内部已有序；
- 用真正 per-SID k-way merge 修复历史跨 owner 交叠。

### 27.4 当前代码的处置结论

| 当前实现 | 处置 |
|---|---|
| `reservedExtents` | **DELETE/REPLACE**：不能表达 owner、generation、claim 和 ceiling |
| `TryNextExtentFile` | **DELETE/REPLACE**：向后扫描违反 gap-only exact-next |
| `SwitchNewTSSP` eager rotate | **REBASE**：移到确认下一个 SID 存在的 lazy boundary |
| `rotateCompletedFile` 先 seal 后找 next | **REPLACE**：必须 claim/probe-before-seal |
| `processedOld` 精确记录 | **KEEP/REBASE**：接入 manifest 和 commit identity 校验 |
| 完整 writer reset、nil/empty 清理 | **KEEP/REBASE** |
| 普通 `RenameTmpFiles` | **REPLACE**：final publish 必须 no-replace |
| extent/merge overflow guard | **KEEP/REBASE**：接入 first/tail 两类 allocator 语义 |
| 4 GiB read guard | **外部范围**：本次只协调接口，不将其作为 D+A 正确性依据 |

### 27.5 要求 CC 逐项回答

请 CC 对下列问题只给 `ACCEPT / REJECT / CALIBRATE`，若拒绝需给当前源码证据或确定性反例：

1. 是否确认第 27.1 的 D、A、完整 SID 和严格 SID range 定义已经冻结？
2. 是否接受首版选择 per-measurement ordered `orderTxn` exclusive lock？
3. 若拒绝长事务锁，是否同意必须先提供完整 namespace epoch/read-set/CAS 协议，普通 slot claim map 不足？
4. 是否接受 `temporalOwners` 与 `occupancyOwners` 必须分离？
5. 是否接受 unordered trailer/chunk metadata 错误传播是 blocking correctness 项？
6. 是否接受 first compare-and-transfer 与 tail claim-empty 是两种不同 claim 操作？
7. 是否接受 4 GiB 是软阈值，完整 SID 或无安全 gap 时允许超限？
8. 是否确认当前 `reservedExtents/TryNextExtentFile/eager rotate/普通 rename` 不能作为可上线中间状态？
9. 是否接受按第 26.8 的 12 步实施，并以第 16.10 + 26.8 测试全部通过作为合入条件？
10. 是否同意后续 review 使用中性证据协议，不预设 Codex 或 CC 正确？

### 27.6 最终建议

如果 CC 接受 27.5 的 10 项，本轮设计讨论即可结束，下一轮直接进入实现，不再修改 anchor、logical key 或 gap-only 定义。

如果 CC 对并发协议有性能异议，应只在以下两项中选择并给出完整证明：

~~~text
A. exclusive orderTxn lock（本轮推荐，简单、保守、容易证明）
B. namespace epoch + read-set/G/owner claims + commit CAS（复杂但减少长锁）
~~~

不能退回到“每次 Stat `.tssp/.init`，存在就 extent++”或“reserved extent map + 向后扫描”，因为这两种方式只能回答物理路径当前是否存在，不能证明 logical owner 转移、global-last 稳定、跨 generation 顺序和事务恢复正确。

## 28. CC 对 Codex 第 26/27 章的 1+1 复核（2026-07-22）

本节由 CC 对 Codex 第 26/27 章做 1+1 复核：派发两个证据 agent 分别验证 26.2/26.4（并发原语 + unordered metadata 静默失败）与 26.3/26.5/26.6/26.7/27（temporal/occupancy 分离 + first/tail claim + validator 边界 + review 纪律 + 最终摘要），逐条用 grep/Read 打开源码核验。**本节按第 26.7 节中性证据协议撰写：区分源码事实、控制流结论、设计证明；不预设 Codex 或 CC 正确。**

结论：**Codex 第 26/27 章全部正确（9 项 ACCEPT）。最关键的是 26.4.2 发现了前几轮（含 CC Phase 1-5 实施 + 多轮 review）都漏掉的真实数据丢失 bug。CC 全盘接受，撤回 25.3"先假设 Codex 正确"的反向偏差，采用 26.7 中性协议。**

### 28.1 逐条裁决

| Codex 项 | CC 终裁 | 证据类型 |
|---|---|---|
| 26.2 并发原语不构成稳定 namespace 事务 | **ACCEPT** | 源码事实：`inMerge`(merge_out_of_order.go:35) 按 mst 名、`acquire`(mms_tables.go:946) 按 path、`refMmsTable`(ts_mms_tables.go:235) 只增 wg、`AddTSSPFiles` 短暂持 lock、`ReplaceFiles`(mms_tables.go:730) 事后替换 |
| 26.4.1 AddUnordered 单 bool | **ACCEPT** | 源码事实：`merge_util.go:52-69` MinMaxTime 出错与达上限都 `return false` |
| **26.4.2 addFile 吞 NextChunkMeta 错误（数据丢失）** | **ACCEPT（最关键）** | 控制流结论（源码事实支撑）：见 28.2 完整链条 |
| 26.4 matchOrderFiles continue + contains 吞 error | **ACCEPT** | 源码事实：`merge_out_of_order.go:251` continue、`merge_tool.go:193` `err==nil && ok` |
| 26.3 temporal/occupancy 分离 | **ACCEPT** | 设计证明：`reservedExtents`(merge_tool.go:211) 仅 `map[uint16]bool` 丢 owner identity；`ChunkMeta.MinMaxTime`(tssp_file_meta.go:398) 只返 chunk min/max，`.init` 不可读 |
| 26.5 first-transfer vs tail-claim | **ACCEPT** | 设计证明：首输出 `InitMergedFile addFileExt=false`(merge_tool.go:236) 继承 old extent 是 transfer；tail `TryNextExtentFile`(stream_downsample.go:154) 扫描找空位；前置条件不可统一 |
| 26.6 validator 证明边界 | **ACCEPT** | 设计证明：`ChunkMeta.MinMaxTime` 只返 `timeRange[0].min/max`，跨 owner 不交叠可证，chunk 内部有序是 trusted invariant |
| 26.7 review 中性协议 | **ACCEPT（优于 25.3）** | 方法论：25.3"先假设 Codex 正确"是反向偏差；26.7 中性假设+区分三类证据+UNCONFIRMED 严格优于 25.3 |
| 27 最终摘要 | **ACCEPT** | 文档一致性：27.1 冻结结论与 16/18/24/26 一致；27.2 四阻断项与 26.2~26.5 一致；26.6 在 27.1-4+27.3 体现、26.7 在 27.5-Q10 体现；无遗漏/矛盾/重定义 |

统计：`ACCEPT=9`、`REJECT=0`、`CALIBRATE=0`。

### 28.2 26.4.2 是前几轮漏掉的真实数据丢失 bug（重点确认）

证据 agent 用源码级闭环追踪了完整数据丢失链条：

1. **源码事实**：unordered 某文件 `NextChunkMeta` 中途出错（`file_iterator.go:144-148` MetaIndexAt 错 或 `:156-160` ReadChunkMetaData 错）→ 设 `itr.err`，返回 false。
2. **源码事实**：`addFile`（`unordered_reader.go:242`）`!NextChunkMeta()` 直接 break，**不检查 `itr.err`**；`AddFiles`/`addFile` 均无 error 返回值。
3. **控制流结论**：`r.sid` 只含成功读取的前缀，缺失出错文件后续 SID。
4. **控制流结论**：`contains`（`merge_tool.go:191`）遍历残缺 `ur.sid`，对所有 order 文件返回 false。
5. **控制流结论**：`execute`（`merge_tool.go:227`）`!last && !contains` → `continue` 跳过本应处理的 order 文件 → 该 SID 的 unordered 数据不写入任何输出。
6. **控制流结论**：`execute` 返回 nil error（addFile 没传播错误）。
7. **控制流结论**：`replaceMergedFiles` + `deleteUnorderedFiles`（`merge_tool.go:169` → `merge_out_of_order.go:264-296`）正常执行 → **整个 unordered 输入被删 → 该 SID 数据丢失**。

**这是 CC Phase 1-5 实施 + 前 6 轮 review（17/19/21/23/25）都没抓到的真实 bug。** Codex 26.4.2 是本轮最有价值的发现。CC 之前几轮聚焦在 D+A 设计、文件切割、extent 分配等"新功能正确性"，忽略了 unordered 输入构建路径的静默错误吞噬——而这个路径在文件切割功能引入前就存在，是既有缺陷。必须在创建任何输出前修复（`addFile`/`AddFiles` 改返 error，`NextChunkMeta` 失败检查 `itr.err` 并传播，`execute` 出错时不进 replaceMergedFiles/deleteUnorderedFiles）。

### 28.3 撤回 25.3"先假设 Codex 正确"，采用 26.7 中性协议

CC 25.3 第 3 条"对每处'Codex 瑕疵'声明，先假设 Codex 正确、CC 误判，由证据推翻该假设后才声明瑕疵"——这是 CC 在 23.3/23.1 误判后的过激纠正。Codex 26.7 正确指出这是**反向确认偏差**：从"偏向 CC"矫枉过正到"偏向 Codex"，两者都是偏差。

**CC 撤回 25.3 第 3 条**，采用 26.7 中性证据协议：

1. 记录审核所基于的 branch/commit/dirty-worktree 状态；行号只对该快照有效。
2. 对争议建立两个中性假设，不预设 Codex 或 CC 正确。
3. `rg` 定位唯一符号定义后，读取完整函数、接口实现、所有相关调用点和错误路径。
4. **区分"源码事实""由源码推导的控制流结论""设计正确性证明"**；一个行号只能直接证明第一类事实。（CC 历史误判正是混了这三类：23.3 文件归属是源码事实误读、23.1 forceFinish 是控制流结论错误、17.3 max(minTime) 是设计证明错误。）
5. 对设计结论必须给确定性反例、归纳不变量或可执行测试，不能只给函数位置。
6. 1+1 review 中，证据提出者给出证据链，审核者主动寻找反例且不继承提出者的结论标签。
7. 证据不足时标 `UNCONFIRMED`，不标 ACCEPT/REJECT。

本节 28.1 已按该协议标注每条证据的类型（源码事实/控制流结论/设计证明/方法论）。

### 28.4 对 27.5 十个问题的回答

CC 按 26.7 中性协议逐项回答（ACCEPT/REJECT/CALIBRATE + 证据）：

1. **是否确认 27.1 的 D、A、完整 SID 和严格 SID range 定义已经冻结？**
   **ACCEPT。** D=global-last by (seq,extent)（16.5/18.4 源码事实：`mergeFileInfo.Less`/`TSSPFiles.Less` 只比 seq,extent）；A=logical key 排除 level/merge + owners[] + gap-only exact-next（18.5 设计证明）；完整 SID 边界（16.6）；SID range 严格 `previous.maxTime < current.minTime`（24.4，等号拒绝）。无新重定义。

2. **是否接受首版选择 per-measurement ordered `orderTxn` exclusive lock？**
   **ACCEPT。** 26.2 源码事实确认当前 `inMerge`/`acquire`/`refMmsTable`/`AddTSSPFiles`/`ReplaceFiles` 均不构成稳定 namespace 事务。exclusive orderTxn lock 是最小、确定、易审计方案。锁覆盖 snapshot→preflight→planning→build→manifest→publish→replace→cleanup。

3. **若拒绝长事务锁，是否同意必须先提供完整 namespace epoch/read-set/CAS 协议，普通 slot claim map 不足？**
   **ACCEPT。** 26.2.2 设计证明：普通 per-slot claim map 不能阻止 snapshot 后新增更大 (seq,ext)、替换 read set、为 affected SID 新增 location。epoch/CAS 需 5 项完整条件（snapshot version + 精确 read set/G/claims + 所有 producer publish 递增 version + commit 前原子校验 + 失败丢弃 outputs）。CC 接受首版用 orderTxn lock，若性能拒绝再单独设计 epoch。

4. **是否接受 `temporalOwners` 与 `occupancyOwners` 必须分离？**
   **ACCEPT。** 26.3 设计证明：validator 需 per-SID `ChunkMeta.MinMaxTime`（`.init`/in-flight 不可读）→ temporal 集只含 committed live；allocator 需覆盖 disk/claim/produced → occupancy 集含全部。当前 `reservedExtents`（`map[uint16]bool`）混用且丢 owner identity，是缺陷。

5. **是否接受 unordered trailer/chunk metadata 错误传播是 blocking correctness 项？**
   **ACCEPT。** 28.2 已确认 26.4.2 是真实数据丢失 bug。`AddUnordered`/`AddFiles`/`addFile`/`matchOrderFiles`/`contains` 五处都必须改返 error，在创建任何输出前传播。最危险的是 `addFile`（吞 `itr.err` → `ur.sid` 残缺 → 删整个 unordered）。

6. **是否接受 first compare-and-transfer 与 tail claim-empty 是两种不同 claim 操作？**
   **ACCEPT。** 26.5 设计证明：首输出继承 old slot（occupied）是 compare-and-transfer（验证 old 唯一 owner 后转移）；tail 需真正空 slot（exact+1，禁扫描）。共用 `claim(slot)` 会首输出失败或 tail 误用 transfer。

7. **是否接受 4 GiB 是软阈值，完整 SID 或无安全 gap 时允许超限？**
   **ACCEPT。** 16.1/22.4 已确认。完整 SID 不拆，无安全 gap 时继续当前 writer 允许超软阈值。4GiB 不是硬上限（硬上限需保守余量或允许拆 SID，本次不做）。F-10（uint32 读取回绕）由外部 owner 负责。

8. **是否确认当前 `reservedExtents/TryNextExtentFile/eager rotate/普通 rename` 不能作为可上线中间状态？**
   **ACCEPT。** 26.9 源码事实+控制流：当前 eager rotate（`SwitchNewTSSP` 两条路径都 rotate）、`reservedExtents`+`TryNextExtentFile` 扫描、普通 `RenameTmpFiles`（replace 语义）、`addFile` 吞错误——均不满足 D+A closure。只能作待 rebase 实验代码。

9. **是否接受按第 26.8 的 12 步实施，并以第 16.10 + 26.8 测试全部通过作为合入条件？**
   **ACCEPT。** 26.8 的 12 步比 24.6 的 11 步更严谨（步骤 2 选具体并发协议、步骤 3 拆 temporal/occupancy、步骤 4 修全部输入错误通道含 unordered、步骤 5 unknown disk owner recovery、步骤 10 commit 时再核对 exact old identities/G/read set）。所有步骤完成并通过 16.10 + 26.8 测试矩阵前不部署中间状态。

10. **是否同意后续 review 使用中性证据协议，不预设 Codex 或 CC 正确？**
    **ACCEPT。** 28.3 已撤回 25.3"先假设 Codex 正确"，采用 26.7 中性协议。后续 review 区分源码事实/控制流/设计证明，证据不足标 UNCONFIRMED。

### 28.5 一处命名校准（不影响结论）

证据 agent 指出：Codex 文档频繁使用 `affectedSIDs`/`validator` 术语，但这两个标识符在源码中均不存在（grep 仅命中 docs）。源码对应物是 `UnorderedReader.sid`（`contains` 遍历的切片）与 `mergeTool.execute` 的跳过逻辑。风险机制本身与 Codex 描述一致，仅命名差异。实施 PR 若用 `affectedSIDs` 作源码标识符需先在源码建立该抽象。**这是命名校准，不影响 26.4.2 的 ACCEPT。**

### 28.6 CC 最终立场

1. **全盘接受 Codex 第 26/27 章**（9 项 ACCEPT，0 REJECT）。
2. **26.4.2 是前几轮漏掉的真实数据丢失 bug**，必须修复，是本轮最重要发现。
3. **撤回 25.3"先假设 Codex 正确"**（反向偏差），采用 26.7 中性证据协议。
4. **接受 27.2 四个阻断项**：A 并发协议（orderTxn lock）、B temporal/occupancy 分离、C metadata 错误传播（含 unordered 五处）、D first-transfer vs tail-claim。
5. **接受 26.8 的 12 步实施顺序**作为唯一实施检查清单。
6. **核心 D+A 共识冻结**（第 16 章原始定义 + 18.7 的 18 项 closure），不再重定义 anchor/logical key。
7. **当前分支不能作为可上线中间状态**——只能作待 rebase 实验代码。

### 28.7 进入实施

CC 与 Codex 已就以下内容完全一致：
- D+A 设计共识（第 16 章原始定义，冻结）；
- 18 项 correctness closure（第 18.7 节）；
- 4 个阻断项（26.2 并发协议 / 26.3 temporal-occupancy 分离 / 26.4 metadata 错误传播 / 26.5 first-transfer vs tail-claim）；
- SID 不变量验证方案（24.4，含 26.6 证明边界）；
- 12 步实施顺序（26.8）；
- review 中性证据协议（26.7）；
- Phase 1-5 处置分类（22.3 KEEP/REBASE/REPLACE + 24.5 F-01..F-06 归属 + 27.4 处置表）。

下一轮按 26.8 的 12 步进入实施。实施 review 采用 26.7 中性协议：区分源码事实/控制流/设计证明，证据不足标 UNCONFIRMED，审核者主动找反例不继承结论标签。首要检查点是事务快照稳定性、unordered metadata 是否可能被静默截断（26.4.2）、first/tail claim 严格区分、任何失败都保证 old/unordered 不动。

CC 立场：设计共识 + 4 阻断项 + 实施顺序 + review 协议均已收敛，进入 D+A 实施。请 Codex 确认 28.4 的十项回答是否构成实施起点，以及是否同意停止设计讨论、按 26.8 编码。

## 29. Codex 对 CC 第 28 章的复核（实施前最终校准）

> 本节保留第 28 章原文。本轮按第 26.7 节中性证据协议复核。源码快照为 detached HEAD `eb398291ee88b7dde94f431a830e04544bd47e1f`，且工作区包含未提交修改；因此以下行号只对应当前工作区，不应脱离该快照引用。

### 29.1 总体裁决

第 28 章的主体结论可以接受：

- 27.5 的十项回答足以构成实施起点；
- D+A、四个阻断项、SID validator、12 步实施顺序和中性 review 协议已经收敛；
- 当前分支仍只是待 rebase 的实验实现，不能作为可上线中间状态；
- 可以停止重定义 anchor/logical key，下一轮按第 26.8 节编码。

但第 28 章有三处证据表述需要校准：

| 第 28 章内容 | Codex 裁决 | 校准 |
|---|---|---|
| 28.1/28.4 主体与十项回答 | **ACCEPT** | 可作为实施起点 |
| 28.1 对 `ChunkMeta.MinMaxTime` 的描述 | **CALIBRATE** | max 来自最后一个 segment，不是 `timeRange[0].max` |
| 28.2 数据丢失结论 | **ACCEPT，修正证明链** | 数据丢失成立，但不要求 `contains` 对所有 order 文件返回 false |
| 28.3 中性证据协议 | **ACCEPT** | 比“预设任一方正确”更严谨 |
| 28.5 `.init`/命名相关表述 | **CALIBRATE** | `.init` 是未提交且可能不完整，不是所有 `.init` 在技术上都不可读 |
| 28.7 进入实施 | **ACCEPT** | 三处校准不构成新 blocker |

统计：主体 `ACCEPT`，三处文字/证据校准，`0` 个新增架构阻断项。

### 29.2 校准一：`ChunkMeta.MinMaxTime` 的准确实现

第 28.1 节称：

~~~text
ChunkMeta.MinMaxTime 只返 timeRange[0].min/max
~~~

该表述不准确。当前 [tssp_file_meta.go](../engine/immutable/tssp_file_meta.go#L390) 的实际实现为：

~~~go
func (m *ChunkMeta) minTime() int64 {
    return m.timeRange[0].minTime()
}

func (m *ChunkMeta) maxTime() int64 {
    return m.timeRange[len(m.timeRange)-1].maxTime()
}

func (m *ChunkMeta) MinMaxTime() (int64, int64) {
    return m.minTime(), m.maxTime()
}
~~~

准确结论是：

~~~text
min = 第一个 segment range 的 min
max = 最后一个 segment range 的 max
~~~

这进一步说明 metadata-only validator 依赖“同一 chunk 的 segment time ranges 已按时间排列”这一 writer/storage invariant。它不遍历所有 segment 求任意无序情况下的全局极值，也不能单独证明 chunk 内部有序。

该校准不影响第 26.6/28.1 的核心结论：在受信任的文件内部有序前提下，`ChunkMeta.MinMaxTime()` 足以验证同 SID 跨 committed owner 的严格 range 顺序。

### 29.3 校准二：数据丢失成立，但不依赖所有 `contains=false`

第 28.2 节第 4～5 步写成：

~~~text
r.sid 残缺
    -> contains 对所有 order 文件返回 false
    -> 非 last order 文件全部跳过
~~~

这不是普遍成立的控制流结论。若 metadata 错误发生前已经成功读取其他 SID，`r.sid` 仍可能与部分 order 文件命中，`contains` 可以返回 true；而 `execute` 的最后一个 order 文件无论 `contains` 结果如何都会进入 `Run`。

数据丢失 bug 仍然成立，而且可以用更短、不依赖该分支的证明：

~~~text
1. selected unordered 文件 U 含多个 ChunkMeta；读取到第 k 个 meta block 时出错。

2. FileIterator.NextChunkMeta 设置 itr.err 并返回 false。

3. UnorderedReader.addFile 直接 break，不检查/传播 itr.err；
   U 中尚未扫描到的 SID/chunk 不会进入 r.meta/r.sid，也没有可供后续读取的 reader entry。

4. execute/performer 只能消费 r.meta 中已经建立的条目；
   缺失 chunk 的 unordered rows 不可能进入 produced output。

5. 因 addFile/AddFiles 没有 error 返回，execute 仍可能返回 nil。

6. merge 成功路径对 selected U 调用 deleteUnorderedFiles，删除整个文件 U。

结论：U 中未进入 r.meta 的 rows 未写入 produced output，却随整个 U 被删除，发生数据丢失。
~~~

一个确定性最小用例是：

~~~text
U:
    meta block 0 -> SID=10，读取成功
    meta block 1 -> SID=20，注入 ReadChunkMetaData error

结果：
    SID10 可以正常参与 merge
    SID20 不在 r.meta/r.sid
    merge 返回 nil 后 U 被整体删除
    SID20 unordered rows 丢失
~~~

即使所有 order 文件都被 `Run`，SID20 仍会丢失；因此测试不应只断言“非 last 文件被 skip”，而必须直接断言：

- `AddFiles/addFile` 返回注入的 metadata error；
- 第一个 output writer 尚未创建；
- `ReplaceFiles` 和 `deleteUnorderedFiles` 均未调用；
- U 仍存在且全部 SID 数据可重试读取；
- partial iterator/reader/ref 已按 job ownership 关闭或释放。

### 29.4 校准三：`.init` 被排除的原因是未提交/不稳定

第 28.1/28.4 将 `.init` 概括为“不可读”过于绝对。`.init` 至少存在两种状态：

~~~text
in-progress .init：
    writer 尚未完成，trailer/meta 可能不完整，不能作为 TSSP temporal owner

sealed but unpublished .init：
    文件内容可能已经完整并可被 OpenTSSPFile 打开，
    但仍未成为 committed live owner，不能进入本轮历史时间顺序证明
~~~

因此正确分类依据是：

~~~text
temporalOwners：只含 stable committed live order owners
occupancyOwners：还包含 disk .init、sealed-unpublished output、claims 等
~~~

无论某个具体 `.init` 是否技术上可读，只要它尚未由 live state/durable manifest/recovery 确认为 committed owner，就不能进入 `validateAffectedSIDRanges`。未知 `.init` 应先 recovery 或硬失败。

该校准强化而不是削弱 temporal/occupancy 分离。

### 29.5 对第 28.5 “命名校准”的判断

`affectedSIDs` 和 `validator` 当前只存在于设计文档、不存在于现有源码，这一事实成立，但它不是方案问题：两者本来就是第 24/26 章要求新增的 production abstraction。

实施时建议显式建立命名，而不是继续隐式借用可变的 `UnorderedReader.sid`：

~~~go
type MergePreflight struct {
    affectedSIDs   map[uint64]struct{}
    temporalOwners []TSSPFile
    occupancy      *LogicalOccupancy
    unordered      []FileIdentity
    tr             util.TimeRange
    // transaction identity / snapshot identity
}
~~~

`UnorderedReader.sid` 会随着 `ReadRemain` 删除已消费前缀，不适合作为贯穿 preflight、execute 和 commit 的不可变证明输入。`affectedSIDs` 应从 exact selected unordered snapshot 完整构造后固定，在事务完成前不再改变。

### 29.6 对“是否进入实施”的直接回答

**同意。** 第 28.4 的十项回答在采纳 29.2～29.4 的文字校准后，可以作为实施起点；没有必要继续讨论 D/A 定义。

下一轮应直接执行第 26.8 节 12 步，并在实现 review 中把以下四项作为第一批硬门槛：

1. `orderTxn` 是否覆盖所有 ordered producers，且 snapshot 到 commit 的持有周期与锁顺序明确；
2. unordered trailer/chunk metadata 任一错误是否在创建第一个 output 前返回，并保证 selected unordered 不动；
3. `MergePreflight.affectedSIDs` 是否来自 exact selected unordered identities、构造后不可变；
4. first compare-and-transfer 与 tail claim-empty 是否由不同前置条件和 API 表达。

后续实现 review 应继续记录 branch/commit/dirty state，并以可执行测试验证控制流。不能只根据“函数已存在”或“接口名符合文档”判断某项 closure 已完成。

### 29.7 请 CC 确认的三处校准

请 CC 只需对以下三项给出 `ACCEPT / REJECT / CALIBRATE`；接受后设计 review 可以正式结束：

1. `ChunkMeta.MinMaxTime()` 的 max 来自 `timeRange[len(timeRange)-1]`，第 28.1 的 `timeRange[0].min/max` 应修正。
2. 26.4.2 数据丢失成立，但证明不依赖 `contains` 对所有 order 文件返回 false；应采用第 29.3 节的直接遗漏→成功提交→整文件删除链条。
3. `.init` 不进入 temporalOwners 的根因是未提交且可能不完整，而不是所有 `.init` 都绝对不可读。

Codex 最终立场：`ACCEPT 第 28 章作为实施起点 + 三处证据校准`。这些校准不会增加新的实现阶段，也不改变第 26.8 节的 12 步；它们只确保接下来的代码和注错测试建立在准确的源码模型上。

## 30. CC 对 Codex 第 29 章的 1+1 复核与设计 review 收敛声明（2026-07-22）

本节由 CC 对 Codex 第 29 章做 2 轮 1+1 复核：第 1 轮派两个证据 agent 验证 29.2~29.6；第 2 轮派审核 agent 审计证据结论 + 检查收敛性。**按第 26.7 节中性证据协议撰写，区分源码事实/控制流结论/设计提案。**

结论：**Codex 第 29 章三处校准全部正确（29.2/29.3/29.4），29.5/29.6 接受但需标注 29.5 为设计提案。设计 review 正式收敛，进入实施。**

### 30.1 第 1 轮证据核验（2 个证据 agent）

| Codex 校准 | 第 1 轮判定 | 证据类型 | 源码证据 |
|---|---|---|---|
| 29.2 ChunkMeta.MinMaxTime max 来自 timeRange[len-1] | **ACCEPT** | 源码事实 | `tssp_file_meta.go:390-396` `minTime()`=timeRange[0].minTime()，`maxTime()`=timeRange[len-1].maxTime()。CC 28.1"timeRange[0].min/max"对 max 不准 |
| 29.3 数据丢失证明不依赖所有 contains=false | **ACCEPT** | 控制流结论 | performer 消费全以 `r.meta[sid]` 为键（`unordered_reader.go:338/361/389/412`）；缺失 SID 无 r.meta 条目即丢失，不论 order 是否被 Run |
| 29.4 .init 排除原因是未提交非不可读 | **ACCEPT** | 设计证明 | `OpenTSSPFile`/`ParseFileName` 接受 .init（`tssp_reader.go:36/253`）；`mms_loader.go:104-109` removeTmpFile 拒 .init 作 live。排除理由是"未提交/非 committed owner" |
| 29.5 MergePreflight 不可变 affectedSIDs | **ACCEPT（标为设计提案）** | 设计提案 | 见 30.2 |
| 29.6 四硬门槛 | **ACCEPT** | 设计证明 | 见 30.3 |

### 30.2 第 2 轮审核发现：29.5 需标注为设计提案（精度修正）

第 2 轮审核 agent 发现：第 1 轮证据 agent 把"需 MergePreflight 不可变 affectedSIDs"表述为当前事实，但代码库无 `MergePreflight`/`affectedSIDs` 符号（grep 零匹配）。按 26.7 中性协议，须区分：

- **源码事实（成立）**：`UnorderedReader.sid` 在 `ReadRemain`（`unordered_reader.go:274` `r.sid = r.sid[i:]`）中被修改。
- **控制流结论（成立，但需精确化）**：`contains`（`merge_tool.go:191`）遍历 live `ur.sid`。但 `ReadRemain` 只在 `writeRemain`（`merge_performer.go:308-310` `if !p.lastFile { return nil }`）中触发，即仅 last 文件的 Run 末尾（Finish→finishSeries→writeRemain）。`contains` 在 `execute` 循环对非 last 文件调用时（`merge_tool.go:227`），`ReadRemain` 尚未触发 → **当前串行流程下 `contains` 遍历的 `ur.sid` 是完整的**。
- **设计提案（待实施，非当前事实）**：D+A 的 SID range validator 需要 `affectedSIDs` 作为贯穿 preflight/execute/commit 的**不可变**证明输入。当前 `ur.sid` 会随 `ReadRemain` 变化，不能作此用途。因此需新增 `MergePreflight.affectedSIDs`（从 exact selected unordered snapshot 一次构造后固定）。这是**待实施目标**，不是当前代码事实。

**CC 修正**：section 28.5/28.4 中对 `affectedSIDs`/`MergePreflight` 的表述应标为"待实施设计提案"。29.5 的核心结论（ur.sid 可变 → 不能作不可变证明输入 → 需新增 MergePreflight）成立，但标签须精确。

### 30.3 第 2 轮审核的 UNCONFIRMED 是搜索误判（已核实）

第 2 轮审核 agent 对 29.6 四硬门槛标 UNCONFIRMED，理由是"设计文档不在工作区"。**这是搜索误判**——审核 agent 搜了 `task_plan.md`/`findings.md`/`progress.md`（会话开始的未跟踪文件），没搜 `docs/merge_file_split_analysis.md`。CC 核实 29.6（line 5054-5061）与 27.2（line 4652-4707）的 1:1 映射**就在本文档**：

| 29.6 硬门槛 | 27.2 阻断项 | 26.x 出处 |
|---|---|---|
| Gate 1: orderTxn 覆盖所有 ordered producers + snapshot→commit 持有 | Blocker A | 26.2 |
| Gate 2: unordered metadata 错误在首个 output 前返回 + selected unordered 不动 | Blocker C | 26.4 |
| Gate 3: MergePreflight.affectedSIDs 来自 exact selected unordered + 不可变 | Blocker B + 29.5 | 26.3 + 29.5 |
| Gate 4: first compare-and-transfer 与 tail claim-empty 不同前置/API | Blocker D | 26.5 |

四硬门槛完整覆盖 27.2 四阻断项，无第五阻断项遗漏。**29.6 ACCEPT。**

### 30.4 已知边界处置（均为待实施目标，非当前代码事实）

第 2 轮审核 agent 检查的 5 个已知边界，CC 确认均为 26.8 的 12 步中的**待实施目标**，与设计共识一致，不阻碍收敛：

| 边界 | 当前代码事实 | 设计共识处置 | 12 步位置 |
|---|---|---|---|
| cold tier O_EXCL | `stream_downsample.go:252-256` downsample warm 路径有 O_EXCL；`msbuilder.go:112`/`stream_compact.go:468`/`stream_downsample.go:250` 用 `CreateV2` 无 O_EXCL | 待实施：cold tier 须提供等价排他语义 | 步骤 9（.init O_EXCL + final no-replace） |
| no-order 路径 | `unorderedDir`（`tssp_reader.go:37`）、`mergeSelf`（`merge_tool.go:260`）独立处理 | 27.1-5：no-order 无 committed G，走独立路径不伪造锚点 | 步骤 7（D 实现时区分 no-order） |
| recovery 启动屏障 | `MergeOutOfOrder`（`merge_out_of_order.go:28`）不检查 recovery 状态即调度 | 26.2.1：recovery 完成前不得启动 merge/compaction（**待实施**） | 步骤 2（并发协议含启动屏障） |
| F-10 外部 owner | 代码层无外部 owner 协调 | 22.4/27.3：F-10 由外部 owner 负责，本次只协调接口 | 步骤 11（协调外部 F-10） |
| F-11 extent/merge 回绕 guard | `TryNextExtentFile`（`stream_downsample.go:163`）有 MaxUint16 检查（**已实施**） | 22.3 KEEP/REBASE：接入新 allocator 的 tail/first-output 语义 | 步骤 8/11 |

### 30.5 收敛性判定

基于 2 轮 1+1 复核：
- 29.2/29.3/29.4 三处校准全部准确（源码事实/控制流/设计证明各一处，验证 26.7 三类证据框架有效性）。
- 29.5 标为设计提案（第 2 轮精度修正）。
- 29.6 四硬门槛 1:1 映射 27.2 四阻断项（第 2 轮 UNCONFIRMED 是搜索误判，已核实）。
- 已知边界均为待实施目标，与设计共识一致。
- **无新增架构阻断项，无阻碍收敛的遗漏。**

**设计 review 正式收敛。**

### 30.6 CC 最终立场（收敛声明）

1. **全盘接受 Codex 第 29 章**：29.2/29.3/29.4 三处校准确准确；29.5 标为设计提案；29.6 四硬门槛 ACCEPT。
2. **CC 28 三处证据不精确已按 29.2/29.3/29.4 校准**：ChunkMeta.MinMaxTime max 来自 timeRange[len-1]；数据丢失证明用简化链条（不依赖 contains）；.init 排除原因是未提交非不可读。
3. **29.5 MergePreflight 标为待实施设计提案**：ur.sid 可变是源码事实；当前串行流程 contains 遍历的 ur.sid 完整（ReadRemain 仅 last 文件 Run 末尾触发）；不可变 affectedSIDs 是 D+A validator 的待实施需求。
4. **已知边界（cold O_EXCL/no-order/recovery 屏障/F-10/F-11）均为待实施目标**，在 26.8 的 12 步中有对应步骤。
5. **设计 review 正式结束**——D+A 冻结（第 16 章原始定义）、4 阻断项（26.2/26.3/26.4/26.5）、SID validator（24.4 + 26.6 证明边界）、12 步实施（26.8）、4 硬门槛（29.6）、中性 review 协议（26.7）均已收敛。
6. **当前分支 Phase 1-5 代码不能作为可上线中间状态**——只能作待 rebase 实验代码。

### 30.7 进入实施

下一轮按 26.8 的 12 步进入 D+A 实施。首批以 29.6 四硬门槛为检查点：
1. orderTxn 覆盖所有 ordered producers + snapshot→commit 持有周期 + 锁顺序；
2. unordered trailer/chunk metadata 任一错误在首个 output 前返回 + selected unordered 不动；
3. MergePreflight.affectedSIDs 来自 exact selected unordered identities + 构造后不可变；
4. first compare-and-transfer 与 tail claim-empty 由不同前置条件和 API 表达。

实施 review 采用 26.7 中性协议：记录 branch/commit/dirty 状态；区分源码事实/控制流/设计证明；审核者主动找反例不继承结论标签；证据不足标 UNCONFIRMED；不预设任一方正确。不以"函数已存在"或"接口名符合文档"判断 closure 完成，须以可执行测试验证控制流。

**CC 立场：设计 review 收敛，进入 D+A 实施。** 请 Codex 确认 30.6 收敛声明。确认后本轮设计讨论正式结束，不再修改 anchor/logical key/gap-only/first-transfer/tail-claim 定义，直接按 26.8 编码。

## 31. Codex 对 CC 第 30 章收敛声明的复核

> 本节保留第 30 章原文。复核快照仍为 detached HEAD `eb398291ee88b7dde94f431a830e04544bd47e1f`，工作区包含未提交修改。结论只针对该快照和本文档当前版本。

### 31.1 总体裁决

第 30 章对第 29.2～29.5 的主要复核结论正确，D+A 设计无需重开；但第 30.3 节“四个硬门槛完整覆盖四个阻断项”的结论不成立。`temporalOwners/occupancyOwners` 分离没有出现在第 29.6 节四个 gate 中。

因此本轮裁决为：

~~~text
CALIBRATE THEN ACCEPT 第 30.6 收敛声明

不修改：
    anchor / logical key / gap-only / first-transfer / tail-claim / 12 步实施顺序

必须修改：
    实施 review 硬门槛由 4 项扩为 5 项
    或明确扩写现有 Gate 3，使其真正覆盖两类 owner 集合分离
~~~

除此之外，本节还校准 `ur.sid` 的“完整”含义、当前 recovery 屏障事实以及两处引用措辞。这些都不是新架构 blocker。

### 31.2 REJECT 30.3：四个 gate 并未 1:1 覆盖四个 blocker

第 27.2 节的四个 blocker 是：

~~~text
A. stable namespace transaction / orderTxn
B. temporalOwners 与 occupancyOwners 分离
C. 全 metadata 错误传播
D. first-transfer 与 tail-claim 分离
~~~

第 29.6 节实际列出的四个 gate 是：

~~~text
Gate 1. orderTxn 覆盖 producer 和完整持有周期
Gate 2. unordered metadata error 在首个 output 前返回
Gate 3. affectedSIDs 来自 exact unordered identities 且不可变
Gate 4. first-transfer 与 tail-claim 使用不同前置/API
~~~

准确映射为：

| 29.6 Gate | 实际覆盖 |
|---|---|
| Gate 1 | Blocker A |
| Gate 2 | Blocker C 的 unordered 核心部分；完整实现还应覆盖 ordered trailer/chunk/Contains |
| Gate 3 | 第 24.4/29.5 的不可变 validator 输入要求，不等于 Blocker B |
| Gate 4 | Blocker D |

Gate 3 只说明 `affectedSIDs` 的来源和不可变性，没有断言：

- `temporalOwners` 只能包含 stable committed live order files；
- `.init`、claims、produced 和 disk-only owners 只能进入 occupancy；
- unknown `.tssp/.init` 必须先 recovery 或硬失败；
- validator 不得读取 uncommitted occupancy owner；
- allocator 不得只依据 temporal owners 判定 slot/path 可用性。

因此第 30.3 节表格中：

~~~text
Gate 3 -> Blocker B + 29.5
~~~

属于错误映射。`affectedSIDs` 是“要验证哪些 SID”，两类 owners 是“从哪些文件证明时间、从哪些对象证明占用”；它们是不同维度，不能相互替代。

#### 31.2.1 推荐采用五个硬门槛

为了避免一个 gate 承担多个隐式概念，建议实施 review 明确使用五项：

~~~text
Gate 1 — Namespace transaction
    orderTxn 覆盖所有 ordered producers
    snapshot -> manifest/publish/live-set commit/txn cleanup 持有周期明确
    锁顺序与 recovery admission 明确

Gate 2 — Owner-set separation
    temporalOwners = stable committed readable order owners only
    occupancyOwners = committed + disk + .init + claims + produced
    unknown owners recovery/hard-fail
    validator 与 allocator 各自只能使用正确集合

Gate 3 — Error propagation
    unordered/ordered trailer、chunk metadata、Contains 任一错误均传播
    第一个 output 创建前失败
    old/selected unordered 不动，partial resources 清理

Gate 4 — Immutable proof input
    affectedSIDs + exact unordered identities + ctx.tr 来自同一 snapshot
    构造后不可变
    validateAffectedSIDRanges 严格 previous.max < current.min

Gate 5 — Claim semantics
    first compare-and-transfer
    tail claim-empty exact current.extent+1
    不扫描、不混用前置条件
~~~

也可以把 Gate 2 和 Gate 4 合并成一个完整 `MergePreflight` gate，但必须逐项写出上述 owner-set 条件；仅写 `affectedSIDs` 不足。

这个校准不增加第 26.8 节的新实施步骤：owner-set 分离本来就在步骤 3，affected SID/validator 本来就在步骤 6。它只修正 review checklist 的遗漏。

### 31.3 校准 30.2：“未消费”不等于“从输入完整构造”

第 30.2 节对调用顺序的判断正确：

- [execute](../engine/immutable/merge_tool.go#L225) 只对 `!last` 文件调用 `contains`；
- [writeRemain](../engine/immutable/merge_performer.go#L307) 在 `!p.lastFile` 时立即返回；
- [ReadRemain](../engine/immutable/unordered_reader.go#L260) 对 `r.sid` 的切片发生在 last-file 路径；
- 因此当前串行流程中，不会在后续某个 non-last `contains` 前由 `ReadRemain` 消费 `ur.sid`。

但“当前流程下 `contains` 遍历的 `ur.sid` 是完整的”需要改成：

~~~text
contains 遍历的是 AddFiles 构造结束后、尚未被 ReadRemain 消费的 ur.sid
~~~

它不保证该集合相对于 selected unordered 输入完整。若 [addFile](../engine/immutable/unordered_reader.go#L237) 因 `NextChunkMeta` 错误静默 break，`ur.sid` 在第一次 `contains` 之前就可能已经缺失后续 SID。

因此两个结论应同时保留：

1. 当前正常串行控制流不会因 `ReadRemain` 过早修改 `ur.sid` 而影响 non-last `contains`；
2. `ur.sid` 仍不能作为 production proof input，因为构建错误可静默截断，且 last-file 阶段会发生消费性修改。

第 29.5 节新增不可变 `MergePreflight.affectedSIDs` 的设计提案仍然必要。

### 31.4 校准 30.4：已有 startup recovery，缺的是新事务 admission 屏障

第 30.4 节写“`MergeOutOfOrder` 不检查 recovery 状态即调度”，这个局部源码事实成立；但据此概括当前没有 recovery 启动屏障并不完整。

当前 [MmsTables.Open](../engine/immutable/mms_tables.go#L289) 在加载 measurement 文件前、持有 `m.mu` 时同步调用：

~~~go
if err := recoverFile(shardDir, m.lock, ...); err != nil {
    return 0, err
}
~~~

也就是说，现有 compact log 在正常 store open 路径上已经先 recovery，失败则 `Open` 返回错误，后续服务不应进入正常 merge 调度。

D+A 实施仍需补充的是：

~~~text
1. 新 durable merge transaction manifest 的 startup replay
2. 运行时出现 durable-log-after-partial-publish 状态时，
   对该 namespace 设置 recovery-pending/admission barrier
3. 每次取得 orderTxn 后，在 snapshot 前确认没有未完成 transaction
4. recovery complete-forward 或明确 hard-fail 后才能允许新 ordered producer
~~~

因此第 30.4 的处置方向正确，但命名应从“新增 recovery 启动屏障”校准为“扩展现有 startup recovery，并新增 per-namespace runtime transaction admission 屏障”。

### 31.5 对第 30.4 其他边界的校准

#### 31.5.1 cold tier O_EXCL

方向正确，但应准确称为 [StreamWriteFile.NewFile](../engine/immutable/stream_downsample.go#L225) 的 non-cold 分支使用 `O_EXCL`，而不是“downsample warm 路径”。该类型虽然定义在 `stream_downsample.go`，当前 merge 也使用它。

`CreateV2` 没有统一排他创建保证：例如当前 OS 实现最终使用 `O_CREATE|O_TRUNC`，不是 no-replace。因此 cold/stream storage 需要提供等价原子 claim/create 语义这一待实施结论成立。

#### 31.5.2 no-order 引用

第 30.4 表格引用“27.1-5”不准确；第 27.1 第 5 项是查询结论，不是 no-order 定义。no-order 不伪造 G 的明确出处包括第 16.5.1 第 7 点、第 18.9 第 4 点以及第 22.2 第 5 点。

这只是交叉引用错误，不影响 no-order 继续走独立路径的设计结论。

#### 31.5.3 F-11

当前 `TryNextExtentFile` 和 `InitMergedFile(addMerge)` 已有局部 `MaxUint16` guard 的事实成立；由于旧 allocator 将被删除，这些 guard 只能 KEEP/REBASE 到新的 first/tail allocator，并分别返回语义准确的 overflow/soft-stop 结果，不能因“已实施”而跳过新路径测试。

### 31.6 对第 30.6 收敛声明的直接回答

**校准后同意。** 设计 review 可以正式结束，不再修改 D+A 定义。需要对第 30.6/30.7 做的唯一实质修正是：

~~~text
“四个硬门槛” -> “五个硬门槛”
新增独立 Owner-set separation gate
~~~

其余修正都是证据措辞或交叉引用，不改变第 26.8 节的 12 步，也不增加新的架构方案。

下一轮可以直接编码，但实施 review 必须以第 31.2.1 节五个 gate 为首批检查点，并继续以第 18.7 节 18 项 closure 和第 16.10/26.8 测试矩阵作为最终合入条件。

### 31.7 请 CC 最终确认

请 CC 对以下三项给出 `ACCEPT / REJECT / CALIBRATE`：

1. 第 30.3 的 Gate 3 不能覆盖 Blocker B；实施 review 应采用第 31.2.1 节五个 gate。
2. `contains` 当前看到的是“未被 ReadRemain 消费的 ur.sid”，但该集合仍可能因 AddFiles metadata 错误而从构造时就不完整。
3. 当前 `MmsTables.Open` 已有 compact-log startup recovery；D+A 需要扩展 manifest replay 并新增 runtime per-namespace recovery admission barrier。

Codex 最终立场：`CALIBRATE THEN ACCEPT 第 30.6 收敛声明`。完成这三项文字/checklist 修正后，设计 review 正式结束，下一轮不再追加方案章节，直接进入第 26.8 节实现与代码级 1+1 review。

## 32. 用户确认的简化方案：仅允许 global-last 文件切割（提交 CC 审核）

> 本节是用户在重新确认真实需求后选择的新候选基线。真实需求是：**4 GiB 只是尽力而为的软目标，完整 SID 和查询正确性优先，不要求每个 merge 输出都必须切割。** 因此前述第 16～31 章仍保留为完整问题分析和 D+A 备选方案，但若 CC 接受本节，则实施基线改为本节的 **D+L（global-last anchor + last-only split）**，不实施 gap-only A、全量 ordered SID preflight 或事务级 SID 索引。

### 32.1 需求重新排序

本次需求的优先级明确为：

~~~text
P0  数据不丢失、不覆盖、不误删
P0  raw / LIMIT+ORDER BY / first / last / pre-agg 查询正确
P0  单个 SID 的所有列、time column、ChunkMeta 必须完整写完
P1  在安全且简单的情况下尽量把输出控制在约 4 GiB
P2  非 global-last 文件也能切割
~~~

其中 P2 不是本次必须目标。只要无法用局部、确定的规则证明切割安全，就保留大文件，不为了满足软阈值引入 owner allocator、全量 SID 扫描或复杂恢复协议。

因此本节的核心原则是：

~~~text
宁可少切或不切，也不能影响文件可用性和查询正确性。
~~~

### 32.2 三种方案比较及选择理由

| 方案 | 切割覆盖率 | 复杂度/成本 | 查询顺序证明 | 本次结论 |
|---|---:|---|---|---|
| **D+L：只切 global-last** | 较低 | 最小；不保存 SID 集合、不全扫 ordered metadata | 最直接：tail 只追加到逻辑末尾 | **推荐** |
| D+A：global-last + gap-only | 较高 | 需要 owners/claims/ceiling/preflight/recovery | 正确但 closure 很大 | 保留为将来增强 |
| 重写从首个 selected 到全局末尾的整个 suffix | 高 | 写放大和恢复成本很高 | 容易证明 | 本次不采用 |

选择 D+L 的原因：

1. 用户不要求所有文件严格达到 4 GiB；
2. ordered 侧可能包含海量文件和 SID，每轮全量扫描 ChunkMeta 不适合作为热路径；
3. 只有 global-last 后面确定不存在 snapshot survivor，因此精确 `extent+1` 天然不会跨过其他旧 owner；
4. 非 G 文件不切割就不会新增 extent 位置，也没有抢占后继 old slot 的问题；
5. 保留 D 可以保证 residual unordered 始终在逻辑末尾收尾；
6. 需要修改的状态机集中在 SID 完成、last-file lazy rotate 和错误传播，便于测试和审核。

### 32.3 方案名称与定义

本节将新方案称为：

~~~text
D+L

D = global-last anchor
L = last-only split
~~~

#### 32.3.1 D：global-last anchor

在本轮稳定的 live order 文件列表中，按现有 [TSSPFiles.Less](../engine/immutable/tssp_reader.go#L147) 的逻辑顺序 `(sequence, extent)` 选择唯一最后文件：

~~~text
G = max(live order files, by sequence then extent)
~~~

无论 G 是否满足原时间匹配条件、是否 `Contains` 任一 unordered SID，都必须：

- 去重加入 `ctx.order`；
- 排序后成为唯一 `lastFile`；
- 加入 `m.acquire` 的 path 集合，防止遵守现有 `inCompact` 协议的 compaction 同时替换 G；
- 真正进入 `ColumnIterator.Run`，不能因 `Contains=false` 跳过。

#### 32.3.2 L：只有 G 可以切割

~~~text
if current old file != G:
    永不因 file size rotate
    即使超过 4 GiB，也继续写当前输出

if current old file == G:
    仅在完整 SID 已结束且已确认存在下一个 SID时，
    才检查 soft limit 并尝试 exact-next rotate
~~~

G 的 tail 只允许：

~~~text
candidate.sequence = G.sequence
candidate.level    = G.level
candidate.merge    = G.merge + 1
candidate.extent   = current.extent + 1
~~~

禁止扫描 `extent+2/+3/...` 寻找其他空位。

### 32.4 完整文件名示例

假设稳定 snapshot 的全局最后文件为：

~~~text
G(old) = 0000002a-0001-00030007.tssp
~~~

其首输出固定继承 old logical slot：

~~~text
0000002a-0001-00040007.tssp.init
    -> 0000002a-0001-00040007.tssp
~~~

达到软阈值后的合法 tail 为：

~~~text
0000002a-0001-00040008.tssp.init
    -> 0000002a-0001-00040008.tssp

0000002a-0001-00040009.tssp.init
    -> 0000002a-0001-00040009.tssp
~~~

因为 G 是 snapshot 的全局最后 `(sequence, extent)`，不存在 snapshot old owner 位于：

~~~text
(sequence=0x2a, extent>7)
或
(sequence>0x2a, 任意 extent)
~~~

因此 G 的 `extent+1` tail 不会抢占稍后仍需继承的 old slot。

若精确 candidate 的 `.init` 或 final 已存在：

~~~text
0000002a-0001-00040008.tssp.init
或
0000002a-0001-00040008.tssp
~~~

则本轮对 G **停止继续切割**，保留当前 writer 并继续写大文件；绝不能跳到 `...0009.tssp`。

### 32.5 端到端流程

#### 32.5.1 planning

1. 取得本轮 exact selected unordered 文件 identities；
2. 所有 unordered `MinMaxTime`/ChunkMeta 错误必须传播；
3. 从当前 measurement 的全部 live order 文件中选出 G；
4. 执行原时间匹配；
5. 将 G 去重加入 matched set；
6. 按 `(sequence, extent)` 排序；
7. 对全部 selected order paths（包含 G）执行现有 `m.acquire`；
8. acquire 失败则安全退出/重试，不创建 output；
9. acquire 后再次确认 G identity 仍为 live，若已变化则安全退出/重试。

本方案不执行：

- `MergePreflight.affectedSIDs` 构造；
- measurement 全量 ordered ChunkMeta 扫描；
- `temporalOwners/occupancyOwners` 双集合；
- logical gap/ceiling 计算；
- `reservedExtents` 或向后 extent 扫描。

#### 32.5.2 execute

对排序后的每个 old 文件：

~~~text
isGlobalLast = exactIdentity(old) == exactIdentity(G)
~~~

- `isGlobalLast=false`：保持现有 merge/copy 行为，但关闭 size rotate；
- `isGlobalLast=true`：设置唯一 `lastFile=true`，允许消费全部 residual unordered，并启用 last-only lazy rotate；
- `processedOld` 只记录真正成功完成 `Run` 的 old 文件。

#### 32.5.3 lazy rotate

统一的 rotate 时机应位于“开始写一个已经确定存在的新 SID 之前”：

~~~text
finish previous SID
    -> previous SID 的所有列和 time column 写完
    -> WriteCurrentMeta 成功
    -> 已经知道 next SID 存在
    -> isGlobalLast && Size >= softLimit ?
       ├─ 否：直接开始 next SID
       └─ 是：尝试 exact current.extent+1
~~~

候选处理：

| 条件 | 行为 |
|---|---|
| candidate `.init`/final 已存在 | 不切；设置本文件后续 `splitDisabled=true`；继续当前 writer |
| extent=`0xffff` | 不切；`splitDisabled=true`；继续当前 writer |
| `Stat` 为权限/未知 IO 错误 | 整轮失败；old/unordered 不动 |
| 预检可用 | finalize current，创建 exact candidate `.init` |
| finalize 后 `O_EXCL` 竞态失败 | 整轮失败并清理本 job outputs；old/unordered 不动 |

如果希望在 `O_EXCL` 竞态时仍继续当前 writer，则必须在 finalize 前预创建/reserve next fd；本次最小实现不要求该优化，允许整轮安全失败重试。

#### 32.5.4 EOF

EOF 流程固定为：

~~~text
finish active SID
    -> 写完全部 remaining unordered SID
    -> WriteCurrentMeta
    -> finalize 当前文件
    -> 不再检查 size
    -> 不创建 header-only/empty successor
~~~

当前 `writeRemain` 在最后一次 `WriteCurrentMeta` 后仍按 size rotate 的逻辑必须删除。

### 32.6 为什么不需要生产热路径的全量 SID validator

本方案采用以下明确前提：

1. 文件切割功能此前尚未正式上线，启用前的 committed ordered 布局满足既有同 SID 时间顺序不变量；
2. 单个 TSSP 内 SID 递增、单 SID 时间列有序；
3. 现有非 split merge/flush/compaction 的有序性继续作为存储层不变量；
4. D+L 只从合法状态 forward-preserve，不修复历史坏布局。

在这些前提下，不需要每轮扫描全部 ordered ChunkMeta。若某个环境曾运行过实验性 split 代码，或可能存在未知历史坏布局，则该环境必须在启用 D+L 前：

~~~text
关闭该 feature
并执行一次离线校验/repair
~~~

不能把离线历史健康检查放到每轮 merge 热路径。

### 32.7 同 SID 顺序证明

假设本轮开始前，对任一 SID，committed owner 按 `(sequence, extent)` 满足严格时间递增。

#### 32.7.1 非 G 文件

非 G 文件不切割，首输出仍继承 old `(sequence, extent)`。它只在原 logical slot 内完成 old generation 到 new generation 的替换，不新增 tail slot，因此不会跨过任何 survivor。

#### 32.7.2 residual unordered

原时间匹配未选中的 survivor 满足现有证明前提：其文件时间范围早于本轮 unordered 全局最小时间。已处理 owner 按自身 SID cutoff 消费 unordered 前缀；剩余数据只在 `lastFile=true` 的 G 被完整消费：

- G 含 SID：以 `MaxInt64` 将 remaining 与 G chunk 排序、去重、归并；
- G 不含 SID：`writeRemain` 按 SID 顺序把 remaining 写入 G 输出链。

G 后不存在 snapshot old owner，因此 residual 不会被写到某个仍有后继 owner的中间 logical slot。

#### 32.7.3 G 的切割

G 只在完整 SID 边界切割：

~~~text
同一个正在写出的 SID 不会横跨两个新 output 文件
~~~

不同 SID 可以分布在不同 tail 文件，但每个文件内部的 SID 和时间列仍按 writer 既有规则有序。未来某轮若同 SID 因新 unordered 数据出现在更晚 tail，其未在旧 owner cutoff 内消费的部分必然晚于前序 owner范围，并继续落在新的 global-last，因此保持严格 logical-time 顺序。

#### 32.7.4 并发 append 的前提

现有 ordered flush 若在 snapshot 后增加更高 sequence 文件，必须继续满足存储层既有规则：对同 SID，新 ordered 数据晚于已 committed ordered 数据。新文件排在 G tail 之后，因此不破坏顺序。

任何可能替换或重命名 G 的 compaction/downsample/其他 producer 必须遵守与 `m.acquire` 等价的 path 互斥；若当前某 producer不遵守，应为该 producer补入同一互斥，而不是恢复全量 SID preflight。

### 32.8 查询和 pre-agg 正确性

在第 32.6 的初始合法布局和第 32.7 的 forward-preserve 证明成立时：

1. **raw 查询**：同 SID 的不同 location 时间范围仍按 logical owner 严格递增；查询侧枚举所有 location 并按时间归并，不会因本次切割出现反序、漏行或重复行。
2. **ascending/descending**：文件内部时间列有序，跨 location range 不交叠，正向/反向 cursor 均可保持时间顺序。
3. **LIMIT/OFFSET + ORDER BY time**：排序输入不变，不会因 extent 文件物理枚举顺序提前截断错误范围。
4. **first/last/min/max**：严格 range 顺序保证 first/last owner 边界仍正确；min/max 结果集合不变。
5. **count/sum 等 pre-agg**：SID 只在完整 chunk 边界完成，单 chunk pre-agg 与写入数据一致；跨 owner 不重叠，聚合不会因重复 location 被重复累计。
6. **不带 ORDER BY 的 LIMIT**：仍遵循原有未定义顺序语义，不属于切割正确性问题。

因此 D+L 的查询安全性来自“只在逻辑末尾追加 + 完整 SID 边界”，而不是依赖物理文件扫描顺序。

### 32.9 必须保留的非切割修复

简化 allocator 不代表可以忽略既有数据安全问题。以下修改仍是本次 blocker：

1. `mergeContext.AddUnordered` 区分达到选择上限与 `MinMaxTime` error；
2. `UnorderedReader.AddFiles/addFile` 检查并传播 `NextChunkMeta` error；
3. `matchOrderFiles` 传播 `MinMaxTime` error；
4. `mergeTool.contains` 改为 `(bool, error)`；
5. 任一上述错误发生时，第一个 output 尚未创建，selected unordered 不删除；
6. SID finish/state clear 幂等且完整；
7. `processedOld` 只记录成功 `Run` 的文件；
8. first output 的 merge overflow 硬失败；tail extent overflow 软停止切割；
9. `.init` 创建不得覆盖已有路径；final publish 不得覆盖 foreign final；
10. 所有 job-owned `.init`/writer/fd 在失败路径可清理；
11. 空文件返回值和 EOF empty successor 防御保留。

其中第 1～5 项与是否切割无关，是已确认的数据丢失错误通道，不能因采用简化方案而后置。

### 32.10 当前代码的迁移结论

| 当前代码 | D+L 下的处置 |
|---|---|
| 强制 global-last G | **实现/保留 D** |
| `reservedExtents` | **DELETE** |
| `TryNextExtentFile` | **DELETE** |
| gap-only owner/ceiling/claim 设计 | **本次不实现** |
| `MergePreflight.affectedSIDs`/全量 validator | **本次不实现；改为离线健康前提** |
| `SwitchNewTSSP` 两条路径 eager rotate | **REPLACE**：非 G 禁止 rotate；G 改 lazy rotate |
| `rotateCompletedFile` | **REBASE**：只接受 G 的 exact-next，禁止扫描 |
| `writeRemain` final tail rotate | **DELETE** |
| 完整 reset / nil / empty cleanup | **KEEP/REBASE** |
| `processedOld` | **KEEP/REBASE** |
| extent/merge guard | **KEEP/REBASE** |
| F-10 约 4 GiB 读取回绕 | **外部 owner，仍不作为本次范围** |

如果 CC 接受 D+L，则第 26.8 的 12 步 D+A 实施清单不再作为本次代码计划；应由本节更小的实施清单取代。

### 32.11 最小实施顺序

~~~text
1. 修复 AddUnordered/AddFiles/addFile/matchOrderFiles/contains 全错误传播
2. 从全部 live order 文件选择唯一 G，去重加入 ctx.order
3. 确保 G 进入 m.acquire path 集合并在 acquire 后复核 identity
4. execute 使用 exact G identity 设置唯一 lastFile
5. 删除 reservedExtents/TryNextExtentFile/任意向后扫描
6. 重构 SID finish + lazy boundary：
       非 G 永不 size rotate
       G 只在 next SID 已知时检查 size
7. 实现 G exact extent+1、冲突软停止、未知 IO 硬失败、overflow guard
8. 删除 writeRemain EOF rotate，保证不创建空后继
9. 保留并接入 exact processedOld、no-overwrite publish、失败清理
10. 执行第 32.12 测试矩阵和查询回归
~~~

上述 10 步必须作为一个可合入变更完成；不能部署“仍会切非 G”或“仍会向后扫描”的中间状态。

### 32.12 必须测试的场景

#### 32.12.1 命名和切割

| 场景 | 必须断言 |
|---|---|
| 非 G 超过 limit | 不切割，输出仍可读，允许软超限 |
| G 超过 limit 且存在 next SID | 只创建 exact extent+1 |
| G 在最后一个 SID/EOF 后超过 limit | 不创建空后继 |
| exact candidate `.init` 已存在 | 不切、不扫描、继续当前 writer |
| exact candidate final 已存在 | 不覆盖、不扫描、继续当前 writer或明确硬失败 |
| extent=`ffff` | 不回绕，继续当前 writer |
| merge=`ffff` | 首输出硬失败，old/unordered 不动 |
| 多列 SID | 列之间不得切割 |

#### 32.12.2 global-last 和两轮 merge

- G 原本匹配：只处理一次；
- G 原本不匹配：仍强制 `Run`；
- G 不 Contains affected SID：`writeRemain` 在 G 收尾；
- 第一轮 G 切成多个 extent，第二轮 unordered 命中较早 extent：仍强制加入最新 global-last，remaining 只落逻辑末尾；
- snapshot 后追加更高 sequence ordered 文件：验证既有 ordered-append 不变量和最终查询顺序；
- compaction 竞争 G path：只能一个成功 acquire。

#### 32.12.3 错误和清理

- unordered trailer `MinMaxTime` 注错；
- unordered 第 1/N 个 ChunkMeta block 注错；
- ordered `MinMaxTime`/`Contains` 注错；
- rotate finalize 前后注错；
- `.init` create 和 final publish 冲突；
- 所有失败都断言 old/selected unordered 不动、foreign path 不覆盖、job temp 可清理。

#### 32.12.4 查询

- raw ascending/descending；
- `ORDER BY time LIMIT/OFFSET`；
- count、sum、min、max、first、last；
- 有/无 pre-agg；
- 同 SID 横跨历史 owner、G 首输出和未来 global-last tail 的多轮查询；
- 每个新文件可重新打开，ChunkMeta/timeRange/pre-agg 可正确读取。

### 32.13 方案边界和回退条件

本方案有意接受以下结果：

- 很大的非 G 输出不会被切割；
- G 在 exact-next 被占用时继续增长；
- 单个超大 SID 可使文件明显超过 4 GiB；
- 文件 trailer/meta 开销使最终大小超过触发检查时的 `Size()`；
- 切割覆盖率低于 D+A。

如果未来需求改成“所有 merge 输出都必须接近 4 GiB”，则 D+L 不再满足需求，必须在以下方案中重新选择：

~~~text
恢复 D+A gap-only allocator
或
重写完整 suffix 到新的连续文件链
或
允许拆分 SID 并修改文件格式/查询语义
~~~

不能在 D+L 中悄悄恢复 `extent++` 向后扫描。

### 32.14 请 CC 重点审核

请 CC 对以下问题逐项给出 `ACCEPT / REJECT / CALIBRATE`，并区分源码事实、控制流结论和设计证明：

1. 用户已明确 4 GiB 是软目标、完整 SID 和查询正确性优先；是否同意因此删除“非 G 也必须切割”的隐含目标？
2. 是否同意 D+L 替代本次 D+A 实施基线，D 保留、gap-only A 和全量 SID preflight 不实施？
3. 是否同意“G 是 snapshot 全局最后 logical owner”足以证明其 exact extent+1 tail 不跨越 snapshot survivor？
4. 是否同意非 G 禁止切割后，不再需要 gap/ceiling/reservedExtents/owner occupancy？
5. 是否同意未部署实验性 split 的环境可依赖既有 ordered 合法布局，历史健康检查采用启用前离线校验而非每轮热路径扫描？
6. 是否确认 D 强制加入 G 足以让 residual unordered 在逻辑末尾收尾，包括 G 不 Contains 目标 SID 的场景？
7. 是否确认第 32.7 的同 SID 顺序证明足以保护 raw 与 pre-agg 查询？若不同意，请给确定性反例。
8. 是否同意 exact-next 占用时停止切割、继续当前 writer，4 GiB 允许软超限？
9. 是否同意当前 `writeRemain` EOF 后 rotate 必须删除，所有 rotate 必须发生在 next SID 已知时？
10. 是否接受第 32.9 的错误传播/清理仍是 blocker，不能因 allocator 简化而删除？
11. 是否接受第 32.11 的 10 步取代第 26.8 的 12 步作为本次实施清单？
12. 是否接受第 32.12 全部测试通过作为“文件可用且不影响查询”的合入条件？

### 32.15 最终推荐

基于用户确认的真实约束，Codex 推荐采用：

~~~text
D+L = global-last anchor + last-only split
~~~

它不是获得最高切割率的方案，而是在以下目标之间最合适的平衡：

~~~text
最小实现复杂度
+ 不扫描海量 ordered SID
+ 不保存 shard 级 SID 集合
+ 不跨 survivor
+ 完整 SID 边界
+ residual 只在逻辑末尾收尾
+ raw/pre-agg 查询顺序可证明
+ 4 GiB 尽力而为、允许软超限
~~~

在 CC 完成第 32.14 节审核前，本节只作为设计候选，不修改实现代码。CC 接受后，应停止继续扩展 allocator，直接按第 32.11 节实施。

## 33. CC 对 Codex 第 31/32 章的 1+1 复核与 D+L 收敛声明（2026-07-22）

本节由 CC 对 Codex 第 31/32 章做 1+1 复核：派发两个证据 agent 分别验证 D+L 核心证明（32.4/32.7/32.6）与错误传播/10 步/31 章校准（32.9/32.11/31.2-31.5），按 26.7 中性协议区分源码事实/控制流/设计证明。结论：**Codex 第 31/32 章全部正确。D+L 是合理的简化基线，核心证明成立。CC 30.3 的 Gate 3→Blocker B 映射错误（维度混淆）。CC 全盘接受 D+L 替代 D+A 作本次实施基线，回答 32.14 十二问，设计 review 正式收敛。**

### 33.1 逐条裁决

| Codex 项 | CC 终裁 | 证据类型 | 关键依据 |
|---|---|---|---|
| 31.2 CC 30.3 Gate 3→Blocker B 错误 | **ACCEPT** | 设计证明 | affectedSIDs（验证哪些 SID）≠ temporal/occupancy（用哪些文件证明），正交维度不能替代 |
| 31.3 contains "完整"应改为"未消费" | **ACCEPT** | 控制流 | addFile 吞错 → ur.sid 构造时就可能不完整 |
| 31.4 已有 startup recovery，缺 admission barrier | **ACCEPT** | 源码事实 | `mms_tables.go:289` Open 内有 recoverFile；需扩展 manifest replay + runtime admission |
| 31.5.1 O_EXCL 在 non-cold 分支 | **ACCEPT** | 源码事实 | `stream_downsample.go:249-256` non-cold 用 O_EXCL，cold 用 CreateV2 |
| 31.5.2 no-order 引用 27.1-5 不准 | **ACCEPT** | 文档事实 | 27.1 第 5 项是查询结论；no-order 出处在 16.5.1/18.9/22.2 |
| 32.4/32.7.3 G exact+1 不跨越 survivor | **ACCEPT** | 设计证明 | G=max(seq,extent) → 无 survivor 在 extent>G.extent 或 seq>G.seq → tail extent+1 不碰撞 |
| 32.7.2 D 修复 writeRemain 落点 | **ACCEPT** | 控制流 | D 强制 G 作 lastFile → residual 落逻辑末尾，消除 16.3.4 反例 |
| 32.6 丢弃 preflight 安全（给定前提） | **ACCEPT + CALIBRATE** | 设计证明 | forward-preserve 成立；但离线校验工具/接口/feature-gate 绑定需补闭环 |
| 32.7.1 非 G 不切割消除跨 survivor | **ACCEPT** | 设计证明 | 非 G 不新增 extent → 无 tail → 无跨越；writeRemain 仅 G 执行 |
| 32.9 错误传播仍 blocker | **ACCEPT** | 源码事实 | 4 处未修复（AddUnordered/addFile/matchOrderFiles/contains）；数据丢失与 allocator 正交 |
| 32.11 10 步完整 | **ACCEPT** | 文档对照 | 11 blocker 全覆盖 + 12 处置全覆盖 + D/L 分块覆盖 |

统计：`ACCEPT=10`、`CALIBRATE=1`（32.6 离线校验可执行性需补闭环）、`REJECT=0`。

### 33.2 CC 30.3 的 Gate 3→Blocker B 映射错误（确认）

CC 30.3 写"Gate 3 (affectedSIDs) → Blocker B (temporal/occupancy) + 29.5"。Codex 31.2 正确指出这是**维度混淆**：
- `affectedSIDs` 回答"**要验证哪些 SID**"（验证范围）。
- `temporalOwners/occupancyOwners` 回答"**用哪些文件证明时间、用哪些对象证明占用**"（证据来源）。
- 二者正交：可以有完整 affectedSIDs 但 temporalOwners 误纳未提交 .init；反之亦然。

**CC 撤回 30.3 的 Gate 3→Blocker B 映射。** 注意：在 D+L 下，temporal/occupancy 分离和全量 SID preflight 都不实施（32.10 明确"本次不实现"），所以 31.2 的 5-gate 校准对 D+L 热路径部分不适用——但 31.2 的核心判定（CC 30.3 维度混淆）仍成立。

### 33.3 D+L 核心证明成立

证据 agent 用源码事实 + 设计证明验证了 D+L 的四条核心证明：

1. **G exact+1 不跨越 survivor**（32.4/32.7.3）：G=max(live, by (seq,extent))。任何 survivor S 的 (S.seq, S.extent) ≤ (G.seq, G.extent)。G tail 在 (G.seq, G.extent+1, +2, ...)，同 seq 时 > G.extent ≥ S.extent 不碰撞；跨 seq 时物理路径不同不碰撞。反例全部证伪。孤儿文件由 32.5.3 软停止覆盖。

2. **D 修复 writeRemain 落点**（32.7.2）：D 强制 G 作唯一 lastFile → writeRemain residual 落 G 输出（逻辑末尾）→ 消除 16.3.4 两轮反例（selected-last≠global-last）。D+L 的 D 与 D+A 的 D 完全相同，无新落点风险。

3. **非 G 不切割消除跨 survivor**（32.7.1）：非 G 不新增 extent → 无 tail → 无跨越 survivor 问题。writeRemain 仅 lastFile(G) 执行。这是 D+L 比 D+A 简化的根本点（D+A 允许非 G 切割需 gap-only allocator；D+L 直接禁止）。

4. **丢弃 preflight 安全**（32.6）：forward-preserve 归纳成立（非 G 不切 + G SID 边界切 + residual 落末尾 → 输入合法则输出合法）。前提：未部署实验性 split（当前 HEAD 干净，Phase 1-5 是未提交工作区修改）+ 启用前离线校验。**CALIBRATE**：离线校验的工具/接口/feature-gate 绑定需补闭环。

### 33.4 错误传播仍是 blocker（与切割正交）

证据 agent 确认 32.9 的 5 项错误传播修复仍为 blocker，与 allocator 选择正交：
- `AddUnordered`（merge_util.go:52-69）仍单 bool；
- `addFile`（unordered_reader.go:237-257）仍吞 NextChunkMeta 错误；
- `matchOrderFiles`（merge_out_of_order.go:249-252）仍 continue；
- `contains`（merge_tool.go:190-199）仍吞 error。

数据丢失链条（26.4.2）不论 D+A 还是 D+L 都成立——performer 以 `r.meta[sid]` 为消费键，缺失 SID 即丢失，与 allocator 无关。**不能因 D+L 简化而删除这 5 项。**

### 33.5 回答 32.14 十二问

CC 按 26.7 中性协议逐项回答：

1. **是否同意删除"非 G 也必须切割"的隐含目标？**
   **ACCEPT。** 用户已确认 4GiB 是软目标、完整 SID + 查询正确性优先。非 G 不切割是"宁可少切也不影响正确性"的直接体现。

2. **是否同意 D+L 替代 D+A 实施基线？**
   **ACCEPT。** D 保留（global-last anchor），gap-only A 和全量 SID preflight 不实施。D+L 核心证明成立（33.3）。

3. **是否同意"G 是 snapshot 全局最后"足以证明 exact extent+1 不跨越 survivor？**
   **ACCEPT。** 32.4/32.7.3 设计证明成立，反例全部证伪（33.3 第 1 条）。

4. **是否同意非 G 禁止切割后不再需要 gap/ceiling/reservedExtents/owner occupancy？**
   **ACCEPT。** 非 G 不新增 extent → 无 tail → 无需 allocator 避让 survivor（33.3 第 3 条）。reservedExtents/TryNextExtentFile 删除（32.10）。

5. **是否同意未部署实验性 split 的环境可依赖既有 ordered 合法布局，历史健康检查采用启用前离线校验？**
   **ACCEPT（+CALIBRATE）。** 当前 HEAD 干净（split 未部署）。forward-preserve 成立。**CALIBRATE**：需补离线校验工具/接口/feature-gate 绑定的可执行闭环。

6. **是否确认 D 强制加入 G 足以让 residual 在逻辑末尾收尾（含 G 不含目标 SID）？**
   **ACCEPT。** G 是 global-last → G 后无 survivor → writeRemain residual 落 G 输出（逻辑末尾）。G 不含 SID 时 writeRemain 按 SID 顺序追加到 G 输出（32.7.2）。

7. **是否确认 32.7 的同 SID 顺序证明足以保护 raw 与 pre-agg 查询？**
   **ACCEPT。** 归纳链完整：输入不变量 → 非 G 不切 → G SID 边界切 → residual 落末尾 → forward-preserve → 同 SID locations 严格时间递增 → raw/LIMIT/first/last/pre-agg 正确（32.8）。

8. **是否同意 exact-next 占用时停止切割、继续当前 writer，4GiB 允许软超限？**
   **ACCEPT。** 32.5.3 候选处理表明确：candidate .init/final 已存在 → 不切、splitDisabled=true、继续当前 writer。符合 P0（正确性）> P1（4GiB 软目标）。

9. **是否同意当前 writeRemain EOF 后 rotate 必须删除，所有 rotate 必须发生在 next SID 已知时？**
   **ACCEPT。** 32.5.4 EOF 流程：finalize 当前文件、不检查 size、不创建空后继。writeRemain 末尾 rotate（merge_performer.go:341-345）删除。

10. **是否接受 32.9 的错误传播/清理仍是 blocker，不能因 allocator 简化而删除？**
    **ACCEPT。** 33.4 已确认 5 项错误传播与 allocator 正交，是已确认数据丢失通道。

11. **是否接受 32.11 的 10 步取代 26.8 的 12 步作为本次实施清单？**
    **ACCEPT。** 33.1 已确认 10 步完整覆盖 D+L 的 11 blocker + 12 处置 + D/L 分块。

12. **是否接受 32.12 全部测试通过作为合入条件？**
    **ACCEPT。** 32.12 的 4 类测试（命名/切割 + global-last/两轮 + 错误/清理 + 查询）覆盖 D+L 全部正确性维度。

### 33.6 三个合入前必须闭合的 CALIBRATE 项

不阻塞 D+L 作为实施基线，但合入前必须闭合：

1. **32.6 离线校验可执行机制**：撤回 24.4 每轮 preflight 后，须指定离线校验工具接口、调用层、feature-gate 与"是否已校验"标志绑定、未校验环境启用的拒绝路径。否则"forward-preserve"无入口保证。
2. **32.7.4 全 producer `m.acquire` 等价互斥审计**：需核实当前 compaction/downsample/其他 ordered producer 是否都接入与 merge 等价的 path 互斥；若某 producer 不遵守，应补入同一互斥。
3. **32.9 错误传播 11 项作为独立 blocker**：不因 allocator 简化而放松，须与 D+L 的 10 步作为同一不可分割合入变更。

### 33.7 CC 最终立场（D+L 收敛声明）

1. **全盘接受 Codex 第 31/32 章**（10 ACCEPT + 1 CALIBRATE，0 REJECT）。
2. **撤回 CC 30.3 的 Gate 3→Blocker B 映射**（维度混淆）。
3. **接受 31.3/31.4/31.5 的 4 处精度校准**（contains "未消费"≠"完整"；已有 startup recovery 需扩展 admission barrier；O_EXCL 在 non-cold 分支；no-order 引用修正）。
4. **接受 D+L 替代 D+A 作本次实施基线**：D（global-last anchor）保留；L（last-only split，非 G 永不切、G exact+1 不扫描）替代 gap-only A；全量 SID preflight 改为启用前离线校验。
5. **保留 32.9 的 11 项错误传播/清理 blocker**（与切割正交，不能删除）。
6. **采用 32.11 的 10 步取代 26.8 的 12 步**作本次实施清单。
7. **三个 CALIBRATE 项合入前闭合**（离线校验闭环 + 全 producer 互斥审计 + 错误传播独立 blocker）。
8. **第 16~31 章的 D+A 完整分析保留为未来增强备选**（若需求改为"所有输出必须接近 4GiB"，回退 D+A 或 suffix 重写）。

### 33.8 进入实施

设计 review 正式收敛于 **D+L（global-last anchor + last-only split）**。下一轮按 32.11 的 10 步进入实施：

~~~text
1. 修复 AddUnordered/AddFiles/addFile/matchOrderFiles/contains 全错误传播
2. 从全部 live order 选唯一 G，去重加入 ctx.order
3. G 进 m.acquire path + acquire 后复核 identity
4. execute 用 exact G identity 设唯一 lastFile
5. 删除 reservedExtents/TryNextExtentFile/向后扫描
6. 重构 SID finish + lazy boundary（非 G 永不 rotate，G lazy rotate）
7. G exact extent+1 + 冲突软停止 + 未知 IO 硬失败 + overflow guard
8. 删除 writeRemain EOF rotate，不创建空后继
9. 保留 processedOld + no-overwrite publish + 失败清理
10. 执行 32.12 测试矩阵和查询回归
~~~

实施 review 采用 26.7 中性协议。10 步必须作为一个不可分割合入变更完成，不部署"仍切非 G"或"仍向后扫描"的中间状态。

**CC 立场：D+L 设计 review 收敛，进入实施。** 请 Codex 确认 33.5 十二问回答 + 33.6 三个 CALIBRATE 项。确认后设计讨论正式结束，按 32.11 编码。

## 34. Codex 对 CC 第 33 章的 1+1 复核（提交 CC 再审）

> 本节保留第 33 章原文，不把外部 review 直接视为最终事实。Codex 按第 26.7 节中性协议重新进行了 1+1 复核：证据组逐项核对当前分支源码和控制流，对抗审核组独立寻找确定性反例，再由 Codex 汇总裁决。结论是：**D+L 仍是本次推荐基线，但第 33 章的“10 ACCEPT + 1 CALIBRATE、0 REJECT”和“设计正式收敛、直接进入实施”不能维持。当前应为 ACCEPT D+L 方向、CALIBRATE 证明前提、REJECT 现有 10 步已经覆盖全部 blocker。**

### 34.1 复核总裁决

| 第 33 章结论 | Codex 复核 | 说明 |
|---|---|---|
| D+L 替代 D+A 作为本次基线 | **ACCEPT** | 只切 global-last 能在现有命名和替换模型下缩小冲突面，是本次最小安全方向 |
| G 的 exact extent+1 不跨 snapshot survivor | **ACCEPT + CALIBRATE** | 数学证明成立，但必须先保证唯一 logical owner、完整 live snapshot 和 exact identity |
| D 强制 G 可以修复 residual 落点 | **ACCEPT + CALIBRATE** | 设计成立；当前代码仍使用 selected-last，并未实现 D |
| G 不含目标 SID 时仍能保持 SID/time 顺序 | **ACCEPT + CALIBRATE** | 依赖输入合法、所有 metadata error 已传播和统一 lazy boundary |
| 32.9 错误传播仍是 blocker | **ACCEPT** | 已确认存在数据丢失通道 |
| 32.11 的 10 步已经完整覆盖 11 项 blocker | **REJECT** | 缺 pre-output decision pass、cold 原子创建、foreign final 和 publish/recovery 闭环 |
| 离线 validator + durable feature gate 是 clean upgrade 的必需入口 | **REJECT** | 在“实验 split 从未部署”的既定前提下不是算法必需项，会重新引入复杂状态机 |
| 当前可以结束设计讨论并直接编码 | **REJECT** | 先按本节第 34.10 节修订最小实施清单，再进入编码 |

本章的范围需要单独澄清：文件大小阈值只用于触发“完整 SID 写完后是否尝试切割”的判断；本章不讨论 4 GiB 是硬约束还是软约束，也不以 4 GiB 作为选择 D+L 或判断查询正确性的依据。`meta` 中 `uint32` offset/size 的越界或读取回绕不属于本次修改范围，由独立改动处理；本章只审核文件切割后的命名唯一性、提交原子性、SID/time 布局和查询可用性。

### 34.2 必须区分“D+L 设计成立”和“当前代码已经实现”

第 33 章是在接受一个待实施设计，不应被理解为当前工作区代码已经满足 D+L。当前分支仍有以下事实：

1. [matchOrderFiles](../engine/immutable/merge_out_of_order.go#L235) 只在没有任何时间匹配文件时才补最后文件，并非每轮都从全部 live order 中强制加入 G；
2. [execute](../engine/immutable/merge_tool.go#L225) 使用 selected set 的最后文件设置 lastFile，不是 exact global-last identity；
3. [execute](../engine/immutable/merge_tool.go#L206) 仍构造 reservedExtents；
4. [TryNextExtentFile](../engine/immutable/stream_downsample.go#L144) 仍会向后扫描 extent；
5. [SwitchNewTSSP](../engine/immutable/column_iterator.go#L169) 对所有已处理文件都可能 rotate，非 G 尚未禁止切割；
6. [writeRemain](../engine/immutable/merge_performer.go#L339) 在最后一个 remaining SID 后仍可能 rotate；
7. AddUnordered、AddFiles/addFile、matchOrderFiles、contains 的错误仍未完整传播。

因此，本节以下裁决审核的是“第 33 章是否足以作为完整实施规格”，不是否定 D+L 的方向。

### 34.3 Blocker A：contains 只传播 error 仍不能满足“首个 output 前失败”

第 32.9 节第 5 项要求：

~~~text
任一输入 metadata/Contains 错误发生时，
第一个 output 尚未创建。
~~~

当前 [execute](../engine/immutable/merge_tool.go#L225) 把 contains 判定与写出循环交错执行。即使把 contains 改成 (bool, error)，仍可出现以下确定性流程。

假设排序后的文件为：

~~~text
A = 00000028-0001-00030000.tssp
B = 00000029-0001-00030000.tssp
G = 0000002a-0001-00030007.tssp
~~~

执行过程可能是：

~~~text
contains(A) = true
→ 创建 00000028-0001-00040000.tssp.init
→ Run(A) 成功

contains(B) 读取 ChunkMeta 时返回 IO error
→ 此时首个 output 已经创建
~~~

这说明第 33.8 节第 1 步“修复全错误传播”不等于第 32.9 节第 5 项已经闭合。

#### 34.3.1 推荐的最小修正

在创建任何 StreamWriteFile 之前，增加一个只读 decision pass：

~~~text
UnorderedReader.AddFiles 全部成功
→ 对所有非 G matched order 文件完成 contains 判定
→ 构造 processPlan[fileIdentity] = process / skip
→ 所有判定成功
→ 才创建第一个 output
~~~

这不是 measurement 全量 ordered SID validator：

- 不扫描所有 ordered ChunkMeta；
- 不保存 shard 级 SID 集合；
- 只复用 UnorderedReader 已经持有的 unordered SID；
- contains 工作量与现有执行路径相同，只是从写出循环前移。

若不采用这个 pre-output decision pass，则必须正式修改第 32.9 节第 5 项为“允许创建 job-owned 临时输出，但发生错误后必须完整清理且绝不 publish”。考虑到清理本身也可能失败，Codex 推荐保留原强契约并前移 contains。

### 34.4 Blocker B：candidate .init 与 final 不能采用相同的软停止语义

假设：

~~~text
G(old) = 0000002a-0001-00030007.tssp

first output:
0000002a-0001-00040007.tssp.init
→ 0000002a-0001-00040007.tssp

exact tail candidate:
0000002a-0001-00040008.tssp.init
→ 0000002a-0001-00040008.tssp
~~~

第 32.4/32.5 节和第 33.5 节第 8 问把 candidate .init 与 final 已存在统一处理为“停止切割、继续当前 writer”。这个结论只对 foreign .init 可以成立，对未注册 final 不充分。

#### 34.4.1 candidate .init 已存在

~~~text
0000002a-0001-00040008.tssp.init 已存在
~~~

最小安全行为可以是：

~~~text
不覆盖
不删除
splitDisabled = true
继续当前 writer，允许文件软超限
~~~

.init 不会被正常查询 loader 当作 committed TSSP 使用。

#### 34.4.2 candidate final 已存在

~~~text
0000002a-0001-00040008.tssp 已存在
但不在本轮 committed live snapshot
~~~

这表示至少存在一种异常：

- 未完成 recovery 的 orphan final；
- 未受 admission 约束的并发 producer；
- 历史 namespace 污染；
- live snapshot 不完整。

当前 [fileLoader.Load](../engine/immutable/mms_loader.go#L61) 会枚举合法 final .tssp，[loadTsspFile](../engine/immutable/mms_loader.go#L128) 会在重启时加载它。本轮如果只软停止切割并继续提交 0000002a-0001-00040007.tssp，重启后 foreign 0000002a-0001-00040008.tssp 仍可能进入 live set，破坏唯一 owner 或 non-overlap。

因此推荐行为必须改为：

~~~text
candidate .init exists → 软停止切割
candidate final exists → 整轮硬失败，不 publish，不删除 old/unordered
unknown Stat error     → 整轮硬失败
~~~

只有 recovery/admission 层已经能够确定该 final 是本 job 所有并完成隔离时，才允许采用其他恢复动作；merge 不能自行猜测 owner。

### 34.5 Blocker C：cold .init 创建和 final publish 尚未实现原子 no-overwrite

#### 34.5.1 cold CreateV2 会截断已有路径

当前 [StreamWriteFile.NewFile](../engine/immutable/stream_downsample.go#L249)：

~~~text
cold     → fileops.CreateV2
non-cold → O_CREATE | O_EXCL | O_RDWR
~~~

默认 OS 实现中，[CreateV2](../lib/fileops/os_fs.go#L166) 调用 Create，而 [Create](../lib/fileops/os_fs.go#L157) 使用：

~~~text
O_RDWR | O_CREATE | O_TRUNC
~~~

因此 cold 分支执行 Stat 后，若另一方在 CreateV2 前创建同名 .init，当前 writer 会截断 foreign 文件。单纯“先 Stat”不能关闭 TOCTOU。

#### 34.5.2 最小方案不需要为本次切割扩展 cold backend

本次范围不包含为 cold backend 新增一套分布式 namespace claim。对不支持原子 create-if-absent 的 backend，最小方案推荐：

~~~text
若 backend 不支持原子 create-if-absent：
    对 tail 设置 splitDisabled=true
    继续当前 writer
~~~

即 cold/不支持 exclusive claim 的 backend 可以暂不执行 D+L tail split。这是文件名 claim 能力不足时的安全降级，与 `meta uint32` 的容量或回绕问题无关。

如果 CC 要求 cold 也必须切割，则必须明确增加等价 O_EXCL/create-if-absent primitive、竞态测试和恢复语义，不能只依赖 Stat。

#### 34.5.3 final publish 也必须闭合

当前 [RenameTmpFiles](../engine/immutable/mms_tables.go#L843) 调用普通 rename；[RenameFile](../lib/fileops/file_ops.go#L218) 的接口说明明确写明目标存在时会替换目标。

因此还需二选一：

1. 提供真正的 no-replace publish；
2. 或证明从 exact .init claim 到 final publish 期间，同一 output namespace 始终由本 job 独占，并由现有 compact log/recovery 处理多文件部分发布。

第 33.8 节只写“no-overwrite publish”，没有指定其中任一闭环，因此不能据此宣称实施清单完整。

### 34.6 Blocker D：必须廉价验证唯一 logical G

现有 [TSSPFiles.Less](../engine/immutable/tssp_reader.go#L147) 和 [mergeFileInfo.Less](../engine/immutable/merge_util.go#L121) 都只比较：

~~~text
(sequence, extent)
~~~

level 与 merge 不参与逻辑排序，它们属于 physical/exact identity。

若 live set 意外同时存在：

~~~text
0000002a-0001-00030007.tssp
0000002a-0002-00090007.tssp
~~~

二者 physical path 不同，但 logical key 都是：

~~~text
(sequence=0x2a, extent=7)
~~~

Less 对两者互相都返回 false，无法证明存在唯一 G。任取其中一个作为 G 并替换，都可能让另一个同 logical slot survivor 留下。

#### 34.6.1 推荐的最小检查

在 matchOrderFiles 本来就会执行的 live order 文件扫描中：

~~~text
logicalKey = (sequence, extent)
exactIdentity = (sequence, level, merge, extent, order, path)

若一个 logicalKey 对应两个不同 exactIdentity：
    创建 output 前硬失败
~~~

该检查：

- 只与文件数成正比；
- 不读取 ChunkMeta；
- 不存 SID；
- 不需要持久化；
- 可以与选择 G 同一次扫描完成。

因此它比离线全 SID validator 更符合本次“尽力切割、正确性优先”的约束。

### 34.7 CALIBRATE：m.acquire 是 input path 互斥，不是 namespace transaction

[MmsTables.acquire](../engine/immutable/mms_tables.go#L946) 只登记传入的 exact path 字符串：

~~~text
inCompact[path] = claimed
~~~

它不会：

- 冻结整个 measurement；
- 阻止 future ordered append；
- 自动 reserve 尚不存在的 tail candidate；
- 自动覆盖所有 producer。

当前已核实：

1. merge 会对 selected order paths 调用 acquire；
2. level/full compaction 会对其 input group 调用 acquire；
3. downsample 不直接调用 acquire，但会先 [DisableCompAndMerge](../engine/shard.go#L1889)，并等待现有 compaction/merge 退出，因此属于等价屏障；
4. 普通 flush append 不检查 inCompact，但它使用递增 [NextSequence](../engine/immutable/mms_tables.go#L454)，不会与 G 的相同 sequence tail 发生文件名冲突；
5. 普通写入只有 time 大于 LastFlushTime 才进入 ordered，否则进入 unordered，见 [appendFields](../engine/mutable/ts_table.go#L188)，因此正常 flush append 不是本方案的确定性时间反例。

所以第 33.6 节第 2 项应保留为实际审计任务，但不应要求所有 producer 机械调用 m.acquire。正确要求是：

~~~text
替换/重命名 G 的 producer
必须持有 G exact path 的 acquire 或等价全局屏障；

正常 append producer
必须继续满足新 sequence + per-SID ordered time boundary。
~~~

### 34.8 REJECT：clean upgrade 不需要 durable 离线校验 feature gate

第 32.6 节已经给出明确部署前提：

1. 实验性 merge split 从未正式部署；
2. committed ordered 布局满足既有同 SID 时间顺序不变量；
3. D+L 只 forward-preserve，不修复历史坏布局。

在这些前提下，新增以下机制不是 D+L 的算法必要条件：

- 独立 split feature gate；
- 每 shard/measurement 的 durable “已校验”标志；
- validator/repair 状态机；
- 未校验拒绝路径；
- 新 shard、恢复和升级后的标志生命周期。

这会把已经简化的 D+L 再次扩展成新的持久化协议，也无法替代运行时必须具备的 exact collision、unique logical G 和 no-overwrite 防线。

Codex 推荐：

~~~text
clean upgrade：
    依赖既有 ordered 合法布局
    不增加 durable validated marker
    使用第 34.6 节的廉价文件级检查

曾运行实验 split 或已知布局异常的环境：
    feature 保持关闭
    作为运维前置条件执行离线检查/repair
    不把该流程放入每轮 merge 热路径
~~~

如果 CC 仍认为所有 clean upgrade 环境都必须持久化 “validated” 状态，请给出一个满足既定部署前提、但没有该标志就会由 D+L 新产生错误的确定性反例。

### 34.9 查询与 pre-agg 结论必须写成条件性 ACCEPT

查询侧并不对 ordered locations 做真正的 k-way merge。

- [LocationCursor.Less](../engine/immutable/location_cursor.go#L62) 按 ChunkMeta minTime 排序；
- [LocationCursor.ReadData](../engine/immutable/location_cursor.go#L200) 随后逐 location 顺序读取；
- [AddLocationsWithLimit](../engine/tsm_merge_cursor.go#L136) 的源码注释也明确说明正确性依赖同 SID locations 的时间范围不重叠。

例如：

~~~text
0000002a-0001-00040007.tssp
SID=42, time=[1,100]

0000002a-0001-00040008.tssp
SID=42, time=[50,60]
~~~

ascending cursor 会先读完 [1,100]，再读 [50,60]，不会得到全局时间有序结果；ORDER BY time LIMIT/OFFSET、first/last 以及依赖无重复 owner 的 pre-agg 都可能错误。

这不是对 D+L 的反例，而是说明 D+L 必须先证明并保持：

~~~text
同 SID 的 committed locations 按逻辑 owner 时间范围严格递增且不重叠
~~~

在以下条件全部成立时，查询结论才可以 ACCEPT：

1. 输入 committed 布局合法；
2. 唯一 logical G 检查通过；
3. D 强制 G，residual 只在逻辑末尾收尾；
4. 所有 input metadata/Contains error 已传播；
5. G 只在完整 SID 边界切割；
6. foreign final 不被 admission；
7. concurrent producer 保持既有时间边界。

满足这些条件后，D+L forward-preserve non-overlap，raw、ascending/descending、ORDER BY time LIMIT/OFFSET、first/last 和 pre-agg 才不会因本次切割产生错乱。

### 34.10 修订后的最小 10 步实施清单

Codex 不建议恢复 D+A、全量 SID validator 或复杂 allocator。只需把第 33.8 节的 10 步修订为：

~~~text
1. 输入阶段：
   修复 AddUnordered/AddFiles/addFile/matchOrderFiles/contains 全错误传播；
   在首个 writer 前完成所有 contains decision，生成 processPlan。

2. 一次 live order 文件扫描：
   检查 (sequence, extent) 唯一 logical owner；
   选择 exact global-last G；
   去重加入 ctx.order。

3. 对 processPlan 全部 order paths（含 G）执行 acquire；
   acquire 后复核 G exact identity 仍 live；
   失败则不创建 output。

4. execute 只用 exact identity 判断 G：
   非 G 永不因 size rotate；
   G 是唯一 lastFile；
   processedOld 只记录成功 Run 的文件。

5. 删除 reservedExtents、TryNextExtentFile 和任意 extent 向后扫描。

6. 统一 SID lazy boundary：
   finish previous SID；
   写完 boundary 前的 remaining unordered SID；
   已知 next SID 存在后才检查 size；
   仅 G 可尝试 rotate；
   再 ChangeSid/start next SID。

7. G tail 只尝试 exact current.extent+1：
   .init exists       → splitDisabled，继续当前 writer；
   final exists       → 整轮硬失败；
   extent=0xffff      → splitDisabled；
   unknown IO         → 整轮硬失败；
   backend 无原子 claim → splitDisabled。

8. EOF：
   finish active SID 和全部 remaining unordered；
   finalize 当前文件；
   不再检查 size，不创建空后继。

9. 提交与失败：
   .init 不得覆盖 foreign path；
   final publish 必须 no-replace 或具备等价 namespace 独占证明；
   复用并验证 compact log/recovery 的多文件部分发布语义；
   失败时 old/unordered 不动，清理所有 job-owned writer/fd/.init。

10. 执行第 32.12 节测试矩阵，并新增：
    contains 后置错误但首 output 尚未创建；
    duplicate logical (sequence, extent)；
    foreign .init 与 foreign final 的不同处理；
    cold/backend 无 exclusive create 时不切割；
    final publish collision；
    restart loader 不 admission foreign tail。
~~~

这仍然是 D+L：

~~~text
不保存 shard 级 SID
不全扫 ordered ChunkMeta
不计算 gap/ceiling
不向后扫描 extent
不处理 meta uint32 offset/size 越界或读取回绕
遇到无法局部证明安全的情况就不切或失败
~~~

### 34.11 请 CC 再次审核

请 CC 按源码事实、控制流、设计证明三个维度，对以下问题逐项给出 ACCEPT / REJECT / CALIBRATE：

1. 是否同意 D+L 继续作为本次基线，但撤回“第 33.8 节原 10 步已经完整”的结论？
2. 是否同意 contains 必须在首个 writer 前完成全部 process/skip 决策，才能满足第 32.9 节第 5 项？
3. 是否同意 candidate .init 可以软停止，而未注册 candidate final 必须硬失败，二者不能合并处理？
4. 是否确认 cold CreateV2 在默认 OS 实现中具有 O_TRUNC 语义，不能作为原子 no-overwrite？
5. 在本次不新增 cold backend namespace claim 的范围下，是否接受“不支持原子 create-if-absent 的 backend 暂不切 tail”？
6. 是否同意 G 选择时增加 O(file count)、不读 ChunkMeta 的 logical slot 唯一性检查？
7. 是否同意普通 ordered flush append 在新 sequence 和 LastFlushTime 分类不变量成立时不是反例？
8. 是否同意 producer 审计接受 m.acquire 之外的等价屏障，例如 downsample 的 DisableCompAndMerge + wait？
9. 是否同意 clean upgrade 不引入 durable validated marker；实验 split 环境由 feature 关闭 + 离线运维前置处理？
10. 是否同意 raw/pre-agg 查询结论必须以同 SID location ranges non-overlap 为明确前提？
11. 是否确认现有 RenameTmpFiles/compact log 已足以提供多文件 no-replace publish 与部分失败恢复；若不足，请指出最小缺口？
12. 是否接受第 34.10 节修订后的 10 步替代第 33.8 节原清单？

### 34.12 Codex 最终意见

Codex 对第 33 章的最终裁决是：

~~~text
ACCEPT:
    D+L 作为本次简化基线
    非 G 不切，G exact-next
    只在完整 SID 边界判断并执行切割
    错误传播仍为 blocker

CALIBRATE:
    exact-next 证明必须增加唯一 logical G 前提
    producer 互斥必须区分 replace 与 normal append
    查询正确性必须明确依赖 non-overlap

REJECT:
    第 33 章原 10 步已覆盖全部 blocker
    foreign final 可以只软停止
    cold Stat + CreateV2 足以 no-overwrite
    clean upgrade 必须增加 durable validator feature gate
    当前设计 review 已完全收敛并可直接编码
~~~

因此设计不需要回退到复杂 D+A，但必须先由 CC 审核第 34.10 节的修订清单。CC 接受后，再按该清单实施；在此之前不应继续扩展 allocator，也不应把当前 partial implementation 视为可合入状态。

## 35. CC 对 Codex 第 34 章的 1+1 复核与 D+L 修订基线收敛（2026-07-22）

本节由 CC 对 Codex 第 34 章做 1+1 复核：派发两个证据 agent 分别验证 4 个 blocker（34.3/34.4/34.5/34.6）与 4 项校准（34.7/34.8/34.9/34.10），按 26.7 中性协议区分源码事实/控制流/设计证明。结论：**Codex 第 34 章全部正确。CC 第 33 章"设计收敛、直接编码"过早——4 个 blocker 是真实实施缺口，34.10 修订的 10 步才覆盖全部。CC 全盘接受，采用 34.10 修订 10 步作实施基线。**

### 35.1 范围确认（与用户澄清一致）

Codex 34.1 明确："本章不讨论 4 GiB 是硬约束还是软约束，也不以 4 GiB 作为选择 D+L 或判断查询正确性的依据。`meta` 中 `uint32` offset/size 的越界或读取回绕不属于本次修改范围，由独立改动处理。"这与用户澄清一致：4GiB 软约束不需强调，meta uint32 越界由其他同事处理。**CC 确认此范围，后续不再讨论 4GiB 软硬性或 meta uint32。**

### 35.2 逐条裁决

| Codex 项 | CC 终裁 | 证据类型 | 关键依据 |
|---|---|---|---|
| 34.3 contains 交错致首 output 已创建 | **ACCEPT** | 控制流 | `merge_tool.go:225-250` contains(A)→InitMergedFile(A)创建 .init→Run(A)→contains(B) 失败时 A 的 .init 已落盘 |
| 34.4 .init 软停 vs final 硬失败 | **ACCEPT** | 源码事实 | `mms_loader.go:88` .tssp→loadTsspFile（加载进 live set）；`:90-109` .init→removeTmpFile（删除）；foreign final 重启后进 live set 破坏唯一 owner |
| 34.5 cold O_TRUNC + rename replace | **ACCEPT** | 源码事实 | `stream_downsample.go:250` cold→CreateV2→`os_fs.go:157` Create 用 O_TRUNC；non-cold `:256` 已 O_EXCL；`mms_tables.go:852` RenameTmpFiles→`file_ops.go:218` RenameFile replace 语义 |
| 34.6 唯一 logical G 检查 | **ACCEPT** | 源码事实 | `tssp_reader.go:147-155` Less 仅比 (seq,extent)；两文件同 (seq,extent) 不同 level/merge → Less 互返 false → 无法确定唯一 G |
| 34.7 m.acquire path 互斥 + producer 区分 | **ACCEPT + CALIBRATE** | 源码事实 | `mms_tables.go:946` acquire 按 path；`shard.go:1889` DisableCompAndMerge；`mms_tables.go:454` NextSequence；`ts_table.go:188` LastFlushTime 分类。**CALIBRATE**：DisableCompAndMerge 的 wait 语义需补证 |
| 34.8 不需 durable validator gate | **ACCEPT** | 设计证明 | 32.6 前提下 forward-preserve + 34.6 廉价检查充分；marker 会重引入持久化协议与简化目标矛盾 |
| 34.9 查询条件性 ACCEPT（依赖 non-overlap） | **ACCEPT** | 源码事实+设计证明 | `location_cursor.go:62` Less 按 minTime、`:200` ReadData 顺序读非 k-way；`tsm_merge_cursor.go:156-162` 注释明说 non-overlap 是前提；7 条件为 non-overlap 充分前提 |
| 34.10 修订 10 步完整 | **ACCEPT + CALIBRATE** | 文档对照 | 4 blocker 全覆盖。**CALIBRATE**：步骤 10 加直接 non-overlap 测试用例；步骤 9 RenameTmpFiles no-replace 缺口需 CC 确认（源码是普通 rename 有 replace 风险） |

统计：`ACCEPT=8`（含 3 CALIBRATE）、`REJECT=0`。

### 35.3 CC 第 33 章的过早收敛（确认）

CC 33 称"设计 review 正式收敛、进入实施"。Codex 34 正确指出 4 个实施缺口：

1. **34.3**：contains 改 (bool,error) 是必要但不充分——execute 交错结构导致首 output 可能在后续 contains 失败前已创建。需 pre-output decision pass。
2. **34.4**：.init 与 final 不能同为软停止——.init 不被 loader 加载（软停安全），final 会被 loadTsspFile 加载（foreign final 必须硬失败）。
3. **34.5**：cold CreateV2 用 O_TRUNC（截断 foreign 文件），RenameTmpFiles 用 replace rename——都不是原子 no-overwrite。
4. **34.6**：Less 只比 (seq,extent) 无法证明唯一 G——需 O(file count) 的 logical slot 唯一性检查。

**CC 撤回 33.7 第 7 点"设计 review 正式收敛"和 33.8"进入实施"。** 改为：采用 34.10 修订 10 步后，设计 review 收敛。

### 35.4 三个 CALIBRATE 项

1. **34.7 DisableCompAndMerge wait 语义**：证据 agent 指出 `shard.go:1889` DisableCompAndMerge 的"wait"语义未在函数体内直接验证。需确认它是否等待现有 compaction/merge 退出（若是则等价屏障成立；若仅设 flag 则需补 wait）。这是实施时的审计任务，不阻塞基线。
2. **34.10 步骤 10 加直接 non-overlap 测试**：当前步骤 10 通过"duplicate logical"和"foreign tail"间接覆盖 non-overlap。建议加直接测试："同 SID 两 location 时间范围严格递增且不重叠"。
3. **34.10 步骤 9 RenameTmpFiles no-replace 缺口**：源码 `mms_tables.go:843` RenameTmpFiles 调 `f.Rename`→`RenameFile`（replace 语义）。步骤 9 写"final publish 必须 no-replace 或具备等价 namespace 独占证明"，但当前代码是 replace。需 CC 确认：是改 RenameTmpFiles 为 no-replace，还是证明 compact log/recovery 提供等价独占。这是 34.11 第 11 问的核心。

### 35.5 回答 34.11 十二问

CC 按 26.7 中性协议逐项回答：

1. **是否同意 D+L 继续作基线，撤回"33.8 原 10 步已完整"？**
   **ACCEPT。** D+L 方向对；33.8 原 10 步缺 4 个 blocker 的处理，撤回"已完整"。

2. **是否同意 contains 必须在首 writer 前完成全部 process/skip 决策？**
   **ACCEPT。** 34.3 控制流证明：交错结构下 contains(B) 失败时 A 的 .init 已创建。需 pre-output decision pass（34.10 步骤 1）。

3. **是否同意 .init 软停、未注册 final 硬失败？**
   **ACCEPT。** 34.4 源码事实：.init→removeTmpFile（安全），final→loadTsspFile（foreign 进 live set 破坏 owner）。

4. **是否确认 cold CreateV2 默认 OS 实现有 O_TRUNC？**
   **ACCEPT。** 34.5 源码事实：`os_fs.go:157` Create 用 O_RDWR|O_CREATE|O_TRUNC；CreateV2 调 Create。

5. **是否接受"不支持原子 create-if-absent 的 backend 暂不切 tail"？**
   **ACCEPT。** 34.5.2 设计证明：cold/backend 无原子 claim 时 splitDisabled=true 是安全降级。与 meta uint32 无关。

6. **是否同意 G 选择时增加 O(file count) 唯一性检查？**
   **ACCEPT。** 34.6 源码事实：Less 仅比 (seq,extent)，同 logical slot 不同 level/merge 时无法确定唯一 G。检查 O(file count)、不读 ChunkMeta、可与选 G 同次扫描。

7. **是否同意普通 flush append 在新 seq + LastFlushTime 不变量下不是反例？**
   **ACCEPT。** 34.7 源码事实：`mms_tables.go:454` NextSequence 递增 seq；`ts_table.ts:188` time>LastFlushTime 进 ordered。新 seq 不与 G tail 同 seq 冲突。

8. **是否同意 producer 审计接受 m.acquire 之外的等价屏障（如 downsample DisableCompAndMerge+wait）？**
   **ACCEPT（+CALIBRATE）。** 34.7 源码事实：downsample 用 DisableCompAndMerge。**CALIBRATE**：需补证 DisableCompAndMerge 的 wait 语义（35.4 第 1 项）。

9. **是否同意 clean upgrade 不引入 durable validated marker？**
   **ACCEPT。** 34.8 设计证明：32.6 前提下 forward-preserve + 34.6 廉价检查充分。marker 会重引入持久化协议。与用户"4GiB 不强调、meta uint32 外部"的简化方向一致。

10. **是否同意 raw/pre-agg 查询结论必须以 non-overlap 为明确前提？**
    **ACCEPT。** 34.9 源码事实：LocationCursor 非 k-way merge；`tsm_merge_cursor.go:156-162` 注释明说 non-overlap 是前提。7 条件为 non-overlap 充分前提。

11. **是否确认现有 RenameTmpFiles/compact log 已提供多文件 no-replace publish 与部分失败恢复？**
    **CALIBRATE（需实施时确认）。** 源码事实：`mms_tables.go:843` RenameTmpFiles 用普通 rename（replace 语义），**不是 no-replace**。compact log 的多文件部分发布语义需实施时验证是否提供等价 namespace 独占。若不足，步骤 9 需改 RenameTmpFiles 为 no-replace 或新增独占证明。这是 35.4 第 3 项的开放点。

12. **是否接受 34.10 修订 10 步替代 33.8 原清单？**
    **ACCEPT。** 34.10 修订 10 步覆盖 4 blocker + 3 CALIBRATE，比 33.8 完整。

### 35.6 CC 最终立场（D+L 修订基线收敛）

1. **全盘接受 Codex 第 34 章**（8 ACCEPT 含 3 CALIBRATE，0 REJECT）。
2. **撤回 33.7/33.8 的"设计收敛、进入实施"**——4 个 blocker 是真实缺口，33.8 原 10 步不完整。
3. **采用 34.10 修订 10 步**作本次实施基线（覆盖 34.3 pre-output decision / 34.4 .init vs final / 34.5 cold O_TRUNC+no-replace / 34.6 唯一 logical G）。
4. **接受 34.8 不引入 durable validator gate**（与用户"4GiB 不强调、meta uint32 外部"一致）。
5. **接受 34.9 查询条件性 ACCEPT**（依赖 non-overlap，7 条件为前提）。
6. **三个 CALIBRATE 项**（DisableCompAndMerge wait / non-overlap 直接测试 / RenameTmpFiles no-replace 缺口）在实施时闭合。
7. **范围确认**：不讨论 4GiB 软硬、不处理 meta uint32（由独立改动负责）、不实施 D+A gap-only/全量 SID preflight/temporal-occupancy 分离。
8. **D+L 核心不变**：D=global-last anchor、L=last-only split（非 G 永不切、G exact+1 不扫描）、完整 SID 边界、错误传播 blocker 保留。

### 35.7 进入实施

设计 review 收敛于 **D+L + 34.10 修订 10 步**。下一轮按 34.10 的 10 步进入实施：

~~~text
1. 输入阶段：全错误传播 + pre-output contains decision pass（34.3）
2. live order 扫描：唯一 logical owner 检查（34.6）+ 选 G
3. acquire 全部 order paths + 复核 G identity
4. execute 用 exact G identity：非 G 永不 rotate，G 唯一 lastFile
5. 删除 reservedExtents/TryNextExtentFile
6. 统一 SID lazy boundary
7. G tail exact+1：.init exists 软停、final exists 硬失败（34.4）、overflow 软停、IO 硬失败、backend 无原子 claim 软停（34.5）
8. EOF：不创建空后继
9. 提交：.init 不覆盖 + final no-replace（34.5）+ recovery + 失败清理
10. 测试矩阵（含 non-overlap 直接测试 + contains 后置错误 + duplicate logical + foreign .init vs final + cold 不切 + publish collision + restart loader）
~~~

实施 review 采用 26.7 中性协议。10 步作为一个不可分割合入变更。三个 CALIBRATE 项（DisableCompAndMerge wait / non-overlap 直接测试 / RenameTmpFiles no-replace）在实施时闭合。

**CC 立场：D+L 修订基线收敛，进入实施。** 请 Codex 确认 35.5 十二问回答 + 35.6 立场。确认后设计讨论正式结束，按 34.10 编码。

## 36. Codex 对 CC 第 35 章的 1+1 复核（提交 CC 再审）

本节保留第 35 章原文。Codex 继续按第 26.7 节中性协议执行 1+1：第一层证据组核对当前分支源码、发布流程和恢复状态机；第二层对抗审核组审核证据链，并重点尝试反驳 `.init` 软停、普通 rename 等价独占以及“设计已经收敛”三个结论。

两层审查得到相同终裁：

~~~text
ACCEPT:
    D+L 写出算法继续作为基线
    pre-output processPlan
    唯一 logical G + exact identity
    非 G 不切、G 只在完整 SID 边界 exact+1
    查询正确性条件为同 SID location ranges non-overlap

CALIBRATE:
    .init 与 final 的碰撞策略
    cold/backend 无 atomic claim 的覆盖范围
    DisableCompAndMerge 的准确屏障范围

REJECT:
    当前 RenameTmpFiles + compact log 已等价提供安全多文件发布
    restart loader 不 admission foreign tail 已有对应机制
    第 35.7 节所称整体设计已收敛、可以直接编码
~~~

因此准确表述应改为：**D+L 的文件选择、SID 边界和查询不变量已经基本收敛；output namespace ownership、发布与恢复协议尚未收敛。**

### 36.1 对第 35 章主要结论的裁决

| 第 35 章结论 | Codex 1+1 终裁 | 说明 |
|---|---|---|
| 35.1 范围确认 | **ACCEPT** | 沿用既定范围，不扩大本次修改 |
| 34.3 pre-output contains decision | **ACCEPT** | processPlan 只按文件保存 decision，不新增 shard 级 SID 集合 |
| 34.4 `.init` 软停、final 硬失败 | **CALIBRATE** | loader 事实正确，但 recovery 先于 loader；`.init` 可能仍有可恢复 publisher |
| 34.5 cold O_TRUNC、rename replace | **ACCEPT 源码事实 / REJECT 当前闭环** | 第 35 章尚未选定 namespace proof 或真正 no-replace |
| 34.6 唯一 logical G | **ACCEPT** | 必须扫描 measurement 全部 live ordered 文件，并保存 G exact identity |
| 34.7 DisableCompAndMerge wait | **ACCEPT，疑点已闭合** | 它等待已登记的 compaction/merge，不是全 measurement producer transaction |
| 34.8 不增加 durable validator gate | **ACCEPT** | 但 validator 与 transaction recovery ownership 不是同一概念 |
| 34.9 查询条件性正确 | **ACCEPT** | 必须用真实输出验证 non-overlap，不得依赖 cursor 自动 k-way merge |
| 34.10 修订 10 步已经完整 | **REJECT** | 第 9 步仍是未选择的设计分支，第 10 步含没有实现机制支撑的断言 |
| 35.7 整体设计收敛、进入实施 | **REJECT** | 只能称 D+L 算法基线收敛 |

### 36.2 `DisableCompAndMerge` 的 wait 已经证实，但范围要写准

[DisableCompAndMerge](../engine/immutable/mms_tables.go#L232) 的实现明确是：

~~~go
func (m *MmsTables) DisableCompAndMerge() {
    m.disableCompAndMerge()
    m.wg.Wait()
}
~~~

`m.wg` 的登记点覆盖：

1. [LevelCompact](../engine/immutable/compact.go#L136)；
2. [FullCompact](../engine/immutable/compact.go#L483)；
3. [MergeOutOfOrder](../engine/immutable/merge_out_of_order.go#L49)。

所以第 35.4 节第 1 个 CALIBRATE 可以关闭：downsample 调用该函数后，已登记的 level/full compaction 和 OOO merge 已退出。

但它不等待普通 flush、loader、`TSSPFiles.wg` 或 downsample 自身。因此它应被称为“当前 compaction/merge 的 quiescence barrier”，不能被描述成整个 measurement namespace transaction。普通 flush 仍需依赖新 sequence 与既有 ordered time boundary 证明不会生成同名 output。

### 36.3 `.init exists → 软停止切割` 不能无条件 ACCEPT

第 35.2/35.5 节用 loader 的行为证明 `.init` 可以软停止：

~~~text
.tssp → loadTsspFile
.init → removeTmpFile
~~~

这只描述 loader 本身，不是完整启动顺序。[MmsTables.Open](../engine/immutable/mms_tables.go#L289) 在调用 loader 前，先执行 [recoverFile](../engine/immutable/mms_tables.go#L298)；loader 到 [第 322 行](../engine/immutable/mms_tables.go#L322) 才开始扫描 measurement。

因此一个已存在的：

~~~text
0000002a-0001-00040008.tssp.init
~~~

可能有两种身份：

1. 没有 compact log、没有 active writer 的裸 orphan，最终可由 loader 删除；
2. 被 durable compact log 引用的事务输出，启动 recovery 会先把它发布为：

~~~text
0000002a-0001-00040008.tssp
~~~

仅凭路径后缀无法区分二者。若当前 merge 看到 `.init` 后只设置 `splitDisabled=true`，仍提交自己的首输出，旧事务日后可能继续发布同一 lineage 的 tail。

为避免新增 compact-log ownership 查询或 manifest，本次推荐最简单的 fail-closed 规则：

~~~text
candidate .init exists  → 整轮 merge 硬失败，old/unordered 不动
candidate final exists  → 整轮 merge 硬失败，old/unordered 不动
unknown Stat/IO error   → 整轮 merge 硬失败
extent 无法继续         → 不创建 candidate，停止本次切割，继续当前 writer
backend 不支持安全 claim → 不创建 candidate，停止本次切割，继续当前 writer
~~~

如果 CC 仍希望 `.init exists` 软停止，则必须先证明它没有 active/recoverable owner。该证明需要关联 compact log 或 transaction identity，会重新引入本次试图避免的复杂分类，因此 Codex 不推荐。

### 36.4 当前多文件 publish/recovery 存在确定性混合集反例

[ReplaceFiles](../engine/immutable/mms_tables.go#L730) 先写 durable compact log，再调用 [RenameTmpFiles](../engine/immutable/mms_tables.go#L843) 逐个发布新文件。底层 [RenameFile](../lib/fileops/file_ops.go#L218) 的接口契约明确允许替换已经存在的目标。

compact log 只记录 basename。[newFileExist](../engine/immutable/compaction_file_info.go#L285) 将同名 `.init` 或 final 都视为该 new output 已存在，不保存 job token、内容 hash 或 owner identity。

以下状态可直接触发混合集：

~~~text
old（仍完整存在）:
    0000002a-0001-00030007.tssp

compact log 记录的 new:
    0000002a-0001-00040007.tssp.init
    0000002a-0001-00040008.tssp.init

故障后的磁盘状态:
    0000002a-0001-00030007.tssp       exists
    0000002a-0001-00040007.tssp       exists，首个 new 已发布
    0000002a-0001-00040008.tssp.init  missing
    0000002a-0001-00040008.tssp       missing
~~~

[processLog](../engine/immutable/compaction_file_info.go#L236) 的行为是：

1. 发现 new 只存在 1/2；
2. 发现 old 存在 1/1；
3. 进入“恢复 old”分支并返回 nil；
4. 已发布的 `0000002a-0001-00040007.tssp` 不会被删除；
5. [procCompactLog](../engine/immutable/compaction_file_info.go#L352) 随后删除 compact log；
6. loader 同时加载 old 和这个 partial final。

最终 committed live set 变成：

~~~text
0000002a-0001-00030007.tssp
0000002a-0001-00040007.tssp
~~~

二者占用同一 logical `(sequence=0x2a, extent=7)`，且可能包含相同 SID/time 数据。查询侧无法依靠 minTime 顺序消化这种重复或 overlap。

还有一个更直接的问题：[procCompactLog](../engine/immutable/compaction_file_info.go#L373) 在 `processLog` 返回错误后只记录日志，随后仍删除 compact log并最终返回 nil。恢复失败后丢失 durable 恢复依据，下一次启动无法重试。

因此第 35.5 节第 11 问必须从 `CALIBRATE` 改为：

> **REJECT：当前 RenameTmpFiles/compact log 不提供 multi-file no-replace，也不提供所有部分失败状态下的幂等恢复。**

### 36.5 “restart loader 不 admission foreign tail”目前没有实现机制

当前 [fileLoader.Load](../engine/immutable/mms_loader.go#L61) 会加载目录中所有扩展名合法的 `.tssp`。一个没有 compact log 的：

~~~text
0000002a-0001-00040008.tssp
~~~

与合法 committed tail 在文件名上没有区别。若不增加 manifest、job identity 或 admission metadata，loader 无法知道它是不是“foreign”。

所以第 34.10/35.7 的测试项：

~~~text
restart loader 不 admission foreign tail
~~~

必须二选一：

1. 缩小为“存在 compact log 的歧义/失败事务必须 fail-closed，在 recovery 成功前不得进入 loader”；
2. 若坚持识别任意无 log foreign final，则必须增加持久 owner/admission 机制。

本次推荐第 1 种。第 2 种会重新引入用户已经否定的复杂持久状态机。

### 36.6 cold/backend 策略不能只描述 tail 而忽略首输出

[StreamWriteFile.NewFile](../engine/immutable/stream_downsample.go#L222) 同时服务于：

1. 每个 processed old 的首输出；
2. G 切割产生的后续 tail。

默认 OS 的 cold [CreateV2](../engine/immutable/stream_downsample.go#L249) 最终走 `O_TRUNC`。因此“backend 无原子 claim 时只禁用 tail”只消除了本次新增 tail 的风险，不自动证明首输出 namespace 安全。

例如 old G：

~~~text
0000002a-0001-00030007.tssp
~~~

对应首输出和 tail 是：

~~~text
首输出: 0000002a-0001-00040007.tssp.init
tail:    0000002a-0001-00040008.tssp.init
~~~

第 35 章必须明确步骤 9 的范围：

- 如果只处理“切割新增的 tail”，则首输出继续依赖既有 input acquire + output namespace 不变量，不应声称本次为所有 merge output 增加了原子 no-overwrite；
- 如果声称所有 output 都具备 no-overwrite，则必须同时改首输出创建、发布和 recovery，这会显著扩大本次修改。

Codex 推荐第一种最小范围，但需要把 namespace 独占证明写完整。

### 36.7 推荐的最小 namespace 独占证明

为避免改造通用 `RenameTmpFiles`、所有 filesystem backend 和 loader manifest，Codex 推荐把第 34.10 节第 9 步具体化为以下证明与 fail-closed 规则：

1. 一次扫描 measurement 全部 live ordered 文件，检查 logical `(sequence, extent)` 唯一并选择 G exact identity；
2. 在首个 writer 前 acquire 本轮依赖的全部 ordered input paths，至少包含 processPlan 全部输入和 exact G；
3. acquire 后复核所有 planned input 仍 live，并复核 G exact identity；
4. acquire 一直持有到 `ReplaceFiles` 完成或整轮失败清理结束；
5. 对每个 planned 首输出同时检查 exact `.init` 与 final；任一存在则整轮失败；
6. G tail 只尝试 exact `current.extent+1`，不扫描；候选 `.init`/final 任一存在则整轮失败；
7. 支持 exclusive-create 的 backend 才创建 tail；不支持时不切割，不触碰 candidate；
8. compaction/merge 通过 input acquire 排除；downsample 通过 DisableCompAndMerge+wait 排除；普通 ordered flush 使用新 sequence，不生成 G lineage 的同名输出；
9. startup recovery 在 loader 和后台 producer 前执行；存在失败事务时必须 fail-closed，不能一边保留歧义 final 一边继续 loader；
10. 在上述内部 producer 闭包成立时，普通 rename 的 destination 按证明不可能存在；若 CC 不接受这项证明，则必须选择 scoped atomic no-replace，并同步重写 recovery 的 collision 语义，不能只替换 live publish 的一个调用。

这里的逻辑状态只按文件保存，不按 SID 保存。它不会为 1e8 个 shard SID 建表。

### 36.8 compact-log recovery 的最小闭环

不引入新 manifest 的前提下，至少应把当前日志恢复状态机改成 fail-closed：

| 磁盘状态 | 最小安全动作 |
|---|---|
| 每个 new 都有 `.init` 或同名 final | 幂等 roll-forward：发布剩余 `.init`，再删除 old |
| 没有任何 new final，old 全部存在，部分 new 缺失 | rollback/清理 job-owned `.init`，保留 old |
| 已有部分 new final，同时另一个 new 缺失 | 状态歧义：保留 log并令 shard open 失败，不进入 loader |
| old/new 的 rename/remove 返回错误 | 保留 log并返回错误，下一次启动重试 |
| `.init` 与同名 final 同时存在 | ownership/collision 歧义：保留 log并失败 |

在不保存 job identity 的情况下，恢复只能依赖“内部 namespace 独占”前提认定 log 中的同名 final 属于该事务。若这一前提不能成立，仅靠现有 basename log 无法安全区分 foreign final。

### 36.9 processPlan 不需要保存全部 SID

pre-output decision pass 不等于全 shard SID preflight：

1. 当前 [UnorderedReader](../engine/immutable/unordered_reader.go#L207) 已经维护本轮 selected unordered 文件的 `sid []uint64` 与 `meta map[uint64]...`；
2. `processPlan` 只需要为每个 ordered file 保存 `{exactIdentity, process/skip, isG}`；
3. 空间复杂度是 O(order file count)，不是 O(shard SID count)；
4. `contains` 只是提前遍历既有 `ur.sid`，不得复制 `ur.sid`，也不得构造 SID×file 矩阵。

因此本轮方案不需要 `MergePreflight` 保存 1e8 SID，也不需要扫描全部 ordered ChunkMeta。

### 36.10 查询与 pre-agg 测试仍是合入前必需项

[LocationCursor.Less](../engine/immutable/location_cursor.go#L62) 对 ordered location 按 `minTime` 排序，[ReadData](../engine/immutable/location_cursor.go#L200) 逐 location 读取，不执行 k-way merge。因此 D+L 只有在同 SID location ranges 严格 non-overlap 时才安全。

当前 `TestAddLocationsWithLimit_SameSidMultiExtent` 只验证 global `firstTime` 与 location 数量，没有从 D+L 产生真实输出，也没有读取实际数据验证顺序。

步骤 10 至少增加：

1. 用真实 merge 生成 base + tail；
2. 对每个 SID 收集所有 committed location ranges，断言相邻范围 `prev.maxTime < next.minTime`；
3. raw ascending/descending 与 unsplit oracle 一致；
4. `ORDER BY time + LIMIT/OFFSET` 与 oracle 一致；
5. first/last 与 oracle 一致；
6. pre-agg 与未切割结果一致；
7. 构造 `[1,100] + [50,60]` 负例，锁定当前 cursor 不具备 overlap 容错；
8. 验证单个 SID 永远不会因为本次 size boundary 被拆到两个新 extent。

### 36.11 修订后的最小 10 步实施清单

第 35.7 节的 10 步应替换为：

~~~text
1. 输入与计划：
   AddUnordered/AddFiles/addFile/matchOrderFiles/contains 全错误传播；
   首 writer 前完成 per-file contains decision，processPlan 不复制 SID。

2. live ordered 扫描：
   检查 logical (sequence, extent) 唯一；
   选择并保存 exact global-last G；
   G 去重加入 processPlan。

3. acquire 与首输出 preflight：
   acquire processPlan 全部 input paths（含 G）；
   复核 input/G identity；
   检查每个 planned 首输出的 .init/final，存在即整轮失败；
   首 output 创建前完成上述步骤。

4. execute：
   exact identity 判断 G；
   非 G 永不 rotate；
   G 是唯一 lastFile；
   processedOld 只包含成功 Run 的文件。

5. 删除 allocator：
   删除 reservedExtents、TryNextExtentFile 和任意 extent 向后扫描。

6. 统一 SID lazy boundary：
   finish previous SID；
   写完 boundary 前 remaining unordered SID；
   已知 next SID 存在后才判断 size；
   仅 G 可尝试 rotate；
   再 ChangeSid/start next SID。

7. G tail exact-next：
   只尝试 current.extent+1；
   candidate .init/final exists → 整轮失败；
   unknown IO → 整轮失败；
   无安全 claim 能力 → 不切割并继续当前 writer；
   不扫描其他 extent。

8. EOF：
   finish active SID 与 remaining unordered；
   finalize 当前文件；
   不再次判断 size，不创建空后继。

9. publish/recovery：
   input acquire 持有到 ReplaceFiles/失败清理结束；
   明确采用 namespace 独占证明，或实现 scoped no-replace，不能留作编码时选择；
   partial final + missing new 必须 fail-closed；
   recovery error 保留 compact log并阻止 loader；
   失败时 old/unordered 不动，清理 job-owned fd/.init。

10. 测试：
    contains 后置错误且首 output 未创建；
    duplicate logical owner；
    首输出与 tail 的 .init/final collision；
    backend 无 claim 时不切 tail；
    partial publish/recovery fail-closed；
    recovery error 不删 log；
    真实 non-overlap + raw/LIMIT/first-last/pre-agg oracle；
    EOF 无空后继。
~~~

### 36.12 请 CC 再次审核

请 CC 对以下问题逐项给出 ACCEPT / REJECT / CALIBRATE：

1. 是否同意第 35.4 节 `DisableCompAndMerge` wait 疑点已经闭合，但其范围仅是已登记 compaction/merge？
2. 是否同意 `.init` 可能先被 compact-log recovery 发布，因此不能仅凭 loader 删除行为证明无条件软停安全？
3. 是否接受最小规则：candidate `.init` 或 final 任一存在均整轮失败，而不是扫描或软停？
4. 是否确认当前 partial-final + missing-new 反例会保留 old 与 partial final并删除 log？
5. 是否同意 recovery 报错时必须保留 compact log并阻止 loader？
6. 是否同意“restart loader 不 admission foreign tail”只能在 log-backed 失败事务范围内实现；识别任意无 log foreign final 需要额外 manifest？
7. 是否同意步骤 9 必须在编码前明确选择 namespace 独占证明或 scoped no-replace，不能继续保留二选一占位符？
8. 是否接受本次推荐的 namespace 独占证明，以避免改造通用 RenameTmpFiles 和所有 backend？
9. 若不接受 namespace proof，请 CC 指出哪个内部 producer 能在 G acquire 持有期间生成同名首输出或 tail；随后再选择 scoped no-replace。
10. 是否同意 processPlan 只按文件保存，不增加 shard 级 SID 内存？
11. 是否接受第 36.11 节清单替代第 35.7 节清单？
12. 是否同意最终状态应写为“D+L 算法基线收敛，发布/恢复协议待 CC 闭合”，而不是“整体设计收敛、直接编码”？

### 36.13 Codex 最终意见

第 35 章对 D+L 核心、错误传播、唯一 G、SID boundary 和查询 non-overlap 的接受是正确的；`DisableCompAndMerge` 的 wait 也已经由源码闭合。

但第 35 章把真正的提交 blocker 放入“实施时确认”，同时宣称整体设计已经收敛，这两个结论不能同时成立。当前仍需 CC 明确接受以下三点：

1. existing `.init` 不再无条件软停，默认整轮 fail-closed；
2. 采用第 36.7 节 namespace 独占证明，或明确选择更复杂的 scoped no-replace；
3. compact-log recovery 遇到 partial final、missing new 或任何错误时保留日志并阻止 loader。

三点闭合后即可按第 36.11 节进入实现；在此之前，只能说 D+L 算法方向收敛，不能把当前 10 步视为完整可合入方案。

## 37. CC 对 Codex 第 36 章的 1+1 复核与 D+L 完整基线收敛（2026-07-22）

本节由 CC 对 Codex 第 36 章做 1+1 复核：派发两个证据 agent 分别验证 36.2/36.3/36.4（DisableCompAndMerge wait + .init 不能软停 + compact-log mixed-set 反例）与 36.5/36.6/36.7/36.8/36.11（loader 不识别 foreign + cold 覆盖首输出 + namespace 独占证明 + recovery fail-closed + 修订 10 步），按 26.7 中性协议区分源码事实/控制流/设计证明。结论：**Codex 第 36 章全部正确。CC 第 35 章把发布/恢复 blocker 放入"实施时确认"且宣称"整体收敛"是过早的。CC 全盘接受，采用 36.7 namespace 独占证明 + 36.8 fail-closed recovery + 36.11 修订 10 步。**

### 37.1 逐条裁决

| Codex 项 | CC 终裁 | 证据类型 | 关键依据 |
|---|---|---|---|
| 36.2 DisableCompAndMerge wait 证实、范围有限 | **ACCEPT** | 源码事实 | `mms_tables.go:232` `disableCompAndMerge(); m.wg.Wait()`；wg 登记仅 LevelCompact/FullCompact/MergeOutOfOrder |
| 36.3 .init 不能无条件软停 | **ACCEPT** | 控制流 | `mms_tables.go:298` Open 先 recoverFile 后 loader(:322)；recoverFile 可将 .init rename→.tssp → loader 加载为 live owner |
| 36.4 compact-log mixed-set 反例 | **ACCEPT（最关键）** | 控制流 | `compaction_file_info.go:236-283` processLog partial new+all old→restore old 返回 nil；`:352-384` procCompactLog 对 nil 和 error 都删 log；partial publish→old+partial final 同 (seq,extent) 双 owner |
| 36.5 loader 不识别 foreign final | **ACCEPT** | 源码事实 | `mms_loader.go:61-95` loadTsspFile 仅按 seq 跳过旧文件，不校验来源/job identity |
| 36.6 cold O_TRUNC 覆盖首输出非只 tail | **ACCEPT** | 源码事实 | `stream_downsample.go:222-259` NewFile 服务首输出+tail；cold `:249` CreateV2→`os_fs.go:157` O_TRUNC |
| 36.7 namespace 独占证明 | **ACCEPT（合理）** | 设计证明 | acquire+DisableCompAndMerge+wait+NextSequence+recovery-before-loader 闭包下，无内部 producer 能生成 G extent+1 同名文件；destination 不存在→replace 不触发 |
| 36.8 recovery fail-closed 表 | **ACCEPT（需编码改造）** | 设计证明 | 5 状态表覆盖 36.3/36.4 反例；但现状 procCompactLog 吞错删 log，须改造为 fail-closed |
| 36.11 修订 10 步 | **ACCEPT** | 文档对照 | 36.3-36.10 全覆盖；步骤 9 强制选定 namespace proof 或 scoped no-replace（不留占位符） |

统计：`ACCEPT=8`、`REJECT=0`。

### 37.2 CC 第 35 章的过早收敛（确认）

CC 35 把三个发布/恢复 blocker 放入"实施时确认"（35.4 CALIBRATE + 35.5 Q11 CALIBRATE）且宣称"整体设计收敛"（35.7）。Codex 36 正确指出这三项是**设计决策**（不是实施细节），必须在编码前选定：

1. **.init 软停**（35.5 Q3 ACCEPT）：36.3 证明 recovery 在 loader 前可能将 .init 发布为 .tssp → .init 不能无条件软停。**改为 fail-closed**。
2. **RenameTmpFiles no-replace**（35.5 Q11 CALIBRATE）：36.4 证明当前 compact-log recovery 有 mixed-set 反例（partial publish → 双 owner + 删 log 不可重试）。**改为 REJECT + 采用 36.7 namespace 独占证明 + 36.8 fail-closed recovery**。
3. **"整体收敛"**（35.7）：36.1 正确指出"D+L 算法收敛但发布/恢复协议未收敛"。**改为"D+L 算法 + 发布/恢复协议均收敛"（本次 37 节闭合）**。

### 37.3 36.4 mixed-set 反例是真实数据损坏（重点确认）

证据 agent 完整追踪了 compact-log recovery 的 mixed-set 反例：

1. `ReplaceFiles`（`mms_tables.go:730`）先写 compact log（仅 basename，无 job token/hash/owner），后 `RenameTmpFiles`（`:843`）逐个 rename（replace 语义，非原子）。
2. 若 partial publish（1 of 2 new 已 rename，2nd missing）+ old 仍在：
3. `processLog`（`compaction_file_info.go:236`）发现 new 1/2 + old 1/1 → "restore old" 分支 → 返回 nil。
4. `procCompactLog`（`:352`）见 nil → 删 log（`:379`）。
5. loader 加载 old + partial final → **同 logical (seq,extent) 双 owner** → 查询侧 minTime 排序无法消化重复/overlap。
6. 且 `procCompactLog` 对 processLog 返回 error 也只记日志仍删 log（`:373-379`）→ **recovery 不可重试**。

**这是前几轮（含 26.4.2 数据丢失）之外又一个真实数据损坏风险，在当前 compact-log recovery 代码中存在。** 必须在 D+L 实施时一并修复（36.8 fail-closed 表）。

### 37.4 采用 36.7 namespace 独占证明（而非 scoped no-replace）

CC 在 36.12 Q7-9（namespace proof vs scoped no-replace）中选择 **36.7 namespace 独占证明**：

理由：
1. 避免**改造通用 `RenameTmpFiles`、所有 filesystem backend 和 loader manifest**——与 D+L 简化目标一致。
2. 证明成立的前提（acquire 全部 input paths + DisableCompAndMerge+wait + NextSequence + recovery-before-loader）都有源码支撑（36.2/34.7），且无反例 producer（证据 agent 扫描所有 .tssp 创建路径，无内部 producer 能在 G acquire 持有期间生成 G extent+1 同名文件）。
3. 在该闭包下，rename destination 不可能存在 → replace 语义不触发 → 普通 rename 安全。

**前提**（编码时必须满足）：
- acquire 持有全部 input paths（含 G）到 ReplaceFiles/失败清理结束；
- acquire 后复核 input/G identity 仍 live；
- 每个首输出和 G tail 的 candidate `.init`/final 存在 → 整轮硬失败；
- 支持 exclusive-create 的 backend 才创建 tail；不支持时不切割；
- startup recovery 在 loader 前执行，失败事务 fail-closed 阻止 loader。

若 CC 后续发现某内部 producer 能突破该闭包，则回退到 scoped no-replace（需重写 recovery collision 语义）。但当前证据下，namespace 独占证明是更小且充分的方案。

### 37.5 采用 36.8 fail-closed recovery 表

CC 接受 36.8 的 5 状态 fail-closed 表，替代当前 procCompactLog 的"吞错删 log"行为：

| 磁盘状态 | 最小安全动作 |
|---|---|
| 每 new 都有 .init 或 final | roll-forward：发布剩余 .init，删 old |
| 无 new final + old 全在 + 部分 new 缺 | rollback：清 job-owned .init，保留 old |
| 部分 new final + 另一 new 缺 | **歧义：保留 log + shard open 失败，不进 loader** |
| rename/remove 错误 | **保留 log + 返回错误，下次启动重试** |
| .init 与 final 同时存在 | **歧义：保留 log + 失败** |

**关键改造**：`procCompactLog`（`compaction_file_info.go:352-384`）对 processLog 返回 error 时**不得删 log**，须保留并返回错误让 `recoverFile`→`Open()` 失败，阻止 loader 加载 partial final。

### 37.6 回答 36.12 十二问

1. **DisableCompAndMerge wait 已闭合、范围仅已登记 compaction/merge？**
   **ACCEPT。** 36.2 源码事实：`m.wg.Wait()` 等待 LevelCompact/FullCompact/MergeOutOfOrder。范围不含 flush/loader/downsample。

2. **.init 可能被 recovery 发布，不能仅凭 loader 删除证明软停安全？**
   **ACCEPT。** 36.3：recoverFile 在 loader 前，可发布 .init→.tssp。

3. **candidate .init 或 final 任一存在均整轮失败？**
   **ACCEPT。** 36.3+36.7：.init 可能被 recovery 发布（不能软停）；final 可能是 foreign（必须硬失败）。统一 fail-closed。

4. **partial-final + missing-new 反例保留 old + partial final 并删 log？**
   **ACCEPT。** 36.4：processLog "restore old" 返回 nil → procCompactLog 删 log → loader 加载双 owner。

5. **recovery 报错时保留 compact log 并阻止 loader？**
   **ACCEPT。** 36.8 表第 4 行：保留 log + 返回错误。改造 procCompactLog 不删 log。

6. **"restart loader 不 admission foreign tail"只能在 log-backed 范围内实现？**
   **ACCEPT。** 36.5：loader 不识别无 log foreign final。只在 log-backed 失败事务 fail-closed（36.8 表第 3-5 行）。

7. **步骤 9 必须编码前选定 namespace proof 或 scoped no-replace？**
   **ACCEPT。** 37.4 已选定 **namespace 独占证明**（36.7）。

8. **接受 namespace 独占证明避免改造通用 RenameTmpFiles？**
   **ACCEPT。** 37.4 已论证。

9. **若不接受 namespace proof，哪个内部 producer 能突破？**
   **N/A（已接受）。** 证据 agent 未发现反例 producer。

10. **processPlan 只按文件保存，不增 shard 级 SID 内存？**
    **ACCEPT。** 36.9：O(order file count)，复用 ur.sid 做 contains，不复制。

11. **接受 36.11 清单替代 35.7？**
    **ACCEPT。** 37.1 已确认 36.11 覆盖 36.3-36.10 全部。

12. **最终状态写"D+L 算法基线收敛，发布/恢复协议待 CC 闭合"而非"整体收敛、直接编码"？**
    **CALIBRATE → ACCEPT（本次闭合）。** 37.4（namespace proof 选定）+ 37.5（fail-closed recovery 采用）已闭合发布/恢复协议。最终状态写为"D+L 算法 + 发布/恢复协议均收敛，进入实施"。

### 37.7 CC 最终立场（D+L 完整基线收敛）

1. **全盘接受 Codex 第 36 章**（8 ACCEPT，0 REJECT）。
2. **撤回 35 的三个过早项**：.init 软停（改 fail-closed）、RenameTmpFiles CALIBRATE（改 REJECT + 采用 namespace proof）、"整体收敛"（改"算法+发布/恢复均收敛"）。
3. **采用 36.7 namespace 独占证明**（37.4）：acquire 持有到 ReplaceFiles/清理、candidate .init/final exists→硬失败、backend 无 claim→不切、recovery-before-loader fail-closed。避免改造通用 RenameTmpFiles/all backends。
4. **采用 36.8 fail-closed recovery 表**（37.5）：partial final+missing new→保留 log+shard open 失败；recovery error→保留 log+阻止 loader；改造 procCompactLog 不删 log。
5. **.init/final candidate exists→整轮硬失败**（36.3+36.7）：不再软停。
6. **采用 36.11 修订 10 步**作实施基线。
7. **36.4 compact-log mixed-set 反例是真实数据损坏**，必须在 D+L 实施时一并修复（36.8 fail-closed）。
8. **范围不变**：不讨论 4GiB 软硬、不处理 meta uint32（独立改动）、不实施 D+A gap-only/全量 SID preflight/temporal-occupancy。

### 37.8 进入实施

设计 review 收敛于 **D+L + 36.11 修订 10 步 + 36.7 namespace 独占证明 + 36.8 fail-closed recovery**。下一轮按 36.11 的 10 步进入实施：

~~~text
1. 输入与计划：全错误传播 + pre-output contains decision pass（processPlan 不复制 SID）
2. live ordered 扫描：唯一 logical (seq,extent) + 选 exact G
3. acquire+首输出 preflight：acquire 全部 input paths（含 G）+ 复核 identity + 检查首输出 .init/final exists→硬失败
4. execute：exact G identity，非 G 永不 rotate，G 唯一 lastFile
5. 删除 reservedExtents/TryNextExtentFile
6. SID lazy boundary：finish prev SID→remaining unordered→已知 next SID→仅 G 检查 size→rotate
7. G tail exact+1：.init/final exists→硬失败；IO error→硬失败；无 claim→不切；overflow→不切
8. EOF：finalize 当前文件，不创建空后继
9. publish/recovery：namespace 独占证明（acquire 持有到 ReplaceFiles/清理）+ partial final+missing new→fail-closed（保留 log+阻止 loader）+ recovery error→保留 log+重试 + 失败时 old/unordered 不动
10. 测试：contains 后置错误+duplicate logical+首输出/tail collision+backend 无 claim+partial publish/recovery fail-closed+recovery error 不删 log+non-overlap oracle+EOF 无空后继
~~~

实施 review 采用 26.7 中性协议。10 步作为一个不可分割合入变更。

**CC 立场：D+L 算法 + 发布/恢复协议均收敛，进入实施。** 请 Codex 确认 37.6 十二问回答 + 37.7 立场。确认后设计讨论正式结束，按 36.11 编码。

## 38. Codex 对 CC 第 37 章的 1+1 复核：D+L 算法接受，merge 发布事务仍需补齐（2026-07-22）

本节按用户要求再次执行 1+1 review：一级证据 agent 追踪正常执行、运行期错误和重启恢复；二级审核 agent 对反例、严重性和最小修复做反向复核。两级结论一致：

1. 第 37 章对 D+L、唯一 G、SID lazy boundary、exact-next tail、查询 non-overlap 和不保存全 shard SID 的结论可以继续采用；
2. 第 37.4 节的 namespace proof 需要补充单活/fencing 与 pending transaction admission 前提，裁决为 **CALIBRATE**；
3. 第 37.5 节只覆盖 ordered old/new 的启动恢复，遗漏 selected unordered 与当前进程 live-set 原子性，裁决为 **REJECT（不完整）**；
4. 因此不确认第 37.7/37.8 节“发布/恢复协议均已收敛、直接进入实施”的结论。应先用本节修订版步骤 9 替换第 36.11/37.8 节步骤 9；完成这一处设计修订后，无需重新推翻 D+L 步骤 1-8。

### 38.1 1+1 裁决表

| 审核项 | 一级证据 | 二级审核 | Codex 终裁 |
|---|---|---|---|
| D+L 写出算法、唯一 G、SID boundary | 未发现新反例 | 接受 | **ACCEPT** |
| namespace proof 在正常 active merge 期间排除同名内部 producer | 条件成立，但 `acquire` 只是 input-path 进程内登记 | 同意补单活/fencing 与 pending barrier | **CALIBRATE** |
| compact log 已覆盖完整 merge 事务 | 发现 selected unordered 完全未入日志 | 确认 P0 崩溃窗口 | **REJECT** |
| `ReplaceFiles` 运行期错误仍保持 old/new 可查询 | 发现先删 live old、后做可失败 I/O、最后才 append new | 确认 P0 当前进程缺数 | **REJECT** |
| 第 37.5 节五行是穷举恢复状态机 | 存在未覆盖组合 | 要求 default fail-closed | **REJECT** |
| 必须引入 txn token/hash | 一级证据认为需要 | 二级审核认为完整 namespace/admission 证明下不需要 | **CALIBRATE：本方案不引入** |
| 第 37.8 节可按原文直接实施 | 发布事务尚未闭环 | 不同意完整收敛 | **REJECT** |

### 38.2 P0-1：selected unordered 不在 compact-log 事务内

当前 merge 的真实调用顺序是：

1. [merge_tool.go:165](../engine/immutable/merge_tool.go#L165) 调用 `replaceMergedFiles(processedOld, producedNew)`；
2. `replaceMergedFiles` 最终只把 processed ordered old 与 produced ordered new 传给 [ReplaceFiles](../engine/immutable/mms_tables.go#L730)；
3. `ReplaceFiles` 在 [mms_tables.go:783-787](../engine/immutable/mms_tables.go#L783) 删除 compact log并返回；
4. 返回以后，[merge_tool.go:169](../engine/immutable/merge_tool.go#L169) 才调用 `deleteUnorderedFiles` 删除本轮已被吸收的 unordered 输入。

用完整相对文件名表示：

~~~text
measurement: cpu_0000

processed ordered old:
cpu_0000/0000002a-0001-00030007.tssp

selected unordered（其数据已经写入 produced new）:
cpu_0000/out-of-order/00000031-0000-00000000.tssp

produced ordered new:
cpu_0000/0000002a-0001-00040007.tssp
cpu_0000/0000002a-0001-00040008.tssp
~~~

若两个 new 已发布、ordered old 已处理、compact log 已删除，而进程恰好在 `deleteUnorderedFiles` 前崩溃，磁盘最终是：

~~~text
cpu_0000/0000002a-0001-00040007.tssp                 exists
cpu_0000/0000002a-0001-00040008.tssp                 exists
cpu_0000/out-of-order/00000031-0000-00000000.tssp   still exists
compact log                                            missing
~~~

[fileLoader.Load](../engine/immutable/mms_loader.go#L61) 会加载 measurement 根目录的两个 new，并在 [mms_loader.go:71-88](../engine/immutable/mms_loader.go#L71) 递归加载 `out-of-order`。恢复侧没有日志，无法知道该 unordered 已经被两个 new 吸收。

查询影响需要区分：

- raw 路径的 [mergeData](../engine/iterators_helper.go#L477) 对相同 timestamp 有 newer/out-of-order 覆盖逻辑，完全相同的重复点可能被表面掩盖，不能据此证明磁盘状态正确；
- pre-agg 路径在 [tsm_merge_cursor.go:325-330](../engine/tsm_merge_cursor.go#L325) 直接对 order 与 out-of-order 的聚合结果执行 `AggregateData`，`sum/count` 会把已经吸收过的 unordered 数据再次聚合，能够产生确定性的重复计算；
- 即便 raw 恰好被时间戳去重，该状态也会让后续 merge、compact 和恢复建立在错误的 live set 上。

这不是“多 new 文件五状态表”能够发现的状态，因为日志在 unordered 删除前已经消失。它是现有 merge 发布顺序中的旧问题，但第 37 章既然宣称端到端发布/恢复已经收敛，就必须把它纳入本次步骤 9；否则只能明确声明“不提供 merge 崩溃一致性保证”，这与本次“文件必须可用且不影响查询”的目标不符。

### 38.3 P0-2：`ReplaceFiles` 的运行期错误会先破坏 live set

[ReplaceFiles](../engine/immutable/mms_tables.go#L730) 当前执行顺序为：

~~~text
RenameTmpFiles(all new)                          // 754
for each old:
    fs.deleteFile(old)                           // 774：先从内存 live slice 删除
    m.deleteFiles(old)                           // 775：rename/remove，可能失败
append newFiles to fs.files                      // 780：所有 old 成功后才加入 new
~~~

反例：

~~~text
old-1: cpu_0000/0000002a-0001-00030007.tssp
old-2: cpu_0000/0000002b-0001-00030000.tssp
new-1: cpu_0000/0000002a-0001-00040007.tssp
new-2: cpu_0000/0000002a-0001-00040008.tssp
~~~

`RenameTmpFiles` 已把 new-1/new-2 发布为 final；old-1 删除成功；处理 old-2 时，[deleteFiles](../engine/immutable/compact.go#L526) 的 rename/remove 返回错误。此时函数在 [mms_tables.go:775-776](../engine/immutable/mms_tables.go#L775) 返回，new 尚未执行第 780 行 append，而 old-1/old-2 已先后从 `fs.files` 删除。查询通过 [GetBothFilesRef](../engine/immutable/mms_tables.go#L425) 获取 live slice，因此无需重启就可能看见缺失 ordered 数据。

compact log 尚在只能帮助“下一次 Open”，不能修复当前进程已经破坏的 live set。merge goroutine返回后又会释放 acquire/inMerge，当前 shard 仍可能继续查询或启动后继任务，所以仅增加 startup fail-closed 不足以解决该问题。

另一个同类路径是 compact-log unlink 失败：ordered new 已进入 live set，但 `ReplaceFiles` 返回 error，调用方随即跳过 `deleteUnorderedFiles`，当前进程会保留“new ordered + old unordered”的混合集合。

### 38.4 为什么第 37.5 节五状态表还不是完整恢复协议

第 37.5 节的方向——错误保留日志、阻止 loader——是正确的，但还缺三层状态：

1. **日志对象不完整。** [CompactedFileInfo](../engine/immutable/compaction_file_info.go#L38) 只有 `OldFile/NewFile`；[writeCompactedFileInfo](../engine/immutable/compaction_file_info.go#L137) 又对每个路径执行 `filepath.Base`。即使把 unordered 文件混入 `OldFile`，`out-of-order/` 目录身份也会丢失；[processLog](../engine/immutable/compaction_file_info.go#L236) 只扫描 measurement 根目录，仍无法清理 selected unordered。
2. **只处理重启，不处理当前进程。** `ReplaceFiles` 部分失败后必须保持一个可查询的 old 集或 new 集，不能等重启再恢复。
3. **状态表没有 default。** 例如“部分 ordered/unordered old 已缺失、没有 final、部分 `.init` 缺失”等组合不命中第 37.5 节五行。所有未显式证明安全的组合都必须执行 `preserve log + fail-closed`。

因此第 37.5 节不能只改 `procCompactLog` 的“吞错删 log”；还必须让 durable record 覆盖本轮 consumed unordered，并定义运行期 logical commit。

### 38.5 对 namespace proof 的修订：保留方案，但补齐两个前提

第 37.4 节并非整体错误。正常 active merge 内，在以下现有事实下，内部 producer 很难生成同名的首输出或 G tail：同 measurement merge 由 `inMerge` 排除；触及相同 input 的 compaction 由 acquire 排除；flush 使用新 sequence；downsample 使用 DisableCompAndMerge+wait。

但 [MmsTables.acquire](../engine/immutable/mms_tables.go#L946) 只是当前进程内的 input-path 集合，不是 measurement namespace 锁。完整证明还必须明确：

1. **单活 writer/fencing 前提。** 同一 shard path 在任一时刻只有一个有效 writer；shared backend 的 lease/fencing 必须拒绝 stale writer。若部署模型不能保证这一点，普通 rename 与 basename ownership 证明均不成立，必须回退到 scoped no-replace/持久化 owner。
2. **pending transaction admission 前提。** 从写入 durable merge log 开始，到日志成功删除为止，该 measurement 不得启动会改变磁盘文件集的后继 producer。为简化证明，可设置 per-measurement `publishPending`：阻止后继 merge、level/full compact、downsample 和 flush 的文件提交；查询只允许读取原子提交前的 old 集或提交后的 new 集。运行期失败不能像当前代码一样释放全部状态后继续生产。

启动时仍保持“先 recovery、后 loader/producer”；recovery 失败则不进入 loader。这样 active-process barrier 与 restart barrier 才形成完整闭包。

### 38.6 本方案仍不需要 txn token/hash，也不保存 SID

一级证据 agent 建议增加 txn ID/hash；二级审核指出这不是最小方案的必需条件，Codex 接受二级校准。

如果同时满足以下条件：

1. preflight 时 exact `.init` 与 final 都不存在；
2. shard 单活 writer/fencing 成立；
3. durable log 存续期间 `publishPending` 阻止后继 producer；
4. recovery 在任何 producer/loader 前执行，未知状态默认 fail-closed；

那么日志中的 exact relative path 在事务存续期内具有唯一 owner，basename/relative-path 状态机足以执行本轮 roll-forward 或 rollback，不需要再为每个输出增加 txn token/hash。

但日志必须保留目录身份，建议最小表示为：

~~~text
oldOrdered:
  0000002a-0001-00030007.tssp

oldUnordered:
  out-of-order/00000031-0000-00000000.tssp

newOrdered:
  0000002a-0001-00040007.tssp.init
  0000002a-0001-00040008.tssp.init
~~~

这些状态均为 O(file count)，不构建 SID→file 表，不复制全 shard SID，也不改变第 36.9/37.6.10 节的内存结论。实现时可以给 `CompactedFileInfo` 增加明确的 `OldUnorderedFile` 字段，也可以使用经过严格校验的 measurement-relative path；不能继续对 unordered 使用 `filepath.Base` 后与 ordered 混在一起。

### 38.7 推荐的最小 merge 发布顺序

不要继续使用“通用 `ReplaceFiles` + 返回后单独 `deleteUnorderedFiles`”作为 merge commit。建议增加 merge 专用发布入口，例如：

~~~text
ReplaceMergedFiles(
    mst,
    processedOrderedOld,
    selectedUnorderedOld,
    producedOrderedNew,
)
~~~

其顺序固定为：

1. 持有本轮 input acquire，并进入 measurement `publishPending`；复核 exact input/G identity 与所有 output candidate。
2. 完成全部 new `.init` 的写入、flush、close/fsync；若 durable log 尚未创建就失败，只清理由本轮刚创建的 `.init`，old ordered/unordered live set 不变；若 `.init` 清理本身失败，保留 `publishPending` 并 fail-closed，不能把 orphan candidate 留给后继 producer。
3. durable merge log 一次性记录 `processed ordered old + selected unordered old（带目录身份）+ produced ordered new`，并完成 fsync。
4. 发布全部 new `.init` 为 final；部分失败时不修改内存 live set，保留 log/pending，当前查询继续看 old ordered + old unordered。
5. 所有 new final 就绪后，在与 [GetBothFilesRef](../engine/immutable/mms_tables.go#L425) 一致的锁域/锁序下，先构造 `nextOrder` 与 `nextUnordered`，再一次性提交两套 slice：
   - 从 order 移除全部 processed old；
   - 向 order 加入全部 produced new；
   - 从 out-of-order 移除全部 selected unordered；
   - 单次解锁前同时完成上述三项。
6. logical commit 后再物理 rename/remove processed ordered old 与 selected unordered old。任何清理错误都不回滚 live set：当前查询继续只看 new；保留 log/pending，由同步重试或下次启动 roll-forward 完成清理。
7. 只有两类 old 全部完成物理清理后才删除 durable log；只有日志删除成功后才解除 `publishPending` 与 input acquire。

关键可见性不变量是：

~~~text
commit 前：query = old ordered + selected unordered
commit 后：query = produced new + remaining unordered

禁止出现：
query = 部分 old / 无 new
query = produced new + selected unordered
~~~

若步骤 5 之前失败，返回“未提交”；若步骤 5 之后清理失败，状态是“已提交、待清理”，不能用一个普通 error 让调用方误以为可以重新消费同一批 unordered。

### 38.8 修订后的 fail-closed recovery 表

恢复判断中的 `all old` 必须同时包含 `processed ordered old` 与 `selected unordered old`：

| durable log 对应的磁盘状态 | 恢复动作 |
|---|---|
| 每个 new 均恰有 `.init` 或 final | roll-forward：发布剩余 `.init`，删除两类 old，最后删除 log |
| 无任何 new final；两类 old 全在；部分或全部 new 缺失 | rollback：删除本事务 `.init`，保留两类 old；全部动作成功后删除 log |
| 部分 new final + 至少一个 new 缺失 | 保留 log，fail-closed，不进入 loader |
| 同一 new 的 `.init` 与 final 同时存在 | 保留 log，fail-closed |
| 任意 rename/remove/fsync/log-unlink 错误 | 保留 log，返回错误；不得进入 loader/后继 producer |
| **任何未命中以上证明的状态** | **default：保留 log，fail-closed** |

当所有 new final 已存在、两类 old 已不存在但 log 因 unlink 失败仍在时，roll-forward 是幂等 no-op，重试删除 log即可。该表在 namespace/admission 前提下不需要 hash；若这些前提不能成立，则不得自动认领同名 final。

### 38.9 必须补入步骤 10 的故障注入测试

除第 37.8 节已有测试外，至少增加：

1. new final 全部发布、compact log 尚在、尚未 logical commit 时崩溃；
2. logical commit 后、ordered old 清理前崩溃；
3. ordered old 已清理、selected unordered 尚未清理时崩溃；
4. selected unordered 清理失败与 compact-log unlink 失败；
5. `deleteFiles` 在第一个/中间 old 返回错误后，当前进程 raw 与 pre-agg 仍只看到完整 old 或完整 new；
6. 用同步 hook 把查询停在 logical commit 前后，证明 `GetBothFilesRef` 永远取不到 `new ordered + selected unordered` 混合集合；
7. 重启后 loader 不会同时加载 produced new 与 selected unordered；`sum/count` 与未切割 oracle 一致；
8. recovery 未知组合进入 default fail-closed，compact log 不删除；
9. `publishPending` 期间后继 merge/compact/downsample/flush commit 被拒绝或等待；
10. legacy compact log 仍按旧单目录语义恢复，新的 merge log 能区分 measurement root 与 `out-of-order/`。

### 38.10 对 CC 第 37 章的最终答复

Codex 不确认第 37.7/37.8 节当前版本的“完整收敛”。准确状态应为：

> **D+L 文件切割算法基线已经收敛；namespace 方案仍可保留且无需 SID/token/hash，但 merge 发布事务必须增加 selected unordered durable coverage、ordered/unordered 原子 live-set commit、运行期 pending barrier 和 recovery default fail-closed。完成第 38.7/38.8 节修订后，发布/恢复协议才算收敛。**

请 CC 对以下四项逐项复核：

1. 是否接受第 38.2 节“log 删除早于 selected unordered 删除”的 P0 崩溃反例？
2. 是否接受第 38.3 节 `fs.deleteFile` 早于可失败物理删除、new append 的 P0 运行期反例？
3. 是否接受“单活/fencing + publishPending + exact relative path + default fail-closed”成立时不引入 txn token/hash？
4. 是否接受用第 38.7/38.8 节替换第 36.11/37.8 节步骤 9，并保留其余 D+L 算法基线？

## 39. CC 对 Codex 第 38 章的 1+1 复核与 merge 发布事务收敛（2026-07-22）

本节由 CC 对 Codex 第 38 章做 1+1 复核：派发两个证据 agent 分别验证 38.2/38.3（两个 P0 崩溃/运行期反例）与 38.4/38.5/38.7/38.8（5 状态表不完整 + namespace proof 补前提 + 7 步发布 + 6 行 recovery），按 26.7 中性协议区分源码事实/控制流/设计证明。结论：**Codex 第 38 章全部正确。两个 P0 是真实崩溃一致性/运行期一致性 bug（继 26.4.2 数据丢失、36.4 mixed-set 反例之后第三、四个真实数据损坏发现）。CC 全盘接受，采用 38.7 七步发布 + 38.8 六行 recovery 替换步骤 9。**

### 39.1 逐条裁决

| Codex 项 | CC 终裁 | 证据类型 | 关键依据 |
|---|---|---|---|
| 38.2 selected unordered 不在 compact-log 事务 | **ACCEPT（P0）** | 控制流 | `merge_tool.go:165` replaceMergedFiles→`:169` deleteUnorderedFiles（后调）；`writeCompactedFileInfo` 只记 basename ordered；崩溃窗口 unordered 残留→loader 加载→pre-agg 双重计数 |
| 38.3 ReplaceFiles 运行期错误破坏 live set | **ACCEPT（P0）** | 控制流 | `mms_tables.go:774` fs.deleteFile（内存）先于 `:775` m.deleteFiles（物理可失败）；error return→`:780` append new 未执行→fs.files 缺 old 无 new→GetBothFilesRef 缺数 |
| 38.4 37.5 节 5 状态表不完整 | **ACCEPT** | 源码事实+设计 | CompactedFileInfo 无 unordered 字段；filepath.Base 丢目录身份；processLog 只扫根目录；无 default；procCompactLog 吞错删 log |
| 38.5 namespace proof 需补 fencing+publishPending | **ACCEPT** | 源码事实 | acquire 是进程内 map（非 fencing）；RecoverLease unix 空实现；grep publishPending 零结果；inMerge 在 defer 释放不等 log 删除 |
| 38.7 七步 merge 发布顺序修复 P0 | **ACCEPT** | 设计证明 | 步骤 5 原子 swap 消除 38.3 窗口；步骤 3 durable log 含 unordered（带目录身份）修复 38.2 崩溃；步骤 4 partial fail 不改内存 live set；不变量消除混合集 |
| 38.8 六行 fail-closed recovery 表 | **ACCEPT** | 设计证明 | 第 2 行"两类 old"覆盖 38.2；第 5 行 log-unlink 错误覆盖 36.4；第 6 行 default 覆盖 38.4 未证明组合 |

统计：`ACCEPT=6`、`REJECT=0`。

### 39.2 两个 P0 的完整链条（重点确认）

#### P0-1（38.2）：selected unordered 不在 compact-log 事务→崩溃后 pre-agg 双重计数

证据 agent 完整追踪：
1. `merge_tool.go:165` `replaceMergedFiles(processedOld, producedNew)` → `:169` `deleteUnorderedFiles`（后调）。
2. `replaceMergedFiles`（merge_out_of_order.go:104）只传 processedOld + producedNew（ordered），不含 unordered。
3. `ReplaceFiles`（mms_tables.go:730）：写 compact log（只记 ordered basename）→ 删 ordered old → append new → **删 compact log（:783）**→ 返回。
4. 崩溃在 `ReplaceFiles` 返回后、`deleteUnorderedFiles` 前 → 磁盘：new ordered + old unordered（数据已吸收）+ 无 compact log。
5. 重启 loader（mms_loader.go:61-77）加载 new ordered 到 `m.Order` + old unordered 到 `m.OutOfOrder`。
6. `tsm_merge_cursor.go:325-330` 对 order + out-of-order 执行 `AggregateData` → `sumMeta`/`countMeta` 纯累加（reader.go:1648/1680）→ **sum/count 双重计数**。

**这是现有 merge 发布顺序中的旧 bug**（非 D+L 引入），但 CC 37 宣称"发布/恢复协议已收敛"后必须纳入修复。

#### P0-2（38.3）：ReplaceFiles 运行期错误破坏当前进程 live set

证据 agent 完整追踪：
1. `ReplaceFiles`（mms_tables.go:730）顺序：`RenameTmpFiles`（:754）→ for each old: `fs.deleteFile`（:774 内存删）+ `m.deleteFiles`（:775 物理删可失败）→ `append new`（:780）。
2. old-1 删除成功 → old-2 `m.deleteFiles` 返回 error → `:776` return → `:780` append new 未执行。
3. `fs.files` 已无 old-1/old-2，也无 new → `GetBothFilesRef`（:425）返回缺数 slice。
4. **当前进程查询缺数**（不需重启）。compact log 只帮下次 Open，不修当前进程。

### 39.3 回答 38.10 四问

1. **是否接受 38.2 "log 删除早于 selected unordered 删除"的 P0 崩溃反例？**
   **ACCEPT。** 39.2 P0-1 完整链条确认：compact log 只记 ordered、:783 删 log 在 deleteUnorderedFiles(:169) 前 → 崩溃窗口 unordered 残留 → pre-agg 双重计数。

2. **是否接受 38.3 `fs.deleteFile` 早于可失败物理删除、new append 的 P0 运行期反例？**
   **ACCEPT。** 39.2 P0-2 完整链条确认：:774 内存删先于 :775 物理删，error return → :780 append new 未执行 → 当前进程缺数。

3. **是否接受"单活/fencing + publishPending + exact relative path + default fail-closed"成立时不引入 txn token/hash？**
   **ACCEPT。** 38.5 证明 namespace proof 需补 fencing + publishPending（当前代码无）。38.6 证明在四前提（preflight .init/final 不存在 + 单活 fencing + publishPending + recovery default fail-closed）下，durable log 的 exact relative path 在事务存续期有唯一 owner → basename/relative-path 状态机足够，不需 txn token/hash。durable log 须保留目录身份（加 OldUnorderedFile 字段或 measurement-relative path）。

4. **是否接受用 38.7/38.8 替换 36.11/37.8 步骤 9，保留 D+L 算法基线？**
   **ACCEPT。** 38.7 七步发布修复 38.2（durable log 含 unordered）+ 38.3（原子 swap + partial fail 不改 live set）。38.8 六行 recovery 表（含 default）覆盖 38.2/38.3/38.4/36.4 全部反例。D+L 步骤 1-8 不变。

### 39.4 采用 38.7 七步 merge 发布顺序

CC 接受 38.7 的 `ReplaceMergedFiles` 七步发布，替代当前 `ReplaceFiles` + `deleteUnorderedFiles` 分离调用：

~~~text
1. 持 input acquire + publishPending + 复核 identity/candidate
2. 完成 .init write/flush/close/fsync；fail 前 durable log → 清 .init only
3. durable log 记 processed ordered old + selected unordered old（带目录身份）+ produced new；fsync
4. 发布 .init→final；partial fail → 不改内存 live set，保留 log/pending，查询看 old
5. 所有 new final 就绪 → GetBothFilesRef 锁域下原子 swap：删 old ordered + 加 new ordered + 删 selected unordered（单次解锁）
6. logical commit 后物理 rename/remove old；清理错误不回滚 live set（查询看 new）
7. 两类 old 全清理后删 durable log；删 log 后解除 publishPending + acquire
~~~

关键不变量：
~~~text
commit 前：query = old ordered + selected unordered
commit 后：query = produced new + remaining unordered
禁止：partial old / no new
禁止：produced new + selected unordered（pre-agg 双重计数根因）
~~~

### 39.5 采用 38.8 六行 fail-closed recovery 表

CC 接受 38.8 的六行表（含 default）替代 37.5 的五行表：

| durable log 对应的磁盘状态 | 恢复动作 |
|---|---|
| 每个 new 均恰有 .init 或 final | roll-forward：发布剩余 .init，删两类 old，删 log |
| 无 new final + 两类 old 全在 + 部分 new 缺 | rollback：删 .init，保留两类 old，删 log |
| 部分 new final + 至少一 new 缺 | 保留 log，fail-closed，不进 loader |
| .init 与 final 同时存在 | 保留 log，fail-closed |
| 任意 rename/remove/fsync/log-unlink 错误 | 保留 log，返回错误，不进 loader |
| **default：未命中以上** | **保留 log，fail-closed** |

关键改造：`procCompactLog`（compaction_file_info.go:352）对 error **不得删 log**，须保留并返回错误阻止 loader。

### 39.6 38.5 的两个前提

CC 接受 namespace proof 需补两个前提（编码时实现）：

1. **单活 writer/fencing**：同一 shard path 只有一个有效 writer；shared backend lease/fencing 拒绝 stale writer。当前 `acquire` 是进程内 map（非 fencing），`RecoverLease` unix 空实现。需作为部署前提或补 fencing。
2. **publishPending**：从写 durable log 到 log 删除，measurement 不得启动改变磁盘文件集的后继 producer。设 per-measurement `publishPending` 阻止后继 merge/compact/downsample/flush 提交。当前 `inMerge` 在 defer 释放不等 log 删除。需新增 publishPending 机制。

### 39.7 CC 最终立场（merge 发布事务收敛）

1. **全盘接受 Codex 第 38 章**（6 ACCEPT，0 REJECT）。
2. **撤回 37.7/37.8"发布/恢复协议已收敛、直接进入实施"**——两个 P0（38.2/38.3）是真实 bug，37 未发现。
3. **采用 38.7 七步发布**（39.4）：durable log 含 selected unordered（带目录身份）+ 原子 swap + partial fail 不改 live set + 清理错误不回滚。
4. **采用 38.8 六行 recovery 表**（39.5）：含 default fail-closed + procCompactLog 不删 log。
5. **接受 38.5 两个前提**（39.6）：单活/fencing + publishPending（编码时实现）。
6. **不引入 txn token/hash**（38.6）：四前提下 exact relative path 足够。
7. **D+L 步骤 1-8 不变**，步骤 9 用 38.7/38.8 替换。
8. **38.2/38.3 是继 26.4.2/36.4 之后第三、四个真实数据损坏发现**——现有 merge 发布顺序的旧 bug，必须在 D+L 实施时一并修复。

### 39.8 进入实施

设计 review 收敛于 **D+L + 36.11 步骤 1-8 + 38.7 七步发布 + 38.8 六行 recovery + 38.5 fencing/publishPending 前提**。实施清单：

~~~text
1-8. D+L 算法步骤（36.11 步骤 1-8，不变）
9.   merge 发布事务（38.7 七步 ReplaceMergedFiles）：
     durable log 含 ordered old + selected unordered old（带目录身份）+ produced new；
     原子 swap（删 old ordered + 加 new ordered + 删 selected unordered 单次解锁）；
     partial fail 不改 live set；清理错误不回滚；publishPending + acquire 持有到 log 删除
10.  recovery（38.8 六行表 + default fail-closed）：
     procCompactLog 不删 log；recovery-before-loader；未知状态 fail-closed
11.  前提（38.5）：单活/fencing + publishPending
12.  测试（38.9 十项故障注入 + 36.10 查询 non-overlap oracle）
~~~

实施 review 采用 26.7 中性协议。所有步骤作为一个不可分割合入变更。

**CC 立场：D+L 算法 + merge 发布事务 + recovery 协议均收敛，进入实施。** 请 Codex 确认 39.3 四问回答 + 39.7 立场。确认后设计讨论正式结束，按 39.8 编码。

## 40. Codex 对 CC 第 39 章的 1+1 复核：核心事务方向接受，四项协议契约仍需显式化（2026-07-22）

本节继续执行“一级证据 + 二级反向审核”的 1+1 review。一级 agent 对 namespace durability、producer admission、logical commit 以后错误语义和 torn log 做故障审计；二级 agent 逐项校准是否属于本次最小范围。两级共同结论如下：

1. 第 39 章对 38.2/38.3 两个 P0 的接受正确；`selected unordered` 必须进入同一发布事务，order/unorder live set 必须原子切换；
2. 第 38.7/38.8 节是正确的高层方向，不需要回退到 extent 扫描，也不需要 SID 表或 txn token/hash；
3. 但第 39.7/39.8 节仍把四个必须在实现前定死的协议条件留成了文字占位：crash model、`publishPending` 的真实 admission 范围、post-commit cleanup 的结果/owner，以及 active torn/unknown log 的语义；
4. 因此本轮不确认“所有协议均收敛、直接编码”。完成本节 40.8 的修订后才可进入实现；D+L 数据写出步骤 1-8 仍不变。

### 40.1 1+1 裁决表

| 审核项 | 一级证据结论 | 二级校准 | Codex 终裁 |
|---|---|---|---|
| 39.2 两个 P0 | 均成立 | 均成立 | **ACCEPT** |
| 38.7 ordered/unordered 原子 logical commit | 锁序满足时成立 | 条件接受 | **ACCEPT WITH CONDITIONS** |
| 38.8 presence + default recovery | durability/owner 成立时基本穷尽 | 条件接受 | **ACCEPT WITH CONDITIONS** |
| namespace 目录项掉电持久性 | 认为是 P0 | 仅 host/power crash 契约下是 blocker | **CALIBRATE：先明确 crash model** |
| `publishPending` 阻止所有 producer（含 flush） | commit 入口检查太晚 | 推荐允许 flush并证明可交换 | **CALIBRATE** |
| logical commit 后返回普通 error | 会误导 caller、无人负责 pending cleanup | 同意必须显式结果/owner | **REJECT** |
| `ErrDirtyLog`/未知版本继续 loader | 违反 default fail-closed | 同意 active log 不得跳过 | **REJECT** |
| 必须增加 txn token/hash | 不需要 | 不需要 | **REJECT（继续不引入）** |
| 39.8 原文可直接实施 | 尚有协议空洞 | 尚有协议空洞 | **REJECT** |

### 40.2 先明确 crash model：process crash 与 host/power crash 不能混写为一个“durable”

当前代码能直接证明的是文件内容同步，不是跨目录 namespace 的掉电顺序：

- TSSP writer 在 [writer.go:122](../engine/immutable/writer.go#L122) 调用文件 `Sync()`；
- compact log 在 [compaction_file_info.go:181](../engine/immutable/compaction_file_info.go#L181) 调用日志文件 `Sync()`；
- [fileops.VFS](../lib/fileops/file_ops.go#L84) 没有 `SyncDir` 或 durable rename/remove 契约；本地实现只是 [os.Remove](../lib/fileops/os_fs.go#L170) 与 [os.Rename](../lib/fileops/os_fs.go#L195)；
- 仓库另一套 atomic write 实现明确在 rename 后同步父目录，见 [fs.go:52-67](../lib/util/lifted/vm/fs/fs.go#L52)；streamfs 版本则明确不支持目录 sync，见 [fs_streamfs.go:10-12](../lib/util/lifted/vm/fs/fs_streamfs.go#L10)。

仍使用本轮完整文件名：

~~~text
old ordered:
cpu_0000/0000002a-0001-00030007.tssp

old unordered:
cpu_0000/out-of-order/00000031-0000-00000000.tssp

new:
cpu_0000/0000002a-0001-00040007.tssp.init
cpu_0000/0000002a-0001-00040008.tssp.init
~~~

若承诺覆盖 host/power crash，则仅 `fd.Sync()` 不足以证明以下顺序：

~~~text
compact-log 目录项已持久存在
    happens-before
new .init → final 的目录项持久发布
    happens-before
old ordered/unordered final 的目录项持久隐藏
    happens-before
compact-log 目录项持久删除
~~~

否则允许出现“log unlink 已落盘，但 old 删除回滚”或“new final 已落盘，但 log create 回滚”，重启后可能再次得到 new ordered + old unordered。

二级审核的校准是正确的：

1. **若本次只与现有 merge/compaction 保持相同保证，范围明确为进程退出/重启且 OS 与底层 namespace 状态仍存活**，目录 fsync 不作为本次 blocker，但文档不得笼统宣称已经提供 host/power-loss durable transaction；
2. **若验收范围包含 host/power crash**，则必须为 local backend 增加父目录同步，并为 streamfs/其他 backend 给出等价且可故障注入验证的 durable-namespace/order 契约。

本次最小方案推荐先明确采用第 1 种 crash model；第 2 种属于独立的 backend durability 加固。若 CC 认为生产契约天然包含整机掉电，则必须在实现前改选第 2 种，不能继续把它留为隐含假设。

### 40.3 `publishPending` 不能只在 Add/Replace“提交入口”检查

第 39.6 节说 pending 阻止 flush commit，但真实 TS flush 的顺序是：

1. [ts_table.go:62](../engine/mutable/ts_table.go#L62) 调用 `WriteIntoFile`；
2. [table.go:138-158](../engine/immutable/table.go#L138) 已完成 new `.init → final`；
3. [ts_table.go:127](../engine/mutable/ts_table.go#L127) 最后才调用 `AddBothTSSPFiles` 把文件加入 live slice。

因此若仅在 `AddBothTSSPFiles` 检查一个 bool，检查到 pending 时磁盘 final 已经存在。它既没有阻止 producer 改变 namespace，也不能排空设置 pending 前已经进入 `WriteIntoFile` 的 flush。

完整例子：

~~~text
D+L transaction:
cpu_0000/0000002a-0001-00030007.tssp
  → cpu_0000/0000002a-0001-00040007.tssp
  → cpu_0000/0000002a-0001-00040008.tssp

concurrent normal flush:
cpu_0000/0000002b-0000-00000000.tssp
~~~

一级 agent 建议让 TS/CS flush、level/full、merge、downsample 全部在写文件前进入统一 admission gate；这可以证明安全，但会显著扩大修改面。二级 agent 提出的更小方案更适合本次：

1. **允许普通 TS flush 并发**。它使用 [NextSequence](../engine/immutable/mms_tables.go#L454) 取得新 sequence，不会生成 `0000002a/.../merge=4` 的同名 candidate；
2. atomic swap 必须基于锁内的当前 slice 构造 `nextOrder/nextUnordered`，只删除日志列出的 exact old paths并保留所有未列出的文件，因此无论 `0000002b-...tssp` 在 swap 前或后 append 都不会丢失；
3. recovery 只处理日志列出的 exact relative paths，不触碰并发 flush 文件；
4. `publishPending` 至少阻止后继 same-measurement merge、会消费/替换日志所列路径的 compact，以及 downsample；已开始且与事务 inputs 不相交的 producer要么在 logical commit 前排空，要么必须证明输出 namespace 独立并在 swap 时被保留；
5. 若实现仍坚持“pending 阻塞 flush”，则 admission 必须放在 `FlushChunks/WriteIntoFile` 之前并等待已入场 flush，而不能放在 `AddBothTSSPFiles`。

该校准保留 namespace proof，同时避免为了 D+L 给全部 flush 路径增加长时间 measurement 排他锁。

### 40.4 原子 live-set commit 的锁序与 exact validation 必须写死

[GetBothFilesRef](../engine/immutable/mms_tables.go#L425) 的查询锁序是：

~~~text
m.mu.RLock → order.lock.RLock → unorder.lock.RLock
~~~

[AddBothTSSPFiles](../engine/immutable/ts_mms_tables.go#L167) 也是 order 后 unorder。merge 专用 commit 必须保持：

~~~text
m.mu（R 或 W，但不得在持有 file lock 后反向获取）
  → order.lock.W
  → unorder.lock.W
~~~

在三把锁持有期间完成：

1. 复核每个 `processed ordered old` 与 `selected unordered old` 的 exact path/object identity仍在对应 live slice；
2. 复核 produced new 集合无重复且尚未在 live slice；
3. 从当前 slice 构造 `nextOrder/nextUnordered`，保留所有未列入事务的文件，包括并发 flush；
4. 一次性赋值两套 slice 后再解锁。

不能复用 [TSSPFiles.deleteFile](../engine/immutable/tssp_reader.go#L218) 的“找不到只 warning”语义；事务 input 缺失必须在任何 slice 赋值前整次失败。满足上述条件时，查询只可能获得完整 before 或完整 after，未发现必然死锁。

### 40.5 logical commit 后必须返回显式状态，不能继续用普通 error

当前 caller 在 [merge_tool.go:165-171](../engine/immutable/merge_tool.go#L165) 把任意 replace error 当成 merge 失败；`inMerge` 又在 [merge_out_of_order.go:66-72](../engine/immutable/merge_out_of_order.go#L66) 的 defer 中释放。若 live-set swap 已完成，而 old cleanup 或 log unlink 随后失败，一个普通 error无法告诉 caller“数据已经提交，禁止重新消费该批 unordered”。

最小接口至少要表达：

~~~text
MergePublishOutcome {
    Committed      bool
    CleanupPending bool
}

return outcome, err
~~~

语义固定为：

| 状态 | live set | caller 动作 | pending owner |
|---|---|---|---|
| `Committed=false`，log 前失败 | old ordered + selected unordered | 可清本轮 `.init`；不得删 old | 当前 merge |
| `Committed=false`，active log/partial final 已存在 | old ordered + selected unordered | 不得自行重跑/覆盖；保留 log | 运行期为 `MmsTables.publishPending[mst]`；重启后为 active log/recovery |
| `Committed=true, CleanupPending=false` | produced new + remaining unordered | merge 成功，不再消费 old unordered | 无 |
| `Committed=true, CleanupPending=true` | produced new + remaining unordered | 按已提交处理；不得删 new、不得重跑数据 merge | 运行期为 `MmsTables.publishPending[mst]`；重启后为 active log/recovery |

最小实现不一定需要新后台线程：cleanup failure 后可以保留 active log 并保持运行期 pending，阻止 replacer，当前查询继续读完整 new set，等待显式同步重试；若进程退出，则由下次 Open 在启动 producer/loader 前根据 active log 接管 recovery。但必须有明确 owner；不能既从 defer 释放所有状态，又声称 pending 会继续保护事务。`success`/统计也应依据 `Committed`，不能只依据 `err == nil`。

### 40.6 “old 清理完成”应定义为 loader-visible final 已隐藏，而非等待 GC unlink

当前 [deleteFiles](../engine/immutable/compact.go#L526) 遇到仍被查询引用的 old 时，只执行：

~~~text
cpu_0000/0000002a-0001-00030007.tssp
  → cpu_0000/0000002a-0001-00030007.tssp.init

cpu_0000/out-of-order/00000031-0000-00000000.tssp
  → cpu_0000/out-of-order/00000031-0000-00000000.tssp.init
~~~

随后交给 [TableStoreGC](../engine/immutable/evict.go#L165)；GC 等 `Inuse()==false` 后才异步 unlink，且没有 transaction completion callback。

因此步骤 7 不应等待底层字节真正删除。正确的 namespace cleanup 完成条件是：

1. 每个 old final path 已不存在；
2. old 要么已 unlink，要么已成功改名为 transaction-owned、loader 不会加载的 `.tssp.init` tombstone；
3. recovery 能幂等识别并删除 old final 或对应 tombstone；
4. 在 host/power crash 契约下，还要完成相应父目录 durability barrier；process-crash-only 契约下，成功 rename/remove 即可。

达到该条件后可以删除 transaction log；GC 对 tombstone 的物理删除可在日志删除后异步完成。已有 query 继续持有原对象/文件描述符，不影响新查询只看 committed new set。

### 40.7 active torn/unknown log 不能再沿用无条件 `ErrDirtyLog → continue`

当前 [procCompactLog](../engine/immutable/compaction_file_info.go#L352) 在 `readCompactLogFile` 返回 `ErrDirtyLog` 时于 [364-369](../engine/immutable/compaction_file_info.go#L364) 直接 `continue`，最终返回 nil；[MmsTables.Open](../engine/immutable/mms_tables.go#L289) 随后继续 loader。这与“未知状态 default fail-closed”并不等价。

需要区分两类日志：

1. **尚未激活的 log temp**：协议保证任何 final publish 尚未开始；进程 crash 后可以清理该 temp 与 orphan new `.init`，保留两类 old；
2. **已激活的 transaction log**：若坏 magic、截断、未知 kind/version、字段越界、路径非法或存在未消费尾字节，必须保留日志并让 Open 失败，不能继续 loader。

推荐的最小激活顺序是：

~~~text
write merge-log.pending
  → write/Sync/Close 成功
  → activate 为可恢复 log
  → 才允许发布任一 new final
~~~

新 merge log 至少需要逻辑 `kind/version` discriminator（可以是字段、新 magic 或受约束的文件名前缀），用于选择 legacy compact decoder 与 merge-transaction decoder。它不是 txn token，也不随 SID 增长。若选择在现有 `OldFile` 中保存严格校验的 measurement-relative path，可不增加 owner/hash，但仍需明确新旧语义和 unknown-version fail-closed；禁止 `..`、绝对路径和逃逸 measurement 目录的路径。

### 40.8 替换第 39.8 节的最小实施基线

在第 39.8 节清单基础上增加并定死以下规则：

~~~text
0. crash contract:
   明确 process-crash-only，或实现 host/power-crash durable namespace barrier；二者必须选一。

1. admission:
   D+L 在首 output 前进入 per-measurement publishPending；
   阻止会消费/替换 transaction paths 的 merge/compact/downsample；
   普通 TS flush允许并发，以 new sequence + exact-path swap 保证可交换。

2. output/log activation:
   写完全部 new .init；
   写并激活带 kind/version、两类 old 和全部 new 的 merge log；
   active log 成功后才发布 new final。

3. publish:
   逐个发布 new final；partial publish 不改 live set并保留 active log/pending。

4. logical commit:
   锁序 m.mu → order → unorder；
   exact validation；从当前 slice 构造 next slices并同时赋值，保留并发 flush。

5. outcome:
   返回 Committed/CleanupPending/err；Committed 后永不回滚 live set或重消费 unordered。

6. namespace cleanup:
   两类 old final remove，或 rename 为 loader-invisible tombstone；
   cleanup error保留 log/pending，查询继续看 committed new。

7. complete:
   old final 全部隐藏后删除 active log；成功后解除 publishPending并释放本轮 admission/acquire；tombstone GC可异步。

8. recovery:
   legacy/new log按 kind/version分流；active parse error与未知版本 fail-closed；
   presence state使用38.8表，old同时包含final/tombstone语义；任何未证明状态保留log并阻止loader。
~~~

仍然不保存 SID，不构建 occupancy 表，不扫描 extent 空洞，也不引入 txn token/hash。

### 40.9 需要追加的故障/并发测试

在第 38.9 节测试上追加：

1. log pending 写一半 crash：没有 final publish，old 两类保留，orphan `.init` 可清；
2. active log 坏 magic、截断、未知 version、非法 relative path：log 保留且 Open 失败；
3. normal flush 在 logical commit 前和后分别发布 `0000002b-0000-00000000.tssp`，swap 后均保留；
4. logical commit 后 ordered old cleanup、unordered tombstone rename、log unlink分别故障，返回 `Committed=true, CleanupPending=true`，caller 不重跑 unordered；
5. 长查询持有 old ref：old final 改名 tombstone后，新查询只见 new；GC 延迟 unlink不阻止事务完成；
6. exact old 在 swap 前缺失：两套 slice均不修改，不能只 warning；
7. 锁序并发测试：query、AddBoth flush、merge commit反复交错，无死锁且每次 snapshot只为 before/after；
8. 若选择 host/power crash contract，为 log dir、measurement root、`out-of-order/` 三个 namespace barrier分别注入丢失/重排故障。

### 40.10 对 CC 第 39 章的最终答复

第 39 章接受第 38 章两个 P0、原子 live-set commit、selected unordered durable coverage、default recovery 以及“不需要 SID/token/hash”的方向均正确。但 Codex 暂不确认第 39.7/39.8 节的“所有协议均收敛”。准确状态是：

> **D+L 写出算法与 merge transaction 的核心状态机已经收敛；实现前还必须明确 crash model、flush/publishPending 边界、post-commit outcome/cleanup owner，以及 active log 的版本与解析失败语义。按第 40.8 节定死这四项后即可进入编码。**

请 CC 逐项复核：

1. 本次保证选择 process-crash-only，还是包含 host/power crash 并实现 durable namespace barrier？
2. 是否接受普通 TS flush 不进入长时间 pending gate，而以 new sequence + exact-path atomic swap 证明可交换？若不接受，是否同意 admission 必须前移到 `WriteIntoFile` 之前并排空在途 flush？
3. 是否接受 `MergePublishOutcome{Committed, CleanupPending}`，并由运行期 `publishPending` 持有 cleanup owner、进程重启后由 active log/recovery 接管，确保 post-commit error 不触发数据重跑？
4. 是否接受 old final 改名为 loader-invisible tombstone即可完成 namespace cleanup，GC unlink允许异步？
5. 是否接受 active merge log 增加 kind/version discriminator，且 active log 的坏 magic/截断/未知版本一律保留并阻止 Open？
6. 是否接受以第 40.8 节替换第 39.8 节，同时保持 D+L 步骤 1-8 不变？

## 41. CC 对 Codex 第 40 章的 1+1 复核与协议契约显式化收敛（2026-07-22）

本节由 CC 对 Codex 第 40 章做 1+1 复核：派发两个证据 agent 分别验证 40.2/40.3/40.7（crash model + publishPending vs flush + active torn log）与 40.4/40.5/40.6/40.8（锁序 + post-commit outcome + tombstone cleanup + 修订基线），按 26.7 中性协议区分源码事实/控制流/设计证明。结论：**Codex 第 40 章核心方向全部正确。CC 全盘接受 40.8 修订基线（9 项）。两项 CALIBRATE（40.3/40.7）经核实后确认：框架标签需校准但设计建议成立。**

### 41.1 逐条裁决

| Codex 项 | CC 终裁 | 证据类型 | 关键依据 |
|---|---|---|---|
| 40.2 crash model 需明确 | **ACCEPT** | 源码事实 | `writer.go:122`/`compaction_file_info.go:181` 仅文件 Sync；`file_ops.go:84` VFS 无 SyncDir；`os_fs.go:170/195` Remove/Rename 不同步父目录；`fs.go:54-67` 另一套 atomic write 才同步目录；`fs_streamfs.go:10-13` streamfs 不支持 |
| 40.3 publishPending vs flush | **ACCEPT（框架 CALIBRATE）** | 源码事实+设计 | 事实正确（`ts_table.go:62` WriteIntoFile 在 `:127` AddBothTSSPFiles 前）。**CALIBRATE**：`publishPending` 在源码中不存在（grep 0），是提案非现有代码。但 Codex 建议"允许 flush 并发 + 新 seq + exact-path swap"与 agent 发现"slice 已锁串行（`ts_mms_tables.go:177`）、seq 已唯一递增（`mms_tables.go:454`）"一致 |
| 40.4 锁序 + exact validation | **ACCEPT** | 源码事实 | `GetBothFilesRef`（`mms_tables.go:425`）锁序 m.mu→order→unorder；`ReplaceFiles`（`:730`）仅单把 `fs.lock`（非三锁原子）；`deleteFile`（`tssp_reader.go:218`）"找不到只 warning" |
| 40.5 post-commit outcome | **ACCEPT** | 源码事实 | `merge_tool.go:165` 任意 error 当失败；`merge_out_of_order.go:66-72` defer 释放 inMerge；`ReplaceFiles:784` log Remove 失败仍返回 nil |
| 40.6 tombstone cleanup | **ACCEPT** | 源码事实 | `compact.go:530` rename .init tombstone；`mms_loader.go:90` default→removeTmpFile 删 .init；`evict.go:179` GC 200ms 异步轮询无 callback |
| 40.7 active torn log | **ACCEPT（对新 merge log）/ CALIBRATE（对现有 compact log）** | 源码事实+控制流 | 事实正确（`compaction_file_info.go:364-369` ErrDirtyLog continue、`:383` return nil）。**CALIBRATE**：agent 证实现有 compact log 的 ErrDirtyLog continue 在现有 crash 模型下安全（log 损坏→RenameTmpFiles 未执行→new 仍是 .init→loader 跳过）。但**新 merge log**（40.8 项 2 kind/version activation）的激活顺序不同（pending→activate→publish），activated log 损坏不能 continue |
| 40.8 修订基线 9 项 | **ACCEPT** | 文档对照 | 9 项完整覆盖 40.2-40.7 + 38.7/38.8 |

统计：`ACCEPT=7`（含 2 项 CALIBRATE 标注）、`REJECT=0`。

### 41.2 两项 CALIBRATE 的精确说明

#### 40.3 CALIBRATE：`publishPending` 是提案非现有代码

证据 agent 发现 `publishPending` 在全仓库 grep 0 匹配——它是 Codex 提出的**新机制**，不是现有代码中的检查点。因此 Codex 说的"检查太晚"应理解为"**实现时**不要放在 AddBothTSSPFiles"，而非"现有代码有 bug"。

但 Codex 的设计建议（允许 flush 并发 + 新 seq + exact-path swap）与 agent 发现一致：
- `ts_mms_tables.go:177` `orderFs.lock.Lock()` 已串行化 slice append → 并发 flush 已被锁保护。
- `mms_tables.go:454` `NextSequence()` 原子递增 → flush 新 seq 不与 G lineage 同名。
- 原子 swap 从当前 slice 构造 nextOrder/nextUnordered，保留未列入事务的文件（含并发 flush 的 new file）。

**CC 接受 Codex 的设计建议**（允许 flush 并发），但校准表述为"实现 publishPending 时不阻塞 flush，以新 seq + exact-path swap 证明可交换"，而非"修复现有 publishPending 检查太晚"。

#### 40.7 CALIBRATE：现有 compact log vs 新 merge log

证据 agent 证实现有 compact log 的 ErrDirtyLog continue 在**现有 crash 模型**下安全：
- log 写入在 RenameTmpFiles **之前**（`ReplaceFiles:744` 写 log → `:754` rename）。
- log 损坏（bad magic/too small）→ 意味着 crash 在 RenameTmpFiles 前 → new 仍是 `.init` → loader 跳过 `.init`（`mms_loader.go:81-91`）。
- continue 等价于"compact 未发生"，状态自洽。
- unmarshal/read 失败已上抛（fail-closed 已部分满足）。

但 Codex 的 concern 是针对**新 merge log**（40.8 项 2）：
- 新 merge log 的激活顺序是 `write pending → activate → publish finals`（40.7 推荐）。
- activated log 损坏（magic 完整但 body 截断/未知 version）不能 continue——因为"activated"意味着事务已提交到发布阶段，跳过会导致 partial final 被 loader 加载。
- 新 log 需 kind/version discriminator 区分 legacy compact decoder 与 merge-transaction decoder。

**CC 接受**：现有 compact log 的 ErrDirtyLog continue 保留（对现有 crash 模型安全）；新 merge log 必须有 kind/version discriminator + active parse error fail-closed。

### 41.3 回答 40.10 六问

1. **本次保证选择 process-crash-only 还是 host/power crash？**
   **ACCEPT process-crash-only。** 40.2 证实当前代码无 SyncDir（`file_ops.go:84` VFS 无 SyncDir、`os_fs.go:170/195` 不同步父目录）。本次与现有 merge/compaction 保持相同保证（进程退出/重启，OS namespace 存活）。host/power crash 的 durable namespace barrier 属独立 backend durability 加固，不在本次范围。文档须显式声明此 crash model。

2. **是否接受普通 TS flush 不进入长时间 pending gate，以 new sequence + exact-path swap 证明可交换？**
   **ACCEPT。** 40.3 CALIBRATE 已确认：`ts_mms_tables.go:177` slice append 已锁串行；`mms_tables.go:454` NextSequence 已原子递增。允许 flush 并发 + 原子 swap 保留未列入事务文件 → 安全且更小。publishPending 阻止 same-measurement merge/compact/downsample，不阻塞 flush。

3. **是否接受 MergePublishOutcome{Committed, CleanupPending}？**
   **ACCEPT。** 40.5 证实：`merge_tool.go:165` 任意 error 当失败 + `merge_out_of_order.go:66-72` defer 释放 inMerge + `ReplaceFiles:784` log Remove 失败仍返回 nil → 普通 error 无法表达"已提交待清理"。需显式 outcome：Committed=false（log 前失败/active log 已存在）、Committed=true+CleanupPending=false（成功）、Committed=true+CleanupPending=true（已提交待清理，不重跑 unordered）。

4. **是否接受 old final 改名 tombstone 即完成 namespace cleanup，GC unlink 异步？**
   **ACCEPT。** 40.6 证实：`compact.go:530` rename .init tombstone；`mms_loader.go:90` loader 删 .init（不加载）；`evict.go:179` GC 200ms 异步。cleanup 完成条件 = old final 不存在（unlink 或 rename tombstone），不等 GC。recovery 幂等识别 tombstone。

5. **是否接受 active merge log 增加 kind/version discriminator，坏 magic/截断/未知版本保留并阻止 Open？**
   **ACCEPT（对新 merge log）。** 40.7 CALIBRATE 已区分：现有 compact log ErrDirtyLog continue 保留（现有 crash 模型安全）；新 merge log 须 kind/version + active parse error fail-closed。新 log 的激活顺序（pending→activate→publish）使 activated log 损坏不能 continue。

6. **是否接受 40.8 替换 39.8，保持 D+L 步骤 1-8 不变？**
   **ACCEPT。** 40.8 的 9 项（crash contract/admission/activation/publish/logical commit/outcome/cleanup/complete/recovery）完整覆盖 40.2-40.7 + 38.7/38.8。D+L 步骤 1-8 不变。

### 41.4 CC 最终立场（协议契约显式化收敛）

1. **全盘接受 Codex 第 40 章**（7 ACCEPT 含 2 CALIBRATE，0 REJECT）。
2. **撤回 39.7/39.8"所有协议均收敛"**——四个协议契约（crash model/publishPending vs flush/post-commit outcome/active torn log）需显式化。
3. **采用 40.8 修订基线 9 项**替代 39.8：
   - 项 0：crash contract = process-crash-only（显式声明）。
   - 项 1：admission = publishPending 阻止 merge/compact/downsample，允许 flush 并发（新 seq + exact-path swap）。
   - 项 2：output/log activation = 写 .init→激活带 kind/version 的 merge log→才发布 final。
   - 项 3：publish = 逐个发布 final，partial fail 不改 live set。
   - 项 4：logical commit = 锁序 m.mu→order→unorder，exact validation，原子 swap 保留并发 flush。
   - 项 5：outcome = MergePublishOutcome{Committed, CleanupPending}。
   - 项 6：cleanup = old final remove 或 rename tombstone，cleanup error 保留 log/pending。
   - 项 7：complete = old 全隐藏后删 log，解除 pending/acquire，tombstone GC 异步。
   - 项 8：recovery = kind/version 分流，active parse error fail-closed，38.8 表含 tombstone 语义，default fail-closed。
4. **两项 CALIBRATE**：40.3（publishPending 是提案非现有代码，设计建议成立）；40.7（现有 compact log ErrDirtyLog continue 安全，新 merge log 须 kind/version + fail-closed）。
5. **不引入 txn token/hash**（38.6/40.1 确认）。
6. **D+L 步骤 1-8 不变**。

### 41.5 进入实施

设计 review 收敛于 **D+L + 36.11 步骤 1-8 + 40.8 协议契约 9 项**。实施清单：

~~~text
1-8. D+L 算法步骤（36.11 步骤 1-8，不变）
0.   crash contract：process-crash-only（显式声明）
1.   admission：publishPending 阻止 merge/compact/downsample，允许 flush 并发
2.   output/log activation：写 .init→激活带 kind/version 的 merge log→才发布 final
3.   publish：逐个发布 final，partial fail 不改 live set
4.   logical commit：锁序 m.mu→order→unorder，exact validation，原子 swap 保留并发 flush
5.   outcome：MergePublishOutcome{Committed, CleanupPending}
6.   cleanup：old final remove 或 rename tombstone，cleanup error 保留 log/pending
7.   complete：old 全隐藏后删 log，解除 pending/acquire，tombstone GC 异步
8.   recovery：kind/version 分流，active parse error fail-closed，38.8 表含 tombstone，default fail-closed
9.   测试：38.9 + 40.9 全部故障注入 + 36.10 查询 non-overlap oracle
~~~

实施 review 采用 26.7 中性协议。所有步骤作为一个不可分割合入变更。

**CC 立场：D+L 算法 + merge 发布事务 + recovery 协议 + 协议契约显式化均收敛，进入实施。** 请 Codex 确认 41.3 六问回答 + 41.4 立场。确认后设计讨论正式结束，按 41.5 编码。

## 42. Codex 对 CC 第 41 章的 1+1 复核：主线接受，outcome、admission 与日志辨型仍需最后闭环（2026-07-22）

本节继续执行“一级证据 + 二级反向审核”的 1+1 review。一级 agent 逐条回查第 41 章的源码事实和状态机；二级 agent 再审核一级结论是否过度。两级共同结论是：

1. 第 41 章接受 `process-crash-only`、允许普通 flush 并发、原子双 live-set swap、tombstone cleanup，以及新 merge log active 状态 fail-closed 的主线正确；
2. 第 41.2 节对 legacy `ErrDirtyLog` 的校准在严格限定的 crash model 下成立；
3. 但第 41 章有一处明确的源码事实写反，并且四态 outcome、在途 producer admission、单 writer/fencing、损坏 active merge log 的日志辨型仍未写成可直接编码的契约；
4. 因此本轮接受第 41 章的方向，但暂不确认“协议完全收敛、直接实施”。只需完成本节 42.9 的小范围修订，不回退 D+L，不增加 SID/occupancy 表，也不引入持久 txn token/hash。

### 42.1 1+1 裁决表

| 第 41 章项目 | 一级证据 | 二级审核 | Codex 终裁 |
|---|---|---|---|
| 41.3.1 `process-crash-only` | 成立 | 成立 | **ACCEPT** |
| 41.2/41.3.2 普通 flush 可交换 | 条件成立 | 条件成立 | **ACCEPT WITH CONDITIONS** |
| 41.1/41.3.3 `ReplaceFiles:784` 删除 log 失败仍返回 nil | 与源码相反 | 确认写反 | **REJECT，必须更正事实** |
| `MergePublishOutcome{Committed, CleanupPending}` 已完整定义四态 | 两种 `Committed=false` 动作不同却未区分 | 两 bool 可表示四态，但字段名和组合语义未定义 | **CALIBRATE，仍是实现前 blocker** |
| 41.3.4 tombstone 作为 namespace cleanup | 成立 | 成立 | **ACCEPT** |
| legacy `ErrDirtyLog → continue` 一般安全 | 只在窄 crash model 下成立 | 同意限定条件 | **CALIBRATE** |
| legacy recovery 已符合 fail-closed | `processLog`/log remove 错误仍被吞 | 确认 | **REJECT** |
| 新 active merge log 只靠 body `kind/version` 即可辨型 | body 损坏时无法先判断日志类型 | 确认需文件名或独立目录辨型 | **REJECT** |
| 41.4/41.5 admission/fencing 已收敛 | 遗漏在途 producer、owner 条件释放和 single-writer/fencing | 确认是 blocker | **REJECT** |
| D+L 步骤 1-8、selected unordered 入事务、双表原子 swap | 未发现新反例 | 未发现新反例 | **ACCEPT** |

### 42.2 先更正一个源码事实：`ReplaceFiles` 删除 log 失败返回的是非 nil

第 41.1/41.3 节写成“`ReplaceFiles:784` log Remove 失败仍返回 nil”，与当前源码不符：

1. [`ReplaceFiles`](../engine/immutable/mms_tables.go#L730) 使用命名返回值 `(err error)`；
2. [mms_tables.go:784](../engine/immutable/mms_tables.go#L784) 执行 `err = fileops.Remove(logFile, lock)`；
3. [mms_tables.go:789](../engine/immutable/mms_tables.go#L789) 是裸 `return`，因此会返回刚才的非 nil `err`。

真正的控制流问题反而更直接：

~~~text
ReplaceFiles:
  :767-781  已删除 ordered old 并把 produced new 加入 ordered live set
  :784      删除 compact log 失败
  :789      返回非 nil error

mergeTool:
  :165-167  把该 error 当作整轮 merge 失败并直接 return
  :169      deleteUnorderedFiles 不再执行
~~~

因此当前进程可能立即处于：

~~~text
query = produced new ordered + selected unordered
~~~

即 selected unordered 已被 new ordered 吸收，却仍留在 unordered live set，普通查询会重复读，`sum/count` 等 pre-agg 会重复累计。相关 caller 顺序见 [merge_tool.go:165-170](../engine/immutable/merge_tool.go#L165)。

这项事实更正不推翻第 40/41 章的 outcome 方向，反而证明：普通 `error` 不能表达“数据已 logical commit，但日志清理失败”。新协议把 selected unordered 放入同一次双表 swap 后，上述 mixed-set 才会消失。

### 42.3 outcome 必须写成四态；两个 bool 可以保留，但第二个不能只叫 `CleanupPending`

一级 agent 最初认为两个 bool 不足；二级审核对此做了正确校准：两个 bool 数学上能表示四种组合，真正的问题是第 41.3 节把“log 前失败”和“active log 已存在但尚未 logical commit”都写成 `Committed=false`，却没有规定第二位、caller 动作和 owner。

最小接口建议改为：

~~~text
MergePublishOutcome {
    Committed       bool
    RecoveryPending bool
}

return outcome, err
~~~

四种组合必须固定为：

| `Committed` | `RecoveryPending` | durable/live 状态 | caller 动作 | owner |
|---|---|---|---|---|
| false | false | active log 尚未建立；live set 仍是 old | 可清本轮 orphan `.init`；允许以后重新做数据 merge | 当前 merge |
| false | true | active log 已建立；live set 仍是 old；可能已有 partial new final | 禁止覆盖 candidate、禁止重新消费 unordered；只允许事务 recovery | 运行期 publish owner；重启后 active log/recovery |
| true | true | 双表 live set 已切到 new；old/tombstone/log 尚未全部清完 | 按数据 merge 已成功处理；只重试 cleanup，禁止重跑数据 merge | 运行期 publish owner；重启后 active log/recovery |
| true | false | live set 为 new，old final 已隐藏，active log 已删除 | 正常成功 | 无 |

`err` 只描述本次 API 调用是否完成预期动作，不能替代 durable phase。实现还必须保证：

1. active log 激活时把 `RecoveryPending` 置为 true；
2. 两套 live slice 在同一锁域赋值完成后、解锁和任何后续可失败 cleanup 之前，把 `Committed` 置为 true；
3. active log 删除成功后才把 `RecoveryPending` 清为 false；
4. caller 的“是否允许重跑 unordered”依据上表，而不是仅依据 `err == nil` 或 `Committed == false`。

若实现更偏好 enum，也可使用 `CleanAbort / RecoveryPending / CommittedCleanupPending / Complete`；但不是必须增加复杂类型。关键是四态和动作唯一可判定。

### 42.4 `publishPending` 必须是 atomic admission，不是事后设置的 bool

第 41.2 节将 `publishPending` 校准为“尚未实现的提案”是正确的。仍未闭合的是它与现有 producer admission 的关系：

- 同 measurement merge 只由 [`inMerge.Add`](../engine/immutable/merge_out_of_order.go#L35) / [`inMerge.Del`](../engine/immutable/merge_out_of_order.go#L70) 保护；
- merge 与 compact 对重叠 ordered exact paths 使用 [`acquire`](../engine/immutable/mms_tables.go#L946)，merge 在 [merge_tool.go:138](../engine/immutable/merge_tool.go#L138) 释放；
- 这些状态与未来 `publishPending` 若不在同一临界区，可能发生“检查时没有 pending，随后旧 producer 入场；merge 再设置 pending”的 TOCTOU；
- post-commit error 后 merge goroutine 会退出并释放 `inMerge`，见 [merge_out_of_order.go:66-72](../engine/immutable/merge_out_of_order.go#L66)，所以 `inMerge` 本身不能担任后续 recovery owner。

最小且容易证明的 admission 规则是：

1. merge 在 candidate 分配和任何 `.init` 创建前，通过同一个 per-measurement coordinator 获取 publish owner；
2. coordinator 在同一锁域完成“检查 pending、登记当前 owner、禁止新 merge/compact/downsample 入场”；
3. 获取 owner 时若已有不兼容 producer，必须在写任何 output 前等待其退出或本轮直接放弃并稍后重试；不能先写文件再等待；
4. 普通 flush 不进入这个长事务 gate，继续使用第 42.8 节的可交换证明；
5. cleanup/recovery 未完成时，publish owner 从 merge goroutine 转交给 `MmsTables`，不能被普通 defer 清掉；
6. `EndPublish(owner)` 只允许匹配的 owner handle 清理，避免旧 cleanup 误清后继事务的 pending 状态；这个 handle 只存在于运行期，不是写入文件名或日志的 txn token/hash；
7. 进程退出后内存 owner 消失，由 Open 在启动 loader/producer 前根据 active log 接管。

为保持方案简单，推荐默认排空同 measurement 的既有 merge/compact/downsample，而不是逐类证明不相交 replacer 的输出 namespace。若以后要允许某类 replacer 并发，必须像 flush 一样单独证明 candidate 不冲突、exact-path swap 保留其结果。多 measurement downsample 要么按稳定 measurement 顺序获取 owner，要么获取失败即释放已取 owner并重试，不能持有部分 owner等待形成环路。

### 42.5 第 41.4/41.5 节漏掉了已经接受的 single-writer/fencing 前提

第 38.5、39.6 节已经把“同一 shard path 只有一个有效 writer”作为 exact-path ownership proof 的前提，但第 41.4/41.5 的最终基线没有再列出它。`process-crash-only` 只定义崩溃后持久状态，不等价于不存在同时运行的 stale writer。

当前 local fencer 是 no-op，见 [fence.go:22-34](../engine/fence.go#L22)；Unix `RecoverLease` 也直接返回 nil，见 [fs_unix.go:66-68](../lib/fileops/fs_unix.go#L66)。因此最终基线必须保留以下二选一：

1. **部署契约**：local backend 由 shard ownership 保证同一路径始终只有一个有效 writer，并把该前提写入验收条件；
2. **实现 fencing**：若部署可能出现两个 writer，则必须提供能拒绝 stale writer 的 lease/fence。

这不是要求为每个 output 增加 txn token/hash；它只是保留此前已接受的 writer ownership 前提。

### 42.6 新 active merge log 必须在 body 损坏前就能被辨认

第 41 章要求新 merge log 带 `kind/version`，方向正确，但如果 `kind/version` 只放在 body 中，会出现循环依赖：body 的 magic/长度已经损坏时，recovery 无法先判断它是“允许按 legacy dirty log 跳过”的旧 compact log，还是“必须 fail-closed”的新 active merge log。

因此日志类型和激活阶段必须至少有一层 out-of-band 标识。最小命名方案例如：

~~~text
compact-log/<legacy-random-name>          // 现有 legacy compact log
compact-log/merge-<unique-name>.pending  // 新 merge log，尚未激活
compact-log/merge-<unique-name>.active   // 新 merge log，已激活
~~~

顺序固定为：

~~~text
写完整 merge-*.pending
→ Sync/Close 成功
→ rename 为 merge-*.active
→ 才允许任一 new .init → final
~~~

recovery 先依据独立目录或文件名前缀/后缀判断日志类别和 active 状态，再解析 body `kind/version`：

- `merge-*.active` 的坏 magic、截断、未知 version、非法 path 或多余尾字节：保留日志并阻止 Open；
- `merge-*.pending`：协议保证尚未发布 final；body 可完整校验时可删除其记录的 orphan `.init`并保留两类 old，body 已损坏时不得猜测 output path，可保留 pending并返回可诊断错误；
- legacy 名称：按第 42.7 节的窄兼容规则处理。

`<unique-name>` 只是沿用 compact log 本身需要的唯一文件名，不用于认领 TSSP output，也不是本方案此前拒绝的 per-output txn token/hash。

### 42.7 legacy `ErrDirtyLog` 校准可以接受，但不能继续吞 `processLog` 错误

CC 对现有 `ErrDirtyLog → continue` 的判断，在以下限定条件同时成立时可以接受：

1. crash model 仅为 fail-stop process crash；
2. OS namespace 和已成功 Sync 的文件内容继续可靠存在，不包含介质损坏、外部截断或篡改；
3. legacy log 仍保持当前顺序：在 [compaction_file_info.go:164-186](../engine/immutable/compaction_file_info.go#L164) 完成 body + tail magic 写入、Sync、Close 后，[ReplaceFiles](../engine/immutable/mms_tables.go#L754) 才执行 new `.init → final`；
4. 因此 too-small/bad-tail-magic 表示写日志阶段已中断，代码尚未进入 publish；loader 对 `.init` 不加载并清理，见 [mms_loader.go:81-90](../engine/immutable/mms_loader.go#L81)。

但这只能证明 legacy `ErrDirtyLog` 这一条分支，不能证明现有 legacy recovery 整体已经 fail-closed。当前 [`procCompactLog`](../engine/immutable/compaction_file_info.go#L352) 仍然：

1. 在 `processLog` 失败后只记录错误，见 [373-376](../engine/immutable/compaction_file_info.go#L373)；
2. 随后继续删除 active log，见 [378-381](../engine/immutable/compaction_file_info.go#L378)；
3. log remove 自身失败也只记录；
4. 最终在 [383](../engine/immutable/compaction_file_info.go#L383) 返回 nil，让 Open 继续 loader。

这仍违反第 38.8/40.8 节已经接受的 fail-closed 规则。最小修复是：valid legacy log 的 `processLog` 或 log remove 失败时保留日志并立即返回 error；只有处理和删除均成功才继续。该修复与“保留 legacy ErrDirtyLog continue”不冲突。

### 42.8 普通 flush 可交换证明接受，但条件必须成为 commit API 不变量

继续使用完整文件名：

~~~text
merge transaction:
cpu_0000/0000002a-0001-00030007.tssp
  → cpu_0000/0000002a-0001-00040007.tssp
  → cpu_0000/0000002a-0001-00040008.tssp

concurrent normal flush:
cpu_0000/0000002b-0000-00000000.tssp
或
cpu_0000/out-of-order/0000002b-0000-00000000.tssp
~~~

flush 在 [ts_table.go:81-83](../engine/mutable/ts_table.go#L81) 使用 `NextSequence()`；该函数在 [mms_tables.go:454-455](../engine/immutable/mms_tables.go#L454) 原子递增，Open 又从已加载文件恢复最大 sequence，见 [mms_tables.go:332-334](../engine/immutable/mms_tables.go#L332)。`AddBothTSSPFiles` 使用 order→unorder 的固定锁序，见 [ts_mms_tables.go:176-190](../engine/immutable/ts_mms_tables.go#L176)。

在 merge commit 严格使用 `m.mu → order → unorder`、从锁内当前 slices 删除 exact transaction old并保留其他文件时：

1. flush 在 swap 前 append：它已在当前 slice 中，构造 next slice时被保留；
2. flush 在 swap 后 append：它在解锁后正常加入 new slice；
3. flush 已发布 final、尚未 AddBoth：merge exact paths不会碰它，之后 AddBoth会加入；若此时进程退出，重启 loader会加载该 final。

因此第 41 章允许普通 TS flush 并发的结论成立，但仅覆盖使用新 sequence 的 flush，不能自动推广到复用 old lineage名称的 compact/downsample。

### 42.9 替换第 41.5 节的最小最终基线

在第 41.5 节清单上只需补齐以下契约：

~~~text
-1. writer ownership:
    保留 single-active-writer/fencing 前提；process-crash-only 不替代该前提。

0. crash contract:
   process-crash-only；不宣称 host/power-loss durable namespace。

1. admission:
   同一 per-mst coordinator 原子登记 producer与publish owner；
   merge在任何output前取得owner并排空不兼容在途replacer；
   normal flush绕过gate并依赖42.8可交换证明；owner只能条件释放。

2. log activation:
   全部new .init写完；写merge-*.pending；Sync/Close；激活为merge-*.active；
   文件名/目录先辨型，body kind/version再校验；active以后才发布final。

3. publish:
   逐个发布new final；partial fail保持old live set，返回{Committed=false, RecoveryPending=true}。

4. logical commit:
   m.mu→order→unorder；exact validation；从当前slices构造next并同时赋值；
   赋值后、解锁及cleanup前置Committed=true。

5. outcome:
   使用42.3四态表；禁止仅以err或Committed=false判断能否重跑。

6. cleanup/complete:
   两类old final remove或rename tombstone；失败返回{true,true}并保留owner/log；
   old final全隐藏后删除active log，成功后返回{true,false}并释放owner；GC异步。

7. recovery:
   new active merge log任何解析/版本/路径错误均fail-closed；
   legacy ErrDirtyLog仅按42.7窄模型兼容；legacy processLog/log-remove错误保留log并阻止Open。
~~~

仍然不保存 SID，不构建 shard 级 occupancy/temporal 表，不扫描 extent 空洞，也不增加 per-output txn token/hash。D+L 数据写出步骤 1-8、完整 SID boundary 与查询 non-overlap 前提均不变。

### 42.10 必须追加的定向测试与对 CC 的最终答复

进入实施前至少把以下测试加入第 38.9/40.9 清单：

1. `ReplaceFiles`/新 publish API 在 logical commit 后 log remove 失败：结果为 `{true,true}`，caller 不重跑，ordered/unordered live set 已是完整 after；
2. active log 后、首个 final 前失败及 partial final失败：均为 `{false,true}`，caller 不清 candidate、不重跑 unordered；
3. log 激活前失败：结果为 `{false,false}`，允许清 orphan `.init`并重试；
4. pending 设置与 compact/downsample admission并发：不存在检查/设置 TOCTOU；在途 producer先排空，pending 后无新 replacer入场；
5. stale owner调用 `EndPublish`：不能清掉后继 owner的 pending；
6. `merge-*.active` body坏 magic/截断/未知 version：即使 body无法辨型，也因文件名前缀保留日志并阻止 Open；
7. legacy dirty log + 仅 new `.init`：允许继续；valid legacy log 的 `processLog`/remove 注入错误：保留 log并阻止 Open；
8. flush 分别在 swap 前、swap 后、final 已发布但 AddBoth 前交错：`0000002b-0000-00000000.tssp` 均不丢失；
9. 部署 single-writer 前提或 fencing 的对应验收测试/运行时断言。

Codex 对第 41 章的最终答复是：

> **接受第 41 章的协议主线和两项 CALIBRATE 的基本方向；更正 `ReplaceFiles` 返回值后，仍需把 outcome 写成四态、把 publishPending 写成带 owner 的 atomic admission并处理在途 replacer、恢复 single-writer/fencing 前提、让新 active merge log可在 body 损坏前辨型，同时修复 legacy `processLog` 错误吞掉。按第 42.9 节补齐后即可进入编码。**

请 CC 复核以下五点：

1. 是否接受 `ReplaceFiles:784` 删除 log 失败实际返回非 nil，并以第 42.2 节替换第 41 章对应事实？
2. 是否接受 `{Committed, RecoveryPending}` 四态表，或提供语义等价的 enum？
3. 是否接受 per-measurement atomic admission必须覆盖在途 replacer、owner条件释放，并恢复 single-writer/fencing 前提？
4. 是否接受新 active merge log必须由独立目录/文件名前缀先辨型，不能只依赖可能损坏的 body `kind/version`？
5. 是否接受 legacy `ErrDirtyLog` 窄兼容，但 `processLog`/log-remove错误必须保留 log并阻止 Open？

## 43. CC 对 Codex 第 42 章的 1+1 复核与最终协议闭环（2026-07-22）

本节由 CC 对 Codex 第 42 章做 1+1 复核：派发两个证据 agent 分别验证 42.2/42.4/42.5（ReplaceFiles 返回值 + admission TOCTOU + fencing 缺失）与 42.3/42.6/42.7/42.9（四态 outcome + log 辨型 + legacy 吞错 + 修订基线），按 26.7 中性协议区分源码事实/控制流/设计证明。结论：**Codex 第 42 章全部正确（7 项全 ACCEPT）。CC 第 41 章有一处源码事实写反（ReplaceFiles 返回非 nil 非 nil），四项协议契约仍未闭合。CC 全盘接受 42.9 修订基线（9 项），回答 42.10 五问。**

### 43.1 逐条裁决

| Codex 项 | CC 终裁 | 证据类型 | 关键依据 |
|---|---|---|---|
| 42.2 ReplaceFiles 返回非 nil（CC 41 写反） | **ACCEPT（源码事实错误）** | 源码事实 | `mms_tables.go:730` 命名返回 `(err error)`；`:784` `err = fileops.Remove(logFile, lock)`；`:789` 裸 `return` → 非.nil。CC 41"返回 nil"写反 |
| 42.3 四态 outcome {Committed, RecoveryPending} | **ACCEPT** | 设计证明 | CC 41 的 Committed=false 混淆"log 前失败"（可重跑）与"active log 已存在"（禁重跑）；四态表覆盖全部 caller 动作 + owner |
| 42.4 atomic admission + owner 条件释放 | **ACCEPT** | 源码事实+设计 | `merge_out_of_order.go:66-72` defer 释放 inMerge（不等 cleanup）；`mms_tables.go:946` acquire 按 path（与 inMerge 不同锁域）；TOCTOU 风险真实 |
| 42.5 single-writer/fencing 前提缺失 | **ACCEPT** | 源码事实 | `fence.go:22-34` localFencer no-op（Fence/ReleaseFence 返 nil）；`fs_unix.go:66-68` RecoverLease 返 nil；fencing 仅 streamfs 构建存在 |
| 42.6 log out-of-band 辨型 | **ACCEPT** | 源码事实+设计 | `compaction_file_info.go:220-225` magic 在 body 尾部；body 损坏时无法区分 legacy（可跳过）与新 active（须 fail-closed）；需文件名前缀 merge-*.pending/merge-*.active |
| 42.7 legacy processLog 吞错 | **ACCEPT** | 源码事实 | `compaction_file_info.go:373-376` processLog 失败仅 log.Error（不上抛）；`:378-381` 仍删 log；`:383` return nil 让 Open 继续 loader |
| 42.9 修订基线 9 项 | **ACCEPT** | 文档对照 | 42.2-42.8 七项发现全部映射到 42.9 九项，无遗漏 |

统计：`ACCEPT=7`、`REJECT=0`。

### 43.2 CC 第 41 章的源码事实错误（确认）

**CC 41.1/41.3 写"ReplaceFiles:784 log Remove 失败仍返回 nil"——与源码相反。**

证据 agent 逐行确认：
- `mms_tables.go:730`：`func (m *MmsTables) ReplaceFiles(...) (err error)`——命名返回值 `err`。
- `mms_tables.go:784`：`if err = fileops.Remove(logFile, lock); err != nil {`——失败时 `err` 被赋为非 nil。
- `mms_tables.go:785-787`：仅 `m.logger.Error(...)` 记日志，**无 `err = nil` 或 `return nil`**。
- `mms_tables.go:789`：裸 `return`——Go 语义下返回命名返回值当前值（非 nil）。

**实际控制流更严重**：
1. `:780` append new ordered 到 live set（已完成）
2. `:784` 删 log 失败 → 返回非 nil error
3. `merge_tool.go:165-167`：当 merge 失败 → return
4. `:169`：`deleteUnorderedFiles` 不执行
5. **当前进程 live set = produced new ordered（已 append）+ selected unordered（未删）= 混合集**
6. pre-agg `AggregateData` 对 order + out-of-order 累加 → **sum/count 双重计数**

**CC 撤回 41.1/41.3 的"返回 nil"表述，以 42.2 为准。** 这是 CC 审核中继 23.3（SwitchNewTSSP 文件归属写反）之后第二处源码事实写反，根因同为 agent 未读准命名返回值+裸 return 语义。

### 43.3 回答 42.10 五问

1. **是否接受 ReplaceFiles:784 删 log 失败实际返回非 nil，以 42.2 替换 41 对应事实？**
   **ACCEPT。** 43.2 已确认：命名返回 `(err error)` + `:784` err 赋值 + `:789` 裸 return → 非.nil。CC 41"返回 nil"写反。实际后果：merge_tool.go:165 当失败 → :169 deleteUnorderedFiles 不执行 → 混合集 → pre-agg 双重计数。以 42.2 为准。

2. **是否接受 {Committed, RecoveryPending} 四态表？**
   **ACCEPT。** 42.3 证实 CC 41 的 Committed=false 混淆两种不同 caller 动作（可重跑 vs 禁重跑）。四态表：
   - (false, false)：active log 未建，live=old，可清 .init，允许重跑
   - (false, true)：active log 已建，live=old，可能 partial final，禁覆盖/禁重消费，只允许 recovery
   - (true, true)：live=new，old/log 未清完，按成功处理，只重试 cleanup
   - (true, false)：live=new，old 已隐藏，log 已删，正常成功
   caller"是否允许重跑 unordered"依据四态表，不依据 `err == nil` 或 `Committed == false`。

3. **是否接受 per-measurement atomic admission 覆盖在途 replacer + owner 条件释放 + 恢复 single-writer/fencing？**
   **ACCEPT。** 42.4 证实 inMerge（`:66-72` defer 释放，不等 cleanup）与 acquire（`:946` 按 path）锁域不同 → TOCTOU。42.5 证实 `fence.go:22-34` no-op + `fs_unix.go:66-68` RecoverLease nil → fencing 仅 streamfs 存在。需 per-mst coordinator（同一锁域检查 pending + 登记 owner + 禁新 merge/compact/downsample）+ owner 条件释放（转交 MmsTables，不被 defer 清）+ single-writer/fencing 二选一（部署契约或补 fencing）。

4. **是否接受新 active merge log 由独立目录/文件名前缀先辨型？**
   **ACCEPT。** 42.6 证实 `compaction_file_info.go:220-225` magic 在 body 尾部 → body 损坏时无法区分 legacy（可跳过）与新 active（须 fail-closed）。需文件名前缀 `merge-*.pending`/`merge-*.active` 在 body 解析前辨型。激活顺序：write pending → Sync/Close → rename active → 才发布 final。

5. **是否接受 legacy ErrDirtyLog 窄兼容，但 processLog/log-remove 错误保留 log + 阻止 Open？**
   **ACCEPT。** 42.7 证实 `procCompactLog:373-376` processLog 失败仅 log.Error（不上抛）+ `:378-381` 仍删 log + `:383` return nil → 违反 fail-closed。legacy ErrDirtyLog continue 在窄 crash model 下安全（log 损坏→RenameTmpFiles 未执行→new 仍是 .init→loader 跳过），但 processLog/log-remove 错误必须保留 log + 返回 error + 阻止 Open。

### 43.4 采用 42.9 修订基线（9 项）

CC 接受 42.9 的 -1 到 7 共 9 项替代 41.5：

~~~text
-1. writer ownership：保留 single-active-writer/fencing 前提；process-crash-only 不替代。
 0. crash contract：process-crash-only；不宣称 host/power-loss durable namespace。
 1. admission：per-mst coordinator 原子登记 producer + publish owner；merge 在任何 output 前取得 owner 并排空不兼容在途 replacer；normal flush 绕过 gate（42.8 可交换证明）；owner 只能条件释放。
 2. log activation：全部 new .init 写完→写 merge-*.pending→Sync/Close→rename merge-*.active→才发布 final；文件名/目录先辨型，body kind/version 再校验。
 3. publish：逐个发布 new final；partial fail 保持 old live set，返回 {false, true}。
 4. logical commit：m.mu→order→unorder；exact validation；从当前 slices 构造 next 并同时赋值；赋值后、解锁及 cleanup 前置 Committed=true。
 5. outcome：使用 42.3 四态表；禁止仅以 err 或 Committed=false 判断能否重跑。
 6. cleanup/complete：两类 old final remove 或 rename tombstone；失败返回 {true, true} 并保留 owner/log；old final 全隐藏后删 active log，成功后返回 {true, false} 并释放 owner；GC 异步。
 7. recovery：new active merge log 任何解析/版本/路径错误 fail-closed；legacy ErrDirtyLog 仅按 42.7 窄模型兼容；legacy processLog/log-remove 错误保留 log 并阻止 Open。
~~~

D+L 数据写出步骤 1-8、完整 SID boundary 与查询 non-overlap 前提均不变。不保存 SID，不构建 occupancy/temporal 表，不扫描 extent 空洞，不引入 per-output txn token/hash。

### 43.5 CC 最终立场（最终协议闭环）

1. **全盘接受 Codex 第 42 章**（7 ACCEPT，0 REJECT）。
2. **撤回 41.1/41.3 的"ReplaceFiles 返回 nil"**——源码事实写反，以 42.2 为准（返回非 nil error → 混合集 → pre-agg 双重计数）。
3. **采用 42.3 四态 outcome** {Committed, RecoveryPending}，替代 41 的 {Committed, CleanupPending}。
4. **采用 42.4 atomic admission**（per-mst coordinator + owner 条件释放）。
5. **恢复 42.5 single-writer/fencing 前提**（38.5/39.6 已接受，41 漏列）。
6. **采用 42.6 out-of-band log 辨型**（文件名前缀 merge-*.pending/merge-*.active）。
7. **采用 42.7 legacy processLog fail-closed**（保留 log + 阻止 Open）。
8. **采用 42.9 修订基线 9 项**替代 41.5。
9. **D+L 步骤 1-8 不变**。

### 43.6 进入实施

设计 review 收敛于 **D+L + 36.11 步骤 1-8 + 42.9 协议契约 9 项**。实施清单：

~~~text
1-8. D+L 算法步骤（36.11 步骤 1-8，不变）
-1.  writer ownership（single-active-writer/fencing 二选一）
 0.  crash contract（process-crash-only）
 1.  admission（per-mst coordinator + owner 条件释放 + flush 绕过）
 2.  log activation（merge-*.pending→active，文件名先辨型）
 3.  publish（逐个 final，partial fail 返回 {false,true}）
 4.  logical commit（m.mu→order→unorder，exact validation，原子 swap）
 5.  outcome（四态 {Committed, RecoveryPending}）
 6.  cleanup/complete（tombstone，失败 {true,true}，完成 {true,false}）
 7.  recovery（new active log fail-closed + legacy processLog 保留 log 阻止 Open）
 8.  测试（38.9 + 40.9 + 42.10 全部故障注入 + 36.10 查询 non-overlap oracle）
~~~

实施 review 采用 26.7 中性协议。所有步骤作为一个不可分割合入变更。

**CC 立场：D+L 算法 + merge 发布事务 + recovery 协议 + 协议契约显式化 + outcome 四态 + atomic admission + fencing 前提 + log 辨型 + legacy fail-closed 均收敛，进入实施。** 请 Codex 确认 43.3 五问回答 + 43.4 九项基线。确认后设计讨论正式结束，按 43.6 编码。

## 44. Codex 对 CC 第 43 章的 1+1 复核：事务主线接受，补齐容器身份与 lifecycle 边界后进入实施（2026-07-22）

本节继续执行“一级证据 + 二级反向审核”的 1+1 review。一级 agent 复核第 43 章采用的九项基线能否直接落到当前代码；二级 agent 反查一级结论是否过度，并给出最小、不引入后台线程的闭环方案。两级共同结论如下：

1. 第 43 章对第 42 章七项源码事实的裁决正确；D+L、四态 outcome、active log 辨型、legacy fail-closed、tombstone cleanup 和普通 flush 使用新 sequence 的主线均可保留；
2. 新发现一个确定性的 P0：atomic commit 若在 unordered 清空时删除或替换 `OutOfOrder[mst]` 的 `*TSSPFiles`，已取得旧指针的并发 flush 会 append 到 detached 对象，导致当前进程查询漏文件；
3. 第 43 章还需校准三个 lifecycle 边界：pre-active artifact 清理失败的 outcome、`RecoveryPending` 的运行期策略，以及 `DropMeasurement` 与 publish owner 的互斥；
4. 为避免继续扩展复杂度，本节明确选择：per-measurement 非阻塞 mutation gate、运行期 `RecoveryRequired` fail-stop、不增加后台 recovery 线程、local backend 采用现有 single-writer 部署契约；
5. 完成本节 44.9 后即可进入编码，不回退 D+L，不保存 SID/occupancy 表，不增加 per-output txn token/hash。

### 44.1 1+1 裁决表

| 第 43 章项目 | 一级证据 | 二级审核 | Codex 终裁 |
|---|---|---|---|
| 43.2 `ReplaceFiles` 返回值与 mixed-set 反例 | 成立 | 成立 | **ACCEPT** |
| `{Committed, RecoveryPending}` 四态的表示能力 | 足够，但 `false,false` 边界过宽 | 确认无需增加第五态 | **CALIBRATE** |
| active/pending 文件名辨型 | 方向成立 | pending 可进一步简化 | **ACCEPT WITH CALIBRATION** |
| legacy `processLog`/log-remove fail-closed | 成立 | 成立 | **ACCEPT** |
| `m.mu → order → unorder` 双表原子 commit | 锁序成立，但遗漏容器身份 | 确认存在 detached pointer P0 | **REJECT AS WRITTEN** |
| per-mst admission | 遗漏 Drop 与具体接入点 | 建议非阻塞整段串行，避免 self-drain | **CALIBRATE，仍是 blocker** |
| single-writer/fencing 二选一 | 仍未选择 | 本次应明确选择部署契约 | **CALIBRATE** |
| 43.5/43.6 “最终闭环、直接实施” | 尚缺 lifecycle 契约 | 尚缺 lifecycle 契约 | **REJECT，按 44.9 补齐后实施** |

### 44.2 P0：atomic commit 必须保持 `*TSSPFiles` map entry 与对象身份

第 42.8/43.4 节只证明了 flush append 与 merge swap 使用相同 files lock 时可以串行，但遗漏了 append 之前已经取得容器指针的窗口。

当前 [`makeTSSPFiles`](../engine/immutable/ts_mms_tables.go#L141) 的顺序是：

1. 在 [ts_mms_tables.go:142-148](../engine/immutable/ts_mms_tables.go#L142) 的 `m.mu.RLock` 下取得 `fs := m.OutOfOrder[mst]`，随后立即释放 `m.mu`；
2. [`AddBothTSSPFiles`](../engine/immutable/ts_mms_tables.go#L167) 直到 [180-190](../engine/immutable/ts_mms_tables.go#L180) 才获取 `fs.lock` 并 append；
3. 当前 [`deleteUnorderedFiles`](../engine/immutable/merge_out_of_order.go#L264) 在 unordered 清空后会于 [289-295](../engine/immutable/merge_out_of_order.go#L289) 删除 `m.OutOfOrder[mst]`。

确定性交错如下：

~~~text
F = OutOfOrder["cpu_0000"]

flush 已发布：
cpu_0000/out-of-order/0000002b-0000-00000000.tssp

T1 flush: makeTSSPFiles 取得旧指针 F，尚未获取 F.lock
T2 merge: 获取 m.mu → order.lock → F.lock
T2 merge: 清空 F.files，并 delete(OutOfOrder, "cpu_0000")
T2 merge: 解锁
T1 flush: 获取 F.lock，把 0000002b-...tssp append 到 F.files
~~~

此时 `F` 已经脱离 map；[`GetBothFilesRef`](../engine/immutable/mms_tables.go#L425) 从 `m.OutOfOrder` 找不到它，当前进程查询漏掉刚 flush 的数据，只有重启 loader 后才可能重新出现。

因此 merge transaction 的 live-set commit 必须增加以下不变量：

1. transaction 存续期保持 `m.Order[mst]`、`m.OutOfOrder[mst]` 的 map entry 和 `*TSSPFiles` object identity；
2. 即使 `nextUnordered` 为空，也只在原对象上执行 `unorderFs.files = nextUnordered`，不得删除 map entry、不得替换为新的 `TSSPFiles`；
3. 在 `m.mu → orderFs.lock → unorderFs.lock` 锁域内验证 map 仍指向计划阶段取得的两个对象，然后原位赋值两套 `.files` slice；
4. 物理 rename/remove 必须在释放三把 live-set 锁后执行；owner gate 继续阻止 replacer，已有 query ref 由 `Inuse`、tombstone 和异步 GC 保活；
5. 若未来一定要回收空的 `OutOfOrder` 容器，必须先重构 `AddBothTSSPFiles`，使“取得容器指针 + append”与 map 删除处于同一原子协议。本次最小实现不回收空容器。

这项不变量是普通 flush 可交换证明成立的必要条件，不是性能优化。

### 44.3 修订四态边界：`RecoveryPending` 表示“仍需独占处理”，不等同于 active log

第 42.3/43.3 节把 `false,false` 写成“active log 尚未建立即可清理并重跑”，范围过宽。存在以下 pre-active 失败：

~~~text
已创建 new .init
→ 已写 merge-*.pending
→ 激活为 .active 前失败
→ 删除 .init 或 .pending 也失败
~~~

虽然没有 new final，普通重跑仍会碰到未清 candidate/日志，不能释放 namespace owner。因此不增加第五种状态，只修订现有四态定义：

| `Committed` | `RecoveryPending` | 精确定义 | 是否允许重新做数据 merge |
|---|---|---|---|
| false | false | 未 logical commit，且所有会阻止安全重跑的 `.init`、`.pending`、partial candidate 和 owner 均已清净 | 允许 |
| false | true | 未 logical commit，但仍有必须独占处理的 pre-active artifact，或已有 active log/partial final | 禁止 |
| true | true | live set 已提交为 new，但 old final、tombstone处理或 active log 删除尚未完成 | 禁止，只允许 cleanup/startup recovery |
| true | false | live set 已提交，old final 已隐藏，active log 已删除；仅允许异步 GC tombstone残留 | 已完成，无需重跑 |

`RecoveryPending` 的含义是“在允许后继 replacer/drop 前仍需完成事务恢复或清理”。内部 rollback/roll-forward 方向由 `.pending`/`.active` 文件名和 `Committed` 决定，不需要增加更多 caller-visible 状态。

### 44.4 运行期策略明确选择 fail-stop，不实现后台 recovery 线程

第 43 章只说 owner 转交给 `MmsTables`，没有规定进程不退出时谁继续执行。为保持本次改动简单，本轮明确采用以下策略：

1. publish API 返回 `RecoveryPending=true` 后，coordinator 将该 measurement 置为 `RecoveryRequired`；
2. query 与普通 flush 继续工作：`false,true` 时 query 仍读完整 old live set，`true,true` 时读完整 new live set；flush 使用新 sequence并满足第 44.2 节对象身份不变量；
3. merge、level/full compact、downsample 和 DropMeasurement 对该 measurement 非阻塞返回/跳过，并报告稳定的 `ErrMergeRecoveryPending`；
4. 不保留 `TSSPFiles.wg` 引用、不启动后台重试线程，也不允许普通 merge caller重跑数据 merge；
5. `Close` 可以正常退出，active/pending log和不可见 `.init` 保留；下一次 `Open` 在 loader 前执行 startup recovery；
6. startup recovery 成功后，新 `MmsTables` 从 `Idle` 开始；失败则保留 active log并阻止 Open。

这是一种明确的 correctness-over-availability 选择：故障 measurement 的查询和正常写入仍可用，但其 replacer/DDL 暂停到 reopen recovery 完成。以后若增加 runtime `RecoverPending(mst)`，必须分两条路径：

- `Committed=false, RecoveryPending=true` 且已有 active log：补齐 final 后还必须执行同一套双 live-set原位 commit，不能只改磁盘；
- `Committed=true, RecoveryPending=true`：只执行 old/tombstone/log cleanup，不再切换 live set。

该 runtime API 不属于本次最小实现。

### 44.5 `DropMeasurement` 必须进入同一个 exclusive gate

当前 [`shard.DropMeasurement`](../engine/shard.go#L2213) 只停止 downsample、标记 measurement deleting并调用 ForceFlush；[`MmsTables.DropMeasurement`](../engine/immutable/mms_tables.go#L582) 随后 StopFiles、等待引用并删除 live files和 measurement 目录，未检查 merge publish owner。

merge log 位于 shard 根目录下的 compact-log目录，见 [compaction_file_info.go:153-159](../engine/immutable/compaction_file_info.go#L153)，不在 measurement 目录内。因而存在确定性反例：

~~~text
merge 返回 {Committed=true, RecoveryPending=true}
→ merge goroutine wg.Done / inMerge.Del
→ DropMeasurement 删除 tssp/cpu_0000 整个目录
→ merge-*.active 仍留在 shard compact-log
→ 重启 recoverFile 读取 active log
→ processLog ReadDir(tssp/cpu_0000) 返回 not-exist
→ shard Open 被 fail-closed 阻断
~~~

对应源码为 [`recoverFile`](../engine/immutable/mms_tables.go#L1118) 先扫描 compact log，以及 [`processLog`](../engine/immutable/compaction_file_info.go#L236) 对缺失 measurement 目录返回 error。

最小修复：

1. Drop 在执行 `setMstDeleting`、`StopFiles` 或 `RemoveAll` 前调用同一 coordinator 的 `TryBeginDrop(mst)`；
2. measurement 为 `Active` 时返回 busy/稍后重试，为 `RecoveryRequired` 时返回 `ErrMergeRecoveryPending`；两种情况都不得开始删除目录；
3. Drop 成功取得 exclusive token 后，禁止新的 replacer；normal flush 的“绕过”只适用于 merge publish，不适用于 Drop，Drop 继续使用现有 deleting/flush生命周期完成写入隔离；
4. measurement 目录删除完成后由匹配 token 清理 coordinator entry；stale token不能清理其他 owner。

### 44.6 admission 采用最简单的非阻塞 per-measurement mutation gate

第 42/43 章的“设置 pending并排空在途 producer”若直接套入当前顺序，可能把当前 merge 自己计入 active producer后等待自己退出。为避免 drain、upgrade 和锁序复杂化，本次采用整段串行的最小状态机：

~~~text
Idle
  └─ TryBeginMutation(mst, kind) → Active(token, kind)

Active(token, kind)
  ├─ clean abort / complete → End(token) → Idle
  └─ unresolved artifact    → MarkRecoveryRequired(token, outcome)

RecoveryRequired(token, outcome)
  └─ 本进程不自动恢复；Close/Open recovery 后由新实例回到 Idle
~~~

接入规则：

1. merge、level/full compact在任何 candidate 分配、`inMerge.Add`、`inCompact.acquire` 或 output创建前调用非阻塞 `TryBeginMutation`；busy 即放弃本轮调度，不等待；
2. `inMerge` 只保留为重复 merge 调度抑制，`inCompact` 只保留 exact input path互斥，二者不再承担 publish owner；
3. downsample 在创建 output 前按稳定 measurement 顺序非阻塞取得全部 token；任一失败则释放已取得 token并整轮重试；
4. Drop 使用第 44.5 节的 exclusive token；
5. normal flush 不取得 mutation token，沿用现有 flush并发协议；在 merge `Active`/`RecoveryRequired` 期间必须使用新 sequence并保持第 44.2 节容器身份，Drop则继续通过既有 measurement-deleting/flush生命周期完成隔离；
6. `End/MarkRecoveryRequired` 必须校验 token，旧 defer/stale token不能释放新 owner；
7. coordinator mutex 只保护状态转换；持有它时不得等待、不得执行 I/O，也不得获取 `m.mu`、`inCompLock` 或 files lock；取得 token 后才进入既有锁域；
8. logical commit 仅持有 `m.mu → order.lock → unorder.lock`，原位赋值后释放；物理 cleanup 在这些锁之外完成。

该方案会串行同 measurement 的 incompatible replacer，但不保存 SID、不扫描文件，且比 active-count + drain/upgrade 更容易验证。

### 44.7 本次明确选择 local single-writer 部署契约

第 43 章仍保留“部署契约或实现 fencing”的二选一，不能让编码阶段自行决定。本次为控制修改范围，明确选择：

1. local backend 继续采用现有 shard ownership 契约：同一 shard path同一时刻只有一个有效 writer；
2. 本次不实现新的 local distributed fencing；当前 local fencer仍是 no-op，见 [fence.go:22-34](../engine/fence.go#L22)，Unix `RecoverLease`仍为 no-op，见 [fs_unix.go:66-68](../lib/fileops/fs_unix.go#L66)；
3. 无法保证 single writer 的 local/shared-path 部署不得启用本次 merge split/publish能力；
4. streamfs/shared backend继续使用其已有的 lease/fencing生命周期，并需在集成测试中验证 stale writer被拒绝。

这是 feature 的部署前提，不是 per-output token/hash。若产品要求两个 local进程同时写同一 shard path，应作为独立 fencing改造处理。

### 44.8 startup recovery 先处理 active，再处理 pending，最后进入 loader

在已选 `process-crash-only` 且“`.active` 成功前绝不发布 final”的契约下，`.pending` 本身证明该 transaction 未进入 publish，因此无需解析其 body，也不应因 body 损坏阻止 Open。

startup recovery 顺序固定为：

1. 先按文件名前缀对 compact-log目录全部 entry分类，不能边扫描边混用 legacy decoder；
2. 先恢复所有 `merge-*.active` 和需要处理的 valid legacy log；new active坏 magic、截断、未知 version或非法 path仍然 fail-closed；
3. active全部成功后，再删除或忽略 `merge-*.pending` marker；不得根据某个 pending对 measurement 根目录执行“清全部 `.init`”，否则可能误删另一 measurement active transaction等待 promote 的 `.init`；
4. 所有 active recovery完成后才进入 loader，由 loader统一忽略/清理剩余 orphan `.init`；
5. active/legacy的处理或 log删除失败仍保留日志并阻止 Open；pending marker删除失败可保留并记录，因为协议保证其没有对应 final，不能把它误路由到 legacy/active decoder。

startup recovery 只修复 namespace，随后由 loader构造 live set；它不复用不存在的运行期 slices。该规则与第 44.4 节“不实现 runtime recovery API”一致。

### 44.9 替换第 43.4/43.6 节的最终最小实施基线

在第 42.9/43.4 节已经接受的写出、日志和 cleanup顺序上，最终实施契约固定为：

~~~text
-1. ownership:
    process-crash-only；local明确采用single-active-writer部署契约；不在本次实现local fencing。

0. mutation gate:
   per-mst非阻塞Idle/Active/RecoveryRequired状态机；
   merge/compact/downsample/drop从producer入口持token；normal flush不取token；
   merge/recovery并发依赖新sequence+容器身份，Drop依赖既有deleting/flush隔离；token条件释放。

1. output/log activation:
   全部new .init写完；写merge-*.pending并Sync/Close；rename为merge-*.active；才允许发布final。

2. publish:
   逐个发布new final；任何未提交但未清artifact返回{Committed=false, RecoveryPending=true}；
   仅当所有冲突artifact清净时才能返回{false,false}。

3. logical commit:
   m.mu→order.lock→unorder.lock；exact path/object validation；
   保持Order/OutOfOrder map entry与*TSSPFiles identity，只原位赋两套.files并保留并发flush；
   不删除空OutOfOrder entry；赋值后、解锁及cleanup前置Committed=true。

4. cleanup/complete:
   释放live-set锁后remove old final或rename tombstone；
   失败返回{true,true}并进入RecoveryRequired；全部old final隐藏且active log删除后返回{true,false}。

5. runtime policy:
   不启动后台recovery；RecoveryRequired时query/normal flush继续，replacer/drop返回ErrMergeRecoveryPending；
   Close允许退出，由下次Open接管。

6. Drop lifecycle:
   Drop先取得exclusive token；Active/RecoveryRequired时不得StopFiles或删除measurement目录。

7. startup recovery:
   active优先且严格fail-closed；legacy processLog/log-remove错误保留log；
   pending不解析、不发布，active完成后处理pending，最后loader清orphan .init。
~~~

D+L 数据写出步骤 1-8、完整 SID boundary、同 SID location non-overlap 和 raw/pre-agg 查询结论均不变。仍不保存 SID，不构建 occupancy/temporal表，不扫描 extent空洞，不增加 per-output txn token/hash。

### 44.10 定向测试与对 CC 第 43 章的最终答复

在第 42.10/43.6 测试清单上至少追加：

1. flush 在 `makeTSSPFiles` 取得旧 unordered指针后暂停；commit 清空 unordered；恢复 flush 后 map仍指向同一对象且查询可见新文件；
2. commit 后 unordered为空，断言 `OutOfOrder[mst]` entry和对象地址不变，仅 `.files` 为空；
3. pre-active `.init`/`.pending` cleanup失败返回 `{false,true}`，不能释放 token或重跑；全部清理成功才返回 `{false,false}`；
4. `RecoveryRequired` 下 raw/count/sum查询和 normal flush继续正确，merge/compact/downsample/Drop均非阻塞拒绝；
5. `Close` 不等待 RecoveryRequired owner；重新 `Open` 在 loader前恢复并回到 Idle；
6. Drop 与 Active/RecoveryRequired并发时不进入 StopFiles/RemoveAll，返回 busy或 `ErrMergeRecoveryPending`；
7. gate不会等待当前 producer自己；所有 early-return释放正确，stale token不能释放后继 owner；
8. 两个 measurement分别存在 active与pending log：active所需 `.init` 不会被 pending cleanup误删；
9. physical cleanup期间有长查询持有 old ref：live-set锁已释放，新查询只见 new，旧查询可完成；
10. feature启用环境满足 local single-writer契约；不满足时拒绝启用。

Codex 对第 43 章的最终答复是：

> **接受第 43 章对第 42 章的源码裁决和事务安全主线，但不接受原文“最终闭环、直接实施”。atomic commit还必须保持 `*TSSPFiles` object identity；四态需覆盖 pre-active cleanup失败；运行期明确采用无后台线程的 RecoveryRequired fail-stop；Drop必须加入同一gate；admission采用非阻塞整段串行；本次明确选择local single-writer部署契约。按第 44.9 节实现后即可进入编码。**

请 CC 复核以下六点：

1. 是否接受 unordered清空时保留 `OutOfOrder[mst]` map entry与对象身份，只原位替换 `.files`？
2. 是否接受 pre-active artifact清理失败属于 `{false,true}`，`{false,false}` 仅表示 namespace已清净、可以安全重跑？
3. 是否接受本次不做后台/runtime recovery，改为 per-mst `RecoveryRequired` fail-stop并由下次 Open接管？
4. 是否接受 `DropMeasurement` 进入同一个 exclusive gate，pending时不得删除 measurement目录？
5. 是否接受非阻塞 per-mst mutation token代替 drain/upgrade，并采用第 44.6 节锁域规则？
6. 是否接受本次 local backend选择 single-writer部署契约，无法保证时禁用该能力？

## 45. 范围纠正后的最终方案：只审查乱序合并写出，并在查询安全前提下尽力切割（提交 CC 审核）

> 本节根据用户对范围的再次确认重写本次实施基线。第 38～44 章保留为历史讨论记录，但其中围绕 normal flush、compact/downsample、DropMeasurement、`*TSSPFiles` object identity、mutation gate、publish transaction 和 startup recovery 的扩展，不再作为本次 merge 文件切割的实施内容或阻塞项。**本次只修改“selected unordered 合并进 ordered output”这条路径；其他流程维持当前行为。**

### 45.1 目标与边界

本次唯一目标是：

~~~text
在乱序文件合并进有序文件时，
保证输出文件可打开、数据不丢不重、raw 与 pre-agg 查询正确，
并在完整 SID 边界上尽可能按 fileSizeLimit 切割文件。
~~~

优先级固定为：

1. 一个 SID 的全部字段、time column、ChunkMeta 和 pre-agg 必须完整；
2. 不覆盖现存文件，不误删未处理的 ordered 文件；
3. `processedOld` 与 `producedNew` 必须准确，一轮 merge 只执行一次现有 `ReplaceFiles`；
4. 在上述条件可局部证明时尝试切割；无法安全切割时继续写当前文件；
5. 不为了提高切割率引入 SID 表、extent 空洞扫描、跨流程 gate 或新的恢复协议。

本章不要求修改查询、flush、compact、downsample、drop 或 recovery 代码。若这些流程存在既有问题，应独立分析和修复，不能继续扩大本次 merge split 的改动范围。

### 45.2 当前乱序合并主流程与本次真正的改动点

当前主流程是：

~~~text
createMergeContext / matchOrderFiles
    -> mergeTool.execute
       -> 对 selected ordered files 逐个运行 ColumnIterator.Run
       -> mergePerformer 复制原 chunk 或合并 unordered
       -> 收集 producedNew
    -> ReplaceFiles(processedOld, producedNew, true)
    -> ReplaceFiles 成功后 deleteUnorderedFiles
~~~

源码对应关系：

- [`matchOrderFiles`](../engine/immutable/merge_out_of_order.go#L235) 选择本轮 ordered 输入；
- [`execute`](../engine/immutable/merge_tool.go#L201) 逐个处理 ordered 文件；
- [`ColumnIterator.Run`](../engine/immutable/column_iterator.go#L96) 在 `WriteOriginal` 与真正 merge 两条路径间分流；
- [`WriteOriginal`](../engine/immutable/merge_performer.go#L429) 复制不需要合并的完整 chunk；
- [`finishSeries`](../engine/immutable/merge_performer.go#L255) 写完 merge SID 的剩余列、time column 和 ChunkMeta；
- [`replaceMergedFiles`](../engine/immutable/merge_out_of_order.go#L104) 把实际处理的旧文件与全部新文件交给现有 `ReplaceFiles`；
- [`mergeTool.merge`](../engine/immutable/merge_tool.go#L154) 只在 ordered replace 成功后删除 selected unordered。

因此本次需要解决的只有三件事：

1. 哪个 ordered output 允许新增 extent；
2. 在哪个 SID 边界执行 rotate；
3. 多个 output 如何与实际处理的 old 文件一次性替换。

### 45.3 推荐基线：D+L（global-last + last-only split）

以下是**待实施规则，不是当前代码现状**。当前 [`matchOrderFiles`](../engine/immutable/merge_out_of_order.go#L235) 的条件实际为 `ctx.tr.Overlaps(min, max) || min > ctx.tr.Max`，并且只在结果为空时 fallback 到列表末文件；它尚未保证每轮都选择并处理 G。

待实施时，从当前 measurement 的全部 live ordered 文件列表中，按现有 [`TSSPFiles.Less`](../engine/immutable/tssp_reader.go#L147) 的逻辑顺序选择唯一最后文件：

~~~text
G = max(all live ordered files, by sequence then extent)
~~~

这里只遍历已经在内存中的、已按 `(sequence, extent)` 排序的 ordered file list，时间复杂度为 `O(order file count)`；不读取全部 ChunkMeta，不保存任何 SID。可在同一趟相邻检查中验证 logical key 唯一，额外空间为 `O(1)`：

~~~text
若两个不同 exact path 拥有相同 (sequence, extent):
    在创建首个 output 前返回错误
~~~

这不是 SID 占用表，也不扫描磁盘 extent 空洞。G 必须保存 exact identity/path，后续不能只用“当前 slice 的最后下标”代替 G 身份。

规则固定为：

~~~text
old != G:
    保持现有 copy/merge 行为
    禁止因 size 触发 rotate

old == G:
    强制进入本轮 order set
    作为唯一 lastFile
    允许接收全部 remaining unordered
    允许在完整 SID 边界尝试 rotate
~~~

即使 G 不满足原时间匹配条件，或者 G 自身不包含 selected unordered 的 SID，也必须：

1. 去重加入 `ctx.order`；
2. 排序后成为唯一最后输入；
3. 加入现有 `m.acquire` 的 ordered path 集合；
4. 真正执行 `ColumnIterator.Run`，不能被 `contains=false && !last` 分支跳过。

选择 G 的原因不是要求不同 SID 在多个 extent 间全局有序，而是保证新增 tail 只出现在当前 ordered logical list 的末尾，不插到任何 surviving ordered owner 之前。

### 45.4 完整文件名规则

假设本轮全局最后文件为：

~~~text
G(old) = 0000002a-0001-00030007.tssp
~~~

G 的首个输出继续沿用当前 merge 命名规则：

~~~text
0000002a-0001-00040007.tssp.init
    -> 0000002a-0001-00040007.tssp
~~~

首输出规则适用于每个真正进入 `ColumnIterator.Run` 的 old，而不只适用于 G。创建任何首输出前都必须同时检查：

| 首输出状态 | 本轮行为 |
|---|---|
| `old.merge == MaxUint16` | 返回错误；禁止回绕 merge，old 与 selected unordered 保持 live |
| 候选 `.tssp.init` 已存在 | 返回错误；不删除、不扫描、不换 logical slot |
| 候选 `.tssp` 已存在 | 返回错误；防止后续 publish 覆盖正式文件 |
| 检查路径遇到非 `NotExist` IO 错误 | 返回错误 |
| 两条路径均不存在 | 只创建该 exact 首输出 |

原因是当前 [`RenameTmpFiles`](../engine/immutable/mms_tables.go#L843) 会把 `.tssp.init` 发布到同 basename 的 `.tssp`，而底层 rename 允许替换既有目标。只检查 `.init` 不能证明不会覆盖正式文件。

达到配置阈值后，只允许尝试精确的下一个 extent：

~~~text
0000002a-0001-00040008.tssp.init
    -> 0000002a-0001-00040008.tssp

0000002a-0001-00040009.tssp.init
    -> 0000002a-0001-00040009.tssp
~~~

tail 命名规则为：

~~~text
candidate.sequence = current.sequence
candidate.level    = current.level
candidate.merge    = current.merge
candidate.extent   = current.extent + 1
~~~

tail 候选处理必须是：

| exact-next 状态 | 本轮行为 |
|---|---|
| `.tssp.init` 与 `.tssp` 均不存在 | 允许尝试创建 exact-next 并切割 |
| `.tssp.init` 已存在 | 设置 `splitDisabled=true`，不切割，继续当前 writer |
| `.tssp` 已存在 | 本轮 merge 返回错误；不得把未解释的正式文件静默当作可忽略冲突 |
| extent 已到上限 | 设置 `splitDisabled=true`，不回绕，继续当前 writer |
| 检查路径遇到非 `NotExist` IO 错误 | 本轮 merge 返回错误，沿用现有失败清理 |
| 预检查通过但实际创建失败 | 本轮 merge 返回错误；不得扫描后续 extent |

必须在 current writer 仍可继续写时完成上述 tail 预检查，再决定是否封存它。当前 [`rotateCompletedFile`](../engine/immutable/merge_performer.go#L233) 先 `NewTSSPFile` 关闭 current writer、后 `InitMergedFile` 创建 next；若 next 冲突，已经无法兑现“继续当前 writer”，因此该顺序必须重做。

在本次约定的同 measurement merge 写出互斥前提下，不新增跨流程 reservation/gate：merge-local 路径先检查 exact `.init + .tssp`，检查到 stale `.init` 时软停止切割；检查通过后若实际创建仍失败，则整轮返回错误。可以在 merge-local 创建动作使用排他创建，但不得为此改变共享 `StreamWriteFile.NewFile` 的语义。

禁止从 `extent+1` 扫描到 `extent+2/+3/...`。`.init` 冲突或 extent 上限意味着本轮对 G 停止继续切割；正式 `.tssp` 或未知 IO 则 fail closed。这仍符合“查询正确优先、切割尽力而为”的目标。

### 45.5 切割边界：在已知 next SID 后 lazy rotate

当前 diff 在 [`ColumnIterator.Run`](../engine/immutable/column_iterator.go#L114) 的两条路径末尾调用 `SwitchNewTSSP`，并由 [`SwitchNewTSSP`](../engine/immutable/column_iterator.go#L169) 当场完成 `finishSeries + rotate`。这个 eager rotate 不是推荐基线：

1. 它允许非 G 文件切割；
2. 它会在最后一个 SID 后仍创建后继 writer，再依赖 empty-file cleanup；
3. 它把“完成当前 SID”和“确定存在下一个 SID”混成一个动作。

正确边界应移动到“开始一个已经确定存在的新 SID 之前”：

~~~text
finish previous SID
    -> 所有普通列写完
    -> time column 写完
    -> WriteCurrentMeta 成功
    -> 已知 next SID 确实存在
    -> old == G && !splitDisabled && Size() >= fileSizeLimit ?
       ├─ 否：继续当前 writer
       └─ 是：尝试 exact current.extent+1
    -> ChangeSid(next SID)
    -> 开始写 next SID
~~~

这可以直接利用 `SeriesChanged(nextSID, ...)` 已经处于“next SID 已知”的事实。实现可以删除 post-chunk `SwitchNewTSSP`；如果为了接口兼容暂时保留，它也只能记录 pending boundary，不能在未知是否还有 next SID 时立即封存文件。

三条路径分别为：

#### 45.5.1 `WriteOriginal` 路径

[`WriteOriginal`](../engine/immutable/merge_performer.go#L429) 完整复制原 chunk data 并调用 `WriteMeta`。该 SID 的原 ChunkMeta 和原 pre-agg 已经随 chunk 一起完成。rotate 只能发生在下一个 `SeriesChanged` 已到达之后、复制下一个 SID 之前。

#### 45.5.2 正常 merge 路径

[`finishSeries`](../engine/immutable/merge_performer.go#L255) 必须先完成：

~~~text
writeRemainCol
    -> writeMergedTime
    -> WriteCurrentMeta
    -> 写完 boundary 前应落入当前 logical owner 的 remaining unordered
~~~

完成后该 SID 的数据、time column、ChunkMeta 和重新生成的 pre-agg 才是完整状态，随后才能在 next SID 前 rotate。

#### 45.5.3 `writeRemain` 路径

G 会通过 [`writeRemain`](../engine/immutable/merge_performer.go#L307) 写入 unordered-only SID。每当回调确认 SID 已变化时：

~~~text
WriteCurrentMeta(previous unordered SID)
    -> previous SID 完整
    -> 当前回调已经证明 next unordered SID 存在
    -> 必要时尝试 G exact-next rotate
    -> ChangeSid(next unordered SID)
~~~

还必须区分两种调用上下文：

1. `SeriesChanged(currentOrderedSID, ...)` 已经知道下一个 ordered SID。它先 `finishSeries(currentOrderedSID)`，其中 `writeRemain(currentOrderedSID)` 写完所有 `< currentOrderedSID` 的 unordered-only SID；返回后，最后一个 remaining SID 已完整且 `currentOrderedSID` 仍是已知 next SID，因此还要在 `ChangeSid(currentOrderedSID)` 前执行一次 lazy size check。
2. `Finish/EOF` 通过 `writeRemain(MaxInt64)` 消费尾部数据，但此时没有真实 next SID，不得执行尾部 size check。

因此要删除当前 [`writeRemain` 尾部](../engine/immutable/merge_performer.go#L339) 的无条件 rotate，并把“最后 remaining SID → 已知 ordered SID”的检查放到 `SeriesChanged` 返回路径；不能简单删掉尾部检查后遗漏这个合法边界。

#### 45.5.4 EOF

EOF 固定为：

~~~text
finish active SID
    -> 写完全部 remaining unordered SID
    -> WriteCurrentMeta
    -> finalize 当前文件
    -> 不再检查 size
    -> 不创建 empty successor
~~~

### 45.6 切割逻辑必须限制在 merge 路径

[`StreamWriteFile.NewFile`](../engine/immutable/stream_downsample.go#L222) 被 merge 与 downsample 路径复用；downsample 的 [`InitFile`](../engine/immutable/stream_downsample.go#L346) 会调用它。compact 则使用独立的 [`StreamIterators.NewFile`](../engine/immutable/stream_compact.go#L444)，并不共用这个函数。当前 diff 在通用 `StreamWriteFile.NewFile` 中加入 `TryNextExtentFile`、全局向后扫描、`O_EXCL` 和更广的 reset，会改变 downsample，仍不符合本次范围。

推荐增加只由 merge performer 调用的局部能力，例如：

~~~text
tryRotateMergedFileExactNext() (rotated bool, err error)
~~~

该能力只负责：

1. 计算当前 merged output 的 exact `extent+1`；
2. 检查 exact `.tssp.init` 与 `.tssp`；
3. 冲突或 extent 上限返回 `rotated=false, err=nil`，并由 performer 设置 `splitDisabled`；
4. 未知 IO、封存失败或创建失败返回 error；
5. 在 current writer 仍可继续时完成候选预检查；只有允许切割才封存当前文件；
6. 成功时加入 `producedNew`、重置 merge writer 状态并打开 exact-next；
7. 不扫描其他 extent，不修改 downsample 的文件选择策略；compact 使用独立实现，本来就不受影响。

merge rotate 后确实需要清理新文件的 trailer、meta-index、ChunkMeta、bloom/pre-agg builder、SID 和 row-count 等 writer-local 状态，但该 reset 应由 merge rotate 路径显式调用或通过局部 option 触发，不能借本次需求改变其他 caller 的语义。

### 45.7 `processedOld` 与一次性替换

当前 diff 将旧的 `(sequence, extent)` 反向匹配改为显式 `processedOld`，方向正确：

~~~text
processedOld:
    只追加真正成功执行完 ColumnIterator.Run 的 old ordered file

producedNew:
    收集首输出与所有成功切割 tail
~~~

随后仍只调用一次现有：

~~~go
ReplaceFiles(mst, processedOld, producedNew, true)
~~~

对应 [`replaceMergedFiles`](../engine/immutable/merge_out_of_order.go#L104)。这样：

1. 被 `contains=false && !last` 跳过的 ordered 文件不会误删；
2. 一个 old 产生多个 output 时，全部 output 一次加入 live set；
3. 不存在“每切一个文件就执行一次 replace”；
4. `ReplaceFiles` 失败时不进入 [`deleteUnorderedFiles`](../engine/immutable/merge_tool.go#L169)；
5. 替换流程、compact log 和 unordered 删除顺序继续沿用现状，本次不重写事务协议。

### 45.8 查询与 pre-agg 正确性证明

本方案不要求 extent 文件之间按不同 SID 的大小或时间全局有序。查询正确性需要保持的是：

~~~text
单文件内 SID 有序；
单 SID 的时间列有序；
一个正在写出的 SID 不跨两个新 output；
同 SID 的既有 ordered location ranges 不因本次切割产生重叠或倒置。
~~~

证明分为三部分：

1. **非 G 文件**：不新增 extent，只在原 `(sequence, extent)` logical slot 内由 old generation 替换为 new generation，不会把某个 SID owner 插到 survivor 后面；
2. **G 文件**：只在完整 SID 边界切割，所以同一个 SID 的全部列、time 和 meta 留在同一个新文件；tail 只追加在 global-last 后面；
3. **remaining unordered**：只有 G 作为 `lastFile` 消费最后剩余数据，unordered-only SID 也只在完整 SID 边界切割，不会落进中间 logical slot。

两种写入方式的 pre-agg 都保持一致：

- `WriteOriginal` 复制原数据和原 ChunkMeta/pre-agg；
- 真正 merge 的 SID 由现有 writer 按合并后的列和 time 重新生成 pre-agg。

查询侧 [`AddLocations`](../engine/tsm_merge_cursor.go#L100) 会枚举命中的文件；ordered [`LocationCursor.Less`](../engine/immutable/location_cursor.go#L62) 按 location 最小时间排序，[`ReadData`](../engine/immutable/location_cursor.go#L200) 逐 location 读取，[`ReadMeta`](../engine/immutable/location_cursor.go#L112) 使用各 chunk 的 pre-agg。因此在输入 ordered 布局本来合法、且本次保持上述完整 SID/non-overlap 不变量的前提下：

- raw ascending/descending 不会新增反序、漏行或重复；
- `ORDER BY time LIMIT/OFFSET` 的有序输入不变；
- count、sum、min、max、first、last 不会因本次切割重复累计；
- 不带 `ORDER BY` 的 LIMIT 继续沿用现有未定义顺序语义；
- 不需要为本功能修改查询代码。

### 45.9 对当前 diff 的保留、重做与移出范围

| 当前改动 | 本次结论 | 原因 |
|---|---|---|
| `processedOld` 显式记录 | **KEEP** | 精确替换实际处理的 old 文件 |
| `producedNew` 支持多个输出并一次 Replace | **KEEP** | 文件切割的必要能力 |
| `Size()` 在完整 SID 边界参与判断 | **KEEP/REBASE** | 改到 G 的 lazy boundary 使用 |
| `NewTSSPFile` nil/empty 防御 | **只 KEEP merge performer 的 nil guard** | EOF 不制造空后继；不得借本次改变 downsample 共用的 empty cleanup 语义 |
| 两条路径末尾 eager `SwitchNewTSSP` | **REPLACE** | 改为 next SID 已知后的 lazy boundary |
| 所有 old 文件都允许切割 | **DELETE** | 只有 exact G 可以切割 |
| `reservedExtents` | **DELETE** | D+L 不需要占用表 |
| `TryNextExtentFile` 向后扫描 | **DELETE** | exact-next 冲突即停止切割 |
| `writeRemain` 最后一个 SID 后 rotate | **DELETE** | EOF 不创建后继文件 |
| 通用 `StreamWriteFile.NewFile` 行为重构 | **移出本次范围** | 避免改变 downsample；compact 使用独立 `StreamIterators.NewFile` |
| 查询侧 `AddLocationsWithLimit` 与同 SID 跨 extent 测试 | **移出本次范围** | D+L 的写侧不变量正是 size split 不拆同一个 SID |
| 查询侧为 split 增加的新逻辑 | **本方案不要求** | 写侧保证单 SID 不被 size boundary 拆分 |
| 第 38～44 章新增 transaction/gate/recovery 设计 | **不作为本次 blocker** | 其他流程保持现状 |

与 merge split 无直接关系的 `WriteMeta`、读取、query 优化或其他通用修复应拆分到独立改动，本章不对其作合入要求。

### 45.10 最小测试矩阵

#### 45.10.1 切割与命名

1. 非 G 超过阈值：不切割，输出文件可重新打开；
2. G 超过阈值且存在 next SID：只创建 exact `extent+1`；
3. G 连续多次达到阈值：依次创建 exact `extent+1`、`extent+2`，每次都有真实 next SID，绝不跳号；
4. G 最后一个 SID 完成后超过阈值：不创建 empty successor；
5. exact-next `.tssp.init` 已存在：不封存 current、不切、不扫描，后续 SID 继续写入 current writer；
6. exact-next `.tssp` 已存在：本轮硬失败，不覆盖、不扫描；
7. exact-next 检查遇到未知 IO、或预检查后创建失败：本轮硬失败，不扫描；
8. extent 到达上限：不回绕，继续当前 writer；
9. 任一 processed old 的首输出 `.tssp.init` 或 `.tssp` 已存在：在写入该 output 前硬失败；
10. old 的 merge 到达上限：首输出前硬失败，不回绕；
11. live ordered list 存在重复 `(sequence, extent)` exact owner：首输出前硬失败；
12. 多列 SID：任何列之间、普通列与 time column 之间都不得切文件。

#### 45.10.2 三条 SID 路径

1. `WriteOriginal` SID 在 copy + WriteMeta 后保持完整；
2. ordered + unordered merge SID 的数据、time、ChunkMeta 和 pre-agg一致；
3. 多个 unordered-only SID 通过 G 的 `writeRemain` 写出，并只在相邻 SID 间切割；
4. `writeRemain(currentOrderedSID)` 的最后 remaining SID 写完后，可在已知 `currentOrderedSID` 前切割；
5. EOF 的最后 remaining SID 写完后不切割；
6. G 原本不匹配或 `Contains=false` 时仍被处理，并接收全部 residual unordered。

#### 45.10.3 替换

1. 中间 ordered 文件被跳过时不进入 `processedOld`，replace 后仍存活；
2. 一个 G 产生多个 tail 时，所有 `producedNew` 只通过一次 `ReplaceFiles` 加入；
3. output 创建或 `ReplaceFiles` 注错时，不删除 selected unordered，不覆盖 foreign 文件；
4. replace 成功后，old 被准确移除，所有新文件均可重新打开。

#### 45.10.4 查询

1. raw ascending/descending；
2. `ORDER BY time LIMIT/OFFSET`；
3. count、sum、min、max、first、last；
4. 有 pre-agg 与禁用 pre-agg 两条路径结果一致；
5. 逐 SID 验证时间严格有序、数据不重不漏；
6. 断言任一 SID 不会因为本次 size boundary 同时出现在两个新 output 文件。

### 45.11 提交 CC 审核的最终结论

本次最终推荐固定为：

> **只审查乱序合并到有序的写出路径。选择并验证全部 live ordered 文件中的唯一 global-last G；非 G 永不因大小切割；G 只在完整 previous SID 已结束且 next SID 已知时尝试 exact `extent+1`。tail 的 stale `.init` 冲突或 extent 上限只停止切割并继续 current writer；正式 `.tssp`、未知 IO、首输出冲突或 merge 回绕风险则整轮失败；绝不向后扫描。所有成功 output 与实际 `processedOld` 只调用一次现有 `ReplaceFiles`，成功后沿用现有流程删除 selected unordered。查询正确性由“单 SID 不被切开、G tail 只追加在 logical last 后、原数据或重建 pre-agg 与 chunk 一致”保证。其他流程和既有恢复/互斥协议保持现状。**

请 CC 只围绕以下问题审核，不再扩展到其他流程：

1. 是否接受第 45.1 节范围，并撤销第 44 节扩展项作为本次 blocker？
2. 是否接受从全部 live ordered file list 选择唯一 G，但不扫描全部 SID/ChunkMeta？
3. 是否确认“非 G 不切 + G exact-next”足以避免新增 tail 插入 survivor 中间？
4. 是否接受 rotate 必须发生在 next SID 已知的 lazy boundary，EOF 后不再 rotate？
5. 是否接受 tail `.init` 冲突/extent 上限时停止切割并继续 current writer，而正式 `.tssp`/未知 IO/首输出冲突 hard fail，且所有情况都禁止向后扫描？
6. 是否确认 `WriteOriginal`、正常 merge、`writeRemain` 三条路径都能保持单 SID 数据/time/meta/pre-agg完整？
7. 是否确认 `processedOld + producedNew` 一次调用现有 `ReplaceFiles`，而不是逐个 output replace？
8. 是否确认在输入 ordered 布局合法的前提下，第 45.8 节足以保证 raw、LIMIT+ORDER BY、first/last 和 pre-agg 查询不因本次切割出错？
9. 是否同意 merge-specific exact-next helper 不应改变 downsample 使用的通用 `StreamWriteFile.NewFile` 语义，而 compact 已因使用独立 `StreamIterators.NewFile` 自然不受影响？
10. 是否接受第 45.10 节测试全部通过作为本次 merge split 的合入条件？

## 46. CC 对 Codex 第 44/45 章的 1+1 复核与范围收缩后最终基线收敛（2026-07-23）

本节由 CC 对 Codex 第 44/45 章做 1+1 复核：派发两个证据 agent 分别验证 44.2 P0（*TSSPFiles 对象身份）+ 45 章范围收缩安全性 + 45.9 处置表，与 45.3/45.5/45.6/45.8/45.10（D+L 证明 + lazy rotate + merge-specific helper + 查询正确性 + 测试矩阵），按 26.7 中性协议区分源码事实/控制流/设计证明。结论：**Codex 第 44/45 章核心方向全部正确。44.2 P0 真实（*TSSPFiles 对象身份脱钩→查询漏数据）。45 章范围收缩合理（D+L 不加剧既有 bug，查询正确性仅依赖写出侧不变量）。两处 CALIBRATE（45.3 文档表述 + 45.6 compact/downsample 指称错误）。CC 全盘接受 45 章作为最终实施基线。**

### 46.1 逐条裁决

| 项 | CC 终裁 | 证据类型 | 关键依据 |
|---|---|---|---|
| 44.2 P0 *TSSPFiles 对象身份 | **ACCEPT（真实 P0）** | 源码事实+控制流 | `ts_mms_tables.go:142-148` makeTSSPFiles 取指针后释放 m.mu；`:177-189` append 时无 m.mu；`merge_out_of_order.go:289-294` delete map entry；GetBothFilesRef `:429` 查不到→漏数据 |
| 45 章范围收缩（不修 transaction/recovery） | **ACCEPT** | 设计证明 | 38.2/38.3/42.2 均既有 bug（源码已存在）；D+L 不改变 replace/delete/log 顺序→不加剧；45.8 查询正确性仅依赖写出侧不变量 |
| 45.9 处置表 | **ACCEPT** | 文档对照 | 11 项处置与源码事实 + 45.5/45.6/45.8 证明一致 |
| 45.3 G 选择 | **ACCEPT（CALIBRATE：文档表述）** | 源码事实 | `TSSPFiles.Less`（`:147`）按 (seq,extent)；`Order` map 内存可遍历。**CALIBRATE**：当前 `matchOrderFiles`（`:235`）未选 G，45.3 是待实施改动非现状描述 |
| 45.5 lazy rotate 三路径 | **ACCEPT** | 源码事实+控制流 | eager rotate 在 `column_iterator.go:124,143`；writeRemain 已实现 lazy（`:316-330`）但尾部多余 rotate（`:339-346`）需删除；EOF `Finish`（`:205`）不 rotate |
| 45.6 merge-specific helper | **ACCEPT（CALIBRATE：compact 指称错误）** | 源码事实 | `StreamWriteFile.NewFile` 被 merge + **downsample**（`stream_downsample.go:351` InitFile）共用；**compact 用独立 `StreamIterators.NewFile`**（`stream_compact.go:444`）不受影响。文档"compact 共用"应改为"downsample 共用" |
| 45.8 查询正确性仅依赖写出侧 | **ACCEPT** | 源码事实+设计证明 | `LocationCursor.Less`（`:62`）按 minTime；`ReadData`（`:200`）逐 location；证明基于非 G 不新增 extent + G 完整 SID + G tail 落 logical last + pre-agg 一致——全为写出侧不变量 |
| 45.10 测试矩阵 | **ACCEPT（建议补多 tail 链式）** | 文档对照 | 覆盖 D+L 全路径 + 三 SID 路径 + 替换 + 查询；建议补"多 tail 链式切割"用例 |

统计：`ACCEPT=8`（含 2 CALIBRATE）、`REJECT=0`。

### 46.2 44.2 P0 与 45 章范围的边界

证据 agent 提出重要边界判断：

- **44.2 P0（*TSSPFiles 对象身份）发生在 `deleteUnorderedFiles`（merge_out_of_order.go:264-296）内**，而 45 章明确"保留现有 deleteUnorderedFiles 顺序"。
- D+L 不修不加剧 44.2，但也不修——D+L 上线后 44.2 仍存在。
- **44.2 应作为 merge split 的并行前置项**（不是 blocker，但应在同一 PR 簇内修复），而非"既有 bug 独立修复"——因为它直接发生在 merge 写出路径的 `deleteUnorderedFiles` 中，与 45.8 查询正确性证明形成隐性矛盾（45.8 假设 newFiles 可被查询到，但 44.2 会让 unordered 侧 newFiles 丢失）。

**CC 立场**：44.2 作为独立 P0 修复，与 D+L 同 PR 簇但不相互阻塞。38.2/38.3/42.2 作为独立 P1 task。

### 46.3 两项 CALIBRATE

#### 45.3 CALIBRATE：文档应标注"选 G"是待实施改动
当前 `matchOrderFiles`（merge_out_of_order.go:235）只按 `tr.Overlaps(min,max)` 选 order，并在空时 fallback 到 `files[Len()-1]`——**未**强制选 global-last G。45.3 文档行文"从当前 measurement 全部 live ordered 文件列表中…选择"读起来像已实现。**CC 建议文档明确标注"待实施"。**

#### 45.6 CALIBRATE：compact 不共用 StreamWriteFile.NewFile
证据 agent 发现 **compact 用独立 `StreamIterators.NewFile`**（stream_compact.go:444），不含 TryNextExtentFile/O_EXCL/reservedExtents。`StreamWriteFile.NewFile` 被 merge + **downsample**（stream_downsample.go:351 InitFile 路径）共用。文档 45.6 称"compact/downsample 共用"中 compact 部分**事实错误**，应改为"downsample 共用"。merge-specific helper 必要性不变。

### 46.4 回答 45.11 十问

1. **是否接受 45.1 范围，撤销 44 节扩展项作为本次 blocker？**
   **ACCEPT。** 38-44 章 transaction/gate/recovery 扩展移出本次范围。D+L 查询正确性仅依赖写出侧不变量（45.8 证实），不依赖 transaction/recovery。

2. **是否接受从全部 live ordered file list 选 G，不扫描 SID/ChunkMeta？**
   **ACCEPT。** O(order file count)，内存遍历，不读 ChunkMeta。（CALIBRATE：当前 matchOrderFiles 未实现，是待实施改动。）

3. **是否确认"非 G 不切 + G exact-next"足以避免新增 tail 插入 survivor 中间？**
   **ACCEPT。** G=max(seq,extent)→G 后无 survivor→exact+1 不跨越（32.4/33.3 已证）。

4. **是否接受 rotate 必须发生在 next SID 已知的 lazy boundary，EOF 后不再 rotate？**
   **ACCEPT。** 45.5 三条路径（WriteOriginal/merge/writeRemain）均落在"previous SID 完整 + next SID 已知"窗口。EOF finalize 不 rotate（45.5.4）。

5. **是否接受 exact-next 冲突时停止切割并继续当前 writer，禁止向后扫描？**
   **ACCEPT。** 候选 .init/.tssp 任一存在→splitDisabled=true→继续当前 writer。不扫描 extent+2/+3。

6. **是否确认三条路径都能保持单 SID 数据/time/meta/pre-agg 完整？**
   **ACCEPT。** WriteOriginal 复制原 chunk+meta+pre-agg；merge 路径 finishSeries 写完剩余列+time+meta+pre-agg；writeRemain 在 SID 变化时 WriteCurrentMeta(previous) 后才 rotate。

7. **是否确认 processedOld + producedNew 一次调用现有 ReplaceFiles？**
   **ACCEPT。** 45.7 证实：一次 replaceMergedFiles→一次 ReplaceFiles，不逐 output replace。

8. **是否确认输入 ordered 合法前提下 45.8 足以保证 raw/LIMIT/first-last/pre-agg 查询不因切割出错？**
   **ACCEPT。** 45.8 证明基于写出侧不变量（非 G 不新增 extent + G 完整 SID + G tail 落 logical last + pre-agg 一致），不依赖 transaction/recovery。

9. **是否同意 merge-specific helper 不改 compact/downsample 通用 NewFile？**
   **ACCEPT（CALIBRATE：compact 不受影响）。** compact 用独立 StreamIterators.NewFile（stream_compact.go:444），不受 StreamWriteFile.NewFile 改动影响。downsample（InitFile 路径）共用 StreamWriteFile.NewFile，merge-specific helper 避免影响它。

10. **是否接受 45.10 测试全部通过作为合入条件？**
    **ACCEPT（建议补多 tail 链式切割用例）。** 测试矩阵覆盖 D+L 全路径 + 三 SID 路径 + 替换 + 查询。建议补"G 产生 extent+1 后再次达阈值切 extent+2"用例。

### 46.5 CC 最终立场（范围收缩后最终基线收敛）

1. **全盘接受 Codex 第 44/45 章**（8 ACCEPT 含 2 CALIBRATE，0 REJECT）。
2. **接受 45 章范围收缩**：只改"乱序合并写出"路径（选 G + lazy rotate + exact-next + processedOld + 一次 ReplaceFiles）。38-44 章 transaction/gate/recovery 扩展移出本次范围。
3. **44.2 P0（*TSSPFiles 对象身份）作为独立 P0 与 D+L 同 PR 簇修复**——不阻塞 D+L 但应在同簇。
4. **38.2/38.3/42.2 作为独立 P1 task**——既有 bug，D+L 不加剧。
5. **两项 CALIBRATE**：45.3 文档标注"待实施"；45.6 compact→downsample 指称修正。
6. **采用 45.9 处置表**（KEEP/REPLACE/DELETE/移出范围）。
7. **D+L 步骤 1-8 不变**，查询正确性仅依赖写出侧不变量（45.8）。

### 46.6 进入实施

设计 review 收敛于 **D+L（global-last + last-only split）+ 45.9 处置表 + 45.10 测试矩阵**。实施清单：

~~~text
1. 选 G：从全部 live ordered file list 按 (seq,extent) 选唯一 global-last
2. 非 G 永不切割；G 强制加入 ctx.order + acquire + 唯一 lastFile
3. 删除 reservedExtents/TryNextExtentFile/向后扫描
4. lazy rotate：next SID 已知时仅 G 检查 size→exact extent+1→冲突 splitDisabled
5. 三条路径（WriteOriginal/merge/writeRemain）保持单 SID 完整
6. EOF finalize 不 rotate 不创建空后继
7. processedOld + producedNew 一次 ReplaceFiles
8. merge-specific helper 不改通用 NewFile
9. 测试（45.10 四类 + 多 tail 链式建议）
~~~

不保存 SID，不构建 occupancy/temporal 表，不扫描 extent 空洞，不引入 txn token/hash，不修 transaction/recovery/gate/Drop。

**CC 立场：D+L 范围收缩后最终基线收敛，进入实施。** 请 Codex 确认 46.4 十问回答 + 46.5 立场。确认后设计讨论正式结束，按 46.6 编码。

## 47. Codex 对 CC 第 46 章的 1+1 终审：保留 D+L，撤回“全盘接受/直接实施”（2026-07-23）

本节只审查 **selected unordered 合并进 ordered output 时，在查询正常前提下尽可能切割文件**。本轮采用 1+1：证据审查先逐项核对第 45/46 章与当前工作树；独立 reviewer 在未读取证据结论前先做初审，随后再对证据报告做正式二审。两边对 D+L 主线、当前 diff 的偏差和文件名覆盖风险结论一致。

本节是对第 46.5/46.6 的最终校准；第 46 章作为审核历史保留。

### 47.1 总裁决

| 第 46 章结论 | 本轮裁决 | 原因 |
|---|---|---|
| D+L：global-last G + 仅 G 切割 | **ACCEPT** | 在不扫描 SID 的前提下，能够把新增 tail 限制在 ordered logical tail |
| 45.3 的 G 选择是待实施而非现状 | **ACCEPT** | 当前 `matchOrderFiles` 尚未强制加入 G |
| compact 不共用 `StreamWriteFile.NewFile`，downsample 才共用 | **ACCEPT** | compact 使用独立 `StreamIterators.NewFile` |
| 补多 tail 链式测试 | **ACCEPT** | 需要验证 extent 连续递增且每次都在真实 next SID 前切割 |
| `*TSSPFiles` 对象身份存在并发风险 | **源码事实 ACCEPT；纳入本次或要求同 PR REJECT** | 反例依赖并发 flush，属于用户已明确排除的旁路问题 |
| `.tssp.init` 与 `.tssp` 冲突都软停止切割 | **REJECT** | stale init 可软停；正式 final 必须 fail closed，且首输出也要双路径检查 |
| 当前 rotate 能在 next 冲突后继续 current writer | **REJECT** | 当前代码先封存 current、后创建 next；冲突时 current writer 已关闭 |
| 第 46.6 可直接进入编码/当前 diff 已接近基线 | **REJECT** | 当前仍有 eager rotate、非 G 切割、extent 扫描、EOF rotate、共享 writer 与查询侧扩围 |

因此准确结论是：

> **D+L 核心可实施，但必须先以本节补齐 unique G、首输出与 tail 的冲突矩阵、preflight-before-seal 和三个 lazy SID 边界。第 46 节“8 ACCEPT、0 REJECT、全盘接受”应撤回。**

### 47.2 44.2 是独立问题，不与本次 D+L 捆绑

第 46.2 对源码竞态的描述基本成立：

1. [`makeTSSPFiles`](../engine/immutable/ts_mms_tables.go#L141) 可在释放 `m.mu` 后保留旧 `*TSSPFiles` 指针；
2. [`AddBothTSSPFiles`](../engine/immutable/ts_mms_tables.go#L167) 随后才锁该对象并 append；
3. [`deleteUnorderedFiles`](../engine/immutable/merge_out_of_order.go#L264) 在空时删除 `OutOfOrder[mst]` map entry；
4. 查询实际通过 [`GetBothFilesRef`](../engine/immutable/mms_tables.go#L425) 重新查当前 map。第 46.1 将该位置写成 `ts_mms_tables.go:429`，文件名不准确。

但第 46.2 把它解释成与 45.8 的“隐性矛盾”不成立。两类文件和 live set 不同：

~~~text
D+L 本轮 producedNew（ordered）:
measurement/0000002a-0001-00040007.tssp
measurement/0000002a-0001-00040008.tssp
    -> 现有 ReplaceFiles(..., isOrder=true)
    -> 加入 Order[mst]

并发 flush 反例中的新文件（unordered）:
measurement/out-of-order/0000002b-0000-00000000.tssp
    -> 可能 append 到已经脱离 OutOfOrder[mst] map 的旧对象
~~~

[`ReplaceFiles`](../engine/immutable/mms_tables.go#L730) 会在 ordered 集合中删除 `processedOld` 并加入全部 `producedNew`。44.2 丢失的是另一个并发 flush 追加到旧 unordered 对象的文件，不会使本轮 D+L 的 ordered `producedNew` 从 `Order[mst]` 消失。

所以本次立场固定为：

- 44.2 可另立问题跟踪；
- 不作为 merge split blocker；
- 不要求与 D+L 同 PR；
- 本次不修改 flush、对象身份、transaction、recovery 或 gate。

### 47.3 G 的最小、可验证选择

CC 对“当前尚未实现 G”的校准正确，但其源码描述少了一半条件。当前 [`matchOrderFiles`](../engine/immutable/merge_out_of_order.go#L235) 的真实选择条件是：

~~~go
ctx.tr.Overlaps(min, max) || min > ctx.tr.Max
~~~

只有 `ctx.order` 为空时才 fallback 到列表最后文件。它不能保证每轮都处理 global-last。

待实施规则为：

1. 在 measurement 全部 live ordered 文件的已排序列表上做一次 `O(order file count)` 线性遍历；
2. 比较键严格沿用 [`TSSPFiles.Less`](../engine/immutable/tssp_reader.go#L147) 的 `(sequence, extent)`；
3. 相邻文件若具有相同 `(sequence, extent)` 但 exact path 不同，在创建首个 output 前硬失败；
4. 保存 G 的 exact identity/path；
5. 去重强制加入 `ctx.order` 和 acquire path；
6. `execute` 用 exact identity 判断 `lastFile/canSplit`，不能只依赖 `i == order.Len()-1`；
7. 即使 G 不匹配原时间条件或 `contains=false`，也必须真正执行 `ColumnIterator.Run`；
8. 非 G 永不因 size rotate。

重复 logical owner 的完整文件名示例：

~~~text
measurement/0000002a-0001-00030007.tssp
measurement/0000002a-0002-00090007.tssp
~~~

两者 level/merge/path 不同，但 `(sequence=0x2a, extent=0x0007)` 相同；现有比较器认为二者互不小于。静默任选一个为“唯一 G”无法证明命名和 logical tail 安全，因此只报错，不修复、不扫描 SID。

该检查利用已经排序的 file list，只需保存前一个 logical key 和 G exact path，额外空间 `O(1)`；它不是 `MergePreflight` SID 表，也不是 extent occupancy 表。

### 47.4 首输出与 tail 必须采用不同冲突策略

以 G 为例：

~~~text
old:
measurement/0000002a-0001-00030007.tssp

first output:
measurement/0000002a-0001-00040007.tssp.init
    -> measurement/0000002a-0001-00040007.tssp

first tail:
measurement/0000002a-0001-00040008.tssp.init
    -> measurement/0000002a-0001-00040008.tssp
~~~

#### 47.4.1 每个 processed old 的首输出

首输出没有“继续旧 output writer”的退路，因此在写入前统一 fail closed：

| 条件 | 行为 |
|---|---|
| `old.merge == MaxUint16` | **hard fail**，禁止回绕到 0 |
| exact 首输出 `.tssp.init` 已存在 | **hard fail** |
| exact 首输出 `.tssp` 已存在 | **hard fail** |
| Stat 返回非 `NotExist` 错误 | **hard fail** |
| 两条路径均不存在 | 只创建 exact `(old.sequence, old.level, old.merge+1, old.extent)` |

这个规则适用于所有真正 process 的 old，不只适用于 G。不得换 sequence、不得换 extent、不得向后扫描。

#### 47.4.2 G 的 exact-next tail

| 条件 | 行为 |
|---|---|
| `current.extent == MaxUint16` | `splitDisabled=true`，继续 current writer |
| exact-next `.tssp.init` 已存在 | `splitDisabled=true`，继续 current writer |
| exact-next `.tssp` 已存在 | **hard fail** |
| Stat 返回非 `NotExist` 错误 | **hard fail** |
| 两条路径均不存在 | 允许尝试 exact `extent+1` |
| 预检查通过后实际创建失败 | **hard fail**，不再继续、不扫描 |

正式 `.tssp` 与 stale `.init` 不能等价处理。loader 会枚举 measurement 目录中的所有 `.tssp`；一个不在 live set、却占用 exact-next 名称的 final 是未解释的 committed-looking 文件。静默忽略它无法证明重启后的查询正确性。

而 `.init` 不会被当作正式查询文件；在 current writer 尚未封存时发现它，可以安全地放弃本轮后续切割并继续写 current。

当前代码只检查 `.init`，并通过 `TryNextExtentFile` 向后寻找 extent。若磁盘已有：

~~~text
measurement/0000002a-0001-00040008.tssp
~~~

却没有同名 `.init`，当前逻辑仍可能创建：

~~~text
measurement/0000002a-0001-00040008.tssp.init
~~~

随后 [`RenameTmpFiles`](../engine/immutable/mms_tables.go#L843) 发布时，底层 rename 允许替换既有目标。这是确定性的覆盖风险，必须通过首输出和 tail 的 `.init + final` 双检查消除。

### 47.5 rotate 的顺序：先判断，后封存

当前 [`rotateCompletedFile`](../engine/immutable/merge_performer.go#L233) 的顺序是：

~~~text
NewTSSPFile(true)
    -> Flush
    -> 关闭并清空 current writer
AppendMergedFile
InitMergedFile(next)
~~~

因此若 `InitMergedFile(next)` 发现冲突，“继续 current writer”已经不可能。推荐的 merge-local 顺序固定为：

~~~text
maybeRotateBefore(nextSID):
    if !isExactG || splitDisabled || Size() < fileSizeLimit:
        return continue-current

    preflight exact-next .init + .tssp while current writer is still writable

    stale .init / extent upper bound:
        splitDisabled = true
        return continue-current

    final exists / unknown IO:
        return error

    seal current
    append current to producedNew
    reset merge-writer-local state
    create/open exact-next only
    creation failure:
        return error
~~~

本次遵循用户给出的同 measurement merge 写出互斥前提，不引入跨流程 reservation、occupancy 表或 gate。merge-local 创建可以使用排他创建防止打开已有 `.init`；如果预检查后创建仍失败，整轮失败并沿用现有 temp cleanup，不能假装 current writer 仍可继续。

### 47.6 三种 next-known SID 边界与 EOF

“完整 SID 边界”必须落到真实调用点：

1. **ordered/previous → ordered/current**
   [`ColumnIterator.Run`](../engine/immutable/column_iterator.go#L96) 已经读到 `currentSID` 后调用 `SeriesChanged(currentSID, ...)`。`SeriesChanged` 先完整 finish previous，再在开始 current 前调用 `maybeRotateBefore(currentSID)`。

2. **remaining A → remaining B**
   [`writeRemain`](../engine/immutable/merge_performer.go#L307) 的 callback 看到 SID 从 A 变为 B 时，先 `WriteCurrentMeta(A)`；A 完整且 B 已知，此时可调用 `maybeRotateBefore(B)`。

3. **最后 remaining → 已知 ordered/current**
   `SeriesChanged(currentSID, ...)` 内的 `writeRemain(currentSID)` 会写完所有 `< currentSID` 的 unordered-only SID。它返回后，最后 remaining 已完整且 `currentSID` 仍已知，因此还要在 `ChangeSid(currentSID)` 前执行一次 `maybeRotateBefore(currentSID)`。

4. **EOF**
   [`Finish`](../engine/immutable/merge_performer.go#L205) 通过 `finishSeries(MaxInt64)` 写完 active SID 和全部 remaining，但不存在真实 next SID。只 finalize current，不做 size rotate，不创建 empty successor。

因此：

- 删除 `ColumnIterator.Run` 两条 post-chunk `SwitchNewTSSP`；
- 删除 `writeRemain` 尾部无条件 rotate；
- 保留 remaining A→B 的 callback boundary；
- 把 remaining→ordered 的检查放在 `SeriesChanged`；
- `Finish` 不调用 `maybeRotateBefore`。

这同时覆盖 `WriteOriginal`、正常 merge 和 unordered-only SID，且不会把一个 SID 的普通列、time、ChunkMeta 或 pre-agg 拆到两个新 output。

### 47.7 merge-local 实现与一次性替换

merge split 的局部实现只需要：

1. first-output exact 双路径检查；
2. `maybeRotateBefore(nextSID)`；
3. exact-next path 计算；
4. `splitDisabled`；
5. merge writer-local reset；
6. `processedOld` 与 `producedNew`。

不需要：

- 保存 SID；
- 构建全量 `(sequence, extent) → owners` occupancy 表；
- 扫描磁盘 extent 空洞；
- `reservedExtents`；
- `TryNextExtentFile`；
- `maxExtentProbeLimit`；
- 修改 compact/downsample 的文件选择；
- 修改查询 cursor。

compact 使用 [`StreamIterators.NewFile`](../engine/immutable/stream_compact.go#L444)，本来就独立。downsample 会调用共享 [`StreamWriteFile.NewFile`](../engine/immutable/stream_downsample.go#L222)，所以本次不得把 merge 的扫描、排他策略或全量 reset 塞进该通用函数。

替换规则继续采用当前 diff 中方向正确的部分：

~~~text
processedOld:
    仅在对应 ColumnIterator.Run 成功后加入

producedNew:
    收集每个首输出与 G 的全部成功 tail

commit:
    ReplaceFiles(mst, processedOld, producedNew, true) // 仅一次
    成功后才 delete selected unordered
~~~

[`ReplaceFiles`](../engine/immutable/mms_tables.go#L730) 不得按 tail 逐个调用。任何 preflight、写入、rotate 或 replace 错误都不得删除 selected unordered。

### 47.8 查询与 pre-agg：条件性 ACCEPT，不改查询侧

查询安全的必要前提为：

1. 输入 ordered 布局本来合法；
2. logical `(sequence, extent)` 唯一检查通过；
3. exact G 强制 Run；
4. 非 G 不切；
5. G 只在 next-known 的完整 SID 边界切；
6. writer-local ChunkMeta、bloom、pre-agg、SID 与 row-count 状态被正确 reset；
7. 全部 output 一次加入 ordered live set。

满足这些条件后：

- `WriteOriginal` 复制原 chunk data 与原 ChunkMeta/pre-agg；
- 正常 merge 与 `writeRemain` 继续由 writer 根据实际输出重建 pre-agg；
- size boundary 不会使同一个 SID 横跨两个新 output；
- raw ascending/descending、`ORDER BY time LIMIT/OFFSET`、first/last 和 pre-agg 聚合不因本次切割新增错序、漏行或重复。

因此当前 [`tsm_merge_cursor.go`](../engine/tsm_merge_cursor.go#L118) 中“split 后同 SID 可能跨多个 extent”的新注释及配套查询改动与 D+L 不变量相反，应移出本次。查询侧无需为该功能改动。

### 47.9 当前工作树的处置清单

| 当前改动 | 最终处置 |
|---|---|
| `processedOld` 在 `Run` 成功后记录 | **KEEP** |
| performer 收集多个 `producedNew`，最后一次 Replace | **KEEP** |
| `GetFileSizeLimit` 与在安全 SID 边界使用 `Size()` | **KEEP/REBASE** |
| merge performer 的 `file != nil` 防御 | **KEEP** |
| global-last G 选择、exact identity 与强制 Run | **IMPLEMENT** |
| merge 上限检查、first-output 双路径检查 | **IMPLEMENT/REBASE 到 merge-local** |
| merge writer-local reset | **REBASE 到 merge-local rotate** |
| post-chunk `SwitchNewTSSP` 与接口扩展 | **DELETE** |
| 所有 old 都可按 size rotate | **DELETE** |
| `writeRemain` EOF 后 rotate | **DELETE** |
| `reservedExtents`、`TryNextExtentFile`、向后扫描、probe config | **DELETE** |
| 通用 `StreamWriteFile.NewFile` 的扫描/O_EXCL/reset 语义重构 | **移出本次** |
| 共享 `NewTSSPFile` 的 empty cleanup 改语义 | **移出本次；只保留 merge-local nil 防御** |
| `AddLocationsWithLimit` 与同 SID 逆序 location 测试 | **移出本次** |
| 其他共享 writer/读取修复 | **不在本次评价或合入条件中** |

所以当前代码还不能按第 46.6 直接合入；它至少仍存在以下本次问题：

1. 未强制选择并处理 exact G；
2. 非 G 也会切割；
3. post-chunk eager rotate；
4. EOF 可能创建空后继；
5. exact-next 冲突会向后扫描；
6. 只查 `.init`，存在正式文件覆盖风险；
7. 先封存 current，无法在 benign 冲突后继续；
8. 改动共享 downsample writer 与查询路径。

### 47.10 最小合入测试

#### 命名与切割

1. 非 G 超阈值不切；
2. G 依次产生 exact `extent+1`、`extent+2`，不跳号；
3. G 无时间匹配或 `contains=false` 仍强制 Run；
4. tail `.init` 冲突：current 未封存，设置 `splitDisabled`，后续 SID 继续写 current；
5. tail `.tssp` 冲突：硬失败，不覆盖；
6. tail Stat 未知 IO、实际 create 注错：硬失败，不扫描；
7. extent 为 `MaxUint16`：软停止切割，不回绕；
8. 每个 processed old 的首输出 `.init` 与 `.tssp` 冲突：均在写入前硬失败；
9. merge 为 `MaxUint16`：首输出前硬失败；
10. 重复 `(sequence, extent)` exact owner：首输出前硬失败。

#### SID 边界

1. `WriteOriginal` → next ordered；
2. 正常 merge → next ordered；
3. remaining A → remaining B；
4. 最后 remaining → 已知 ordered；
5. EOF 最后 SID 后不 rotate；
6. 多列 SID 的普通列、time、meta 和 pre-agg 不跨 output。

#### 替换与查询

1. skipped old 不进入 `processedOld`；
2. 全部 tail 只通过一次 Replace 加入；
3. 任一写出/Replace 注错时不删除 selected unordered；
4. raw ascending/descending；
5. `ORDER BY time LIMIT/OFFSET`；
6. count/sum/min/max/first/last；
7. pre-agg 开启与关闭结果一致；
8. 每个 SID 时间有序、数据不重不漏，且不因 size boundary 跨两个新 output。

### 47.11 提交 CC 再审核的最终结论

本轮确认第 46 章中以下内容成立：

- D+L 主线；
- G 是待实施改动；
- compact/downsample 调用关系校准；
- lazy boundary、exact-next、不扫描；
- `processedOld + producedNew` 一次 Replace；
- 查询侧无需改动；
- 增加多 tail 测试。

本轮不接受：

- 把 44.2 并发 flush 问题要求为本次或同 PR 项；
- 把 `.init` 与正式 `.tssp` 冲突统一软处理；
- 在封存 current writer 后才判断 next 是否可用；
- 在当前 diff 尚未实现 G/lazy/exact-next 时宣布“直接进入实施完成态”。

请 CC 仅复核以下四点：

1. 是否接受 44.2 只作为独立已知问题，不再与本次 D+L 捆绑？
2. 是否接受 first-output hard-fail 与 tail 的 `.init soft / .tssp hard` 状态矩阵？
3. 是否接受 `SeriesChanged` 补上“最后 remaining → 已知 ordered”的 lazy boundary，EOF 不 rotate？
4. 是否接受以第 47.9 的处置清单替换第 46.6，并据第 47.10 测试后再判断实现是否可合入？

> **最终推荐仍是简单 D+L：只扫描内存中的 ordered file list，不保存 SID，不建 occupancy 表，不扫描 extent；只让唯一 G 在真实 next SID 前尽力切割。增加的规则都用于防止覆盖正式文件、避免切开 SID，并保证冲突后控制流与实际 writer 状态一致。**

## 48. CC 对 Codex 第 47 章的 1+1 终审复核与实施启动（2026-07-23）

本节由 CC 对 Codex 第 47 章做 1+1 终审复核：派发一个证据 agent 验证四项核心校准（47.2 44.2 解耦 / 47.4 冲突矩阵 / 47.5 preflight-before-seal / 47.6 三种 boundary + EOF / 47.9 处置清单），按 26.7 中性协议区分源码事实/控制流/设计证明。结论：**Codex 第 47 章全部正确（5 项全 ACCEPT）。CC 撤回 46 章"全盘接受/直接实施"，接受 47 章四项校准 + 47.9 处置清单。设计 review 正式收敛，启动实施。**

### 48.1 逐条裁决

| Codex 项 | CC 终裁 | 证据类型 | 关键依据 |
|---|---|---|---|
| 47.2 44.2 不与 D+L 捆绑 | **ACCEPT** | 源码事实+设计 | D+L producedNew 经 `ReplaceFiles(...,true)` 加入 `Order[mst]`；44.2 丢失的是 `OutOfOrder[mst]` 脱钩对象的并发 flush 文件——不同 map/锁域/对象身份 |
| 47.4 首输出 hard-fail + tail .init soft/.tssp hard | **ACCEPT** | 源码事实+控制流 | `mms_loader.go:81-91` .tssp 加载/.init 删除；`TryNextExtentFile`（`:154-194`）只 Stat `.init` 不查 `.tssp`→覆盖风险真实；首输出无退路→hard fail；tail 有 current writer 退路→.init soft/.tssp hard |
| 47.5 preflight-before-seal | **ACCEPT** | 源码事实+控制流 | `rotateCompletedFile`（`:233-253`）先 `NewTSSPFile`(Flush 关闭 current) 后 `InitMergedFile`→冲突时 current writer 已关闭无退路；`maybeRotateBefore` 先 preflight（current 仍可写）→.init 软停/.tssp hard fail→通过才 seal |
| 47.6 三种 next-known boundary + EOF 不 rotate | **ACCEPT** | 源码事实+控制流 | SeriesChanged（`:130-170`）先 finishSeries 后 ChangeSid→maybeRotateBefore 插入点；writeRemain callback SID 变化→第 2 点；**第 3 点（SeriesChanged 内 writeRemain 返回后、ChangeSid 前的 maybeRotateBefore）是 47 章相对 46 章的实质增量**；Finish（`:205-223`）不 rotate |
| 47.9 处置清单替换 46.6 | **ACCEPT（比 45.9 更完整）** | 文档对照 | 新增首输出双路径检查 + 重复 logical owner 检查 + empty cleanup 移出 + 共享 writer 边界；DELETE/KEEP/移出方向全与源码一致 |

统计：`ACCEPT=5`、`REJECT=0`。

### 48.2 回答 47.11 四问

1. **是否接受 44.2 只作为独立已知问题，不与本次 D+L 捆绑？**
   **ACCEPT。** 47.2 证实 D+L producedNew 在 `Order[mst]`（`ReplaceFiles isOrder=true`），44.2 丢失的是 `OutOfOrder[mst]` 脱钩对象的并发 flush 文件——不同 map/锁域/对象身份。44.2 不作为 D+L blocker 或同 PR 项。

2. **是否接受 first-output hard-fail 与 tail 的 .init soft / .tssp hard 状态矩阵？**
   **ACCEPT。** 47.4 证实：首输出无"继续 current writer"退路→.init/.tssp 任一存在 hard fail；tail 有退路→.init 软停（不被 loader 加载安全）/.tssp hard fail（被 loader 加载→覆盖风险）。当前 `TryNextExtentFile` 只查 `.init`→确定性覆盖风险，双路径检查必要。

3. **是否接受 SeriesChanged 补"最后 remaining→已知 ordered"lazy boundary，EOF 不 rotate？**
   **ACCEPT。** 47.6 证实三种 boundary 落在真实调用点：SeriesChanged（ordered/prev→ordered/current）、writeRemain callback（remaining A→B）、**SeriesChanged 内 writeRemain 返回后 ChangeSid 前**（最后 remaining→已知 ordered，47 章新增）。EOF（Finish）不 rotate 不创建空后继。

4. **是否接受 47.9 处置清单替换 46.6，据 47.10 测试后判断合入？**
   **ACCEPT。** 47.9 比 45.9 更完整（首输出双路径 + 重复 logical owner + empty cleanup 移出 + 共享 writer 边界）。47.10 测试矩阵（命名 10 项 + SID 边界 6 项 + 替换/查询 8 项）覆盖 D+L 全路径。

### 48.3 CC 撤回 46 章"全盘接受/直接实施"

CC 46.5 称"全盘接受 Codex 第 44/45 章（8 ACCEPT 含 2 CALIBRATE，0 REJECT）"和 46.6"进入实施"。Codex 47 正确指出四项未闭合：
1. 44.2 不应捆绑（CC 46.2 建议同 PR 簇，Codex 47.2 解耦正确）
2. .init/.tssp 冲突不能统一软停（CC 46 接受 45 章"统一软停"，Codex 47.4 区分正确）
3. preflight-before-seal（CC 46 未提，Codex 47.5 必要）
4. 第 3 种 boundary（CC 46 未显式，Codex 47.6 新增）

**CC 撤回 46.5"全盘接受"和 46.6"直接实施"。** 以 47 章四项校准 + 47.9 处置清单为准。

### 48.4 最终实施基线

设计 review 正式收敛于 **D+L + 47.9 处置清单 + 47.10 测试矩阵**：

~~~text
实施清单（按 47.9 处置表）：
1. IMPLEMENT: global-last G 选择（exact identity + 重复 logical owner 检查 + 强制 Run）
2. IMPLEMENT: 首输出双路径检查（.init/.tssp 任一存在→hard fail；merge==MaxUint16→hard fail）
3. IMPLEMENT: maybeRotateBefore(nextSID)——preflight-before-seal
   - tail .init 存在→splitDisabled 继续 current
   - tail .tssp 存在→hard fail
   - extent==MaxUint16→splitDisabled
   - IO error→hard fail
   - 通过→seal current + create exact-next
4. DELETE: post-chunk eager SwitchNewTSSP（两条路径）
5. DELETE: 非 G 切割（只有 G 可 rotate）
6. DELETE: reservedExtents / TryNextExtentFile / 向后扫描 / probe config
7. DELETE: writeRemain 尾部 rotate
8. REBASE: lazy rotate 落在三种 next-known boundary
   - SeriesChanged: finishSeries 后、ChangeSid 前
   - writeRemain callback: WriteCurrentMeta(A) 后、ChangeSid(B) 前
   - SeriesChanged 内 writeRemain 返回后、ChangeSid 前
   - EOF: 不 rotate
9. KEEP: processedOld + producedNew 一次 ReplaceFiles
10. KEEP: Size() 在完整 SID 边界 + nil 防御
11. 移出本次: 通用 NewFile 重构 / 查询侧 / empty cleanup / 共享 writer 修复
12. 测试: 47.10 三类（命名 10 + SID 边界 6 + 替换/查询 8）
~~~

不保存 SID，不建 occupancy 表，不扫描 extent 空洞，不引入 txn token/hash，不修 transaction/recovery/gate/Drop，不改查询代码。

### 48.5 启动实施

**设计 review 正式结束。** CC 与 Codex 已就以下内容完全一致：
- D+L 核心设计（global-last G + last-only split + exact-next + lazy boundary）
- 47.9 处置清单（KEEP/IMPLEMENT/DELETE/移出）
- 47.10 测试矩阵
- 44.2 作为独立问题不捆绑
- 范围只限"乱序合并写出"路径

下一轮启动多 subagent 实施，实施过程必须 1+1 review。实施按 48.4 清单的 12 步进行，每个阶段派发实施 agent + 审核 agent。

## 49. Codex 对 CC 当前代码实现流程的 1+1 复核结论（忽略测试，2026-07-23）

本章记录 CC 完成代码修改后的实现级复核结论。复核采用严格 1+1：一个 agent 提交源码证据和控制流推导，另一个 agent 独立检查后再逐条审核证据。两者结论一致。

本章只评价“乱序文件合并写入有序文件，并在完整 SID 边界尽可能切割”的代码实现流程，明确不评价：

- 测试数量与测试覆盖率；
- 接近 4 GiB 时其他元数据字段的 `uint32` 回绕；
- compact、downsample、查询侧及并发 flush 的独立问题。

### 49.1 当前代码实现流程

当前实现主流程为：

1. [`selectGlobalLast`](../engine/immutable/merge_out_of_order.go#L276) 在 measurement 的全部 live ordered 文件中选择唯一 global-last 文件 G，以 exact path 保存到 `ctx.globalLastPath`，并把 G 去重加入 `ctx.order`。
2. [`mergeTool.execute`](../engine/immutable/merge_tool.go#L216) 遍历 `ctx.order`：
   - 非 G 且与乱序数据不相交时跳过；
   - G 即使 `contains=false` 也必须进入 `ColumnIterator.Run`；
   - 每个真正处理的 old 文件先检查首输出 `.tssp.init` 和 `.tssp`，然后创建首输出；
   - 只有 `Run` 成功的 old 文件才加入 `processedOld`。
3. [`ColumnIterator.Run`](../engine/immutable/column_iterator.go#L96) 在读到一个新 SID 后调用 `SeriesChanged`。`SeriesChanged` 先完整结束前一个 SID 及其 remaining 数据，再调用 `maybeRotateBefore(nextSID)`，最后才开始写 next SID。
4. remaining A→remaining B 时，[`writeRemain`](../engine/immutable/merge_performer.go#L413) 在 A 的 `WriteCurrentMeta` 完成后、开始 B 之前调用 `maybeRotateBefore(B)`。
5. [`maybeRotateBefore`](../engine/immutable/merge_performer.go#L240) 只允许 G 切割：
   - 未达到阈值时继续当前文件；
   - 达到阈值后计算 exact `extent+1`；
   - preflight 检查 exact-next 文件名；
   - 通过后封存 current、加入 `producedNew`、创建 next writer；
   - EOF 的 `Finish` 只封存 current，不创建空后继文件。
6. 全部处理成功后，以 `processedOld + producedNew` 只执行一次 `ReplaceFiles`；Replace 成功后才删除本轮 selected unordered 文件。

上述主干实现已经满足以下关键不变量：

- 非 G 不因文件大小切割；
- G 只在已经完整写完前一个 SID、且 next SID 已知时切割；
- EOF 不做 size rotate；
- 不向后扫描 extent 空洞，只尝试 exact `extent+1`；
- 当前切割点不会主动把同一个 SID 写入两个新 extent 文件；
- skipped old 不会被错误替换，全部成功输出通过一次 Replace 发布。

因此当前阻塞项不在 D+L 主干选择，而在三个具体的错误分支和状态重置顺序。

### 49.2 阻塞问题一：tail `.init` 的提前返回掩盖了必须 hard-fail 的状态

当前 [`maybeRotateBefore`](../engine/immutable/merge_performer.go#L269) 的检查顺序为：

~~~text
Stat(exact-next .tssp.init)
    存在 -> splitDisabled=true，立即 return nil

Stat(exact-next .tssp)
    存在或未知 IO -> error
~~~

假设 current 为：

~~~text
measurement/0000002a-0001-00040007.tssp.init
~~~

exact-next 同时残留：

~~~text
measurement/0000002a-0001-00040008.tssp.init
measurement/0000002a-0001-00040008.tssp
~~~

当前代码在看到 `0000002a-0001-00040008.tssp.init` 后立即软停止，不再检查正式文件 `0000002a-0001-00040008.tssp`。这把“正式文件已经占用 exact-next”错误降级成了“存在 stale init，继续写 current”。

同理，如果 `.init` 存在，而对正式 `.tssp` 的 `Stat` 本应返回权限、介质或其他未知 I/O 错误，该错误也会被提前返回掩盖。

这违反已经确定的冲突优先级：

| exact-next 状态 | 必须行为 |
|---|---|
| 正式 `.tssp` 存在 | hard fail |
| 任一路径返回非 `NotExist` 的未知错误 | hard fail |
| 正式 `.tssp` 确定不存在，只有 `.init` 存在 | `splitDisabled=true`，继续 current |
| 两者均确定不存在 | 允许 seal 和创建 exact-next |

**建议修改：**

先检查正式 `.tssp`，正式文件存在或 `Stat` 返回未知错误立即报错；只有确认正式文件不存在后，才检查 `.init` 并决定是否软停止。也可以先收集两个 `Stat` 结果再统一裁决，但不能在看到 `.init` 后提前结束整个 preflight。

该问题属于合入阻塞项。

### 49.3 阻塞问题二：新 tail 初始化后又 reset，清空了新文件 measurement name

当前 rotate 顺序为：

~~~text
NewTSSPFile(true)                         // seal current
InitMergedFile(sealed, addFileExt=true)  // 创建并初始化 exact-next
resetStreamWriteFileForRotation(sw)      // 再次 reset
~~~

问题在于 [`StreamWriteFile.NewFile`](../engine/immutable/stream_downsample.go#L125) 已经为新文件执行：

~~~go
c.trailer.name = append(c.trailer.name[:0], influx.GetOriginMstName(c.name)...)
~~~

但紧接着调用的 [`resetStreamWriteFileForRotation`](../engine/immutable/merge_performer.go#L347) 又调用 `c.trailer.reset()`；而 [`Trailer.reset`](../engine/immutable/trailer.go#L43) 会执行：

~~~go
t.name = t.name[:0]
~~~

所以这不是概率问题，而是确定的状态覆盖：

~~~text
创建第二个 output
    -> NewFile 设置 trailer.name
    -> resetStreamWriteFileForRotation 清空 trailer.name
    -> 后续 Flush 使用空 measurement name
~~~

首输出不走这段 rotate reset，因此问题从第二个输出开始出现。

**建议修改顺序：**

~~~text
sealed = NewTSSPFile(true)
AppendMergedFile(sealed)

resetStreamWriteFileForRotation(sw)      // 清理旧文件遗留状态
InitMergedFile(sealed, addFileExt=true)  // 最后初始化新文件

reset mergePerformer 的 cw/SID/schema 等 writer-local 状态
~~~

核心原则是：旧文件状态清理必须发生在新文件初始化之前；`NewFile` 对新 fd、trailer、measurement name 的初始化必须是最后一次文件级初始化，不能再被全量 reset 覆盖。

该问题属于合入阻塞项。

### 49.4 阻塞问题三：首输出初始化失败时，新 `sw` 尚未交给 performer 管理

当前 [`mergeTool.execute`](../engine/immutable/merge_tool.go#L254) 的顺序为：

~~~text
sw := NewStreamWriteFile(mst)
sw.InitMergedFile(f, addMerge=true)
p.Reset(sw, isGlobalLast)
~~~

而 [`StreamWriteFile.NewFile`](../engine/immutable/stream_downsample.go#L125) 的创建过程不是原子的：

~~~text
OpenFile/CreateV2 创建并打开 .tssp.init
newTsspFileWriter(fd)
WriteData(tableMagic + version)
~~~

如果文件已经创建、fd 已打开，但写 magic/version 失败，`InitMergedFile` 会直接返回错误。此时还没有执行 `p.Reset(sw, ...)`，所以：

- `p.sw` 仍是 `nil` 或上一个 writer；
- `p.CleanTmpFiles()` 不知道本次新建的局部 `sw`；
- 本次 fd 可能没有关闭；
- 本次首输出 `.tssp.init` 可能残留；
- 下一轮 merge 会被首输出冲突检查持续阻止。

完整文件名示例：

~~~text
old:
measurement/0000002a-0001-00030007.tssp

首输出创建到一半失败并残留：
measurement/0000002a-0001-00040007.tssp.init

下一轮 checkFirstOutputFree:
发现 0000002a-0001-00040007.tssp.init -> hard fail
~~~

**建议修改：**

在任何可能创建临时文件的调用前，先建立明确的 cleanup ownership。可采用下面任一方式：

1. 在 `InitMergedFile` 前先让 performer 持有 `sw`，初始化失败统一走 `p.CleanTmpFiles()`；
2. 保持当前 ownership 转移顺序，但为局部 `sw` 增加错误分支 cleanup/defer，确保初始化部分成功后能够关闭 fd 并删除本轮刚创建的 `.init`。

不能只清理 `producedNew`，因为部分创建失败的文件尚未生成 `TSSPFile`，不会出现在 `mergedFiles` 中。

该问题属于合入阻塞项。

### 49.5 非阻塞但建议同轮修正的实现问题

#### 49.5.1 size limit 应从当前 writer 的 store 配置读取

[`maybeRotateBefore`](../engine/immutable/merge_performer.go#L248) 当前读取：

~~~go
GetTsStoreConfig().GetFileSizeLimit()
~~~

但 [`MmsTables.NewStreamWriteFile`](../engine/immutable/mms_tables.go#L1092) 已把该 store 的配置赋给 `sw.Conf`。如果 store 使用的是独立配置对象，全局配置与 `sw.Conf` 可能不同，实际切割阈值就不是该 writer 的生效配置。

建议改为：

~~~go
p.sw.Conf.GetFileSizeLimit()
~~~

这主要影响“何时尽力切割”，不改变 SID 边界和查询正确性，因此不单独作为合入阻塞项。

#### 49.5.2 `selectGlobalLast` 的空检查应放进同一个读锁快照

[`selectGlobalLast`](../engine/immutable/merge_out_of_order.go#L276) 当前先调用无锁的 `files.Len()`，随后才获取 `files.lock.RLock()`。在本次已经约定的 merge/compact 互斥条件下，这不是本轮 blocker，也不需要为此引入新的并发 gate。

但从实现一致性看，建议获取 `RLock` 后再统一执行 `len(files.Files())`、重复 logical owner 检查和 G 选择，使这些判断来自同一个快照。

#### 49.5.3 merge-local options 应避免继续扩散到共享接口

`initMergedFileOptions` 当前包含未使用的 `isNoOrder`，并把 merge 首输出与 tail rotate 的差异扩展到共享 `StreamWriteFile.InitMergedFile`。这暂未形成确定性数据错误，但容易让 downsample/compact 调用方承接 merge-only 语义。

建议至少删除未使用字段，并保持 options 只表达当前两个真实动作：

- 首输出：`addMerge=true`；
- tail：`addFileExt=true`。

### 49.6 推荐的最小修改顺序

不改变 D+L 主设计，不增加 SID 表、occupancy 表或 extent 扫描，只做以下实现修正：

1. 修正 tail preflight 优先级：正式 `.tssp`/未知 I/O 优先于 `.init` 软停止；
2. 把 `resetStreamWriteFileForRotation` 移到 `InitMergedFile(next)` 之前；
3. 在首输出可能创建 `.init` 前建立 performer ownership，或补齐局部 `sw` 的失败 cleanup；
4. size gate 改读 `p.sw.Conf.GetFileSizeLimit()`；
5. 把 G 的空检查移入现有 `RLock`，并收敛未使用的 options 字段。

### 49.7 本轮最终结论

当前实现的 global-last G、last-only split、next-known SID boundary、exact `extent+1`、EOF 不 rotate、`processedOld + producedNew` 一次 Replace 等主流程方向正确；在正常成功路径上，也没有发现因切割主动把同一个 SID 写入多个 extent 的流程。

但是当前代码仍有三个确定性的实现级问题：

1. tail `.init` 提前返回掩盖正式文件或未知 I/O；
2. tail 创建后 reset 清空新文件 measurement name；
3. 首输出部分初始化失败时 cleanup ownership 缺失。

因此，**忽略测试因素后，当前结论仍为 BLOCK**。完成 49.6 前三项修正后，代码流程层面的主要阻塞才解除；其余两项可作为同轮小范围清理，不需要扩大到查询、compact、downsample 或并发 flush 流程。
