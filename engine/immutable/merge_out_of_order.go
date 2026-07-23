/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
	"fmt"
	"sort"

	"github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

func (m *MmsTables) MergeOutOfOrder(shId uint64, force bool) error {
	contexts := m.createMergeContext(maxCompactor)

	for _, ctx := range contexts {
		if ctx.mst == "" || len(ctx.unordered.seq) == 0 {
			continue
		}
		if !m.inMerge.Add(ctx.mst) {
			log.Info("merging in progress", zap.String("name", ctx.mst))
			continue
		}
		ctx.shId = shId

		select {
		case <-m.closed:
			log.Warn("shard closed", zap.Uint64("id", shId))
			return fmt.Errorf("store closed, shard id: %v", shId)
		case <-m.stopCompMerge:
			log.Info("stop merge", zap.Uint64("id", shId))
			return nil
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			if !m.MergeEnabled() {
				m.wg.Done()
				return nil
			}

			go m.mergeOutOfOrder(ctx, force)
		}
	}

	return nil
}

func (m *MmsTables) mergeOutOfOrder(ctx *mergeContext, force bool) {
	stat := statistics.NewMergeStatistics()
	stat.AddActive(1)
	cLog, logEnd := logger.NewOperation(log, "MergeOutOfOrder", ctx.mst)
	defer func() {
		stat.AddActive(-1)
		m.wg.Done()
		compLimiter.Release()
		m.inMerge.Del(ctx.mst)
		logEnd()
		ctx.Release()
	}()

	if m.compactRecovery {
		defer MergeRecovery(m.path, ctx.mst, ctx)
	}

	tool := newMergeTool(m, cLog)
	tool.merge(ctx, force)
}

func (m *MmsTables) Listen(signal chan struct{}, onClose func()) {
	go func() {
		select {
		case <-m.closed:
			onClose()
		case <-m.stopCompMerge:
			onClose()
		case <-signal:
			return
		}
	}()
}

// replaceMergedFiles swaps the order files that were actually processed by
// execute (processedOld) with the newly produced merged files (producedNew).
//
// Unlike the previous (seq, extent) reverse-matching implementation, this
// trusts the processedOld list assembled by execute — only files that entered
// the itr.Run path (and succeeded) are passed in. Skipped order files are
// excluded, so they survive the merge. ReplaceFiles deletes every file in
// processedOld and adds every file in producedNew.
func (m *MmsTables) replaceMergedFiles(name string, lg *zap.Logger, processedOld, producedNew []TSSPFile) error {
	for _, f := range processedOld {
		fn := f.FileName()
		lg.Info("replace merged file",
			zap.String("old file", fn.String()),
			zap.Int64("old size", f.FileSize()))
	}
	for _, f := range producedNew {
		fn := f.FileName()
		lg.Info("replace merged file",
			zap.String("new file", fn.String()),
			zap.Int64("new size", f.FileSize()))
	}

	return m.ReplaceFiles(name, processedOld, producedNew, true)
}

func (m *MmsTables) getFilesByPath(mst string, path []string, order bool) (*TSSPFiles, error) {
	files := NewTSSPFiles()
	files.files = make([]TSSPFile, 0, len(path))

	for _, fn := range path {
		if m.isClosed() {
			return nil, ErrCompStopped
		}
		f := m.File(mst, fn, order)
		if f == nil {
			return nil, fmt.Errorf("table %v, %v, %t not find", mst, fn, order)
		}

		files.Append(f)
	}

	return files, nil
}

func (m *MmsTables) createMergeContext(limit int) []*mergeContext {
	ret := make([]*mergeContext, 0, limit)
	m.mu.RLock()
	defer m.mu.RUnlock()

	var create = func(mst string, files *TSSPFiles) bool {
		files.lock.RLock()
		defer files.lock.RUnlock()

		ctx := NewMergeContext(mst)
		ret = append(ret, ctx)
		for _, f := range files.Files() {
			if !ctx.AddUnordered(f) {
				return true
			}
		}

		return false
	}

	for k, v := range m.OutOfOrder {
		if v.Len() == 0 || v.closing > 0 {
			continue
		}
		if create(k, v) {
			break
		}
		limit--
		if limit <= 0 {
			break
		}
	}

	return ret
}

func (m *MmsTables) GetOutOfOrderFileNum() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := 0
	for _, v := range m.OutOfOrder {
		total += v.Len()
	}
	return total
}

//lint:ignore U1000 test used only
func (m *MmsTables) tableFiles(name string, order bool) *TSSPFiles {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mmsTbls := m.Order
	if !order {
		mmsTbls = m.OutOfOrder
	}

	return mmsTbls[name]
}

func (m *MmsTables) removeFile(f TSSPFile) {
	if f.Inuse() {
		if err := f.Rename(f.Path() + tmpFileSuffix); err != nil {
			log.Error("failed to rename file", zap.String("path", f.Path()), zap.Error(err))
			return
		}
		nodeTableStoreGC.Add(false, f)
		return
	}

	err := f.Remove()
	if err != nil {
		nodeTableStoreGC.Add(false, f)
		log.Error("failed to remove file", zap.String("path", f.Path()), zap.Error(err))
		return
	}
}

func mergeFirst(outLen int, outSize, orderFileSize int64) bool {
	if outLen == 1 {
		return false
	}
	if float64(outSize) > float64(MaxSizeOfFileToMerge)*MergeFirstRatio {
		return false
	}
	var avgMergeFileSize int64
	if outLen != 0 {
		avgMergeFileSize = outSize / int64(outLen)
	} else {
		avgMergeFileSize = outSize
	}

	return avgMergeFileSize < MergeFirstAvgSize && orderFileSize > MergeFirstDstSize
}

func (m *MmsTables) matchOrderFiles(ctx *mergeContext) {
	files, ok := m.getTSSPFiles(ctx.mst, true)
	if !ok {
		log.Warn("No order file is matched.", zap.String("measurement", ctx.mst))
		return
	}

	files.lock.RLock()
	defer files.lock.RUnlock()

	for _, f := range files.Files() {
		if m.isClosed() {
			return
		}
		min, max, err := f.MinMaxTime()
		if err != nil {
			continue
		}

		if ctx.tr.Overlaps(min, max) || min > ctx.tr.Max {
			ctx.order.add(f)
		}
	}

	if ctx.order.Len() == 0 {
		ctx.order.add(files.Files()[files.Len()-1])
	}
}

// selectGlobalLast identifies G — the global-last live ordered file for ctx.mst
// (the file with the largest (seq, extent) in the full live ordered set, not
// just the time-matched subset in ctx.order) — and records its exact path in
// ctx.globalLastPath. execute compares against this path (instead of
// i == order.Len()-1) so that G always carries lastFile semantics for the
// maxOrderTime=MaxInt64 split, even when G was de-dup-added to ctx.order after
// the time-matched files.
//
// G is also de-dup-added to ctx.order so that acquire(ctx.order.path) pins it
// and execute's iteration visits it. A duplicate logical owner — two adjacent
// live files with the same (seq, extent) but different exact paths — is a hard
// failure: the lineage is ambiguous and merge must not proceed.
func (m *MmsTables) selectGlobalLast(ctx *mergeContext) error {
	files, ok := m.getTSSPFiles(ctx.mst, true)
	if !ok {
		return fmt.Errorf("selectGlobalLast: no order files for measurement %s", ctx.mst)
	}

	// Lock before reading files.Len() (49.5.2): TSSPFiles.files is a slice
	// that concurrent merge/compact goroutines append to, so an unsynchronized
	// Len() here races with append and can observe a stale length. Take the
	// RLock first, then read Len and Files under the same critical section.
	files.lock.RLock()
	defer files.lock.RUnlock()

	if files.Len() == 0 {
		return fmt.Errorf("selectGlobalLast: no order files for measurement %s", ctx.mst)
	}

	liveFiles := files.Files()
	if len(liveFiles) == 0 {
		return fmt.Errorf("selectGlobalLast: empty order file set for measurement %s", ctx.mst)
	}

	// liveFiles is sorted by (seq, extent). Two adjacent files sharing the same
	// (seq, extent) but different paths indicate a duplicate logical owner —
	// the lineage is ambiguous, so fail hard rather than risk a wrong G.
	// LevelAndSequence returns (level, seq); the logical-owner key is (seq, extent).
	_, prevSeq := liveFiles[0].LevelAndSequence()
	prevExt := liveFiles[0].FileNameExtend()
	prevPath := liveFiles[0].Path()
	for i := 1; i < len(liveFiles); i++ {
		_, seq := liveFiles[i].LevelAndSequence()
		ext := liveFiles[i].FileNameExtend()
		path := liveFiles[i].Path()
		if seq == prevSeq && ext == prevExt && path != prevPath {
			return fmt.Errorf("selectGlobalLast: duplicate logical owner (seq=%d, extent=%d) with distinct paths %q vs %q for measurement %s",
				seq, ext, prevPath, path, ctx.mst)
		}
		prevSeq, prevExt, prevPath = seq, ext, path
	}

	g := liveFiles[len(liveFiles)-1]
	ctx.globalLastPath = g.Path()

	// De-dup-add G to ctx.order so its path is pinned by acquire(ctx.order.path)
	// and execute's iteration visits it. If G was already time-matched it is
	// already present and we skip the append.
	for _, p := range ctx.order.path {
		if p == ctx.globalLastPath {
			return nil
		}
	}
	ctx.order.add(g)
	return nil
}

func (m *MmsTables) deleteUnorderedFiles(mst string, files []TSSPFile) {
	tfs, ok := m.getTSSPFiles(mst, false)
	if !ok {
		return
	}

	noFiles := true
	func() {
		tfs.lock.Lock()
		defer tfs.lock.Unlock()

		for _, f := range files {
			tfs.deleteFile(f)
			m.removeFile(f)
		}
		if tfs.Len() > 0 {
			noFiles = false
			sort.Sort(tfs)
		}
	}()

	if !noFiles {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tfs, ok = m.OutOfOrder[mst]
	if ok && tfs.Len() == 0 {
		delete(m.OutOfOrder, mst)
	}
}
