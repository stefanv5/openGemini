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
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

const (
	// total number of unordered files is less than this value,
	// may skip the merge operation
	mergeMinFileNum = 5

	// total size of unordered files is less than this value,
	// may skip the merge operation
	mergeMinUnorderedSize = 1 * 1024 * 1024

	mergeMaxInterval = 300 // 5min

	MergeFirstAvgSize = 10 * 1024 * 1024
	MergeFirstDstSize = 10 * 1024 * 1024
	MergeFirstRatio   = 0.5
)

type mergeTool struct {
	mts  *MmsTables
	lmt  *lastMergeTime
	stat *statistics.MergeStatItem
	lg   *logger.Logger
	zlg  *zap.Logger
}

func newMergeTool(mts *MmsTables, lg *zap.Logger) *mergeTool {
	return &mergeTool{
		mts: mts,
		lmt: mts.lmt,
		lg:  logger.NewLogger(errno.ModuleMerge),
		zlg: lg,
	}
}

func (mt *mergeTool) skip(ctx *mergeContext) bool {
	ok := len(ctx.unordered.seq) < mergeMinFileNum &&
		ctx.unordered.size < mergeMinUnorderedSize &&
		mt.lmt.Nearly(ctx.mst, time.Second*mergeMaxInterval)

	if ok {
		statistics.NewMergeStatistics().AddSkipTotal(1)
		mt.zlg.Info("new and small unordered files, merge later")
	} else {
		mt.lmt.Update(ctx.mst)
	}

	return ok
}

func (mt *mergeTool) mergeUnorderedSelf(ctx *mergeContext, unordered *TSSPFiles) bool {
	if mergeFirst(unordered.Len(), ctx.unordered.size, ctx.order.size) {
		statistics.NewMergeStatistics().AddMergeSelfTotal(1)
		mt.zlg.Info("merge first",
			zap.Int("unordered file count", len(ctx.unordered.seq)),
			zap.Uint64s("unordered sequences", ctx.unordered.seq),
			zap.Int64("unordered size", ctx.unordered.size))

		mt.mergeSelf(ctx, unordered)
		return true
	}

	return false
}

func (mt *mergeTool) mergePrepare(ctx *mergeContext, force bool) bool {
	mt.zlg.Info("merge info",
		zap.String("path", mt.mts.path+"/"+ctx.mst),
		zap.Uint64("shard id", ctx.shId),
		zap.Int("unordered file count", len(ctx.unordered.seq)),
		zap.Uint64s("unordered sequences", ctx.unordered.seq),
		zap.Int64("unordered size", ctx.unordered.size))

	mt.stat = statistics.NewMergeStatItem(ctx.mst, ctx.shId)
	if !force && mt.skip(ctx) {
		return false
	}

	mt.mts.matchOrderFiles(ctx)
	if ctx.order.Len() == 0 {
		mt.zlg.Warn("no order file is matched")
		return false
	}

	// Select G (global-last live ordered file) and de-dup-add it to ctx.order
	// before acquire so its path is pinned. Hard-fail (skip this merge) on a
	// duplicate logical owner — the lineage is ambiguous and merge must not
	// proceed against an ambiguous G.
	if err := mt.mts.selectGlobalLast(ctx); err != nil {
		mt.zlg.Error("select global last order file failed", zap.Error(err))
		return false
	}

	mt.zlg.Info("order file info",
		zap.Int("order file count", len(ctx.order.seq)),
		zap.Uint64s("order sequences", ctx.order.seq),
		zap.Int64("order file size", ctx.order.size),
		zap.String("global last file", ctx.globalLastPath))

	if !mt.mts.acquire(ctx.order.path) {
		mt.zlg.Warn("acquire is false, skip merge")
		return false
	}

	return true
}

func (mt *mergeTool) merge(ctx *mergeContext, force bool) {
	if !mt.mergePrepare(ctx, force) {
		return
	}

	orderWg, inorderWg := mt.mts.refMmsTable(ctx.mst, true)
	var unordered, order *TSSPFiles
	var err error
	success := false

	defer func() {
		mt.mts.unrefMmsTable(orderWg, inorderWg)
		mt.mts.CompactDone(ctx.order.path)
		if !success {
			statistics.NewMergeStatistics().AddErrors(1)
		}
	}()

	order, unordered, err = mt.getTSSPFiles(ctx)
	if err != nil {
		mt.zlg.Error("failed to get files", zap.Error(err))
		return
	}

	if !force && mt.mergeUnorderedSelf(ctx, unordered) {
		return
	}

	func() {
		mt.stat.StatOrderFile(ctx.order.size, ctx.order.Len())
		mt.stat.StatOutOfOrderFile(ctx.unordered.size, ctx.unordered.Len())

		mergedFiles, processedOld, err := mt.execute(ctx.mst, ctx.globalLastPath, order, unordered)
		if err != nil {
			mt.zlg.Error("failed to merge unordered files", zap.Error(err))
			return
		}

		mt.stat.StatMergedFile(SumFilesSize(mergedFiles.Files()), mergedFiles.Len())
		if err := mt.mts.replaceMergedFiles(ctx.mst, mt.zlg, processedOld, mergedFiles.Files()); err != nil {
			mt.zlg.Error("failed to replace merged files", zap.Error(err))
			return
		}
		mt.mts.deleteUnorderedFiles(ctx.mst, unordered.Files())
		mt.stat.Push()
		success = true
	}()
}

func (mt *mergeTool) getTSSPFiles(ctx *mergeContext) (*TSSPFiles, *TSSPFiles, error) {
	ctx.Sort()
	order, err := mt.mts.getFilesByPath(ctx.mst, ctx.order.path, true)
	if err != nil {
		return nil, nil, err
	}

	unordered, err := mt.mts.getFilesByPath(ctx.mst, ctx.unordered.path, false)
	if err != nil {
		return nil, nil, err
	}

	return order, unordered, err
}

func (mt *mergeTool) contains(ur *UnorderedReader, f TSSPFile) bool {
	for _, sid := range ur.sid {
		ok, err := f.Contains(sid)
		if err == nil && ok {
			return true
		}
	}

	return false
}

func (mt *mergeTool) execute(mst string, globalLastPath string, order, unordered *TSSPFiles) (*TSSPFiles, []TSSPFile, error) {
	ur := NewUnorderedReader(mt.lg)
	ur.AddFiles(unordered.Files())
	p := NewMergePerformer(ur, mt.stat)

	// processedOld records the order files that actually entered the Run path
	// (and succeeded). Files that were skipped via `continue` (disjoint and
	// not last) are NOT included. replaceMergedFiles uses this list directly
	// instead of reverse-matching by (seq, extent), so skipped files are left
	// untouched.
	var processedOld []TSSPFile

	var err error
	for _, f := range order.Files() {
		// isGlobalLast is decided by exact identity (G's path), not by
		// position i == order.Len()-1. G — the global-last live ordered file —
		// is the only file that carries lastFile semantics (maxOrderTime is
		// forced to MaxInt64 in mergePerformer.Handle/SeriesChanged so the
		// remaining unordered data is consumed). G MUST Run even when
		// contains=false (it has to absorb the unordered tail); non-G files
		// are skipped when they don't intersect any unordered series.
		isGlobalLast := f.Path() == globalLastPath
		if !isGlobalLast && !mt.contains(ur, f) {
			continue
		}

		// First-output dual-path check (47.4.1). Before creating the first
		// merged output for this processed old file, verify the exact first
		// output path — (f.seq, f.level, f.merge+1, f.extent), produced by
		// InitMergedFile(f, {addMerge:true, addFileExt:false}) — is free at
		// both the .tssp.init (in-progress) and .tssp (finalized) layers.
		// A collision here means the merge generation lineage is ambiguous;
		// hard-fail this merge run rather than shadow or overwrite a sealed
		// file.
		if err = mt.checkFirstOutputFree(mst, f); err != nil {
			break
		}

		sw := mt.mts.NewStreamWriteFile(mst)
		// First file of a new merge generation: addMerge=true enters the new
		// merge counter (so replaceMergedFiles can later identify this as a
		// produced file of the processed old), addFileExt=false inherits the
		// old extent (keeps the (seq, extent) lineage stable across generations).
		if err = sw.InitMergedFile(f, initMergedFileOptions{addMerge: true}); err != nil {
			// Cleanup ownership (49.4): on first-output InitMergedFile failure
			// the sw is NOT yet handed to the performer (p.Reset below has not
			// run), so p.CleanTmpFiles at the tail of execute will not touch
			// it. InitMergedFile may have already created the .init fd (NewFile
			// opens the fd before any later step can fail), so we must
			// explicitly release the fd and remove the .init file here to
			// avoid leaking a file descriptor and a stray .init that would
			// collide with the next merge attempt's first-output probe
			// (checkFirstOutputFree hard-fails on an existing .init).
			sw.Close(true)
			break
		}

		p.Reset(sw, isGlobalLast)
		itr := NewColumnIterator(NewFileIterator(f, mt.lg))

		mt.mts.Listen(itr.signal, func() {
			itr.Close()
		})
		if err = itr.Run(p); err != nil {
			break
		}
		processedOld = append(processedOld, f)
	}

	if err != nil {
		p.CleanTmpFiles()
		return nil, nil, err
	}

	return p.MergedFiles(), processedOld, nil
}

// checkFirstOutputFree verifies the first merged output path for processed-old
// file f is free. The first output name is (f.seq, f.level, f.merge+1, f.extent)
// — mirroring InitMergedFile(f, {addMerge:true, addFileExt:false}) which sets
// c.fileName = f.FileName() then merge++ (extent untouched).
//
// Both the .tssp.init (in-progress) and .tssp (finalized) layers are probed.
// Either existing is a hard failure: .init means a concurrent writer is mid-
// creation; .tssp means a sealed file already occupies that lineage slot.
// merge==MaxUint16 is also a hard failure (merge counter overflow).
func (mt *mergeTool) checkFirstOutputFree(mst string, f TSSPFile) error {
	fn := f.FileName()
	if fn.merge == math.MaxUint16 {
		return fmt.Errorf("merge counter overflow: file(%s) merge=%d",
			f.Path(), fn.merge)
	}
	first := fn
	first.merge++

	dir := filepath.Join(mt.mts.path, mst)
	initPath := first.Path(dir, true)   // .tssp.init
	finalPath := first.Path(dir, false) // .tssp

	if _, err := fileops.Stat(initPath); err == nil {
		return fmt.Errorf("file(%s) exist", initPath)
	} else if !os.IsNotExist(err) {
		return err
	}
	if _, err := fileops.Stat(finalPath); err == nil {
		return fmt.Errorf("file(%s) exist", finalPath)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (mt *mergeTool) mergeSelf(ctx *mergeContext, files *TSSPFiles) {
	data, ids := mt.readUnorderedRecords(files)

	mergedFile, err := mt.saveRecords(ctx, files.Files()[0].FileName(), data, ids)
	if err != nil {
		mt.zlg.Error("new tmp file fail", zap.Error(err))
		return
	}
	if mergedFile == nil {
		mt.zlg.Info("no merged files")
		return
	}

	err = mt.mts.ReplaceFiles(ctx.mst, files.Files(), []TSSPFile{mergedFile}, false)
	if err != nil {
		mt.zlg.Error("failed to replace files", zap.Error(err))
	}
}

func (mt *mergeTool) readUnorderedRecords(files *TSSPFiles) (map[uint64]*record.Record, []uint64) {
	var data = make(map[uint64]*record.Record)
	var ids []uint64

	for _, f := range files.Files() {
		fi := NewFileIterator(f, mt.lg)
		itr := NewChunkIterator(fi)
		itr.WithLog(mt.lg)

		for itr.Next() {
			sid := itr.GetSeriesID()
			rec, ok := data[sid]
			tmp := itr.GetRecord()

			if !ok {
				rec = &record.Record{}
				rec.ResetWithSchema(tmp.Schema.Copy())
				data[sid] = rec
				ids = append(ids, sid)
			} else {
				rec.PadRecord(tmp)
			}

			rec.AppendRec(tmp, 0, tmp.RowNums())
		}
	}

	return data, ids
}

func (mt *mergeTool) saveRecords(ctx *mergeContext, fileName TSSPFileName,
	data map[uint64]*record.Record, ids []uint64) (TSSPFile, error) {

	sh := record.NewColumnSortHelper()
	defer sh.Release()

	fileName.merge++
	fileName.lock = mt.mts.lock
	builder := NewMsBuilder(mt.mts.path, ctx.mst, mt.mts.lock, mt.mts.Conf,
		len(data), fileName, 0, nil, int(ctx.unordered.size), config.TSSTORE)
	var err error
	defer func(msb **MsBuilder) {
		if err != nil {
			ReleaseMsBuilder(*msb)
		}
	}(&builder)

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	for _, sid := range ids {
		rec, ok := data[sid]
		if !ok {
			continue
		}

		rec = sh.Sort(rec)
		builder, err = builder.WriteRecord(sid, rec, nil)
		if err != nil {
			return nil, err
		}
	}

	var mergedFile TSSPFile
	mergedFile, err = builder.NewTSSPFile(true)

	return mergedFile, err
}
