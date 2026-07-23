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
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

type mergePerformer struct {
	mh   *record.MergeHelper
	ur   *UnorderedReader
	sw   *StreamWriteFile
	cw   *columnWriter
	stat *statistics.MergeStatItem

	// New file after merge
	mergedFiles TSSPFiles

	// Is the last ordered file?
	// The remaining unordered data that does not intersect the series
	// needs to be written into this file
	lastFile bool

	// splitDisabled is set when an in-progress split attempt cannot proceed
	// (candidate exact-next path already has a .tssp.init, or extent would
	// overflow MaxUint16). Once set, the current G run continues to EOF
	// without further rotation attempts. Sticky for the lifetime of this
	// performer run.
	splitDisabled bool

	// The series of the current ordered data does not exist in the unordered data
	noUnorderedSeries bool

	// The column in the current ordered data does not exist in the unordered data
	noUnorderedColumn bool

	// current series ID
	sid uint64

	// current column schema
	ref *record.Field

	// schema of unordered data
	unorderedSchemas record.Schemas

	// merged time column
	mergedTimes   []int64
	mergedTimeCol *record.ColVal

	nilCol record.ColVal
}

func NewMergePerformer(ur *UnorderedReader, stat *statistics.MergeStatItem) *mergePerformer {
	return &mergePerformer{
		mh:            record.NewMergeHelper(),
		ur:            ur,
		mergedFiles:   TSSPFiles{},
		mergedTimeCol: &record.ColVal{},
		stat:          stat,
	}
}

func (p *mergePerformer) Reset(sw *StreamWriteFile, last bool) {
	p.sid = 0
	p.sw = sw
	p.cw = newColumnWriter(sw, GetMaxRowsPerSegment4TsStore())
	p.lastFile = last
	p.splitDisabled = false
}

func (p *mergePerformer) Handle(col *record.ColVal, times []int64, lastSeg bool) error {
	// Unordered data does not contain the data of the series
	if p.noUnorderedSeries {
		return p.write(p.ref, col, times, lastSeg)
	}

	maxOrderTime := times[len(times)-1]
	if p.lastFile && lastSeg {
		maxOrderTime = math.MaxInt64
	}

	unorderedCol, unorderedTimes, err := p.readUnordered(maxOrderTime)
	if err != nil {
		return err
	}
	if unorderedCol != nil {
		record.CheckCol(unorderedCol, p.ref.Type)
	}

	return p.merge(col, unorderedCol, times, unorderedTimes, p.ref, lastSeg)
}

func (p *mergePerformer) SeriesChanged(sid uint64, orderTimes []int64) error {
	p.stat.OrderSeriesCount++

	if err := p.finishSeries(sid); err != nil {
		return err
	}
	// Lazy split boundary (47.6 SeriesChanged + last-remaining→ordered):
	// the previous series (and any remaining unordered data with sid <
	// nextSID, drained inside finishSeries via writeRemain) is fully
	// flushed. Probe rotation before starting the new series. This single
	// call covers both the ordered/prev→ordered/current boundary and the
	// final remaining-A→ordered/current boundary (finishSeries is the
	// common tail of both paths).
	if err := p.maybeRotateBefore(sid); err != nil {
		return err
	}
	if len(orderTimes) == 0 {
		p.sid = 0
		return nil
	}

	maxOrderTime := orderTimes[len(orderTimes)-1]
	if p.lastFile {
		maxOrderTime = math.MaxInt64
	}

	if err := p.ur.InitTimes(sid, maxOrderTime); err != nil {
		return err
	}

	unorderedTimes := p.ur.ReadAllTimes()
	p.noUnorderedSeries = len(unorderedTimes) == 0

	if p.noUnorderedSeries {
		p.unorderedSchemas = nil
		p.mergedTimes = append(p.mergedTimes[:0], orderTimes...)
	} else {
		p.unorderedSchemas = p.ur.ReadSeriesSchemas(sid, maxOrderTime)
		p.mergedTimes = MergeTimes(orderTimes, unorderedTimes, p.mergedTimes[:0])
		p.stat.IntersectSeriesCount++
	}

	p.mergedTimeCol.Init()
	p.mergedTimeCol.AppendTimes(p.mergedTimes)

	p.sid = sid
	p.ref = nil
	p.sw.ChangeSid(p.sid)

	return nil
}

func (p *mergePerformer) ColumnChanged(ref *record.Field) error {
	p.ref = ref
	p.noUnorderedColumn = true

	sl := len(p.unorderedSchemas)
	if p.noUnorderedSeries || sl == 0 {
		return p.sw.AppendColumn(ref)
	}

	pos := 0
	for i := 0; i < sl; i++ {
		uRef := &p.unorderedSchemas[i]
		if uRef.Name < ref.Name {
			// columns that exist only in unordered data
			if err := p.writeUnorderedCol(uRef); err != nil {
				return err
			}
			pos++
			continue
		}

		if uRef.Name == ref.Name {
			pos++
			p.noUnorderedColumn = false
		}

		break
	}

	p.unorderedSchemas = p.unorderedSchemas[pos:]
	return p.sw.AppendColumn(ref)
}

func (p *mergePerformer) Finish() error {
	if err := p.finishSeries(math.MaxInt64); err != nil {
		return err
	}

	file, err := p.sw.NewTSSPFile(true)
	if err != nil {
		return err
	}

	// F-04: nil guard. NewTSSPFile may return (nil, nil) when the stream is
	// empty (errEmptyFile is already cleaned up inside NewTSSPFile). Skip
	// appending in that case to avoid nil-deref downstream.
	if file != nil {
		p.AppendMergedFile(file)
	}

	return nil
}

// maybeRotateBefore is the lazy split boundary for the G (global-last) file.
// It is invoked at series-change points (SeriesChanged and writeRemain
// callback) AFTER the previous series has been fully flushed to the current
// writer and BEFORE the next series (nextSID) is started.
//
// Pre-flight (47.5): the current writer is still writable when we probe the
// candidate exact-next path, so on a soft-stop (.init exists / extent
// overflow) we can keep appending to the current file. Only after the
// pre-flight passes do we seal the current file and create the exact-next.
//
// Sticky: once splitDisabled is set, this method becomes a no-op for the
// remainder of the performer run.
//
// nextSID is accepted for symmetry/documentation; the decision to rotate is
// based only on file size and path availability, not on the SID value.
func (p *mergePerformer) maybeRotateBefore(nextSID uint64) error {
	_ = nextSID
	if !p.lastFile || p.splitDisabled {
		return nil
	}

	// Size gate: only consider rotation when the current writer has reached
	// its configured file-size limit. Read the limit from p.sw.Conf (the
	// writer's own config) rather than the process-global TsStoreConfig: the
	// writer may have been constructed with a per-store Config copy that
	// diverges from the global, and the gate must reflect what the writer
	// actually enforces.
	if p.sw.Size() < p.sw.Conf.GetFileSizeLimit() {
		return nil
	}

	// Build the candidate exact-next file name from the current writer's
	// live fileName: (seq, level, merge, extent+1). NewFile(addFileExt=true)
	// will later increment extent, so we probe extent+1 here to detect
	// collisions before committing.
	cur := p.sw.fileName
	if cur.extent == math.MaxUint16 {
		// extent would overflow; give up on splitting for this run.
		p.splitDisabled = true
		return nil
	}
	candidate := cur
	candidate.extent++

	dir := filepath.Join(p.sw.dir, p.sw.name)
	initPath := candidate.Path(dir, true)   // .tssp.init
	finalPath := candidate.Path(dir, false) // .tssp

	// Pre-flight order (49.2): probe the FINALIZED .tssp path first, then the
	// in-progress .init path. A sealed .tssp at the exact-next slot is a hard
	// failure (ambiguous lineage — we must not shadow or overwrite a sealed
	// file) and must be surfaced even if a stale .init also happens to be
	// present; checking .init first would soft-stop and silently hide the
	// harder conflict. Only when the .tssp slot is clear do we check .init
	// for a soft-stop (concurrent writer mid-creation).
	if _, err := fileops.Stat(finalPath); err == nil {
		return fmt.Errorf("file(%s) exist", finalPath)
	} else if !os.IsNotExist(err) {
		// Genuine IO error probing the finalized path — surface it.
		return err
	}

	// .tssp slot is clear. Probe the .init path. Existence means another
	// writer (or a crashed prior run) is mid-creation of this exact-next
	// file; soft-stop so we keep appending to the current writer.
	if _, err := fileops.Stat(initPath); err == nil {
		p.splitDisabled = true
		return nil
	} else if !os.IsNotExist(err) {
		// Genuine IO error probing the path — surface it.
		return err
	}

	// Pre-flight passed. Seal the current writer into a TSSPFile and append
	// it to the merged set. NewTSSPFile(true) Flush+CreateTSSPFileReader on
	// the current fd; the returned file carries the pre-rotation fileName
	// (the current extent, pre-increment).
	sealed, err := p.sw.NewTSSPFile(true)
	if err != nil {
		return err
	}
	if sealed != nil {
		p.AppendMergedFile(sealed)
	}

	// Reset stale file-level state from the just-sealed file BEFORE
	// initializing the next file. StreamWriteFile.NewFile (called inside
	// InitMergedFile) only resets the embedded TableData (trailerData,
	// bloomFilter, metaIndexItems, inMemBlock) and sets trailer.name; it
	// leaves trailer (struct fields like idCount/minTime/maxId), mIndex,
	// pair, cmOffset, currentCMOffset, chunkRows, maxChunkRows, fileSize,
	// dstMeta, schema, and rowCount carrying stale values from the sealed
	// file. Those would corrupt the next file's meta/offset index if left
	// in place.
	//
	// Ordering rationale (49.3): NewFile is the LAST file-level
	// initialization for the new fd/trailer/measurement name. It must run
	// AFTER the stale-state reset, so the reset cannot clobber anything
	// NewFile established. The mirror reference is StreamIterators.reset in
	// stream_compact.go (consecutive-file rotation on a stream writer),
	// which follows the same "clear-then-init" shape.
	resetStreamWriteFileForRotation(p.sw)

	// Create the exact-next file in-place on the same StreamWriteFile.
	// InitMergedFile(sealed, {addFileExt:true}) sets c.fileName = sealed's
	// name (== pre-rotation extent) and then NewFile(true) increments
	// extent, yielding exactly the candidate path we just probed.
	if err := p.sw.InitMergedFile(sealed, initMergedFileOptions{addFileExt: true}); err != nil {
		// Current file is already sealed; we cannot recover this run.
		return err
	}

	// Reset merge-performer writer-local state for the fresh file. The new
	// writer has a clean fd/trailer, so the column writer must be replaced
	// (the old cw holds remain/remainTime segments for the sealed file) and
	// all active-series state must be cleared.
	p.cw = newColumnWriter(p.sw, GetMaxRowsPerSegment4TsStore())
	p.sid = 0
	p.ref = nil
	p.unorderedSchemas = nil
	p.noUnorderedSeries = false
	p.noUnorderedColumn = false
	p.mergedTimes = p.mergedTimes[:0]
	p.mergedTimeCol.Init()
	p.nilCol.Init()

	return nil
}

// resetStreamWriteFileForRotation clears the StreamWriteFile file-level
// fields that NewFile (and its internal TableData.reset) does not touch.
// Without this, a second file created on the same StreamWriteFile inherits
// stale trailer / currentCMOffset / pair / chunkRows / dstMeta state from
// the previously-sealed file, corrupting the new file's chunk-meta offsets
// and id/time index.
//
// This is the merge-rotation analogue of StreamIterators.reset
// (stream_compact.go:430). It lives here rather than in stream_downsample.go
// to keep the general NewFile/InitMergedFile path unchanged for compaction
// and downsample callers.
func resetStreamWriteFileForRotation(c *StreamWriteFile) {
	c.trailer.reset()
	c.mIndex.reset()
	c.pair.Reset(c.name)
	c.cmOffset = c.cmOffset[:0]
	c.currentCMOffset = 0
	c.chunkRows = 0
	c.maxChunkRows = 0
	c.fileSize = 0
	c.dstMeta = ChunkMeta{}
	c.schema = c.schema[:0]
	c.rowCount = make(map[string]int)
}

func (p *mergePerformer) finishSeries(sid uint64) error {
	if err := p.writeRemainCol(); err != nil {
		return err
	}

	if err := p.writeMergedTime(); err != nil {
		return err
	}

	if err := p.sw.WriteCurrentMeta(); err != nil {
		return err
	}

	return p.writeRemain(sid)
}

func (p *mergePerformer) AppendMergedFile(file TSSPFile) {
	p.mergedFiles.Append(file)
}

func (p *mergePerformer) MergedFiles() *TSSPFiles {
	return &p.mergedFiles
}

func (p *mergePerformer) CleanTmpFiles() {
	for _, f := range p.mergedFiles.Files() {
		if err := f.Remove(); err != nil {
			logger.GetLogger().Error("failed to remove tmp file", zap.String("file", f.Path()))
		}
	}

	if p.sw != nil {
		p.sw.Close(true)
	}
}

func (p *mergePerformer) writeRemainCol() error {
	if len(p.unorderedSchemas) == 0 {
		return nil
	}

	for _, item := range p.unorderedSchemas {
		if err := p.writeUnorderedCol(&item); err != nil {
			return err
		}
	}

	p.unorderedSchemas = nil
	return nil
}

// Write the data whose sid is smaller than maxSid in the unordered data
func (p *mergePerformer) writeRemain(maxSid uint64) error {
	if !p.lastFile {
		return nil
	}

	var lastSid uint64 = 0
	err := p.ur.ReadRemain(maxSid, func(sid uint64, ref record.Field, col *record.ColVal, times []int64) error {
		if lastSid != sid {
			if err := p.sw.WriteCurrentMeta(); err != nil {
				return err
			}
			// Lazy split boundary (47.6 remaining-A→remaining-B):
			// the previous remaining sid is sealed via WriteCurrentMeta
			// above. Probe rotation before starting the new remaining sid.
			if err := p.maybeRotateBefore(sid); err != nil {
				return err
			}
			p.sid = sid
			p.sw.ChangeSid(sid)
			lastSid = sid
		}

		if err := p.sw.AppendColumn(&ref); err != nil {
			return err
		}

		return p.write(&ref, col, times, true)
	})

	if err == nil {
		err = p.sw.WriteCurrentMeta()
	}

	return err
}

func (p *mergePerformer) merge(orderCol, unorderedCol *record.ColVal,
	orderTimes, unorderedTimes []int64, ref *record.Field, lastSeg bool) error {
	// No unordered data exists in the time range
	if len(unorderedTimes) == 0 {
		return p.write(ref, orderCol, orderTimes, lastSeg)
	}

	p.mh.AddUnorderedCol(unorderedCol, unorderedTimes)
	mergedCol, mergedTimeCol, err := p.mh.Merge(orderCol, orderTimes, ref.Type)
	if err != nil {
		return err
	}

	return p.write(ref, mergedCol, mergedTimeCol, lastSeg)
}

func (p *mergePerformer) readUnordered(max int64) (*record.ColVal, []int64, error) {
	var times []int64
	var col *record.ColVal
	var err error

	if p.noUnorderedColumn {
		times = p.ur.ReadTimes(p.ref, max)
		col = p.ur.AllocNilCol(len(times), p.ref)
	} else {
		col, times, err = p.ur.Read(p.sid, p.ref, max)
	}

	return col, times, err
}

func (p *mergePerformer) writeUnorderedCol(ref *record.Field) error {
	if err := p.sw.AppendColumn(ref); err != nil {
		return err
	}

	maxTime := p.mergedTimes[len(p.mergedTimes)-1]
	if p.lastFile {
		maxTime = math.MaxInt64
	}

	orderCol := &p.nilCol
	FillNilCol(orderCol, len(p.mergedTimes), ref)
	unorderedCol, unorderedTimes, err := p.ur.Read(p.sid, ref, maxTime)
	if err != nil {
		return err
	}
	return p.merge(orderCol, unorderedCol, p.mergedTimes, unorderedTimes, ref, true)
}

func (p *mergePerformer) writeMergedTime() error {
	if p.sid == 0 {
		return nil
	}

	if err := p.sw.AppendColumn(&timeField); err != nil {
		return err
	}

	return p.cw.writeAll(p.sid, timeRef, p.mergedTimeCol)
}

func (p *mergePerformer) write(ref *record.Field, col *record.ColVal, times []int64, lastSeg bool) error {
	if err := p.cw.write(p.sid, ref, col, times); err != nil {
		return err
	}

	if lastSeg {
		return p.cw.flush(p.sid, ref)
	}

	return nil
}

func (p *mergePerformer) HasSeries(sid uint64) bool {
	return p.ur.HasSeries(sid)
}

func (p *mergePerformer) WriteOriginal(fi *FileIterator) error {
	meta := fi.GetCurtChunkMeta()

	limit := uint32(fileops.DefaultBufferSize * 2)
	offset := meta.offset
	readSize := uint32(0)

	d := p.sw.writer.DataSize() - meta.offset
	meta.offset = p.sw.writer.DataSize()

	var cm *ColumnMeta
	for i := range meta.colMeta {
		cm = &meta.colMeta[i]
		for j := range cm.entries {
			cm.entries[j].offset += d
		}
	}

	var buf []byte
	var err error
	var n int

	for readSize < meta.size {
		if readSize+limit > meta.size {
			limit = meta.size - readSize
		}
		readSize += limit

		buf, err = fi.readData(offset, limit)
		if err != nil {
			return err
		}
		if len(buf) != int(limit) {
			return errno.NewError(errno.ShortRead, len(buf), limit)
		}
		offset += int64(limit)

		n, err = p.sw.writer.WriteData(buf)
		if err != nil {
			return err
		}
		if n != len(buf) {
			return errno.NewError(errno.ShortWrite, n, len(buf))
		}
	}

	return p.sw.WriteMeta(meta)
}

type columnWriter struct {
	sw         *StreamWriteFile
	remain     *record.ColVal
	remainTime *record.ColVal

	limit int
}

func newColumnWriter(sw *StreamWriteFile, limit int) *columnWriter {
	return &columnWriter{
		sw:         sw,
		remain:     &record.ColVal{},
		remainTime: &record.ColVal{},
		limit:      limit,
	}
}

func (cw *columnWriter) writeAll(sid uint64, ref *record.Field, col *record.ColVal) error {
	if col.Len <= cw.limit {
		return cw.sw.WriteData(sid, *ref, *col, nil)
	}

	cols := col.Split(nil, cw.limit, ref.Type)

	for i := range cols {
		if err := cw.sw.WriteData(sid, *ref, cols[i], nil); err != nil {
			return err
		}
	}

	return nil
}

func (cw *columnWriter) write(sid uint64, ref *record.Field, col *record.ColVal, times []int64) error {
	failpoint.Inject("column-writer-error", func() {
		failpoint.Return(fmt.Errorf("failed to wirte column data"))
	})

	cw.remainTime.AppendTimes(times)

	// fast path
	if cw.remain.Len == 0 && (col.Len == cw.limit) {
		defer cw.remainTime.Init()
		return cw.sw.WriteData(sid, *ref, *col, cw.remainTime)
	}

	cw.remain.AppendColVal(col, ref.Type, 0, col.Len)
	if cw.remain.Len != cw.remainTime.Len {
		return errors.New("BUG: The length of the data column is different from that of the time column")
	}

	// data is less than one segment
	if cw.remain.Len < cw.limit {
		return nil
	}

	if cw.remain.Len == cw.limit {
		err := cw.sw.WriteData(sid, *ref, *cw.remain, cw.remainTime)
		cw.remain.Init()
		cw.remainTime.Init()
		return err
	}

	cols, timeCols := cw.splitRemain(ref.Type)
	for i := range cols {
		if cols[i].Len < cw.limit {
			cw.remain = &cols[i]
			cw.remainTime = &timeCols[i]
			break
		}
		if err := cw.sw.WriteData(sid, *ref, cols[i], &timeCols[i]); err != nil {
			return err
		}
	}

	return nil
}

func (cw *columnWriter) splitRemain(typ int) ([]record.ColVal, []record.ColVal) {
	cols := cw.remain.Split(nil, cw.limit, typ)
	times := cw.remainTime.Split(nil, cw.limit, influx.Field_Type_Int)
	cw.remain.Init()
	cw.remainTime.Init()

	return cols, times
}

func (cw *columnWriter) flush(sid uint64, ref *record.Field) error {
	if cw.remain.Len == 0 {
		return nil
	}
	defer func() {
		cw.remain.Init()
		cw.remainTime.Init()
	}()

	return cw.sw.WriteData(sid, *ref, *cw.remain, cw.remainTime)
}
