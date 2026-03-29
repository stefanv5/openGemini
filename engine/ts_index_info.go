// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/immutable"
)

type TSIndexInfoImpl struct {
	immTables []*immutable.MmsReaders
	memTables []MemDataReader
	cursors   []comm.KeyCursor
}

func NewTsIndexInfo(immTables []*immutable.MmsReaders, memTables []MemDataReader, cursors []comm.KeyCursor) comm.TSIndexInfo {
	return &TSIndexInfoImpl{
		immTables: immTables,
		memTables: memTables,
		cursors:   cursors,
	}
}

func (f *TSIndexInfoImpl) IsEmpty() bool {
	return f == nil || len(f.cursors) == 0
}

func (f *TSIndexInfoImpl) SetCursors(cursors []comm.KeyCursor) {
	f.cursors = cursors
}

func (f *TSIndexInfoImpl) GetCursors() []comm.KeyCursor {
	return f.cursors
}

func (f *TSIndexInfoImpl) Ref() {
	f.RefFiles()
	f.RefMemTables()
}

func (f *TSIndexInfoImpl) Unref() {
	f.UnRefFiles()
	f.UnRefMemTables()
}

func (f *TSIndexInfoImpl) RefFiles() {
	for _, immTable := range f.immTables {
		for _, file := range immTable.Orders {
			file.Ref()
			file.RefFileReader()
		}
		for _, file := range immTable.OutOfOrders {
			file.Ref()
			file.RefFileReader()
		}
	}
}

func (f *TSIndexInfoImpl) UnRefFiles() {
	for _, immTable := range f.immTables {
		f.unRefFiles(immTable.Orders)
		f.unRefFiles(immTable.OutOfOrders)
	}
}

func (f *TSIndexInfoImpl) RefMemTables() {
	for _, memTable := range f.memTables {
		memTable.Ref()
	}
}

func (f *TSIndexInfoImpl) UnRefMemTables() {
	for _, memTable := range f.memTables {
		memTable.UnRef()
	}
}

func (f *TSIndexInfoImpl) unRefFiles(files immutable.TableReaders) {
	for _, file := range files {
		// UnrefFileReader: when reader ref reaches 0, delegates to ShardLeaseCache
		// (Lease timer starts). If no LeaseCache is set, closes directly.
		// Returns true if the file entered the Lease window; in that case,
		// the Lease "owns" the file lifecycle and the caller must NOT call
		// file.Unref() to avoid double-decrement of tsspFile ref.
		if file.UnrefFileReader() {
			continue
		}
		// Normal path: no Lease was entered, caller owns tsspFile ref decrement.
		file.Unref()
	}
}
