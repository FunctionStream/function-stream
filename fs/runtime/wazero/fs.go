/*
 * Copyright 2024 Function Stream Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wazero

import (
	"bytes"
	"io/fs"

	. "github.com/tetratelabs/wazero/experimental/sys"
	"github.com/tetratelabs/wazero/sys"
)

type memoryFS struct {
	FS
	m map[string]File
}

func (f *memoryFS) OpenFile(path string, _ Oflag, _ fs.FileMode) (File, Errno) {
	if path == "." {
		return &memoryFile{isDir: true}, 0
	}
	if file, ok := f.m[path]; ok {
		return file, 0
	}
	return nil, ENOENT
}

type memoryFile struct {
	File
	isDir    bool
	readBuf  bytes.Buffer
	writeBuf bytes.Buffer
}

func newMemoryFS(m map[string]File) FS {
	return &memoryFS{
		m: m,
	}
}

func (f *memoryFile) Read(p []byte) (n int, errno Errno) {
	n, _ = f.readBuf.Read(p)
	errno = 0
	return
}

func (f *memoryFile) Write(buf []byte) (n int, errno Errno) {
	n, _ = f.writeBuf.Write(buf)
	errno = 0
	return
}

func (f *memoryFile) IsDir() (bool, Errno) {
	return f.isDir, 0
}

func (f *memoryFile) Close() Errno {
	return 0
}

func (f *memoryFile) Stat() (sys.Stat_t, Errno) {
	return sys.Stat_t{}, 0
}
