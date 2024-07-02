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
	"github.com/functionstream/function-stream/common"
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
		return &oneShotFile{isDir: true}, 0
	}
	if file, ok := f.m[path]; ok {
		return file, 0
	}
	return nil, ENOENT
}

func newMemoryFS(m map[string]File) FS {
	return &memoryFS{
		m: m,
	}
}

type oneShotFile struct {
	File
	isDir  bool
	input  []byte
	output []byte
}

func (f *oneShotFile) Read(p []byte) (n int, errno Errno) {
	copy(p, f.input)
	return len(p), 0
}

func (f *oneShotFile) Write(buf []byte) (n int, errno Errno) {
	f.output = make([]byte, len(buf))
	copy(f.output, buf)
	return len(buf), 0
}

func (f *oneShotFile) IsDir() (bool, Errno) {
	return f.isDir, 0
}

func (f *oneShotFile) Close() Errno {
	return 0
}

func (f *oneShotFile) Stat() (sys.Stat_t, Errno) {
	return sys.Stat_t{
		Size: int64(len(f.input)),
	}, 0
}

type logWriter struct {
	log *common.Logger
}

func (f *logWriter) Write(buf []byte) (n int, err error) {
	f.log.Info(string(buf))
	return len(buf), nil
}
