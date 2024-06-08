package wazero

import (
	"bytes"
	. "github.com/tetratelabs/wazero/experimental/sys"
	"github.com/tetratelabs/wazero/sys"
	"io/fs"
)

type memoryFS struct {
	FS
	m map[string]File
}

func (f *memoryFS) OpenFile(path string, flags Oflag, perm fs.FileMode) (File, Errno) {
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

func newMemoryFile() File {
	return &memoryFile{
		readBuf:  bytes.Buffer{},
		writeBuf: bytes.Buffer{},
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

func (f *memoryFile) Dev() (uint64, Errno) {
	return 0, 0
}

func (f *memoryFile) Ino() (sys.Inode, Errno) {
	return 0, 0
}

func (f *memoryFile) IsDir() (bool, Errno) {
	return f.isDir, 0
}

func (f *memoryFile) IsAppend() bool {
	return false
}

func (f *memoryFile) Close() Errno {
	return 0
}

func (f *memoryFile) Stat() (sys.Stat_t, Errno) {
	return sys.Stat_t{}, 0
}
