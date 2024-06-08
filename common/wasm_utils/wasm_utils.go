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

package wasm_utils

import "unsafe"

// StringToPtr returns a pointer and size pair for the given string in a way
// compatible with WebAssembly numeric types.
// The returned pointer aliases the string hence the string must be kept alive
// until ptr is no longer needed.
func StringToPtr(s string) (uint32, uint32) {
	ptr := unsafe.Pointer(unsafe.StringData(s))
	return uint32(uintptr(ptr)), uint32(len(s))
}

//// StringToLeakedPtr returns a pointer and size pair for the given string in a way
//// compatible with WebAssembly numeric types.
//// The pointer is not automatically managed by TinyGo hence it must be freed by the host.
//func StringToLeakedPtr(s string) (uint32, uint32) {
//	size := C.ulong(len(s))
//	ptr := unsafe.Pointer(C.malloc(size))
//	copy(unsafe.Slice((*byte)(ptr), size), s)
//	return uint32(uintptr(ptr)), uint32(size)
//}

func PtrSize(ptr, size uint32) uint64 {
	return (uint64(ptr) << 32) | uint64(size)
}

func ExtractPtrSize(ptrSize uint64) (uint32, uint32) {
	return uint32(ptrSize >> 32), uint32(ptrSize)
}
