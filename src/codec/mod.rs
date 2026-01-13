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

// Codec module - Codec utility module
//
// Provides basic serialization and deserialization utilities, including:
// - Variable-length integer encoding/decoding (VarInt)
// - ZigZag encoding/decoding
// - String and byte array encoding/decoding
// - Basic type encoding/decoding
// - Protocol Buffers codec

mod primitive;
mod protobuf;
mod string_codec;
mod varint;
mod zigzag;

pub use protobuf::*;
pub use string_codec::*;
pub use varint::*;
