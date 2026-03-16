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

use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let out_dir = Path::new(&manifest_dir).join("generated");

    log::info!("Generated code will be placed in: {}", out_dir.display());

    let cli_dir = out_dir.join("cli");
    let service_dir = out_dir.join("service");

    std::fs::create_dir_all(&cli_dir)?;
    std::fs::create_dir_all(&service_dir)?;

    // 1. function_stream.proto → CLI (client) and Service (server)
    tonic_build::configure()
        .out_dir(&cli_dir)
        .build_client(true)
        .build_server(false)
        .compile_protos(&["proto/function_stream.proto"], &["proto"])?;

    tonic_build::configure()
        .out_dir(&service_dir)
        .build_client(false)
        .build_server(true)
        .compile_protos(&["proto/function_stream.proto"], &["proto"])?;

    // 2. fs_api.proto → with file descriptor set + serde for REST/JSON
    let api_dir = out_dir.join("api");
    std::fs::create_dir_all(&api_dir)?;

    let descriptor_path =
        PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("fs_api_descriptor.bin");

    tonic_build::configure()
        .out_dir(&api_dir)
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(&descriptor_path)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".", "#[serde(rename_all = \"camelCase\")]")
        .build_client(false)
        .build_server(false)
        .compile_protos(&["proto/fs_api.proto"], &["proto"])?;

    log::info!("Protocol Buffers code generated successfully");
    println!("cargo:rustc-env=PROTO_GEN_DIR={}", out_dir.display());
    println!("cargo:rerun-if-changed=proto/function_stream.proto");
    println!("cargo:rerun-if-changed=proto/fs_api.proto");

    Ok(())
}
