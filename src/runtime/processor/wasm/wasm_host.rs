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

use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::output::OutputSink;
use crate::runtime::processor::wasm::wasm_cache;
use crate::storage::state_backend::{StateStore, StateStoreFactory};
use std::sync::{Arc, OnceLock};
use wasmtime::component::{Component, HasData, Linker, Resource, bindgen};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

static GLOBAL_ENGINE: OnceLock<Arc<Engine>> = OnceLock::new();

fn enable_incremental_compilation(config: &mut Config) -> bool {
    if !wasm_cache::is_cache_enabled() {
        return false;
    }

    #[cfg(feature = "incremental-cache")]
    {
        if let Some(cache_store) = wasm_cache::create_cache_store() {
            let _ = config.enable_incremental_compilation(cache_store);
            return true;
        }
    }

    false
}

fn get_global_engine(_wasm_size: usize) -> anyhow::Result<Arc<Engine>> {
    if let Some(engine) = GLOBAL_ENGINE.get() {
        return Ok(Arc::clone(engine));
    }

    let engine = GLOBAL_ENGINE.get_or_init(|| {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.async_support(false);
        config.cranelift_opt_level(wasmtime::OptLevel::Speed);
        config.debug_info(false);
        config.generate_address_map(false);
        config.parallel_compilation(true);
        enable_incremental_compilation(&mut config);

        let engine = Engine::new(&config).unwrap_or_else(|e| {
            panic!("Failed to create global wasm engine: {}", e);
        });

        Arc::new(engine)
    });

    Ok(Arc::clone(engine))
}

bindgen!({
    world: "processor",
    path: "wit",
    with: {
        "functionstream:core/kv.store": FunctionStreamStoreHandle,
        "functionstream:core/kv.iterator": FunctionStreamIteratorHandle,
    }
});

use functionstream::core::kv::{self, ComplexKey, Error, HostIterator, HostStore};

pub struct FunctionStreamStoreHandle {
    pub name: String,
    pub state_store: Box<dyn StateStore>,
}

impl Drop for FunctionStreamStoreHandle {
    fn drop(&mut self) {}
}

pub struct FunctionStreamIteratorHandle {
    pub state_iterator: Box<dyn crate::storage::state_backend::StateIterator>,
}

pub struct HostState {
    pub wasi: WasiCtx,
    pub table: ResourceTable,
    pub factory: Arc<dyn StateStoreFactory>,
    pub output_sinks: Vec<Box<dyn OutputSink>>,
}

pub struct HostStateData;

impl HasData for HostStateData {
    type Data<'a> = &'a mut HostState;
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl kv::Host for HostState {}

// Implement WASI traits required by bindgen!
// These delegate to wasmtime_wasi::p2::add_to_linker_sync which handles the actual implementation
use crate::runtime::processor::wasm::wasm_host::wasi::cli::{
    environment, exit, stderr, stdin, stdout, terminal_input, terminal_output, terminal_stderr,
    terminal_stdin, terminal_stdout,
};
use crate::runtime::processor::wasm::wasm_host::wasi::clocks::{monotonic_clock, wall_clock};
use crate::runtime::processor::wasm::wasm_host::wasi::filesystem::{preopens, types};
use crate::runtime::processor::wasm::wasm_host::wasi::io::{error, poll, streams};
use crate::runtime::processor::wasm::wasm_host::wasi::random::{insecure, insecure_seed, random};
use crate::runtime::processor::wasm::wasm_host::wasi::sockets::{
    instance_network, ip_name_lookup, network, tcp, tcp_create_socket, udp, udp_create_socket,
};

impl environment::Host for HostState {
    fn get_environment(&mut self) -> Vec<(String, String)> {
        // Delegate to wasmtime_wasi - these are handled by add_to_linker_sync
        vec![]
    }

    fn get_arguments(&mut self) -> Vec<String> {
        vec![]
    }

    fn initial_cwd(&mut self) -> Option<String> {
        Some(".".to_string())
    }
}

impl exit::Host for HostState {
    fn exit(&mut self, status: Result<(), ()>) {
        let code = match status {
            Ok(()) => 0,
            Err(()) => 1,
        };
        std::process::exit(code);
    }
}

impl error::Host for HostState {}
impl error::HostError for HostState {
    fn drop(
        &mut self,
        _error: wasmtime::component::Resource<error::Error>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }

    fn to_debug_string(&mut self, _error: wasmtime::component::Resource<error::Error>) -> String {
        "WASI error".to_string()
    }
}

impl poll::Host for HostState {
    fn poll(&mut self, _pollables: Vec<wasmtime::component::Resource<poll::Pollable>>) -> Vec<u32> {
        vec![]
    }
}

impl poll::HostPollable for HostState {
    fn drop(
        &mut self,
        _pollable: wasmtime::component::Resource<poll::Pollable>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }

    fn ready(&mut self, _pollable: wasmtime::component::Resource<poll::Pollable>) -> bool {
        false
    }

    fn block(&mut self, _pollable: wasmtime::component::Resource<poll::Pollable>) {
        // Handled by wasmtime_wasi
    }
}

impl streams::Host for HostState {}
impl streams::HostInputStream for HostState {
    fn drop(
        &mut self,
        _stream: wasmtime::component::Resource<streams::InputStream>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }

    fn read(
        &mut self,
        _stream: wasmtime::component::Resource<streams::InputStream>,
        _len: u64,
    ) -> Result<Vec<u8>, streams::StreamError> {
        Err(streams::StreamError::LastOperationFailed(
            wasmtime::component::Resource::new_own(0),
        ))
    }

    fn blocking_read(
        &mut self,
        _stream: wasmtime::component::Resource<streams::InputStream>,
        _len: u64,
    ) -> Result<Vec<u8>, streams::StreamError> {
        Err(streams::StreamError::LastOperationFailed(
            wasmtime::component::Resource::new_own(0),
        ))
    }

    fn skip(
        &mut self,
        _stream: wasmtime::component::Resource<streams::InputStream>,
        _len: u64,
    ) -> Result<u64, streams::StreamError> {
        Err(streams::StreamError::LastOperationFailed(
            wasmtime::component::Resource::new_own(0),
        ))
    }

    fn blocking_skip(
        &mut self,
        _stream: wasmtime::component::Resource<streams::InputStream>,
        _len: u64,
    ) -> Result<u64, streams::StreamError> {
        Err(streams::StreamError::LastOperationFailed(
            wasmtime::component::Resource::new_own(0),
        ))
    }

    fn subscribe(
        &mut self,
        _stream: wasmtime::component::Resource<streams::InputStream>,
    ) -> wasmtime::component::Resource<poll::Pollable> {
        wasmtime::component::Resource::new_own(0)
    }
}

impl streams::HostOutputStream for HostState {
    fn drop(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }

    fn check_write(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
    ) -> Result<u64, streams::StreamError> {
        Ok(0)
    }

    fn write(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
        _contents: Vec<u8>,
    ) -> Result<(), streams::StreamError> {
        Ok(())
    }

    fn blocking_write_and_flush(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
        _contents: Vec<u8>,
    ) -> Result<(), streams::StreamError> {
        Ok(())
    }

    fn flush(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
    ) -> Result<(), streams::StreamError> {
        Ok(())
    }

    fn blocking_flush(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
    ) -> Result<(), streams::StreamError> {
        Ok(())
    }

    fn subscribe(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
    ) -> wasmtime::component::Resource<poll::Pollable> {
        wasmtime::component::Resource::new_own(0)
    }

    fn write_zeroes(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
        _len: u64,
    ) -> Result<(), streams::StreamError> {
        Ok(())
    }

    fn blocking_write_zeroes_and_flush(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
        _len: u64,
    ) -> Result<(), streams::StreamError> {
        Ok(())
    }

    fn splice(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
        _src: wasmtime::component::Resource<streams::InputStream>,
        _len: u64,
    ) -> Result<u64, streams::StreamError> {
        Ok(0)
    }

    fn blocking_splice(
        &mut self,
        _stream: wasmtime::component::Resource<streams::OutputStream>,
        _src: wasmtime::component::Resource<streams::InputStream>,
        _len: u64,
    ) -> Result<u64, streams::StreamError> {
        Ok(0)
    }
}

impl stdin::Host for HostState {
    fn get_stdin(&mut self) -> wasmtime::component::Resource<streams::InputStream> {
        wasmtime::component::Resource::new_own(0)
    }
}

impl stdout::Host for HostState {
    fn get_stdout(&mut self) -> wasmtime::component::Resource<streams::OutputStream> {
        wasmtime::component::Resource::new_own(0)
    }
}

impl stderr::Host for HostState {
    fn get_stderr(&mut self) -> wasmtime::component::Resource<streams::OutputStream> {
        wasmtime::component::Resource::new_own(0)
    }
}

impl terminal_input::Host for HostState {}
impl terminal_input::HostTerminalInput for HostState {
    fn drop(
        &mut self,
        _terminal_input: wasmtime::component::Resource<terminal_input::TerminalInput>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }
}

impl terminal_output::Host for HostState {}
impl terminal_output::HostTerminalOutput for HostState {
    fn drop(
        &mut self,
        _terminal_output: wasmtime::component::Resource<terminal_output::TerminalOutput>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }
}

impl terminal_stdin::Host for HostState {
    fn get_terminal_stdin(
        &mut self,
    ) -> Option<wasmtime::component::Resource<terminal_input::TerminalInput>> {
        None
    }
}

impl terminal_stdout::Host for HostState {
    fn get_terminal_stdout(
        &mut self,
    ) -> Option<wasmtime::component::Resource<terminal_output::TerminalOutput>> {
        None
    }
}

impl terminal_stderr::Host for HostState {
    fn get_terminal_stderr(
        &mut self,
    ) -> Option<wasmtime::component::Resource<terminal_output::TerminalOutput>> {
        None
    }
}

impl monotonic_clock::Host for HostState {
    fn now(&mut self) -> u64 {
        0
    }

    fn resolution(&mut self) -> u64 {
        1_000_000_000
    }

    fn subscribe_instant(&mut self, _when: u64) -> wasmtime::component::Resource<poll::Pollable> {
        wasmtime::component::Resource::new_own(0)
    }

    fn subscribe_duration(
        &mut self,
        _duration: u64,
    ) -> wasmtime::component::Resource<poll::Pollable> {
        wasmtime::component::Resource::new_own(0)
    }
}

impl wall_clock::Host for HostState {
    fn now(&mut self) -> wall_clock::Datetime {
        wall_clock::Datetime {
            seconds: 0,
            nanoseconds: 0,
        }
    }

    fn resolution(&mut self) -> wall_clock::Datetime {
        wall_clock::Datetime {
            seconds: 1,
            nanoseconds: 0,
        }
    }
}

impl types::Host for HostState {
    fn filesystem_error_code(
        &mut self,
        _error: wasmtime::component::Resource<types::Error>,
    ) -> Option<types::ErrorCode> {
        None
    }
}

impl types::HostDirectoryEntryStream for HostState {
    fn read_directory_entry(
        &mut self,
        _stream: wasmtime::component::Resource<types::DirectoryEntryStream>,
    ) -> Result<Option<types::DirectoryEntry>, types::ErrorCode> {
        Ok(None)
    }

    fn drop(
        &mut self,
        _stream: wasmtime::component::Resource<types::DirectoryEntryStream>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }
}

impl types::HostDescriptor for HostState {
    fn drop(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }

    fn read_via_stream(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _length: u64,
    ) -> Result<wasmtime::component::Resource<streams::InputStream>, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn write_via_stream(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _offset: types::Filesize,
    ) -> Result<wasmtime::component::Resource<streams::OutputStream>, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn append_via_stream(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
    ) -> Result<wasmtime::component::Resource<streams::OutputStream>, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn advise(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _offset: types::Filesize,
        _length: types::Filesize,
        _advice: types::Advice,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn sync_data(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn get_flags(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
    ) -> Result<types::DescriptorFlags, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn get_type(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
    ) -> Result<types::DescriptorType, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn set_size(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _size: types::Filesize,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn set_times(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _data_access_timestamp: types::NewTimestamp,
        _data_modification_timestamp: types::NewTimestamp,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn read(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _length: u64,
        _offset: types::Filesize,
    ) -> Result<(Vec<u8>, bool), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn write(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _buffer: Vec<u8>,
        _offset: types::Filesize,
    ) -> Result<u64, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn read_directory(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
    ) -> Result<wasmtime::component::Resource<types::DirectoryEntryStream>, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn sync(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn create_directory_at(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _path: String,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn stat(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
    ) -> Result<types::DescriptorStat, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn stat_at(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _path_flags: types::PathFlags,
        _path: String,
    ) -> Result<types::DescriptorStat, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn set_times_at(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _path_flags: types::PathFlags,
        _path: String,
        _data_access_timestamp: types::NewTimestamp,
        _data_modification_timestamp: types::NewTimestamp,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn link_at(
        &mut self,
        _old_descriptor: wasmtime::component::Resource<types::Descriptor>,
        _old_path_flags: types::PathFlags,
        _old_path: String,
        _new_descriptor: wasmtime::component::Resource<types::Descriptor>,
        _new_path: String,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn open_at(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _path_flags: types::PathFlags,
        _path: String,
        _open_flags: types::OpenFlags,
        _descriptor_flags: types::DescriptorFlags,
    ) -> Result<wasmtime::component::Resource<types::Descriptor>, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn readlink_at(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _path: String,
    ) -> Result<String, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn remove_directory_at(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _path: String,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn rename_at(
        &mut self,
        _old_descriptor: wasmtime::component::Resource<types::Descriptor>,
        _old_path: String,
        _new_descriptor: wasmtime::component::Resource<types::Descriptor>,
        _new_path: String,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn symlink_at(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _old_path: String,
        _new_path: String,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn unlink_file_at(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _path: String,
    ) -> Result<(), types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn is_same_object(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _other: wasmtime::component::Resource<types::Descriptor>,
    ) -> bool {
        false
    }

    fn metadata_hash(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
    ) -> Result<types::MetadataHashValue, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }

    fn metadata_hash_at(
        &mut self,
        _descriptor: wasmtime::component::Resource<types::Descriptor>,
        _path_flags: types::PathFlags,
        _path: String,
    ) -> Result<types::MetadataHashValue, types::ErrorCode> {
        Err(types::ErrorCode::BadDescriptor)
    }
}

impl preopens::Host for HostState {
    fn get_directories(
        &mut self,
    ) -> Vec<(wasmtime::component::Resource<types::Descriptor>, String)> {
        vec![]
    }
}

impl network::Host for HostState {}
impl network::HostNetwork for HostState {
    fn drop(
        &mut self,
        _network: wasmtime::component::Resource<network::Network>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }
}

impl instance_network::Host for HostState {
    fn instance_network(&mut self) -> wasmtime::component::Resource<network::Network> {
        wasmtime::component::Resource::new_own(0)
    }
}

impl udp::Host for HostState {}
impl udp::HostOutgoingDatagramStream for HostState {
    fn drop(
        &mut self,
        _stream: wasmtime::component::Resource<udp::OutgoingDatagramStream>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }

    fn check_send(
        &mut self,
        _stream: wasmtime::component::Resource<udp::OutgoingDatagramStream>,
    ) -> Result<u64, udp::ErrorCode> {
        Ok(0)
    }

    fn send(
        &mut self,
        _stream: wasmtime::component::Resource<udp::OutgoingDatagramStream>,
        _datagrams: Vec<udp::OutgoingDatagram>,
    ) -> Result<u64, udp::ErrorCode> {
        Ok(0)
    }

    fn subscribe(
        &mut self,
        _stream: wasmtime::component::Resource<udp::OutgoingDatagramStream>,
    ) -> wasmtime::component::Resource<poll::Pollable> {
        wasmtime::component::Resource::new_own(0)
    }
}

impl udp::HostIncomingDatagramStream for HostState {
    fn drop(
        &mut self,
        _stream: wasmtime::component::Resource<udp::IncomingDatagramStream>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }

    fn receive(
        &mut self,
        _stream: wasmtime::component::Resource<udp::IncomingDatagramStream>,
        _max_results: u64,
    ) -> Result<Vec<udp::IncomingDatagram>, udp::ErrorCode> {
        Ok(vec![])
    }

    fn subscribe(
        &mut self,
        _stream: wasmtime::component::Resource<udp::IncomingDatagramStream>,
    ) -> wasmtime::component::Resource<poll::Pollable> {
        wasmtime::component::Resource::new_own(0)
    }
}

impl udp::HostUdpSocket for HostState {
    fn drop(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }

    fn start_bind(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
        _network: wasmtime::component::Resource<network::Network>,
        _local_address: network::IpSocketAddress,
    ) -> Result<(), udp::ErrorCode> {
        Err(udp::ErrorCode::NotSupported)
    }

    fn finish_bind(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
    ) -> Result<(), udp::ErrorCode> {
        Err(udp::ErrorCode::NotSupported)
    }

    fn stream(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
        _remote_address: Option<network::IpSocketAddress>,
    ) -> Result<
        (
            wasmtime::component::Resource<udp::IncomingDatagramStream>,
            wasmtime::component::Resource<udp::OutgoingDatagramStream>,
        ),
        udp::ErrorCode,
    > {
        Err(udp::ErrorCode::NotSupported)
    }

    fn local_address(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
    ) -> Result<network::IpSocketAddress, udp::ErrorCode> {
        Err(udp::ErrorCode::NotSupported)
    }

    fn remote_address(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
    ) -> Result<network::IpSocketAddress, udp::ErrorCode> {
        Err(udp::ErrorCode::NotSupported)
    }

    fn address_family(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
    ) -> network::IpAddressFamily {
        network::IpAddressFamily::Ipv4
    }

    fn unicast_hop_limit(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
    ) -> Result<u8, udp::ErrorCode> {
        Err(udp::ErrorCode::NotSupported)
    }

    fn set_unicast_hop_limit(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
        _value: u8,
    ) -> Result<(), udp::ErrorCode> {
        Err(udp::ErrorCode::NotSupported)
    }

    fn receive_buffer_size(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
    ) -> Result<u64, udp::ErrorCode> {
        Err(udp::ErrorCode::NotSupported)
    }

    fn set_receive_buffer_size(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
        _value: u64,
    ) -> Result<(), udp::ErrorCode> {
        Err(udp::ErrorCode::NotSupported)
    }

    fn send_buffer_size(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
    ) -> Result<u64, udp::ErrorCode> {
        Err(udp::ErrorCode::NotSupported)
    }

    fn set_send_buffer_size(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
        _value: u64,
    ) -> Result<(), udp::ErrorCode> {
        Err(udp::ErrorCode::NotSupported)
    }

    fn subscribe(
        &mut self,
        _socket: wasmtime::component::Resource<udp::UdpSocket>,
    ) -> wasmtime::component::Resource<poll::Pollable> {
        wasmtime::component::Resource::new_own(0)
    }
}

impl udp_create_socket::Host for HostState {
    fn create_udp_socket(
        &mut self,
        _address_family: network::IpAddressFamily,
    ) -> Result<wasmtime::component::Resource<udp::UdpSocket>, udp_create_socket::ErrorCode> {
        Err(udp_create_socket::ErrorCode::NotSupported)
    }
}

impl tcp::Host for HostState {}
impl tcp::HostTcpSocket for HostState {
    fn drop(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }

    fn start_bind(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _network: wasmtime::component::Resource<network::Network>,
        _local_address: network::IpSocketAddress,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn finish_bind(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn start_connect(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _network: wasmtime::component::Resource<network::Network>,
        _remote_address: network::IpSocketAddress,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn finish_connect(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<
        (
            wasmtime::component::Resource<streams::InputStream>,
            wasmtime::component::Resource<streams::OutputStream>,
        ),
        tcp::ErrorCode,
    > {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn start_listen(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn finish_listen(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn accept(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<
        (
            wasmtime::component::Resource<tcp::TcpSocket>,
            wasmtime::component::Resource<streams::InputStream>,
            wasmtime::component::Resource<streams::OutputStream>,
        ),
        tcp::ErrorCode,
    > {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn local_address(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<network::IpSocketAddress, tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn remote_address(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<network::IpSocketAddress, tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn is_listening(&mut self, _socket: wasmtime::component::Resource<tcp::TcpSocket>) -> bool {
        false
    }

    fn address_family(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> network::IpAddressFamily {
        network::IpAddressFamily::Ipv4
    }

    fn set_listen_backlog_size(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _value: u64,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn keep_alive_enabled(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<bool, tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn set_keep_alive_enabled(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _value: bool,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn keep_alive_idle_time(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<u64, tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn set_keep_alive_idle_time(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _value: u64,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn keep_alive_interval(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<u64, tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn set_keep_alive_interval(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _value: u64,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn keep_alive_count(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<u32, tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn set_keep_alive_count(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _value: u32,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn hop_limit(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<u8, tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn set_hop_limit(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _value: u8,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn receive_buffer_size(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<u64, tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn set_receive_buffer_size(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _value: u64,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn send_buffer_size(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> Result<u64, tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn set_send_buffer_size(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _value: u64,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }

    fn subscribe(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
    ) -> wasmtime::component::Resource<poll::Pollable> {
        wasmtime::component::Resource::new_own(0)
    }

    fn shutdown(
        &mut self,
        _socket: wasmtime::component::Resource<tcp::TcpSocket>,
        _shutdown_type: tcp::ShutdownType,
    ) -> Result<(), tcp::ErrorCode> {
        Err(tcp::ErrorCode::NotSupported)
    }
}

impl tcp_create_socket::Host for HostState {
    fn create_tcp_socket(
        &mut self,
        _address_family: network::IpAddressFamily,
    ) -> Result<wasmtime::component::Resource<tcp::TcpSocket>, tcp_create_socket::ErrorCode> {
        Err(tcp_create_socket::ErrorCode::NotSupported)
    }
}

impl ip_name_lookup::Host for HostState {
    fn resolve_addresses(
        &mut self,
        _network: wasmtime::component::Resource<network::Network>,
        _name: String,
    ) -> Result<
        wasmtime::component::Resource<ip_name_lookup::ResolveAddressStream>,
        ip_name_lookup::ErrorCode,
    > {
        Err(ip_name_lookup::ErrorCode::NotSupported)
    }
}

impl ip_name_lookup::HostResolveAddressStream for HostState {
    fn resolve_next_address(
        &mut self,
        _stream: wasmtime::component::Resource<ip_name_lookup::ResolveAddressStream>,
    ) -> Result<Option<network::IpAddress>, ip_name_lookup::ErrorCode> {
        Ok(None)
    }

    fn drop(
        &mut self,
        _stream: wasmtime::component::Resource<ip_name_lookup::ResolveAddressStream>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }

    fn subscribe(
        &mut self,
        _stream: wasmtime::component::Resource<ip_name_lookup::ResolveAddressStream>,
    ) -> wasmtime::component::Resource<poll::Pollable> {
        wasmtime::component::Resource::new_own(0)
    }
}

impl random::Host for HostState {
    fn get_random_bytes(&mut self, _len: u64) -> Vec<u8> {
        vec![]
    }

    fn get_random_u64(&mut self) -> u64 {
        0
    }
}

impl insecure::Host for HostState {
    fn get_insecure_random_bytes(&mut self, _len: u64) -> Vec<u8> {
        vec![]
    }

    fn get_insecure_random_u64(&mut self) -> u64 {
        0
    }
}

impl insecure_seed::Host for HostState {
    fn insecure_seed(&mut self) -> (u64, u64) {
        (0, 0)
    }
}

impl HostStore for HostState {
    fn new(&mut self, name: String) -> Resource<FunctionStreamStoreHandle> {
        let state_store = self
            .factory
            .new_state_store(Some(name.clone()))
            .unwrap_or_else(|e| {
                panic!("Failed to create state store: {}", e);
            });

        let handle = FunctionStreamStoreHandle { name, state_store };
        self.table.push(handle).unwrap_or_else(|e| {
            panic!("Failed to push resource to table: {}", e);
        })
    }

    fn put_state(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        store
            .state_store
            .put_state(key, value)
            .map_err(|e| Error::Other(format!("Failed to put state: {}", e)))
    }

    fn get_state(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        store
            .state_store
            .get_state(key)
            .map_err(|e| Error::Other(format!("Failed to get state: {}", e)))
    }

    fn delete_state(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: Vec<u8>,
    ) -> Result<(), Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        store
            .state_store
            .delete_state(key)
            .map_err(|e| Error::Other(format!("Failed to delete state: {}", e)))
    }

    fn list_states(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        store
            .state_store
            .list_states(start, end)
            .map_err(|e| Error::Other(format!("Failed to list states: {}", e)))
    }

    fn put(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: ComplexKey,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let real_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &key.user_key,
        );

        store
            .state_store
            .put_state(real_key, value)
            .map_err(|e| Error::Other(format!("Failed to put: {}", e)))
    }

    fn get(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: ComplexKey,
    ) -> Result<Option<Vec<u8>>, Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let real_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &key.user_key,
        );

        store
            .state_store
            .get_state(real_key)
            .map_err(|e| Error::Other(format!("Failed to get: {}", e)))
    }

    fn delete(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: ComplexKey,
    ) -> Result<(), Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let real_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &key.user_key,
        );

        store
            .state_store
            .delete_state(real_key)
            .map_err(|e| Error::Other(format!("Failed to delete: {}", e)))?;
        Ok(())
    }

    fn merge(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: ComplexKey,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        self.put(self_, key, value)
    }

    fn delete_prefix(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: ComplexKey,
    ) -> Result<(), Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let prefix_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &[],
        );

        let keys_to_delete = store
            .state_store
            .list_states(prefix_key.clone(), {
                let mut end_prefix = prefix_key.clone();
                end_prefix.extend_from_slice(&vec![0xFF; 256]);
                end_prefix
            })
            .map_err(|e| Error::Other(format!("Failed to list keys for delete_prefix: {}", e)))?;

        for key_to_delete in keys_to_delete {
            store.state_store.delete_state(key_to_delete).map_err(|e| {
                Error::Other(format!("Failed to delete key in delete_prefix: {}", e))
            })?;
        }

        Ok(())
    }

    fn list_complex(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        start_inclusive: Vec<u8>,
        end_exclusive: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let start_key = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &start_inclusive,
        );
        let end_key = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &end_exclusive,
        );

        store
            .state_store
            .list_states(start_key, end_key)
            .map_err(|e| Error::Other(format!("Failed to list_complex: {}", e)))
    }

    fn scan_complex(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
    ) -> Result<Resource<FunctionStreamIteratorHandle>, Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let state_iterator = store
            .state_store
            .scan_complex(key_group, key, namespace)
            .map_err(|e| Error::Other(format!("Failed to scan_complex: {}", e)))?;

        let iter = FunctionStreamIteratorHandle { state_iterator };
        self.table
            .push(iter)
            .map_err(|e| Error::Other(format!("Failed to push iterator resource: {}", e)))
    }

    fn drop(&mut self, rep: Resource<FunctionStreamStoreHandle>) -> Result<(), anyhow::Error> {
        self.table
            .delete(rep)
            .map_err(|e| anyhow::anyhow!("Failed to delete store resource: {}", e))?;
        Ok(())
    }
}

impl HostIterator for HostState {
    fn has_next(&mut self, self_: Resource<FunctionStreamIteratorHandle>) -> Result<bool, Error> {
        let iter = self
            .table
            .get_mut(&self_)
            .map_err(|e| Error::Other(format!("Failed to get iterator resource: {}", e)))?;

        iter.state_iterator
            .has_next()
            .map_err(|e| Error::Other(format!("Failed to check has_next: {}", e)))
    }

    fn next(
        &mut self,
        self_: Resource<FunctionStreamIteratorHandle>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        let iter = self
            .table
            .get_mut(&self_)
            .map_err(|e| Error::Other(format!("Failed to get iterator resource: {}", e)))?;

        iter.state_iterator
            .next()
            .map_err(|e| Error::Other(format!("Failed to get next: {}", e)))
    }

    fn drop(&mut self, rep: Resource<FunctionStreamIteratorHandle>) -> Result<(), anyhow::Error> {
        self.table
            .delete(rep)
            .map_err(|e| anyhow::anyhow!("Failed to delete iterator resource: {}", e))?;
        Ok(())
    }
}

impl functionstream::core::collector::Host for HostState {
    fn emit(&mut self, target_id: u32, data: Vec<u8>) {
        let sink_count = self.output_sinks.len();
        let sink = self
            .output_sinks
            .get_mut(target_id as usize)
            .unwrap_or_else(|| {
                panic!("Invalid target_id: {target_id}, available sinks: {sink_count}");
            });

        let buffer_or_event =
            BufferOrEvent::new_buffer(data, Some(format!("target_{}", target_id)), false, false);

        sink.collect(buffer_or_event).unwrap_or_else(|e| {
            panic!("failed to collect output: {e}");
        });
    }

    fn emit_watermark(&mut self, target_id: u32, ts: u64) {
        let sink_count = self.output_sinks.len();
        let sink = self
            .output_sinks
            .get_mut(target_id as usize)
            .unwrap_or_else(|| {
                panic!("Invalid target_id: {target_id}, available sinks: {sink_count}");
            });

        let mut watermark_data = Vec::with_capacity(12);
        watermark_data.extend_from_slice(&target_id.to_le_bytes());
        watermark_data.extend_from_slice(&ts.to_le_bytes());

        let buffer_or_event = BufferOrEvent::new_buffer(
            watermark_data,
            Some(format!("watermark_target_{}", target_id)),
            false,
            false,
        );

        sink.collect(buffer_or_event).unwrap_or_else(|e| {
            panic!("failed to collect watermark: {e}");
        });
    }
}

pub fn create_wasm_host_with_component(
    engine: &Engine,
    component: &Component,
    output_sinks: Vec<Box<dyn OutputSink>>,
    init_context: &crate::runtime::taskexecutor::InitContext,
    task_name: String,
) -> anyhow::Result<(Processor, Store<HostState>)> {
    let mut linker = Linker::new(engine);

    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
        .map_err(|e| anyhow::anyhow!("Failed to add WASI to linker: {}", e))?;

    Processor::add_to_linker::<HostState, HostStateData>(&mut linker, |s| s)
        .map_err(|e| anyhow::anyhow!("Failed to add interfaces to linker: {}", e))?;

    let created_at = init_context
        .task_storage
        .load_task(&task_name)
        .ok()
        .map(|info| info.created_at)
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });

    let factory = init_context
        .state_storage_server
        .create_factory(task_name.clone(), created_at)
        .map_err(|e| anyhow::anyhow!("Failed to create state store factory: {}", e))?;

    let mut store = Store::new(
        engine,
        HostState {
            wasi: WasiCtxBuilder::new()
                .inherit_stdio()
                .inherit_env()
                .inherit_args()
                .build(),
            table: ResourceTable::new(),
            factory,
            output_sinks,
        },
    );

    let processor = Processor::instantiate(&mut store, component, &linker).map_err(|e| {
        let error_msg = format!("Failed to instantiate wasm component: {}", e);
        let mut detailed_msg = error_msg.clone();
        if let Some(source) = e.source() {
            detailed_msg.push_str(&format!(". Source: {}", source));
        }
        anyhow::anyhow!("{}", detailed_msg)
    })?;

    Ok((processor, store))
}

pub fn create_wasm_host(
    wasm_bytes: &[u8],
    output_sinks: Vec<Box<dyn OutputSink>>,
    init_context: &crate::runtime::taskexecutor::InitContext,
    task_name: String,
) -> anyhow::Result<(Processor, Store<HostState>)> {
    let engine = get_global_engine(wasm_bytes.len())?;

    let component = Component::from_binary(&engine, wasm_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to parse WebAssembly component: {}", e))?;

    create_wasm_host_with_component(&engine, &component, output_sinks, init_context, task_name)
}
