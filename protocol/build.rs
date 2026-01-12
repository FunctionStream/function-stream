use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger for build script
    env_logger::init();

    // Create output directories in the protocol package directory
    // Use CARGO_MANIFEST_DIR to get the package root directory
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let out_dir = Path::new(&manifest_dir).join("generated");
    let proto_file = Path::new(&manifest_dir).join("proto/function_stream.proto");
    
    // Note: Cargo doesn't directly support cleaning custom directories via cargo clean.
    // The generated directory will be automatically regenerated on each build if needed.
    // To clean it manually, use: ./clean.sh or make clean or rm -rf protocol/generated
    
    log::info!("Generated code will be placed in: {}", out_dir.display());
    log::info!("Proto file: {}", proto_file.display());

    // Create output directories
    let cli_dir = out_dir.join("cli");
    let service_dir = out_dir.join("service");

    std::fs::create_dir_all(&cli_dir)?;
    std::fs::create_dir_all(&service_dir)?;
    log::info!("Created output directories: {} and {}", cli_dir.display(), service_dir.display());

    // Generate code for CLI - only client code needed
    tonic_build::configure()
        .out_dir(&cli_dir)
        .build_client(true)      // Enable client code generation
        .build_server(false)     // Disable server code generation
        .compile_protos(&["proto/function_stream.proto"], &["proto"])?;

    // Generate code for Service - only server code needed
    tonic_build::configure()
        .out_dir(&service_dir)
        .build_client(false)     // Disable client code generation
        .build_server(true)      // Enable server code generation
        .compile_protos(&["proto/function_stream.proto"], &["proto"])?;

    log::info!("Protocol Buffers code generated successfully");
    println!("cargo:rustc-env=PROTO_GEN_DIR={}", out_dir.display());
    println!("cargo:rerun-if-changed={}", proto_file.display());
    
    Ok(())
}

