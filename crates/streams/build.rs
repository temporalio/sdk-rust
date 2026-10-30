use protox::{
    Compiler,
    file::{ChainFileResolver, DescriptorSetFileResolver, GoogleFileResolver, IncludeFileResolver},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=protos");
    let protos = [
        "protos/temporal/api/enums/v1/stream.proto",
        "protos/temporal/api/stream/v1/message.proto",
        "protos/temporal/api/streamservice/v1/request_response.proto",
        "protos/temporal/sdk/streams/v1/runtime.proto",
    ];
    // The build reads the API protos that these import from the descriptor set of
    // `temporalio-protos`, so the packaged crate needs no file outside itself.
    let api = std::env::var("DEP_TEMPORALIO_PROTOS_DESCRIPTOR_PATH")?;
    println!("cargo:rerun-if-changed={api}");
    let mut resolver = ChainFileResolver::new();
    resolver.add(IncludeFileResolver::new("protos".into()));
    resolver.add(DescriptorSetFileResolver::decode(
        std::fs::read(&api)?.as_slice(),
    )?);
    resolver.add(GoogleFileResolver::new());
    let mut compiler = Compiler::with_file_resolver(resolver);
    compiler
        .include_source_info(true)
        .include_imports(true)
        .open_files(protos)?;
    // protox compiles the protos in Rust, so the crate builds with no protoc, as Core's
    // `vendored-protox` build does.
    let descriptor_file = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("streams.bin");
    std::fs::write(&descriptor_file, compiler.encode_file_descriptor_set())?;
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .skip_protoc_run()
        .file_descriptor_set_path(&descriptor_file)
        // Use the Core types for the Temporal API messages, so that values need no conversion
        // between the crates. Not `.temporal.api.enums`, since Core lacks the Stream enums.
        .extern_path(
            ".temporal.api.common",
            "::temporalio_common::protos::temporal::api::common",
        )
        .extern_path(
            ".temporal.api.failure",
            "::temporalio_common::protos::temporal::api::failure",
        )
        .compile_protos(&protos, &["protos"])?;
    Ok(())
}
