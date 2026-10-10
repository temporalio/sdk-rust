use std::{env, fs, path::PathBuf};

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn main() {
    let sdk_core =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR")).join("../..");
    let manifest = sdk_core.join("crates/sdk-core-wasm-bridge/abi_sources.txt");
    println!("cargo:rerun-if-changed={}", manifest.display());
    let entries = fs::read_to_string(&manifest).expect("read ABI source manifest");
    let mut fingerprint = FNV_OFFSET;
    for entry in entries.lines().filter(|entry| !entry.is_empty()) {
        let source = sdk_core.join(entry);
        println!("cargo:rerun-if-changed={}", source.display());
        let contents = fs::read(&source).expect("read ABI source");
        for byte in entry
            .bytes()
            .chain([0])
            .chain(contents.into_iter())
            .chain([0])
        {
            fingerprint = (fingerprint ^ u64::from(byte)).wrapping_mul(FNV_PRIME);
        }
    }
    let output = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR")).join("abi_fingerprint.rs");
    fs::write(
        output,
        format!("const ABI_FINGERPRINT: u64 = 0x{fingerprint:016x};\n"),
    )
    .expect("write ABI fingerprint");
}
