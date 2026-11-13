#[path = "../../crates/protos/selective_boxing.rs"]
mod selective_boxing;
use std::{env, fs, path::PathBuf};
fn main() {
    let root =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../crates/protos/protos/api_upstream");
    let root = root.to_str().unwrap();
    let fds = protox::compile(
        [format!("{root}/temporal/api/update/v1/message.proto")],
        [root],
    )
    .unwrap();
    for file in &fds.file {
        let path = PathBuf::from(root).join(file.name());
        if path.exists() {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    fs::write(
        out.join("closure-inputs.txt"),
        fds.file
            .iter()
            .map(|f| format!("{root}/{}", f.name()))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .unwrap();
    for mode in ["baseline", "optimized"] {
        let dir = out.join(mode);
        fs::create_dir_all(&dir).unwrap();
        let mut c = prost_build::Config::new();
        c.out_dir(&dir).include_file("mod.rs");
        if mode == "optimized" {
            let selected =
                selective_boxing::select_with_options(&fds, selective_boxing::Options::default());
            fs::write(
                out.join("selected.txt"),
                selected
                    .iter()
                    .map(|s| format!("{}: {}", s.path, s.reason))
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
            .unwrap();
            for s in selected {
                c.boxed(s.path);
            }
        }
        c.compile_fds(fds.clone()).unwrap();
    }
}
