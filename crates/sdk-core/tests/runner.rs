// All non-main.rs tests ignore dead common code so that the linter doesn't complain about about it.
#[allow(dead_code)]
mod common;

use crate::common::integ_dev_server_config;
use anyhow::{anyhow, bail};
use clap::Parser;
use common::INTEG_SERVER_TARGET_ENV_VAR;
use std::{
    env,
    path::{Path, PathBuf},
    process::Stdio,
};
use temporalio_sdk_core::ephemeral_server::{TestServerConfig, default_cached_download};
use tokio::{self, process::Command};

/// This env var is set (to any value) if temporal CLI dev server is in use
const INTEG_TEMPORAL_DEV_SERVER_USED_ENV_VAR: &str = "INTEG_TEMPORAL_DEV_SERVER_ON";
/// This env var is set (to any value) if the test server is in use
const INTEG_TEST_SERVER_USED_ENV_VAR: &str = "INTEG_TEST_SERVER_ON";

#[derive(clap::Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Test harness to run. Anything defined as a `[[test]]` in core's `Cargo.toml` is valid.
    #[arg(short, long, default_value = "integ_tests")]
    test_name: String,

    /// What kind of server to auto-launch, if any
    #[arg(short, long, value_enum, default_value = "temporal-cli")]
    server_kind: ServerKind,

    /// Arguments to pass through to the `cargo test` command. Ex: `--release`
    #[arg(short, long, allow_hyphen_values(true))]
    cargo_test_args: Vec<String>,

    /// Existing test harness executable to run without invoking Cargo.
    #[arg(long, conflicts_with_all = ["cargo_test_args", "just_build"])]
    test_executable: Option<PathBuf>,

    #[arg(long)]
    /// If set, only run the build, not any tests
    just_build: bool,

    /// The rest of the arguments will be passed through to the test harness
    harness_args: Vec<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, clap::ValueEnum)]
enum ServerKind {
    /// Use Temporal-cli
    TemporalCLI,
    /// Use the Java test server
    TestServer,
    /// Do not automatically start any server
    External,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let Cli {
        test_name,
        server_kind,
        cargo_test_args,
        test_executable,
        just_build,
        harness_args,
    } = Cli::parse();
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    // Try building first, so that we error early on build failures & don't start server
    // Unclear why --all-features doesn't work here
    let test_args_preamble = [
        "test",
        "--features",
        "temporalio-common/serde_serialize",
        "--features",
        "test-utilities",
        "--features",
        "ephemeral-server",
        "--features",
        "temporalio-sdk-core/otel",
        "--test",
        &test_name,
    ]
    .into_iter()
    .map(ToString::to_string)
    .chain(cargo_test_args)
    .collect::<Vec<_>>();
    if test_executable.is_none() {
        let mut build_cmd = Command::new(&cargo);
        strip_cargo_env_vars(&mut build_cmd);
        let status = build_cmd
            .args([test_args_preamble.as_slice(), &["--no-run".to_string()]].concat())
            .status()
            .await?;
        if !status.success() {
            bail!("Building integration tests failed!");
        }
    }
    if just_build {
        return Ok(());
    }

    let (server, envs) = match server_kind {
        ServerKind::TemporalCLI => {
            let config =
                integ_dev_server_config(vec!["--http-port".to_string(), "7243".to_string()], true);
            println!("Using temporal CLI: {config:?}");
            (
                Some(
                    config
                        .start_server_with_output(Stdio::null(), Stdio::null())
                        .await?,
                ),
                vec![(INTEG_TEMPORAL_DEV_SERVER_USED_ENV_VAR, "true")],
            )
        }
        ServerKind::TestServer => {
            let config = TestServerConfig::builder()
                .exe(default_cached_download())
                .build();
            println!("Using java test server");
            (
                Some(
                    config
                        .start_server_with_output(Stdio::null(), Stdio::null())
                        .await?,
                ),
                vec![(INTEG_TEST_SERVER_USED_ENV_VAR, "true")],
            )
        }
        ServerKind::External => {
            println!("========================================================");
            println!("Not starting up a server. One should be running already.");
            println!("========================================================");
            (None, vec![])
        }
    };

    let mut cmd = if let Some(test_executable) = test_executable {
        let mut cmd = Command::new(test_executable);
        cmd.args(harness_args);
        cmd
    } else {
        let mut cmd = Command::new(&cargo);
        strip_cargo_env_vars(&mut cmd);
        cmd.args(
            test_args_preamble
                .into_iter()
                .chain(["--".to_string()])
                .chain(harness_args),
        );
        cmd
    };
    if let Some(srv) = server.as_ref() {
        println!("Running on {}", srv.target);
        cmd.env(
            INTEG_SERVER_TARGET_ENV_VAR,
            format!("http://{}", &srv.target),
        );
    }
    let status = cmd.envs(envs).current_dir(project_root()).status().await?;

    if let Some(mut srv) = server {
        srv.shutdown().await?;
    }
    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("Integ tests failed!"))
    }
}

/// Some env vars inherited from the fact that this is called with cargo can cause the *nested*
/// cargo calls to do unnecessary recompiles. Fix that.
fn strip_cargo_env_vars(build_cmd: &mut Command) {
    let envs = env::vars().filter(|(k, _)| k.starts_with("CARGO_"));
    for (k, _) in envs {
        build_cmd.env_remove(k);
    }
}

fn project_root() -> PathBuf {
    Path::new(&env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(1)
        .unwrap()
        .to_path_buf()
}
