use std::{process::Command, time::Duration};

use reqwest::{StatusCode, blocking::Client};
use serde::Deserialize;

const CRATES_IO_SPARSE_INDEX: &str = "https://index.crates.io";
const USER_AGENT: &str =
    "temporalio/sdk-rust release planner (https://github.com/temporalio/sdk-rust)";

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct Package {
    name: String,
    version: String,
    publish: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct IndexEntry {
    vers: String,
}

fn workspace_metadata() -> Result<Metadata, String> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .map_err(|err| format!("failed to run cargo metadata: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|err| format!("failed to parse cargo metadata: {err}"))
}

fn crates_io_packages(metadata: Metadata) -> impl Iterator<Item = Package> {
    metadata.packages.into_iter().filter(|package| {
        package
            .publish
            .as_ref()
            .is_none_or(|registries| registries.iter().any(|registry| registry == "crates-io"))
    })
}

fn sparse_index_path(name: &str) -> String {
    let name = name.to_ascii_lowercase();
    // See https://doc.rust-lang.org/cargo/reference/registry-index.html#index-files
    match name.len() {
        1 => format!("1/{name}"),
        2 => format!("2/{name}"),
        3 => format!("3/{}/{name}", &name[..1]),
        _ => format!("{}/{}/{name}", &name[..2], &name[2..4]),
    }
}

fn version_in_index(index: &str, version: &str) -> Result<bool, String> {
    for line in index.lines() {
        let entry: IndexEntry = serde_json::from_str(line)
            .map_err(|err| format!("failed to parse sparse index entry: {err}"))?;
        if entry.vers == version {
            return Ok(true);
        }
    }
    Ok(false)
}

fn version_is_published(client: &Client, package: &Package) -> Result<bool, String> {
    let url = format!(
        "{CRATES_IO_SPARSE_INDEX}/{}",
        sparse_index_path(&package.name)
    );
    let response = client.get(&url).send().map_err(|err| {
        format!(
            "failed to check {}@{}: {err}",
            package.name, package.version
        )
    })?;
    match response.status() {
        StatusCode::OK => {
            let index = response.text().map_err(|err| {
                format!(
                    "failed to check {}@{}: failed to read sparse index entry: {err}",
                    package.name, package.version
                )
            })?;
            version_in_index(&index, &package.version).map_err(|err| {
                format!(
                    "failed to check {}@{}: {err}",
                    package.name, package.version
                )
            })
        }
        StatusCode::NOT_FOUND => Ok(false),
        status => Err(format!(
            "failed to check {}@{}: sparse index returned unexpected HTTP status {status}",
            package.name, package.version
        )),
    }
}

fn main() -> Result<(), String> {
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|err| format!("failed to create crates.io client: {err}"))?;

    let mut unpublished = Vec::new();
    for package in crates_io_packages(workspace_metadata()?) {
        if version_is_published(&client, &package)? {
            eprintln!(
                "{}@{} is already published; skipping.",
                package.name, package.version
            );
        } else {
            unpublished.push(package.name);
        }
    }
    println!(
        "packages={}",
        serde_json::to_string(&unpublished)
            .map_err(|err| format!("failed to serialize publish plan: {err}"))?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_packages_publishable_to_crates_io() {
        let packages = crates_io_packages(workspace_metadata().expect("workspace metadata"))
            .map(|package| package.name)
            .collect::<Vec<_>>();

        assert!(packages.iter().any(|package| package == "temporalio-sdk"));
        assert!(
            !packages
                .iter()
                .any(|package| package == "temporalio-sdk-core-c-bridge")
        );
    }

    #[test]
    fn builds_sparse_index_paths() {
        assert_eq!(sparse_index_path("temporalio-sdk"), "te/mp/temporalio-sdk");
    }

    #[test]
    fn finds_versions_in_sparse_index_entries() {
        let index = r#"{"vers":"0.7.0","yanked":true}
{"vers":"0.8.0","yanked":false}"#;

        assert_eq!(version_in_index(index, "0.7.0"), Ok(true));
        assert_eq!(version_in_index(index, "0.8.0"), Ok(true));
        assert_eq!(version_in_index(index, "0.9.0"), Ok(false));
        assert!(version_in_index("not json", "0.7.0").is_err());
    }
}
