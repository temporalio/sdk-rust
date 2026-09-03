use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use chrono::{NaiveDate, Utc};
use semver::Version;

const CORE_CRATE: &str = "temporalio-sdk-core";
const BRIDGE_CRATE: &str = "temporalio-sdk-core-c-bridge";
const PROTOS_CRATE: &str = "temporalio-protos";
const RELEASE_TOOL_CRATE: &str = "changelog-release-notes";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn cargo_set_version(root: &Path, args: &[String]) -> Result<(), String> {
    let status = Command::new("cargo")
        .args(args)
        .current_dir(root)
        .status()
        .map_err(|err| format!("failed to run `cargo {}`: {err}", args.join(" ")))?;
    if !status.success() {
        return Err(format!("`cargo {}` failed with {status}", args.join(" ")));
    }
    Ok(())
}

fn turn_over_changelog(
    changelog: &str,
    version: &Version,
    date: NaiveDate,
) -> Result<String, String> {
    let release_prefix = format!("## [{version}]");
    if changelog
        .lines()
        .any(|line| line.starts_with(&release_prefix))
    {
        return Err(format!("CHANGELOG.md already contains {release_prefix}"));
    }

    let header = "## Unreleased";
    let header_start = changelog
        .match_indices(header)
        .find_map(|(index, _)| {
            let at_line_start = index == 0 || changelog.as_bytes().get(index - 1) == Some(&b'\n');
            let after = index + header.len();
            let at_line_end = matches!(changelog.as_bytes().get(after), None | Some(b'\n'));
            (at_line_start && at_line_end).then_some(index)
        })
        .ok_or("CHANGELOG.md is missing an `## Unreleased` section")?;
    let body_start = header_start + header.len();
    let following = &changelog[body_start..];
    let next_heading = following
        .find("\n## ")
        .ok_or("CHANGELOG.md is missing a released-version section")?;
    let body = following[..next_heading].trim();
    let previous_releases = &following[next_heading + 1..];

    let mut output = String::from(&changelog[..header_start]);
    output.push_str(header);
    output.push_str("\n\n");
    output.push_str(&format!("## [{version}] - {date}"));
    if !body.is_empty() {
        output.push_str("\n\n");
        output.push_str(body);
    }
    output.push_str("\n\n");
    output.push_str(previous_releases.trim_start_matches('\n'));
    Ok(output)
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<(Version, Version), String> {
    let mut args = args.into_iter();
    let expected = "expected <sdk-version> <core-protos-version>";
    let sdk_version = args.next().ok_or(expected)?;
    let core_protos_version = args.next().ok_or(expected)?;
    if args.next().is_some() {
        return Err(expected.into());
    }
    let sdk_version =
        Version::parse(&sdk_version).map_err(|err| format!("invalid SDK version: {err}"))?;
    let core_protos_version = Version::parse(&core_protos_version)
        .map_err(|err| format!("invalid Core/Protos version: {err}"))?;
    if core_protos_version.major != 0 {
        return Err(format!(
            "Core/Protos version must remain 0.x, found {core_protos_version}"
        ));
    }
    Ok((sdk_version, core_protos_version))
}

fn main() -> Result<(), String> {
    let (target_sdk, target_core_protos) = parse_args(env::args().skip(1))?;
    let root = workspace_root();

    let changelog_path = root.join("CHANGELOG.md");
    let changelog = fs::read_to_string(&changelog_path)
        .map_err(|err| format!("failed to read {}: {err}", changelog_path.display()))?;
    let changelog = turn_over_changelog(&changelog, &target_sdk, Utc::now().date_naive())?;

    let sdk_update = vec![
        "set-version".into(),
        "--workspace".into(),
        target_sdk.to_string(),
        "--exclude".into(),
        CORE_CRATE.into(),
        "--exclude".into(),
        BRIDGE_CRATE.into(),
        "--exclude".into(),
        PROTOS_CRATE.into(),
        "--exclude".into(),
        RELEASE_TOOL_CRATE.into(),
    ];
    let core_update = vec![
        "set-version".into(),
        "--package".into(),
        CORE_CRATE.into(),
        target_core_protos.to_string(),
    ];
    let protos_update = vec![
        "set-version".into(),
        "--package".into(),
        PROTOS_CRATE.into(),
        target_core_protos.to_string(),
    ];
    let mut sdk_dry_run = sdk_update.clone();
    sdk_dry_run.push("--dry-run".into());
    let mut core_dry_run = core_update.clone();
    core_dry_run.push("--dry-run".into());
    let mut protos_dry_run = protos_update.clone();
    protos_dry_run.push("--dry-run".into());
    cargo_set_version(&root, &sdk_dry_run)?;
    cargo_set_version(&root, &core_dry_run)?;
    cargo_set_version(&root, &protos_dry_run)?;

    cargo_set_version(&root, &sdk_update)?;
    cargo_set_version(&root, &core_update)?;
    cargo_set_version(&root, &protos_update)?;
    fs::write(&changelog_path, changelog)
        .map_err(|err| format!("failed to write {}: {err}", changelog_path.display()))?;

    println!("Prepared SDK {target_sdk} with Core and Protos {target_core_protos}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(value: &str) -> Version {
        Version::parse(value).unwrap()
    }

    #[test]
    fn rejects_core_protos_one_x() {
        assert!(
            parse_args(["1.0.0".into(), "1.0.0".into()])
                .unwrap_err()
                .contains("Core/Protos version must remain 0.x")
        );
    }

    #[test]
    fn turns_over_a_populated_changelog() {
        let input = "# Changelog\n\n## Unreleased\n\n### Added\n* Feature.\n\n## [0.7.0] - 2026-01-01\n\nOld.\n";
        assert_eq!(
            turn_over_changelog(
                input,
                &version("1.0.0"),
                NaiveDate::from_ymd_opt(2026, 8, 27).unwrap()
            )
            .unwrap(),
            "# Changelog\n\n## Unreleased\n\n## [1.0.0] - 2026-08-27\n\n### Added\n* Feature.\n\n## [0.7.0] - 2026-01-01\n\nOld.\n"
        );
    }

    #[test]
    fn turns_over_an_empty_changelog() {
        let input = "# Changelog\n\n## Unreleased\n\n## [0.7.0] - 2026-01-01\n";
        assert_eq!(
            turn_over_changelog(
                input,
                &version("1.0.0-rc.1"),
                NaiveDate::from_ymd_opt(2026, 8, 27).unwrap()
            )
            .unwrap(),
            "# Changelog\n\n## Unreleased\n\n## [1.0.0-rc.1] - 2026-08-27\n\n## [0.7.0] - 2026-01-01\n"
        );
    }

    #[test]
    fn rejects_a_duplicate_changelog_release() {
        let input = "# Changelog\n\n## Unreleased\n\n## [1.0.0] - 2026-01-01\n";
        assert!(
            turn_over_changelog(
                input,
                &version("1.0.0"),
                NaiveDate::from_ymd_opt(2026, 8, 27).unwrap()
            )
            .unwrap_err()
            .contains("already contains")
        );
    }
}
