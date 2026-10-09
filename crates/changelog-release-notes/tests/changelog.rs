use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use changelog_release_notes::{
    ReleaseOptions, assemble_release, check_fragments, collect_fragments, core_notes, git,
    prepare_release, previous_release_tag, published_release_notes, release_section, update_core,
};
use chrono::NaiveDate;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use tempfile::TempDir;

struct Repo {
    _temp: TempDir,
    root: PathBuf,
}

impl Repo {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().to_path_buf();
        git(&root, &["init", "-q"]).unwrap();
        git(&root, &["config", "user.email", "test@example.com"]).unwrap();
        git(&root, &["config", "user.name", "Test"]).unwrap();
        Self { _temp: temp, root }
    }
    fn write(&self, path: &str, text: &str) {
        let path = self.root.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }
    fn commit(&self) -> String {
        git(&self.root, &["add", "."]).unwrap();
        git(&self.root, &["commit", "-qm", "Test change"]).unwrap();
        git(&self.root, &["rev-parse", "HEAD"])
            .unwrap()
            .trim()
            .into()
    }
}

fn core_parent(core: &Repo, revision: &str) -> Repo {
    git(&core.root, &["branch", "-M", "main"]).unwrap();
    let parent = Repo::new();
    parent.write("CHANGELOG.md", "# Changelog\n");
    git(
        &parent.root,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            core.root.to_str().unwrap(),
            "core",
        ],
    )
    .unwrap();
    git(
        &parent.root.join("core"),
        &["checkout", "-q", "--detach", revision],
    )
    .unwrap();
    parent.commit();
    git(&parent.root, &["tag", "1.0.0"]).unwrap();
    parent
}

#[test]
fn core_update_through_preparation_and_publishing() {
    let core = Repo::new();
    let path = "crates/sdk-core/CHANGELOG.md";
    core.write(
        path,
        "# Changelog\n\n## Unreleased\n\n### Fixed\n- Existing fix.\n",
    );
    let old = core.commit();
    let parent = core_parent(&core, &old);
    git(&parent.root, &["tag", "-d", "1.0.0"]).unwrap();
    git(&parent.root, &["tag", "v1.0.0"]).unwrap();
    core.write(path, "# Changelog\n\n## Unreleased\n\n### Fixed\n- Existing fix.\n* A new fix with\n  a [link](https://example.com).\n* Another new fix.\n\n### Breaking Changes\n- A breaking change.\n");
    core.commit();
    core.write(path, "# Changelog\n\n## Unreleased\n\n## [1.0.0] - 2026-10-02\n\n### Fixed\n- Existing fix.\n* A new fix with\n  a [link](https://example.com).\n* Another new fix.\n\n### Breaking Changes\n- A breaking change.\n");
    let new = core.commit();
    assert_eq!(
        update_core(
            &parent.root,
            Path::new("core"),
            Path::new("changelog"),
            None,
            None
        )
        .unwrap(),
        2
    );
    assert_eq!(
        git(&parent.root.join("core"), &["rev-parse", "HEAD"])
            .unwrap()
            .trim(),
        new
    );
    let fragments = collect_fragments(&parent.root, Path::new("changelog")).unwrap();
    assert_eq!(fragments[0].body, "Core: A breaking change.\n");
    assert_eq!(
        fragments[1].body,
        "Core: A new fix with a [link](https://example.com).\nCore: Another new fix.\n"
    );
    assert_eq!(
        Path::new(&fragments[0].path).file_name(),
        Path::new(&fragments[1].path).file_name()
    );
    assert_eq!(
        update_core(
            &parent.root,
            Path::new("core"),
            Path::new("changelog"),
            None,
            None
        )
        .unwrap(),
        0
    );
    parent.write("changelog/fixed/language-llama.md", "A language fix.\n");
    prepare_release(
        &parent.root,
        Path::new("CHANGELOG.md"),
        Path::new("changelog"),
        &options(),
    )
    .unwrap();
    parent.commit();
    parent.write("changelog/fixed/late-llama.md", "A late fix.\n");
    let notes = published_release_notes(
        &parent.root,
        Path::new("CHANGELOG.md"),
        Path::new("core"),
        "1.1.0",
        None,
        "HEAD",
    )
    .unwrap();
    assert_eq!(notes.matches("- Core: A new fix").count(), 1);
    assert_eq!(notes.matches("- Core: Another new fix.").count(), 1);
    assert!(notes.contains("### :boom: Breaking Changes\n\n- Core: A breaking change."));
    assert!(notes.contains("- A language fix."));
    assert!(notes.contains("### SDK Core Commits\n\n- [`"));
    assert!(!notes.contains("#### Commits"));
    assert!(!notes.contains("Existing fix."));
    assert!(!notes.contains("A late fix."));
    assert!(parent.root.join("changelog/fixed/late-llama.md").exists());
    let output = Command::new(env!("CARGO_BIN_EXE_changelog-tool"))
        .args(["release-notes", "--repo"])
        .arg(&parent.root)
        .args([
            "--submodule",
            "core",
            "--version",
            "1.1.0",
            "--output",
            "notes.md",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(parent.root.join("notes.md")).unwrap(),
        notes
    );
}

#[test]
fn consecutive_updates_only_import_each_new_range() {
    let core = Repo::new();
    let path = "crates/sdk-core/CHANGELOG.md";
    core.write(path, "# Changelog\n\n## Unreleased\n");
    let old = core.commit();
    let parent = core_parent(&core, &old);
    for entry in ["First fix.", "Second fix."] {
        let text = fs::read_to_string(core.root.join(path)).unwrap();
        core.write(
            path,
            &format!(
                "{text}{}- {entry}\n",
                if entry == "First fix." {
                    "\n### Fixed\n"
                } else {
                    ""
                }
            ),
        );
        core.commit();
        assert_eq!(
            update_core(
                &parent.root,
                Path::new("core"),
                Path::new("changelog"),
                None,
                None
            )
            .unwrap(),
            1
        );
    }
    let fragments = collect_fragments(&parent.root, Path::new("changelog")).unwrap();
    assert_eq!(fragments.len(), 2);
    let bodies = fragments
        .iter()
        .map(|fragment| fragment.body.as_str())
        .collect::<String>();
    assert_eq!(bodies.matches("Core: First fix.").count(), 1);
    assert_eq!(bodies.matches("Core: Second fix.").count(), 1);
}

#[test]
fn explicit_baseline_backfills_an_already_updated_pin() {
    let core = Repo::new();
    let path = "crates/sdk-core/CHANGELOG.md";
    core.write(path, "# Changelog\n\n## Unreleased\n");
    let old = core.commit();
    core.write(
        path,
        "# Changelog\n\n## Unreleased\n\n### Stabilized\n- A feature is stable.\n",
    );
    let new = core.commit();
    let parent = core_parent(&core, &new);
    assert_eq!(
        update_core(
            &parent.root,
            Path::new("core"),
            Path::new("changelog"),
            Some(&new),
            Some(&old)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        collect_fragments(&parent.root, Path::new("changelog")).unwrap()[0].body,
        "Core: A feature is stable.\n"
    );
}

#[test]
fn invalid_core_imports_leave_checkout_and_fragments_unchanged() {
    for addition in [
        "### Fixed\n- A fix.\n  ```rust\n  example();\n  ```\n",
        "### Surprising\n- A fix.\n",
    ] {
        let core = Repo::new();
        let path = "crates/sdk-core/CHANGELOG.md";
        core.write(path, "# Changelog\n\n## Unreleased\n");
        let old = core.commit();
        let parent = core_parent(&core, &old);
        parent.write(
            "changelog/fixed/existing-otter.md",
            "Existing language fix.\n",
        );
        core.write(path, &format!("# Changelog\n\n## Unreleased\n\n{addition}"));
        core.commit();
        assert!(
            update_core(
                &parent.root,
                Path::new("core"),
                Path::new("changelog"),
                None,
                None
            )
            .is_err()
        );
        assert_eq!(
            git(&parent.root.join("core"), &["rev-parse", "HEAD"])
                .unwrap()
                .trim(),
            old
        );
        assert_eq!(
            collect_fragments(&parent.root, Path::new("changelog"))
                .unwrap()
                .len(),
            1
        );
    }
}

#[test]
fn dirty_or_backward_core_updates_are_rejected() {
    let core = Repo::new();
    let path = "crates/sdk-core/CHANGELOG.md";
    core.write(path, "# Changelog\n\n## Unreleased\n");
    let old = core.commit();
    core.write("internal.txt", "Internal change");
    let new = core.commit();
    let parent = core_parent(&core, &new);
    assert!(
        update_core(
            &parent.root,
            Path::new("core"),
            Path::new("changelog"),
            Some(&old),
            None
        )
        .is_err()
    );
    fs::write(parent.root.join("core/untracked.txt"), "Local work").unwrap();
    assert!(
        update_core(
            &parent.root,
            Path::new("core"),
            Path::new("changelog"),
            None,
            None
        )
        .unwrap_err()
        .to_string()
        .contains("local changes")
    );
    assert_eq!(
        git(&parent.root.join("core"), &["rev-parse", "HEAD"])
            .unwrap()
            .trim(),
        new
    );
}

#[test]
fn empty_release_omits_unchanged_core_and_supports_explicit_tags() {
    let core = Repo::new();
    core.write(
        "crates/sdk-core/CHANGELOG.md",
        "# Changelog\n\n## Unreleased\n",
    );
    let old = core.commit();
    let parent = core_parent(&core, &old);
    prepare_release(
        &parent.root,
        Path::new("CHANGELOG.md"),
        Path::new("changelog"),
        &options(),
    )
    .unwrap();
    parent.commit();
    assert_eq!(
        published_release_notes(
            &parent.root,
            Path::new("CHANGELOG.md"),
            Path::new("core"),
            "1.1.0",
            None,
            "HEAD"
        )
        .unwrap(),
        "## Notable Changes\n\n"
    );
    git(&parent.root, &["tag", "-d", "1.0.0"]).unwrap();
    assert!(
        published_release_notes(
            &parent.root,
            Path::new("CHANGELOG.md"),
            Path::new("core"),
            "1.1.0",
            None,
            "HEAD"
        )
        .is_err()
    );
    assert_eq!(
        published_release_notes(
            &parent.root,
            Path::new("CHANGELOG.md"),
            Path::new("core"),
            "1.1.0",
            Some("HEAD~"),
            "HEAD"
        )
        .unwrap(),
        "## Notable Changes\n\n"
    );
}

#[cfg(unix)]
#[test]
fn checkout_failure_removes_imports_and_reports_rollback_failure() {
    let core = Repo::new();
    let path = "crates/sdk-core/CHANGELOG.md";
    core.write(path, "# Changelog\n\n## Unreleased\n");
    let old = core.commit();
    let parent = core_parent(&core, &old);
    core.write(
        path,
        "# Changelog\n\n## Unreleased\n\n### Fixed\n- New Core fix.\n",
    );
    core.commit();
    parent.write(
        "changelog/fixed/existing-otter.md",
        "Existing language fix.\n",
    );
    let checkout = parent.root.join("core");
    let hook = git(
        &checkout,
        &["rev-parse", "--git-path", "hooks/post-checkout"],
    )
    .unwrap();
    let hook = checkout.join(hook.trim());
    fs::write(&hook, "#!/bin/sh\nexit 1\n").unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
    let error = update_core(
        &parent.root,
        Path::new("core"),
        Path::new("changelog"),
        None,
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("rollback failed"));
    assert_eq!(git(&checkout, &["rev-parse", "HEAD"]).unwrap().trim(), old);
    let fragments = collect_fragments(&parent.root, Path::new("changelog")).unwrap();
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].body, "Existing language fix.\n");
}

#[test]
fn divergent_updates_are_rejected_without_importing_entries() {
    let core = Repo::new();
    let path = "crates/sdk-core/CHANGELOG.md";
    core.write(path, "# Changelog\n\n## Unreleased\n");
    let base = core.commit();
    core.write("branch.txt", "Current branch");
    let old = core.commit();
    let parent = core_parent(&core, &old);
    git(&core.root, &["checkout", "-q", "-B", "other", &base]).unwrap();
    core.write(
        path,
        "# Changelog\n\n## Unreleased\n\n### Fixed\n- A divergent fix.\n",
    );
    let other = core.commit();
    git(
        &parent.root.join("core"),
        &["fetch", "-q", "origin", "other"],
    )
    .unwrap();
    assert!(
        update_core(
            &parent.root,
            Path::new("core"),
            Path::new("changelog"),
            Some(&other),
            None
        )
        .is_err()
    );
    assert_eq!(
        git(&parent.root.join("core"), &["rev-parse", "HEAD"])
            .unwrap()
            .trim(),
        old
    );
    assert!(
        collect_fragments(&parent.root, Path::new("changelog"))
            .unwrap()
            .is_empty()
    );
    assert!(
        update_core(
            &parent.root,
            Path::new("core"),
            Path::new("changelog"),
            Some(&other),
            Some(&base)
        )
        .is_err()
    );
}

fn options() -> ReleaseOptions<'static> {
    ReleaseOptions {
        version: "1.1.0",
        date: NaiveDate::from_ymd_opt(2026, 10, 2).unwrap(),
    }
}

#[test]
fn prepends_grouped_list_items_without_changing_history() {
    let repo = Repo::new();
    let history = "## [1.0.0] - 2026-01-01\n\n### Added\n\nOld note.\n";
    repo.write(
        "CHANGELOG.md",
        &format!("<!-- Intro -->\n# Changelog\n\n{history}"),
    );
    repo.write("changelog/README.md", "Instructions");
    repo.write("changelog/fixed/zany-zebra.md", "Last fix.");
    let body = "\r\nA fix with a [documentation link](https://example.com).\r\n \t\r\nAnother fix. This entry has two sentences.\r\n";
    let entry = "- A fix with a [documentation link](https://example.com).\n- Another fix. This entry has two sentences.\n";
    repo.write("changelog/fixed/amber-otter.md", body);
    repo.write("changelog/added/silly-badger.md", "A feature.\n");
    repo.write(
        "changelog/stabilized/steady-starfish.md",
        "A feature is now stable.\nAnother feature is no longer experimental.\n",
    );
    check_fragments(&repo.root, Path::new("changelog"), None, "HEAD", false).unwrap();
    repo.write(
        "changelog/breaking-changes/dancing-cupcake.md",
        "A breaking change.\n",
    );
    let count = prepare_release(
        &repo.root,
        Path::new("CHANGELOG.md"),
        Path::new("changelog"),
        &options(),
    )
    .unwrap();
    let changelog = fs::read_to_string(repo.root.join("CHANGELOG.md")).unwrap();
    assert!(
        changelog
            .starts_with("<!-- Intro -->\n# Changelog\n\n## [1.1.0] - 2026-10-02\n\n### Added")
    );
    assert!(changelog.contains("### :boom: Breaking Changes\n\n- A breaking change."));
    assert!(changelog.contains("### Added\n\n- A feature.\n\n"));
    assert!(changelog.contains(entry));
    assert!(changelog.contains("- Last fix.\n\n"));
    assert!(changelog.contains("### Stabilized\n\n- A feature is now stable.\n- Another feature is no longer experimental.\n\n"));
    assert!(changelog.find("### Added").unwrap() < changelog.find("### Stabilized").unwrap());
    assert!(changelog.find("### Stabilized").unwrap() < changelog.find("### :boom:").unwrap());
    assert_eq!(changelog.matches("\n- ").count(), 7);
    assert!(changelog.ends_with(history));
    assert!(changelog.find(entry).unwrap() < changelog.find("- Last fix.").unwrap());
    assert_eq!(count, 5);
    assert!(!changelog.contains("Unreleased"));
    assert!(
        collect_fragments(&repo.root, Path::new("changelog"))
            .unwrap()
            .is_empty()
    );
    assert!(
        release_section(&changelog, "1.1.0")
            .unwrap()
            .contains(entry.trim_end_matches('\n'))
    );
    let fragments = collect_fragments(&repo.root, Path::new("changelog")).unwrap();
    assert!(
        assemble_release(&changelog, &fragments, &options())
            .unwrap_err()
            .to_string()
            .contains("already contains")
    );
}

#[test]
fn rejects_invalid_layouts_and_empty_fragments() {
    for (path, body) in [
        ("changelog/fixed/empty.md", " \n"),
        ("changelog/oops/note.md", "Note"),
        ("changelog/note.md", "Note"),
        ("changelog/fixed/nested/note.md", "Note"),
        ("changelog/fixed/note.txt", "Note"),
    ] {
        let repo = Repo::new();
        repo.write(path, body);
        assert!(
            collect_fragments(&repo.root, Path::new("changelog")).is_err(),
            "{path}"
        );
    }
    let repo = Repo::new();
    assert!(collect_fragments(&repo.root, Path::new("../elsewhere")).is_err());
    assert!(assemble_release("# Changelog\n", &[], &options()).is_ok());
    assert!(
        assemble_release(
            "# Changelog\n\n## [Unreleased]\n\nOld pending note.\n",
            &[],
            &options()
        )
        .is_err()
    );
}

#[test]
fn requires_a_new_fragment_not_only_modifications_or_deletions() {
    let repo = Repo::new();
    repo.write("changelog/fixed/old-otter.md", "Original fix.\n");
    let base = repo.commit();
    repo.write("changelog/fixed/old-otter.md", "Updated fix.\n");
    repo.commit();
    assert!(
        check_fragments(
            &repo.root,
            Path::new("changelog"),
            Some(&base),
            "HEAD",
            true
        )
        .is_err()
    );
    repo.write("changelog/added/new-narwhal.md", "New feature.\n");
    repo.commit();
    check_fragments(
        &repo.root,
        Path::new("changelog"),
        Some(&base),
        "HEAD",
        true,
    )
    .unwrap();
    fs::remove_file(repo.root.join("changelog/added/new-narwhal.md")).unwrap();
    repo.commit();
    assert!(
        check_fragments(
            &repo.root,
            Path::new("changelog"),
            Some(&base),
            "HEAD",
            true
        )
        .is_err()
    );
}

#[test]
fn range_notes_survive_fragment_migration_and_assembly() {
    let repo = Repo::new();
    let path = "crates/sdk-core/CHANGELOG.md";
    repo.write(
        path,
        "# Changelog\n\n## Unreleased\n\n### Added\n- Old feature.\n",
    );
    let base = repo.commit();
    repo.write(
        path,
        "# Changelog\n\n## Unreleased\n\n### Added\n- Old feature.\n- New feature.\n",
    );
    repo.commit();
    repo.write(
        path,
        "# Changelog\n\n## [0.1.0]\n\n### Added\n- Old feature.\n",
    );
    repo.write(
        "crates/sdk-core/changelog/added/dancing-otter.md",
        "New feature.\nAnother feature.\n",
    );
    repo.commit();
    let directory = Path::new("crates/sdk-core/changelog");
    repo.write(
        "crates/sdk-core/changelog/stabilized/steady-starfish.md",
        "A feature is no longer experimental.\n",
    );
    repo.commit();
    prepare_release(&repo.root, Path::new(path), directory, &options()).unwrap();
    let end = repo.commit();
    let notes = changelog_release_notes::range::release_notes(&repo.root, &base, &end, path)
        .unwrap()
        .join("\n");
    assert_eq!(notes.matches("- New feature.").count(), 1);
    assert_eq!(notes.matches("- Another feature.").count(), 1);
    assert!(notes.contains("#### Stabilized"));
    assert_eq!(
        notes
            .matches("- A feature is no longer experimental.")
            .count(),
        1
    );
    assert!(!notes.contains("- Old feature."));
}

#[test]
fn selects_previous_release_from_numeric_and_v_prefixed_tags() {
    let repo = Repo::new();
    repo.write("README.md", "Release tags\n");
    repo.commit();
    for tag in [
        "1.8.0",
        "v1.9.0",
        "v1.10.0",
        "1.11.0",
        "v1.12.0",
        "core-v99.0.0",
        "vv99.0.0",
    ] {
        git(&repo.root, &["tag", tag]).unwrap();
    }
    assert_eq!(
        previous_release_tag(&repo.root, "1.10.0").unwrap(),
        "v1.9.0"
    );
    assert_eq!(
        previous_release_tag(&repo.root, "1.11.0").unwrap(),
        "v1.10.0"
    );
    assert_eq!(
        previous_release_tag(&repo.root, "v1.12.0").unwrap(),
        "1.11.0"
    );
    assert!(previous_release_tag(&repo.root, "1.8.0").is_err());
}

#[test]
fn resolves_core_range_from_parent_tags_and_gitlinks() {
    let core = Repo::new();
    core.write(
        "crates/sdk-core/CHANGELOG.md",
        "# Changelog\n\n## Unreleased\n",
    );
    let old = core.commit();
    core.write(
        "crates/sdk-core/CHANGELOG.md",
        "# Changelog\n\n## Unreleased\n\n### Fixed\n- Core fix.\n",
    );
    let new = core.commit();
    let parent = Repo::new();
    git(
        &parent.root,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{old},core"),
        ],
    )
    .unwrap();
    git(&parent.root, &["commit", "-qm", "Old core"]).unwrap();
    git(&parent.root, &["tag", "1.0.0"]).unwrap();
    git(
        &parent.root,
        &["update-index", "--cacheinfo", &format!("160000,{new},core")],
    )
    .unwrap();
    git(&parent.root, &["commit", "-qm", "New core"]).unwrap();
    assert_eq!(
        previous_release_tag(&parent.root, "1.1.0").unwrap(),
        "1.0.0"
    );
    let status = Command::new("git")
        .args(["clone", "-q"])
        .arg(&core.root)
        .arg(parent.root.join("core"))
        .status()
        .unwrap();
    assert!(status.success());
    let notes = core_notes(&parent.root, Path::new("core"), "1.1.0", None, "HEAD").unwrap();
    assert!(notes.contains("- Core fix."));
    assert!(
        core_notes(
            &parent.root,
            Path::new("core"),
            "1.1.0",
            Some("HEAD"),
            "HEAD"
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn cli_prepares_an_explicit_parent_repository() {
    let repo = Repo::new();
    repo.write("CHANGELOG.md", "# Changelog\n");
    repo.write("changelog/fixed/silly-sloth.md", "A fix.\n");
    let output = Command::new(env!("CARGO_BIN_EXE_changelog-tool"))
        .args(["prepare", "--repo"])
        .arg(&repo.root)
        .args(["--version", "1.1.0", "--date", "2026-10-02"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("consumed 1 changelog fragments"));
    let changelog = fs::read_to_string(repo.root.join("CHANGELOG.md")).unwrap();
    assert!(changelog.contains("## [1.1.0] - 2026-10-02"));
    assert!(changelog.contains("- A fix."));
    assert!(!repo.root.join("changelog/fixed/silly-sloth.md").exists());
    repo.write("changelog/fixed/late-llama.md", "Late note.\n");
    let retry = Command::new(env!("CARGO_BIN_EXE_changelog-tool"))
        .args(["prepare", "--repo"])
        .arg(&repo.root)
        .args(["--version", "1.1.0", "--date", "2026-10-02"])
        .output()
        .unwrap();
    assert!(!retry.status.success());
    assert!(repo.root.join("changelog/fixed/late-llama.md").exists());
    assert_eq!(
        fs::read_to_string(repo.root.join("CHANGELOG.md")).unwrap(),
        changelog
    );
}

#[test]
fn invalid_fragments_do_not_change_changelog_or_consume_notes() {
    let repo = Repo::new();
    repo.write("CHANGELOG.md", "# Changelog\n");
    repo.write("changelog/fixed/valid-otter.md", "A valid note.\n");
    repo.write("changelog/fixed/empty-otter.md", " \n");
    assert!(
        prepare_release(
            &repo.root,
            Path::new("CHANGELOG.md"),
            Path::new("changelog"),
            &options()
        )
        .is_err()
    );
    assert_eq!(
        fs::read_to_string(repo.root.join("CHANGELOG.md")).unwrap(),
        "# Changelog\n"
    );
    assert!(repo.root.join("changelog/fixed/valid-otter.md").exists());
    assert!(repo.root.join("changelog/fixed/empty-otter.md").exists());
}

#[test]
fn cli_prepares_and_extracts_an_empty_release() {
    let repo = Repo::new();
    let history = "## [1.0.0] - 2026-01-01\n\n### Fixed\n\n- Old fix.\n";
    repo.write("CHANGELOG.md", &format!("# Changelog\n\n{history}"));
    let output = Command::new(env!("CARGO_BIN_EXE_changelog-tool"))
        .args(["prepare", "--repo"])
        .arg(&repo.root)
        .args(["--version", "1.1.0", "--date", "2026-10-05"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("consumed 0 changelog fragments"));
    let text = fs::read_to_string(repo.root.join("CHANGELOG.md")).unwrap();
    assert!(text.contains("## [1.1.0] - 2026-10-05\n\n"));
    assert!(text.ends_with(history));
    let output = Command::new(env!("CARGO_BIN_EXE_changelog-tool"))
        .args(["notes", "--repo"])
        .arg(&repo.root)
        .args(["--version", "1.1.0"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(release_section(&text, "2.0.0").is_err());
}
