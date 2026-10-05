use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use changelog_release_notes::{
    ReleaseOptions, assemble_release, check_fragments, collect_fragments, core_notes, git,
    prepare_release, previous_release_tag, release_section,
};
use chrono::NaiveDate;
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

fn options() -> ReleaseOptions<'static> {
    ReleaseOptions {
        version: "1.1.0",
        date: NaiveDate::from_ymd_opt(2026, 10, 2).unwrap(),
        allow_empty: false,
        breaking_heading: Some(":boom: Breaking Changes"),
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
    assert_eq!(changelog.matches("\n- ").count(), 5);
    assert!(changelog.ends_with(history));
    assert!(changelog.find(entry).unwrap() < changelog.find("- Last fix.").unwrap());
    assert_eq!(count, 4);
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
    assert!(assemble_release("# Changelog\n", &[], &options()).is_err());
    let mut opts = options();
    opts.allow_empty = true;
    assert!(assemble_release("# Changelog\n", &[], &opts).is_ok());
    assert!(
        assemble_release(
            "# Changelog\n\n## [Unreleased]\n\nOld pending note.\n",
            &[],
            &opts
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
    prepare_release(&repo.root, Path::new(path), directory, &options()).unwrap();
    let end = repo.commit();
    let notes = changelog_release_notes::range::release_notes(&repo.root, &base, &end, path)
        .unwrap()
        .join("\n");
    assert_eq!(notes.matches("- New feature.").count(), 1);
    assert_eq!(notes.matches("- Another feature.").count(), 1);
    assert!(!notes.contains("- Old feature."));
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
