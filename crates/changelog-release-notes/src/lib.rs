//! Shared changelog preparation for Temporal SDK release adapters.

use anyhow::{Context, Result, anyhow, bail};
use chrono::NaiveDate;
use std::{
    fs,
    path::{Component, Path},
    process::Command,
};

pub mod range;
mod update;
pub use update::update_core;

pub const CATEGORIES: [(&str, &str); 7] = [
    ("added", "Added"),
    ("stabilized", "Stabilized"),
    ("changed", "Changed"),
    ("deprecated", "Deprecated"),
    ("breaking-changes", ":boom: Breaking Changes"),
    ("fixed", "Fixed"),
    ("security", "Security"),
];

pub struct ReleaseOptions<'a> {
    pub version: &'a str,
    pub date: NaiveDate,
}

#[derive(Debug)]
pub struct Fragment {
    pub path: String,
    pub category: usize,
    pub body: String,
}

pub fn git(repo: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .context("failed to run git")?;
    if !output.status.success() {
        bail!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    String::from_utf8(output.stdout).context("git output is not UTF-8")
}

fn read(path: &Path) -> Result<String> {
    fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))
}

pub(crate) fn relative(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        bail!("expected a repository-relative path: {}", path.display());
    }
    Ok(())
}

pub fn fragment_category(path: &Path, directory: &Path) -> Result<usize> {
    relative(directory)?;
    let suffix = path.strip_prefix(directory).map_err(|_| {
        anyhow!(
            "fragment outside {}: {}",
            directory.display(),
            path.display()
        )
    })?;
    let parts: Vec<_> = suffix.components().collect();
    if parts.len() != 2 || path.extension().is_none_or(|s| s != "md") {
        bail!("fragment must be <category>/<name>.md: {}", path.display());
    }
    let folder = parts[0].as_os_str().to_str().unwrap_or("");
    CATEGORIES
        .iter()
        .position(|(name, _)| *name == folder)
        .ok_or_else(|| anyhow!("unknown fragment category: {}", path.display()))
}

pub fn collect_fragments(repo: &Path, directory: &Path) -> Result<Vec<Fragment>> {
    relative(directory)?;
    let root = repo.join(directory);
    if !root.exists() {
        return Ok(Vec::new());
    }
    if !fs::symlink_metadata(&root)?.is_dir() {
        bail!("expected a fragment directory: {}", root.display());
    }
    let mut fragments = Vec::new();
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if entry.file_name() == "README.md" && kind.is_file() {
            continue;
        }
        let category = CATEGORIES
            .iter()
            .position(|(folder, _)| entry.file_name() == *folder);
        if !kind.is_dir() || category.is_none() {
            bail!("unexpected changelog path: {}", entry.path().display());
        }
        for file in fs::read_dir(entry.path())? {
            let file = file?;
            let path = file.path();
            if !file.file_type()?.is_file() || path.extension().is_none_or(|s| s != "md") {
                bail!("expected a Markdown fragment: {}", path.display());
            }
            let body = read(&path)?;
            if body.trim().is_empty() {
                bail!("empty fragment: {}", path.display());
            }
            let path = path
                .strip_prefix(repo)?
                .to_str()
                .ok_or_else(|| anyhow!("fragment path is not UTF-8"))?
                .replace('\\', "/");
            fragments.push(Fragment {
                path,
                category: category.unwrap(),
                body,
            });
        }
    }
    fragments.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(fragments)
}

// Only headings outside fences delimit sections; examples may contain release headings.
fn headings<'a>(text: &'a str, prefix: &str) -> Vec<(usize, usize, &'a str)> {
    let mut result = Vec::new();
    let mut offset = 0;
    let mut fence: Option<(char, usize)> = None;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim();
        let first = trimmed.chars().next().unwrap_or(' ');
        let count = trimmed.chars().take_while(|c| *c == first).count();
        if matches!(first, '`' | '~') && count >= 3 {
            match fence {
                None => fence = Some((first, count)),
                Some((c, n)) if c == first && count >= n && trimmed[count..].trim().is_empty() => {
                    fence = None
                }
                _ => {}
            }
        } else if fence.is_none()
            && let Some(value) = line.strip_prefix(prefix)
        {
            result.push((offset, offset + line.len(), value.trim()));
        }
        offset += line.len();
    }
    result
}

fn version_heading(value: &str) -> &str {
    if let Some(value) = value.strip_prefix('[') {
        value.split(']').next().unwrap_or(value)
    } else {
        value.split(" - ").next().unwrap_or(value)
    }
}

fn section<'a>(text: &'a str, version: &str) -> Result<(usize, usize, usize, &'a str)> {
    let headers: Vec<_> = headings(text, "## ")
        .into_iter()
        .filter(|(_, _, heading)| *heading == "Unreleased" || heading.starts_with('['))
        .collect();
    for (index, &(start, body, heading)) in headers.iter().enumerate() {
        if version_heading(heading) == version {
            return Ok((
                start,
                body,
                headers.get(index + 1).map_or(text.len(), |h| h.0),
                heading,
            ));
        }
    }
    Err(anyhow!("could not find changelog section for {version:?}"))
}

pub fn release_section(text: &str, version: &str) -> Result<String> {
    let (_, start, end, _) = section(text, version)?;
    let body = text[start..end].trim_matches(['\r', '\n']);
    if body.trim().is_empty() {
        return Ok(String::new());
    }
    Ok(format!("{body}\n"))
}

pub(crate) fn category_sections(text: &str) -> Vec<(String, String)> {
    let headers: Vec<_> = headings(text, "### ")
        .into_iter()
        .map(|(start, end, heading)| {
            (
                start,
                end,
                heading.strip_suffix(" :boom:").unwrap_or(heading),
            )
        })
        .collect();
    let mut result = Vec::new();
    let first = headers.first().map_or(text.len(), |h| h.0);
    if !text[..first].trim().is_empty() {
        result.push((
            "Other".into(),
            text[..first].trim_matches(['\r', '\n']).into(),
        ));
    }
    for (index, &(_, start, header)) in headers.iter().enumerate() {
        let end = headers.get(index + 1).map_or(text.len(), |h| h.0);
        let body = text[start..end].trim_matches(['\r', '\n']);
        if !body.trim().is_empty() {
            result.push((header.into(), body.into()));
        }
    }
    result
}

pub(crate) fn fragment_entries(body: &str) -> String {
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| format!("- {line}\n"))
        .collect()
}

pub fn assemble_release(
    text: &str,
    fragments: &[Fragment],
    options: &ReleaseOptions<'_>,
) -> Result<String> {
    let ReleaseOptions { version, date } = *options;
    if version.is_empty() || version.contains(['\n', '\r', '[', ']']) {
        bail!("invalid release version");
    }
    if section(text, version).is_ok() {
        bail!("changelog already contains [{version}]");
    }
    if section(text, "Unreleased").is_ok() {
        bail!("migrate Unreleased notes to category folders before preparing a release",);
    }
    let (_, insert, _) = headings(text, "# ")
        .into_iter()
        .find(|(_, _, title)| *title == "Changelog")
        .ok_or_else(|| anyhow!("missing # Changelog heading"))?;
    let mut sections = Vec::new();
    for (index, (_, heading)) in CATEGORIES.iter().enumerate() {
        let mut entries: Vec<_> = fragments.iter().filter(|f| f.category == index).collect();
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        if entries.is_empty() {
            continue;
        }
        let entries: String = entries
            .into_iter()
            .map(|fragment| {
                let entries = fragment_entries(&fragment.body);
                format!("{entries}\n")
            })
            .collect();
        sections.push(format!("### {heading}\n\n{entries}"));
    }
    let sections = sections.join("");
    let preamble = &text[..insert];
    let newline = if preamble.ends_with('\n') { "" } else { "\n" };
    let history = text[insert..].trim_start_matches(['\r', '\n']);
    Ok(format!(
        "{preamble}{newline}\n## [{version}] - {date}\n\n{sections}{history}"
    ))
}

pub fn prepare_release(
    repo: &Path,
    changelog: &Path,
    directory: &Path,
    options: &ReleaseOptions<'_>,
) -> Result<usize> {
    relative(changelog)?;
    let fragments = collect_fragments(repo, directory)?;
    let text = assemble_release(&read(&repo.join(changelog))?, &fragments, options)?;
    fs::write(repo.join(changelog), text)
        .with_context(|| format!("failed to write {}", changelog.display()))?;
    for fragment in &fragments {
        fs::remove_file(repo.join(&fragment.path)).with_context(|| {
            format!(
                "failed to consume {} after writing changelog",
                fragment.path
            )
        })?;
    }
    Ok(fragments.len())
}

pub fn check_fragments(
    repo: &Path,
    directory: &Path,
    base: Option<&str>,
    head: &str,
    require_new: bool,
) -> Result<()> {
    let fragments = collect_fragments(repo, directory)?;
    if let Some(base) = base {
        let changed = git(
            repo,
            &[
                "diff",
                "--name-status",
                "--no-renames",
                "-z",
                base,
                head,
                "--",
                directory
                    .to_str()
                    .ok_or_else(|| anyhow!("invalid directory"))?,
            ],
        )?;
        let fields: Vec<_> = changed.split('\0').filter(|s| !s.is_empty()).collect();
        let mut added = false;
        for pair in fields.chunks_exact(2) {
            if pair[0] == "D" || pair[1] == directory.join("README.md").to_string_lossy() {
                continue;
            }
            fragment_category(Path::new(pair[1]), directory)?;
            if !fragments.iter().any(|f| f.path == pair[1]) {
                bail!("fragment missing from checkout: {}", pair[1]);
            }
            added |= pair[0] == "A";
        }
        if require_new && !added {
            bail!("add a new changelog fragment or apply the skip-changelog label",);
        }
    } else if require_new {
        bail!("--require-new needs --base");
    }
    Ok(())
}

fn numeric_version(value: &str) -> Option<Vec<u64>> {
    let prefix = value
        .split(|c: char| !c.is_ascii_digit() && c != '.')
        .next()?;
    let parts: Option<Vec<_>> = prefix
        .trim_end_matches('.')
        .split('.')
        .map(|p| p.parse().ok())
        .collect();
    parts.filter(|p| p.len() >= 2)
}

pub fn previous_release_tag(repo: &Path, version: &str) -> Result<String> {
    let current = numeric_version(version).ok_or_else(|| anyhow!("invalid version: {version}"))?;
    git(repo, &["tag"])?
        .lines()
        .filter_map(|tag| numeric_version(tag).map(|v| (v, tag.to_owned())))
        .filter(|(v, _)| *v < current)
        .max_by(|a, b| a.0.cmp(&b.0))
        .map(|(_, tag)| tag)
        .ok_or_else(|| anyhow!("no previous release tag before {version}"))
}

fn gitlink(repo: &Path, revision: &str, path: &str) -> Result<String> {
    let output = git(repo, &["ls-tree", revision, "--", path])?;
    let fields: Vec<_> = output.split_whitespace().collect();
    if fields.len() < 3 || fields[0] != "160000" {
        bail!("no submodule {path:?} at {revision:?}");
    }
    Ok(fields[2].into())
}

pub fn core_notes(
    repo: &Path,
    submodule: &Path,
    version: &str,
    from: Option<&str>,
    to: &str,
) -> Result<String> {
    let (submodule, old, new) = core_range(repo, submodule, version, from, to)?;
    if old == new {
        return Ok(String::new());
    }
    Ok(range::release_notes(&submodule, &old, &new, "crates/sdk-core/CHANGELOG.md")?.join("\n"))
}

fn core_range(
    repo: &Path,
    submodule: &Path,
    version: &str,
    from: Option<&str>,
    to: &str,
) -> Result<(std::path::PathBuf, String, String)> {
    relative(submodule)?;
    let previous = match from {
        Some(from) => from.to_owned(),
        None => previous_release_tag(repo, version)?,
    };
    let path = submodule
        .to_str()
        .ok_or_else(|| anyhow!("invalid submodule path"))?;
    let old = gitlink(repo, &previous, path)?;
    let new = gitlink(repo, to, path)?;
    let submodule = repo.join(submodule);
    if !submodule.join(".git").exists() {
        bail!("submodule is not initialized; checkout with submodules",);
    }
    for revision in [&old, &new] {
        if git(
            &submodule,
            &["cat-file", "-e", &format!("{revision}^{{commit}}")],
        )
        .is_err()
        {
            git(&submodule, &["fetch", "--quiet", "origin", revision])?;
        }
    }
    Ok((submodule, old, new))
}

pub fn published_release_notes(
    repo: &Path,
    changelog: &Path,
    submodule: &Path,
    version: &str,
    from: Option<&str>,
    to: &str,
) -> Result<String> {
    relative(changelog)?;
    let section = release_section(&read(&repo.join(changelog))?, version)?;
    let (core, old, new) = core_range(repo, submodule, version, from, to)?;
    let commits = range::commit_notes(&core, &old, &new)?;
    let mut notes = format!("## Notable Changes\n\n{section}");
    if !commits.is_empty() {
        if !notes.ends_with("\n\n") {
            notes.push('\n');
        }
        notes.push_str(&format!("### SDK Core Commits\n\n{}\n", commits.join("\n")));
    }
    Ok(notes)
}
