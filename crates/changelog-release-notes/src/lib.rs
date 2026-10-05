//! Shared changelog preparation for Temporal SDK release adapters.

use chrono::NaiveDate;
use std::{
    fs,
    path::{Component, Path},
    process::Command,
};
use thiserror::Error;

pub mod range;

pub const CATEGORIES: [(&str, &str); 6] = [
    ("added", "Added"),
    ("changed", "Changed"),
    ("deprecated", "Deprecated"),
    ("breaking-changes", "Breaking Changes"),
    ("fixed", "Fixed"),
    ("security", "Security"),
];

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ChangelogError(pub String);

impl From<String> for ChangelogError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for ChangelogError {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

type Result<T> = std::result::Result<T, ChangelogError>;

pub struct ReleaseOptions<'a> {
    pub version: &'a str,
    pub date: NaiveDate,
    pub allow_empty: bool,
    pub breaking_heading: Option<&'a str>,
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
        .map_err(|e| ChangelogError(format!("failed to run git: {e}")))?;
    if !output.status.success() {
        return Err(ChangelogError(
            String::from_utf8_lossy(&output.stderr).trim().into(),
        ));
    }
    String::from_utf8(output.stdout).map_err(|e| ChangelogError(e.to_string()))
}

fn read(path: &Path) -> Result<String> {
    fs::read_to_string(path).map_err(|e| ChangelogError(format!("{}: {e}", path.display())))
}

fn relative(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(ChangelogError(format!(
            "expected a repository-relative path: {}",
            path.display()
        )));
    }
    Ok(())
}

pub fn fragment_category(path: &Path, directory: &Path) -> Result<usize> {
    relative(directory)?;
    let suffix = path.strip_prefix(directory).map_err(|_| {
        ChangelogError(format!(
            "fragment outside {}: {}",
            directory.display(),
            path.display()
        ))
    })?;
    let parts: Vec<_> = suffix.components().collect();
    if parts.len() != 2 || path.extension().is_none_or(|s| s != "md") {
        return Err(ChangelogError(format!(
            "fragment must be <category>/<name>.md: {}",
            path.display()
        )));
    }
    let folder = parts[0].as_os_str().to_str().unwrap_or("");
    CATEGORIES
        .iter()
        .position(|(name, _)| *name == folder)
        .ok_or_else(|| ChangelogError(format!("unknown fragment category: {}", path.display())))
}

pub fn collect_fragments(repo: &Path, directory: &Path) -> Result<Vec<Fragment>> {
    relative(directory)?;
    let root = repo.join(directory);
    if !root.exists() {
        return Ok(Vec::new());
    }
    if !fs::symlink_metadata(&root)
        .map_err(|e| ChangelogError(e.to_string()))?
        .is_dir()
    {
        return Err(ChangelogError(format!(
            "expected a fragment directory: {}",
            root.display()
        )));
    }
    let mut fragments = Vec::new();
    for entry in fs::read_dir(&root).map_err(|e| ChangelogError(e.to_string()))? {
        let entry = entry.map_err(|e| ChangelogError(e.to_string()))?;
        let kind = entry
            .file_type()
            .map_err(|e| ChangelogError(e.to_string()))?;
        if entry.file_name() == "README.md" && kind.is_file() {
            continue;
        }
        let category = CATEGORIES
            .iter()
            .position(|(folder, _)| entry.file_name() == *folder);
        if !kind.is_dir() || category.is_none() {
            return Err(ChangelogError(format!(
                "unexpected changelog path: {}",
                entry.path().display()
            )));
        }
        for file in fs::read_dir(entry.path()).map_err(|e| ChangelogError(e.to_string()))? {
            let file = file.map_err(|e| ChangelogError(e.to_string()))?;
            let path = file.path();
            if !file
                .file_type()
                .map_err(|e| ChangelogError(e.to_string()))?
                .is_file()
                || path.extension().is_none_or(|s| s != "md")
            {
                return Err(ChangelogError(format!(
                    "expected a Markdown fragment: {}",
                    path.display()
                )));
            }
            let body = read(&path)?;
            if body.trim().is_empty() {
                return Err(ChangelogError(format!(
                    "empty fragment: {}",
                    path.display()
                )));
            }
            let path = path
                .strip_prefix(repo)
                .map_err(|e| ChangelogError(e.to_string()))?
                .to_str()
                .ok_or_else(|| ChangelogError("fragment path is not UTF-8".into()))?
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
    Err(ChangelogError(format!(
        "could not find changelog section for {version:?}"
    )))
}

pub fn release_section(text: &str, version: &str) -> Result<String> {
    let (_, start, end, _) = section(text, version)?;
    let body = text[start..end].trim_matches(['\r', '\n']);
    if body.trim().is_empty() {
        return Err(ChangelogError(format!(
            "changelog section for {version:?} is empty"
        )));
    }
    Ok(format!("{body}\n"))
}

pub(crate) fn category_sections(text: &str) -> Vec<(String, String)> {
    let headers: Vec<_> = headings(text, "### ")
        .into_iter()
        .filter(|(_, _, heading)| {
            let heading = heading.strip_prefix(":boom: ").unwrap_or(heading);
            CATEGORIES.iter().any(|(_, category)| *category == heading)
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

pub(crate) fn fragment_entry(body: &str) -> String {
    body.split_inclusive('\n')
        .enumerate()
        .map(|(index, line)| {
            let prefix = if index == 0 {
                "- "
            } else if matches!(line, "\n" | "\r\n") {
                ""
            } else {
                "  "
            };
            format!("{prefix}{line}")
        })
        .collect()
}

pub fn assemble_release(
    text: &str,
    fragments: &[Fragment],
    options: &ReleaseOptions<'_>,
) -> Result<String> {
    let ReleaseOptions {
        version,
        date,
        allow_empty,
        breaking_heading,
    } = *options;
    if version.is_empty() || version.contains(['\n', '\r', '[', ']']) {
        return Err(ChangelogError("invalid release version".into()));
    }
    if section(text, version).is_ok() {
        return Err(ChangelogError(format!(
            "changelog already contains [{version}]"
        )));
    }
    if section(text, "Unreleased").is_ok() {
        return Err(ChangelogError(
            "migrate Unreleased notes to category folders before preparing a release".into(),
        ));
    }
    if fragments.is_empty() && !allow_empty {
        return Err(ChangelogError("release has no changelog notes".into()));
    }
    let (_, insert, _) = headings(text, "# ")
        .into_iter()
        .find(|(_, _, title)| *title == "Changelog")
        .ok_or_else(|| ChangelogError("missing # Changelog heading".into()))?;
    let mut sections = Vec::new();
    for (index, (_, heading)) in CATEGORIES.iter().enumerate() {
        let mut entries: Vec<_> = fragments.iter().filter(|f| f.category == index).collect();
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        if entries.is_empty() {
            continue;
        }
        let heading = if index == 3 {
            breaking_heading.unwrap_or(heading)
        } else {
            heading
        };
        let entries: String = entries
            .into_iter()
            .map(|fragment| {
                let body = fragment_entry(&fragment.body);
                let newline = if body.ends_with('\n') { "" } else { "\n" };
                format!("{body}{newline}\n")
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
        .map_err(|e| ChangelogError(format!("failed to write {}: {e}", changelog.display())))?;
    for fragment in &fragments {
        fs::remove_file(repo.join(&fragment.path)).map_err(|e| {
            ChangelogError(format!(
                "failed to consume {} after writing changelog: {e}",
                fragment.path
            ))
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
                    .ok_or_else(|| ChangelogError("invalid directory".into()))?,
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
                return Err(ChangelogError(format!(
                    "fragment missing from checkout: {}",
                    pair[1]
                )));
            }
            added |= pair[0] == "A";
        }
        if require_new && !added {
            return Err(ChangelogError(
                "add a new changelog fragment or apply the skip-changelog label".into(),
            ));
        }
    } else if require_new {
        return Err(ChangelogError("--require-new needs --base".into()));
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
    let current = numeric_version(version)
        .ok_or_else(|| ChangelogError(format!("invalid version: {version}")))?;
    git(repo, &["tag"])?
        .lines()
        .filter_map(|tag| numeric_version(tag).map(|v| (v, tag.to_owned())))
        .filter(|(v, _)| *v < current)
        .max_by(|a, b| a.0.cmp(&b.0))
        .map(|(_, tag)| tag)
        .ok_or_else(|| ChangelogError(format!("no previous release tag before {version}")))
}

fn gitlink(repo: &Path, revision: &str, path: &str) -> Result<String> {
    let output = git(repo, &["ls-tree", revision, "--", path])?;
    let fields: Vec<_> = output.split_whitespace().collect();
    if fields.len() < 3 || fields[0] != "160000" {
        return Err(ChangelogError(format!(
            "no submodule {path:?} at {revision:?}"
        )));
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
    relative(submodule)?;
    let previous = match from {
        Some(from) => from.to_owned(),
        None => previous_release_tag(repo, version)?,
    };
    let path = submodule
        .to_str()
        .ok_or_else(|| ChangelogError("invalid submodule path".into()))?;
    let old = gitlink(repo, &previous, path)?;
    let new = gitlink(repo, to, path)?;
    if old == new {
        return Ok(String::new());
    }
    let submodule = repo.join(submodule);
    if !submodule.join(".git").exists() {
        return Err(ChangelogError(
            "submodule is not initialized; checkout with submodules".into(),
        ));
    }
    let notes = match range::release_notes(&submodule, &old, &new, "crates/sdk-core/CHANGELOG.md") {
        Ok(notes) => notes,
        Err(_) => {
            git(&submodule, &["fetch", "--quiet", "origin", "main"])?;
            range::release_notes(&submodule, &old, &new, "crates/sdk-core/CHANGELOG.md")?
        }
    };
    Ok(notes.join("\n"))
}
