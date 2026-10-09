use anyhow::{Context, Result, anyhow, bail};
use std::{fs, io::Write, path::Path};

use crate::{CATEGORIES, collect_fragments, git, range, relative};

pub fn update_core(
    repo: &Path,
    submodule: &Path,
    directory: &Path,
    revision: Option<&str>,
    from: Option<&str>,
) -> Result<usize> {
    relative(submodule)?;
    relative(directory)?;
    collect_fragments(repo, directory)?;
    let path = submodule.to_str().context("invalid submodule path")?;
    let gitlink = git(repo, &["ls-files", "--stage", "--", path])?;
    if !gitlink.starts_with("160000 ") {
        bail!("not a registered submodule: {path}");
    }
    let core = repo.join(submodule);
    if !core.join(".git").exists() {
        bail!("initialize the Core submodule before updating it");
    }
    if !git(&core, &["status", "--porcelain"])?.trim().is_empty() {
        bail!("Core checkout has local changes; commit or restore them before updating");
    }
    let old = git(&core, &["rev-parse", "HEAD"])?.trim().to_owned();
    let original_ref = git(&core, &["symbolic-ref", "--quiet", "--short", "HEAD"])
        .map(|branch| branch.trim().to_owned())
        .unwrap_or_else(|_| old.clone());
    git(&core, &["fetch", "--quiet", "origin", "main"])?;
    let target = revision.unwrap_or("origin/main");
    let new = git(
        &core,
        &["rev-parse", "--verify", &format!("{target}^{{commit}}")],
    )?
    .trim()
    .to_owned();
    git(&core, &["merge-base", "--is-ancestor", &old, &new])
        .context("Core updates must move forward along the current history")?;
    let baseline = match from {
        Some(from) => git(
            &core,
            &["rev-parse", "--verify", &format!("{from}^{{commit}}")],
        )?
        .trim()
        .to_owned(),
        None => old.clone(),
    };
    git(&core, &["merge-base", "--is-ancestor", &baseline, &new])
        .context("the import baseline must be an ancestor of the target")?;
    git(&core, &["merge-base", "--is-ancestor", &baseline, &old])
        .context("the migration baseline must also be an ancestor of the current checkout")?;
    if baseline == new && old == new {
        return Ok(0);
    }
    let entries =
        range::categorized_entries(&core, &baseline, &new, "crates/sdk-core/CHANGELOG.md")?;
    let mut fragments = Vec::new();
    for (heading, entries) in entries {
        let category = CATEGORIES
            .iter()
            .position(|(_, name)| {
                *name == heading
                    || (*name == ":boom: Breaking Changes" && heading == "Breaking Changes")
            })
            .ok_or_else(|| anyhow!("unknown Core changelog category: {heading}"))?;
        let mut body = String::new();
        for entry in entries {
            let mut lines = entry
                .iter()
                .map(|line| line.trim())
                .filter(|line| !line.is_empty());
            let first = lines.next().context("empty Core changelog entry")?;
            let first = first
                .strip_prefix("- ")
                .or_else(|| first.strip_prefix("* "))
                .context("Core changelog entries must be Markdown list items")?;
            let continuation: Vec<_> = lines.collect();
            if continuation.iter().any(|line| {
                line.starts_with(['#', '>', '|'])
                    || line.starts_with("```")
                    || line.starts_with("~~~")
                    || line.starts_with("- ")
                    || line.starts_with("* ")
                    || line.chars().next().is_some_and(|c| c.is_ascii_digit())
                        && line.contains(". ")
            }) {
                bail!(
                    "Core entry in {heading} contains block Markdown; rewrite it as concise prose before importing: {first}"
                );
            }
            body.push_str("Core: ");
            body.push_str(first);
            for line in continuation {
                body.push(' ');
                body.push_str(line);
            }
            body.push('\n');
        }
        if !body.is_empty() {
            fragments.push((category, body));
        }
    }
    let names = petname::Petnames::large();
    let stem = fragment_stem(
        repo,
        directory,
        &fragments,
        names.namer(2, "-").iter(&mut rand::rng()),
    )?;
    let mut created = Vec::new();
    let result = (|| -> Result<()> {
        for (category, body) in &fragments {
            let folder = repo.join(directory).join(CATEGORIES[*category].0);
            fs::create_dir_all(&folder)?;
            let path = folder.join(format!("{stem}.md"));
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            created.push(path);
            file.write_all(body.as_bytes())?;
        }
        git(&core, &["checkout", "--quiet", "--detach", &new])?;
        Ok(())
    })();
    if let Err(error) = result {
        let mut failures = Vec::new();
        for path in created {
            if let Err(error) = fs::remove_file(&path) {
                failures.push(format!("{}: {error}", path.display()));
            }
        }
        if let Err(error) = git(&core, &["checkout", "--quiet", &original_ref]) {
            failures.push(error.to_string());
        }
        if !failures.is_empty() {
            return Err(error.context(format!("rollback failed: {}", failures.join("; "))));
        }
        return Err(error);
    }
    Ok(fragments.len())
}

fn fragment_stem(
    repo: &Path,
    directory: &Path,
    fragments: &[(usize, String)],
    names: impl Iterator<Item = String>,
) -> Result<String> {
    names
        .take(100)
        .find(|stem| {
            fragments.iter().all(|(category, _)| {
                !repo
                    .join(directory)
                    .join(CATEGORIES[*category].0)
                    .join(format!("{stem}.md"))
                    .exists()
            })
        })
        .context("could not generate an unused whimsical fragment name")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn collision_in_any_category_preserves_existing_fragment() {
        let repo = TempDir::new().unwrap();
        fs::create_dir_all(repo.path().join("changelog/fixed")).unwrap();
        let existing = repo.path().join("changelog/fixed/dancing-marshmallow.md");
        fs::write(&existing, "Existing note.\n").unwrap();
        let fragments = vec![(0, "Feature.\n".into()), (5, "Fix.\n".into())];
        let names = ["dancing-marshmallow", "giggling-teapot"].map(String::from);
        assert_eq!(
            fragment_stem(
                repo.path(),
                Path::new("changelog"),
                &fragments,
                names.into_iter()
            )
            .unwrap(),
            "giggling-teapot"
        );
        assert_eq!(fs::read_to_string(existing).unwrap(), "Existing note.\n");
        assert!(
            fragment_stem(
                repo.path(),
                Path::new("changelog"),
                &fragments,
                std::iter::repeat("dancing-marshmallow".into())
            )
            .is_err()
        );
    }
}
