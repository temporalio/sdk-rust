//! Shared changelog operations for SDK release scripts.

use anyhow::{Context, Result};
use changelog_release_notes::{
    ReleaseOptions, check_fragments, core_notes, prepare_release, published_release_notes,
    published_repository_release_notes, release_section, update_core,
};
use chrono::NaiveDate;
use clap::{Parser, Subcommand};
use std::{fs, path::PathBuf};

#[derive(Parser)]
#[command(about = "Manage SDK changelog fragments and release notes")]
struct Cli {
    #[arg(long, global = true, default_value = ".", help = "SDK repository root")]
    repo: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Update Core and import its changelog entries as SDK fragments")]
    UpdateCore {
        #[arg(long)]
        submodule: PathBuf,
        #[arg(long)]
        revision: Option<String>,
        #[arg(long)]
        from: Option<String>,
        #[arg(long, default_value = "changelog")]
        fragments: PathBuf,
    },
    #[command(about = "Generate complete release notes from the SDK changelog and Git commits")]
    ReleaseNotes {
        #[arg(long)]
        submodule: Option<PathBuf>,
        #[arg(long)]
        version: String,
        #[arg(long, default_value = "CHANGELOG.md")]
        changelog: PathBuf,
        #[arg(long, required_unless_present = "submodule")]
        from: Option<String>,
        #[arg(long, default_value = "HEAD")]
        to: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    #[command(about = "Validate fragments and optionally require a new PR fragment")]
    Check {
        #[arg(long, default_value = "changelog")]
        fragments: PathBuf,
        #[arg(long)]
        base: Option<String>,
        #[arg(long, default_value = "HEAD")]
        head: String,
        #[arg(long)]
        require_new: bool,
    },
    #[command(about = "Write a dated release section and consume its fragments")]
    Prepare {
        #[arg(long, default_value = "changelog")]
        fragments: PathBuf,
        #[arg(long, default_value = "CHANGELOG.md")]
        changelog: PathBuf,
        #[arg(long)]
        version: String,
        #[arg(long, value_name = "YYYY-MM-DD")]
        date: NaiveDate,
    },
    #[command(about = "Extract the changelog notes for a released version")]
    Notes {
        #[arg(long, default_value = "CHANGELOG.md")]
        changelog: PathBuf,
        #[arg(long)]
        version: String,
    },
    #[command(about = "Extract Core notes using the SDK repository's Git links")]
    CoreNotes {
        #[arg(long)]
        submodule: PathBuf,
        #[arg(long)]
        version: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long, default_value = "HEAD")]
        to: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let repo = cli
        .repo
        .canonicalize()
        .with_context(|| format!("invalid repository: {}", cli.repo.display()))?;
    match cli.command {
        Command::UpdateCore {
            submodule,
            revision,
            from,
            fragments,
        } => {
            let count = update_core(
                &repo,
                &submodule,
                &fragments,
                revision.as_deref(),
                from.as_deref(),
            )?;
            println!("Updated Core; created {count} changelog fragments");
        }
        Command::ReleaseNotes {
            submodule,
            version,
            changelog,
            from,
            to,
            output,
        } => {
            let notes = match submodule {
                Some(submodule) => published_release_notes(
                    &repo,
                    &changelog,
                    &submodule,
                    &version,
                    from.as_deref(),
                    &to,
                )?,
                None => published_repository_release_notes(
                    &repo,
                    &changelog,
                    &version,
                    from.as_deref()
                        .context("--from is required without --submodule")?,
                    &to,
                )?,
            };
            match output {
                Some(path) => fs::write(repo.join(path), notes)?,
                None => print!("{notes}"),
            }
        }
        Command::Check {
            fragments,
            base,
            head,
            require_new,
        } => check_fragments(&repo, &fragments, base.as_deref(), &head, require_new)?,
        Command::Prepare {
            fragments,
            changelog,
            version,
            date,
        } => {
            let count = prepare_release(
                &repo,
                &changelog,
                &fragments,
                &ReleaseOptions {
                    version: &version,
                    date,
                },
            )?;
            println!("Prepared release {version}; consumed {count} changelog fragments");
        }
        Command::Notes { changelog, version } => {
            let text = fs::read_to_string(repo.join(changelog))?;
            let notes = release_section(&text, &version)?;
            print!("{notes}");
        }
        Command::CoreNotes {
            submodule,
            version,
            from,
            to,
        } => {
            let notes = core_notes(&repo, &submodule, &version, from.as_deref(), &to)?;
            if !notes.is_empty() {
                println!("{notes}");
            }
        }
    }
    Ok(())
}
