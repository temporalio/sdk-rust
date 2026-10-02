//! Shared changelog operations for SDK release scripts.

use changelog_release_notes::{
    ReleaseOptions, check_fragments, core_notes, prepare_release, release_section,
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
        #[arg(long)]
        allow_empty: bool,
        #[arg(long)]
        breaking_heading: Option<String>,
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

fn main() -> Result<(), String> {
    let cli = Cli::parse();
    let repo = cli.repo.canonicalize().map_err(|e| e.to_string())?;
    match cli.command {
        Command::Check {
            fragments,
            base,
            head,
            require_new,
        } => check_fragments(&repo, &fragments, base.as_deref(), &head, require_new)
            .map_err(|e| e.to_string())?,
        Command::Prepare {
            fragments,
            changelog,
            version,
            date,
            allow_empty,
            breaking_heading,
        } => {
            let count = prepare_release(
                &repo,
                &changelog,
                &fragments,
                &ReleaseOptions {
                    version: &version,
                    date,
                    allow_empty,
                    breaking_heading: breaking_heading.as_deref(),
                },
            )
            .map_err(|e| e.to_string())?;
            println!("Prepared release {version}; consumed {count} changelog fragments");
        }
        Command::Notes { changelog, version } => {
            let text = fs::read_to_string(repo.join(changelog)).map_err(|e| e.to_string())?;
            let notes = release_section(&text, &version).map_err(|e| e.to_string())?;
            print!("{notes}");
        }
        Command::CoreNotes {
            submodule,
            version,
            from,
            to,
        } => {
            let notes = core_notes(&repo, &submodule, &version, from.as_deref(), &to)
                .map_err(|e| e.to_string())?;
            if !notes.is_empty() {
                println!("{notes}");
            }
        }
    }
    Ok(())
}
