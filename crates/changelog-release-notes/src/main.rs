//! Print SDK Core changelog and commit notes for a Git revision range.

use changelog_release_notes::range::{changelog_path, release_notes};
use std::env;

fn main() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let from = args
        .next()
        .filter(|arg| arg == "--from")
        .and_then(|_| args.next())
        .ok_or("expected --from <sha>")?;
    let to = args
        .next()
        .filter(|arg| arg == "--to")
        .and_then(|_| args.next())
        .ok_or("expected --to <sha>")?;
    let changelog = match args.next().as_deref() {
        None => "core".to_owned(),
        Some("--changelog") => args.next().ok_or("expected --changelog <rust|core>")?,
        Some(_) => return Err("expected --changelog <rust|core>".into()),
    };
    println!(
        "{}",
        release_notes(
            &env::current_dir().map_err(|e| e.to_string())?,
            &from,
            &to,
            changelog_path(&changelog).map_err(|e| e.to_string())?
        )
        .map_err(|e| e.to_string())?
        .join("\n")
    );
    Ok(())
}
