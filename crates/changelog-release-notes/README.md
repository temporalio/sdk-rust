# Shared SDK changelog tooling

`changelog-tool` provides release-note operations for SDK repositories. Invoke the
binary from the pinned sdk-rust checkout, passing the parent SDK repository explicitly:

```bash
cargo run --manifest-path <sdk-rust>/crates/changelog-release-notes/Cargo.toml \
  --bin changelog-tool -- <command> --repo <sdk-repository>
```

Commands:

* `check [--fragments changelog] [--base <sha> --head HEAD --require-new]`
  validates fragments. With Git revisions, it requires an added fragment rather
  than only edits or deletions. The checkout must match `--head`. The calling
  workflow handles override labels and checks out the PR's synthetic merge result.
* `prepare --version <version> --date YYYY-MM-DD [--changelog CHANGELOG.md]
  [--fragments changelog] [--breaking-heading 'Breaking Changes'] [--allow-empty]`
  validates all pending notes, writes the dated release into the changelog, and
  removes the consumed fragments. Empty releases are rejected unless explicitly
  allowed; duplicate versions are always rejected. Validation failures leave the
  changelog and fragments intact.
* `notes --version <version> [--changelog CHANGELOG.md]` prints that dated release's
  body as Markdown, excluding the version heading.
* `core-notes --version <version> --submodule <path> [--from <tag> --to HEAD]`
  resolves Core revisions from the parent repository's Git links and produces
  the existing Core changelog/commit notes. Without `--from`, it selects the
  greatest numeric release tag below the requested version. SDKs with other tag
  conventions should pass their previous tag explicitly.

Changelog and fragment paths are relative to `--repo`. Fragments are `.md` files
directly inside `added`, `changed`, `deprecated`, `breaking-changes`, `fixed`, or
`security` folders. The folder supplies the category. Write a concise entry,
ideally one or two sentences, without a leading list marker (`-`). Write each entry
entirely on one line: assembly turns each nonempty line into a separate list item.
A fragment may contain multiple entries; their order is preserved and blank lines
are ignored.
Entries can contain inline Markdown and documentation links.
Use fun, whimsical lowercase kebab-case filenames such as `dancing-marshmallow.md`.
The root fragment-directory README is ignored. Empty files, unexpected folders,
misplaced files, nested folders, and symlinks are rejected.

Assembly inserts a dated release immediately after `# Changelog`, groups notes in
the folder order above, and sorts filenames within categories. Existing releases
are unchanged. Migrate pending Unreleased notes into fragments and remove that
section before adopting `prepare`; there is no new Unreleased section.

SDK adapters update their version files and refresh their lockfile first, then
invoke `prepare` with the new version and release date. The tool owns changelog
writes and fragment consumption; adapters retain Git commits and PR creation.
If validation fails, SDK-specific version and lock changes remain in the local
worktree for inspection. File-system failures can also leave partial preparation;
stop before committing and inspect the changes before retrying. Fragments merged
after preparation remain for the next release.

The existing `changelog-release-notes --from <sha> --to <sha>
[--changelog rust|core]` interface remains available. Its history traversal includes
fragments beside the selected master changelog, allowing ranges across migration
and assembly without duplicating notes. Rust's own contributor convention and
`prepare-release` binary retain their current Unreleased workflow until migrated.
