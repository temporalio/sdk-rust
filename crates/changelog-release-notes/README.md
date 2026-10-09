# Shared SDK changelog tooling

`changelog-tool` provides release-note operations for SDK repositories. Invoke the
binary from the pinned sdk-rust checkout, passing the parent SDK repository explicitly:

```bash
cargo run --manifest-path <sdk-rust>/crates/changelog-release-notes/Cargo.toml \
  --bin changelog-tool -- <command> --repo <sdk-repository>
```

Commands:

* `update-core --submodule <path> [--revision <ref>] [--fragments changelog]
  [--from <core-revision>]` fetches `origin/main`, updates the Core checkout, and
  imports entries from `crates/sdk-core/CHANGELOG.md` into category fragments.
  The default target is the fetched `origin/main`; an explicit target must resolve
  locally after that fetch. The range starts at the checked-out Core commit unless
  `--from` supplies a migration baseline. Updates must move forward along the
  current history, including when an explicit baseline is supplied.
  Core must be initialized and clean. Unchanged pins create no fragments.
  Filenames are generated from petname's large word lists, with collision retries;
  affected categories share a stem. Each complete Core entry becomes one line,
  without its source list marker and prefixed with `Core: ` to distinguish it
  from language SDK changes. Inline Markdown is preserved; unsupported block
  Markdown and unknown categories cause an error before the checkout changes.
  Review and commit the pin and generated fragments together. The command does
  not stage, commit, update language lockfiles, or resolve bridge compatibility.
  Import failures remove newly created files and restore the original checkout;
  any rollback failures are reported for manual recovery.

* `check [--fragments changelog] [--base <sha> --head HEAD --require-new]`
  validates fragments. With Git revisions, it requires an added fragment rather
  than only edits or deletions. The checkout must match `--head`. The calling
  workflow handles override labels and checks out the PR's synthetic merge result.
* `prepare --version <version> --date YYYY-MM-DD [--changelog CHANGELOG.md]
  [--fragments changelog]`
  validates all pending notes, writes the dated release into the changelog, and
  removes the consumed fragments. Empty releases are allowed; duplicate versions
  are always rejected. Validation failures leave the
  changelog and fragments intact.
* `notes --version <version> [--changelog CHANGELOG.md]` prints that dated release's
  body as Markdown, excluding the version heading. Empty release sections produce
  no output; missing release sections are errors.
* `release-notes --version <version> [--submodule <path>] [--changelog CHANGELOG.md]
  [--from <ref>] [--to <ref>] [--output <file>]` generates publishing notes.
  It extracts the body of `## [<version>]` from the SDK changelog and places it
  under `## Notable Changes` in the generated output.

  With `--submodule`, `--from` and `--to` identify revisions in the SDK repository.
  The command reads the Core submodule pins at those revisions and appends Core
  commit links under `### SDK Core Commits` if there are any. When `--from` is
  omitted, it selects the greatest numeric SDK release tag below the requested
  version, accepting an optional `v` prefix (for example, `1.9.0` or `v1.9.0`).

  Without `--submodule`, `--from` is required. The command appends the SDK
  repository's own commit links between `--from` and `--to` under `### Commits`
  if there are any. Rust's publishing workflow passes its previous SDK release
  tag (`v<version>`) as `--from`.

  `--to` defaults to `HEAD`. Core changelog entries are imported by `update-core`
  and included in the prepared SDK changelog; this command extracts completed
  notes and adds commit links.
  Output goes to stdout unless a file is supplied. Relative output paths resolve
  against `--repo`; absolute output paths are also supported.
* `core-notes --version <version> --submodule <path> [--from <tag> --to HEAD]`
  resolves Core revisions from the parent repository's Git links and produces
  the existing Core changelog/commit notes. Without `--from`, it selects the
  greatest numeric release tag below the requested version, with or without a `v`
  prefix. SDKs with other tag
  conventions should pass their previous tag explicitly.

Changelog and fragment paths are relative to `--repo`. Fragments are `.md` files
directly inside `added`, `stabilized`, `changed`, `deprecated`, `breaking-changes`,
`fixed`, or `security` folders. Use `stabilized` for features that are no longer
experimental. The folder supplies the category. Write a concise entry,
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
are unchanged. Breaking changes use the heading `### :boom: Breaking Changes`.
Migrate pending Unreleased notes into fragments and remove that
section before adopting `prepare`; there is no new Unreleased section.

SDK adapters update their version files and refresh their lockfile first, then
invoke `prepare` with the new version and release date. The tool owns changelog
writes and fragment consumption; adapters retain Git commits and PR creation.
If validation fails, SDK-specific version and lock changes remain in the local
worktree for inspection. File-system failures can also leave partial preparation;
stop before committing and inspect the changes before retrying. Fragments merged
after preparation remain for the next release.

To migrate an SDK, build the new command from a separate sdk-rust checkout and
invoke `update-core` with `--repo <sdk>`, `--revision <new-tool-commit>`, and
`--from <Core-pin-at-the-latest-SDK-release>`. This backfills changes already pinned
since the last release while adopting the new tool. Use the explicit baseline
only once, to avoid importing the same entries again. Subsequent updates run the
tool from the SDK's pinned submodule and use the checked-out commit as the baseline.

The existing `changelog-release-notes --from <sha> --to <sha>
[--changelog rust|core]` interface remains available. Its history traversal includes
fragments beside the selected master changelog, allowing ranges across migration
and assembly without duplicating notes. Rust SDK contributions use root `changelog/`
fragments. Its `prepare-release` adapter updates crate versions, then invokes shared
preparation for the root changelog. Core's changelog retains its Unreleased workflow.
See the repository [contributing guide](../../CONTRIBUTING.md#preparing-a-release).
