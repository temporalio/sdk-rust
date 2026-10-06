# Rust SDK changelog fragments

These fragments describe changes users of the Rust SDK can observe. The separate
`crates/sdk-core/CHANGELOG.md` continues to use its `Unreleased` section for users
of the other Temporal SDKs. A Core behavior change visible to both audiences
normally needs a Rust SDK fragment and a Core entry, worded for each audience.

Add a Markdown file in the appropriate category with a fun, whimsical lowercase
kebab-case filename, such as `fixed/dancing-marshmallow.md`.

| Folder | Changes |
| --- | --- |
| `added` | New features |
| `stabilized` | Features that are no longer experimental |
| `changed` | Changes to existing functionality |
| `deprecated` | Features planned for removal |
| `breaking-changes` | Removed or backwards-incompatible features |
| `fixed` | Bug fixes |
| `security` | Security fixes |

Keep each entry concise, ideally one or two sentences. Each nonempty line becomes
one bullet, so keep each entry on one line and omit the leading `-`. A fragment
can contain multiple entries. Use inline Markdown and links, without headings or
nested lists. Blank lines are ignored.

`cargo changelog-check` validates pending fragments. `CHANGELOG.md` contains only
completed releases. See [CONTRIBUTING.md](../CONTRIBUTING.md#preparing-a-release)
for release preparation and publishing instructions.
