#!/usr/bin/env python3
"""Print changelog entries introduced between two SDK Core commits."""

from __future__ import annotations

import argparse
import dataclasses
import difflib
import re
import subprocess
from collections.abc import Sequence


def _git(args: Sequence[str]) -> str:
    return subprocess.check_output(
        ["git", *args], encoding="utf-8", stderr=subprocess.STDOUT
    ).strip()


def _changelog_entries(text: str) -> dict[str, list[list[str]]]:
    entries: dict[str, list[list[str]]] = {}
    header: str | None = None
    entry: list[str] | None = None
    for line in text.splitlines():
        if line.startswith("### "):
            if entry is not None:
                entries.setdefault(header or "Other", []).append(entry)
                entry = None
            header = line.removeprefix("### ").strip()
        elif line.startswith(("* ", "- ")):
            if entry is not None:
                entries.setdefault(header or "Other", []).append(entry)
            entry = [line]
        elif entry is not None:
            if line.strip():
                entry.append(line)
            else:
                entries.setdefault(header or "Other", []).append(entry)
                entry = None
    if entry is not None:
        entries.setdefault(header or "Other", []).append(entry)
    return entries


@dataclasses.dataclass
class _Entry:
    lines: list[str]
    introduced_header: str | None = None


def _updated_entries(
    previous_entries: dict[str, list[_Entry]],
    current_entries: dict[str, list[list[str]]],
) -> dict[str, list[_Entry]]:
    current: list[tuple[str, list[str]]] = [
        (header, lines)
        for header, header_entries in current_entries.items()
        for lines in header_entries
    ]
    previous: list[_Entry] = [
        previous_entry
        for category_entries in previous_entries.values()
        for previous_entry in category_entries
    ]
    exact_matches: dict[tuple[str, ...], list[_Entry]] = {}
    for entry in previous:
        exact_matches.setdefault(tuple(entry.lines), []).append(entry)

    matches: dict[int, _Entry] = {}
    matched_previous: set[int] = set()
    for current_index, (_, current_lines) in enumerate(current):
        exact = exact_matches.get(tuple(current_lines))
        if exact:
            previous_entry = exact.pop(0)
            matches[current_index] = previous_entry
            matched_previous.add(id(previous_entry))

    candidates: list[tuple[float, int, _Entry]] = []
    for current_index, (_, current_entry) in enumerate(current):
        if current_index in matches:
            continue
        for previous_entry in previous:
            if id(previous_entry) in matched_previous:
                continue
            similarity = difflib.SequenceMatcher(
                a="\n".join(previous_entry.lines),
                b="\n".join(current_entry),
                autojunk=False,
            ).ratio()
            if similarity >= 0.6:
                candidates.append((similarity, current_index, previous_entry))
    for _, current_index, previous_entry in sorted(
        candidates, key=lambda candidate: candidate[0], reverse=True
    ):
        if current_index not in matches and id(previous_entry) not in matched_previous:
            matches[current_index] = previous_entry
            matched_previous.add(id(previous_entry))

    updated: dict[str, list[_Entry]] = {}
    for current_index, (header, current_lines) in enumerate(current):
        previous_entry: _Entry | None = matches.get(current_index)
        updated.setdefault(header, []).append(
            _Entry(
                current_lines,
                previous_entry.introduced_header if previous_entry else header,
            )
        )
    return updated


def changelog_entries(previous_commit: str, current_commit: str) -> list[str]:
    commits = _git(
        [
            "log",
            "--format=%H",
            "--reverse",
            f"{previous_commit}..{current_commit}",
            "--",
            "CHANGELOG.md",
        ]
    ).splitlines()
    entries = {
        header: [_Entry(entry) for entry in header_entries]
        for header, header_entries in _changelog_entries(
            _git(["show", f"{previous_commit}:CHANGELOG.md"])
        ).items()
    }
    for commit in commits:
        entries = _updated_entries(
            entries,
            _changelog_entries(_git(["show", f"{commit}:CHANGELOG.md"])),
        )

    categorized: dict[str, list[_Entry]] = {}
    for header_entries in entries.values():
        for entry in header_entries:
            if entry.introduced_header is not None:
                categorized.setdefault(entry.introduced_header, []).append(entry)

    output: list[str] = []
    for header, header_entries in categorized.items():
        output.extend([f"#### {header}", ""])
        for entry in header_entries:
            output.extend(entry.lines)
        output.append("")
    return output[:-1] if output else []


def _clean_commit_subject(subject: str) -> str:
    subject = subject.encode("ascii", "ignore").decode("ascii")
    subject = re.sub(r"\s+", " ", subject).strip()
    subject = re.sub(r"^:[a-z0-9_+-]+:\s*", "", subject)
    return subject.replace(" : ", ": ")


def _link_prs(subject: str) -> str:
    return re.sub(
        r"\(#([0-9]+)\)",
        r"([#\1](https://github.com/temporalio/sdk-rust/pull/\1))",
        subject,
    )


def release_notes(previous_commit: str, current_commit: str) -> list[str]:
    log_output = _git(
        [
            "log",
            "--format=%H%x00%h%x00%s",
            "--reverse",
            f"{previous_commit}..{current_commit}",
        ]
    )
    if not log_output:
        return []

    lines: list[str] = []
    entries = changelog_entries(previous_commit, current_commit)
    if entries:
        lines.extend(["#### Changelog", "", *entries, ""])
    lines.extend(["#### Commits", ""])
    for line in log_output.splitlines():
        full_hash, short_hash, subject = line.split("\0", 2)
        lines.append(
            f"- [`{short_hash}`](https://github.com/temporalio/sdk-rust/commit/"
            f"{full_hash}) {_link_prs(_clean_commit_subject(subject))}"
        )
    return lines


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="previous_commit", required=True)
    parser.add_argument("--to", dest="current_commit", required=True)
    args = parser.parse_args(argv)
    print("\n".join(release_notes(args.previous_commit, args.current_commit)))


if __name__ == "__main__":
    main()
