#!/usr/bin/env python3
"""Unit tests for changelog_release_notes."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import changelog_release_notes as notes


class ChangelogReleaseNotesTests(unittest.TestCase):
    def test_keeps_final_wording_for_introduced_entry(self) -> None:
        updated = notes._updated_entries(
            {"Added": [notes._Entry(["* Initial wording."], "Added")]},
            {"Added": [["* Final wording."]]},
        )
        self.assertEqual(updated["Added"][0].lines, ["* Final wording."])
        self.assertEqual(updated["Added"][0].introduced_header, "Added")

    def test_excludes_modified_old_entry_and_keeps_new_entry(self) -> None:
        updated = notes._updated_entries(
            {"Added": [notes._Entry(["* Existing feature."])]},
            {"Added": [["* Corrected existing feature."], ["* New feature."]]},
        )
        self.assertEqual(
            [entry.introduced_header for entry in updated["Added"]],
            [None, "Added"],
        )

    def test_preserves_heading_when_entry_moves_and_is_edited(self) -> None:
        updated = notes._updated_entries(
            {"Added": [notes._Entry(["* Initial feature wording."], "Added")]},
            {"Changed": [["* Updated feature wording."]]},
        )
        self.assertEqual(updated["Changed"][0].introduced_header, "Added")

    def test_includes_unrelated_replacement(self) -> None:
        updated = notes._updated_entries(
            {"Added": [notes._Entry(["* Old feature."])]},
            {"Added": [["* New capability for another API."]]},
        )
        self.assertEqual(updated["Added"][0].introduced_header, "Added")

    def test_excludes_multiline_old_entry_modification(self) -> None:
        updated = notes._updated_entries(
            {"Fixed": [notes._Entry(["* Existing fix.", "  Old detail."])]},
            {"Fixed": [["* Existing fix.", "  New detail."]]},
        )
        self.assertIsNone(updated["Fixed"][0].introduced_header)

    def test_keeps_introduced_entry_when_heading_changes(self) -> None:
        updated = notes._updated_entries(
            {"Added": [notes._Entry(["* New feature."], "Added")]},
            {"Released additions": [["* New feature."]]},
        )
        self.assertEqual(updated["Released additions"][0].introduced_header, "Added")

    def test_release_notes_include_linked_commits(self) -> None:
        with (
            patch.object(
                notes, "_git", return_value="full\x00short\x00:boom: Add feature (#12)"
            ),
            patch.object(notes, "changelog_entries", return_value=[]),
        ):
            release_notes = notes.release_notes("old", "new")
        self.assertEqual(
            release_notes,
            [
                "#### Commits",
                "",
                "- [`short`](https://github.com/temporalio/sdk-rust/commit/full) "
                "Add feature ([#12](https://github.com/temporalio/sdk-rust/pull/12))",
            ],
        )


if __name__ == "__main__":
    unittest.main()
