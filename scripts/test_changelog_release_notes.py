#!/usr/bin/env python3
"""Unit tests for changelog_release_notes."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest


def _module() -> object:
    path = pathlib.Path(__file__).with_name("changelog_release_notes.py")
    spec = importlib.util.spec_from_file_location("changelog_release_notes", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ChangelogReleaseNotesTests(unittest.TestCase):
    def test_keeps_final_wording_for_introduced_entry(self) -> None:
        module = _module()
        updated = module._updated_entries(
            {"Added": [module._Entry(["* Initial wording."], "Added")]},
            {"Added": [["* Final wording."]]},
        )
        self.assertEqual(updated["Added"][0].lines, ["* Final wording."])
        self.assertEqual(updated["Added"][0].introduced_header, "Added")

    def test_excludes_modified_old_entry_and_keeps_new_entry(self) -> None:
        module = _module()
        updated = module._updated_entries(
            {"Added": [module._Entry(["* Existing feature."])]},
            {"Added": [["* Corrected existing feature."], ["* New feature."]]},
        )
        self.assertEqual(
            [entry.introduced_header for entry in updated["Added"]],
            [None, "Added"],
        )

    def test_preserves_heading_when_entry_moves_and_is_edited(self) -> None:
        module = _module()
        updated = module._updated_entries(
            {"Added": [module._Entry(["* Initial feature wording."], "Added")]},
            {"Changed": [["* Updated feature wording."]]},
        )
        self.assertEqual(updated["Changed"][0].introduced_header, "Added")

    def test_includes_unrelated_replacement(self) -> None:
        module = _module()
        updated = module._updated_entries(
            {"Added": [module._Entry(["* Old feature."])]},
            {"Added": [["* New capability for another API."]]},
        )
        self.assertEqual(updated["Added"][0].introduced_header, "Added")

    def test_excludes_multiline_old_entry_modification(self) -> None:
        module = _module()
        updated = module._updated_entries(
            {"Fixed": [module._Entry(["* Existing fix.", "  Old detail."])]},
            {"Fixed": [["* Existing fix.", "  New detail."]]},
        )
        self.assertIsNone(updated["Fixed"][0].introduced_header)

    def test_keeps_introduced_entry_when_heading_changes(self) -> None:
        module = _module()
        updated = module._updated_entries(
            {"Added": [module._Entry(["* New feature."], "Added")]},
            {"Released additions": [["* New feature."]]},
        )
        self.assertEqual(updated["Released additions"][0].introduced_header, "Added")


if __name__ == "__main__":
    unittest.main()
