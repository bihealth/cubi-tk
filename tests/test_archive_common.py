"""Tests for ``cubi_tk.archive.common``.

We only run some smoke tests here.
"""

import os
from pathlib import Path

import cubi_tk.archive.common


def test_run_archive_get_file_attributes():
    project = os.path.join(os.path.dirname(__file__), "data", "archive", "project")

    filename = os.path.join(project, "symlinks", "to_outside_file")
    attributes = cubi_tk.archive.common.FileAttributes(
        relative_path="symlinks/to_outside_file",
        resolved=Path(filename).resolve(),
        symlink=True,
        dangling=False,
        outside=True,
        target="../../outside/files/outside_file",
        size=74,
    )
    assert cubi_tk.archive.common.get_file_attributes(filename, project) == attributes
