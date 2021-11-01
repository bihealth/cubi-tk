"""Tests for ``cubi_tk.archive.common``.

We only run some smoke tests here.
"""

import os
from pathlib import Path

import cubi_tk.archive.common


def test_run_archive_get_file_attributes():
    project = os.path.join(os.path.dirname(__file__), "data", "archive", "2021-10-15_project")

    filename = os.path.join(project, "symlinks", "accessible")
    attributes = cubi_tk.archive.common.FileAttributes(
        relative_path="symlinks/accessible",
        resolved=Path(filename).resolve(),
        symlink=True,
        dangling=False,
        outside=True,
        target="../../outside/accessible",
        size=11,
    )
    assert cubi_tk.archive.common.get_file_attributes(filename, project) == attributes
