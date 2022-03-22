"""Tests for ``cubi_tk.archive.common``.

We only run some smoke tests here.
"""

import os
from pathlib import Path

import cubi_tk.archive.common


def test_run_archive_get_file_attributes():
    project = os.path.join(os.path.dirname(__file__), "data", "archive", "project")

    relative_path = os.path.join("raw_data", "batch2", "sample2.fastq.gz")
    filename = os.path.join(project, relative_path)
    attributes = cubi_tk.archive.common.FileAttributes(
        relative_path=relative_path,
        resolved=Path(filename).resolve(),
        symlink=True,
        dangling=False,
        outside=True,
        target=os.path.join("..", "..", "..", "outside", "batch2", "sample2.fastq.gz"),
        size=22,
    )
    assert cubi_tk.archive.common.get_file_attributes(filename, project) == attributes
