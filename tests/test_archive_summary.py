"""Tests for ``cubi_tk.archive.summary``.

We only run some smoke tests here.
"""

import os

import pytest
from pyfakefs import fake_filesystem

from cubi_tk.__main__ import setup_argparse, main


def test_run_archive_summary_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "summary", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_archive_summary_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "summary"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_archive_summary_smoke_test(mocker, requests_mock):
    # --- setup arguments
    summary_table = "/somewhere/summary.tbl"
    project_dir = os.path.join(os.path.dirname(__file__), "data", "archive", "2021-10-15_project")
    project_dir = os.path.normpath(os.path.realpath(project_dir))
    argv = ["archive", "summary", project_dir, summary_table]

    parser, _subparsers = setup_argparse()

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break biomedsheets.
    fs = fake_filesystem.FakeFilesystem()
    fake_open = fake_filesystem.FakeFileOpen(fs)

    # --- add test files
    regression = os.path.join(os.path.dirname(project_dir), "summary.tbl")
    regression = os.path.normpath(os.path.realpath(regression))
    fs.add_real_file(regression)
    fs.add_real_directory(project_dir)
    classes = os.path.join(
        os.path.dirname(__file__), "..", "cubi_tk", "isa_tpl", "archive", "classes.yaml"
    )
    classes = os.path.normpath(os.path.realpath(classes))
    fs.add_real_file(classes)

    # --- mock modules
    mocker.patch("cubi_tk.archive.summary.open", fake_open)

    # --- create output directory
    fs.create_dir(os.path.dirname(summary_table))

    # --- run tests
    res = main(argv)
    assert not res

    mocked = [line.rstrip().split("\t") for line in fake_open(summary_table)][1:]
    target = [line.rstrip().split("\t") for line in open(regression, "rt")][1:]
    assert mocked[0] == target[0]
    assert len(mocked) == len(target)
    j = target[0].index("ResolvedName")
    failed = []
    for i in range(len(target)):
        if mocked[i][-j] != target[i][-j]:
            failed.append(i)
    assert len(failed) == 0
