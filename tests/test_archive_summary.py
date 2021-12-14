"""Tests for ``cubi_tk.archive.summary``.

We only run some smoke tests here.
"""

import os

import pytest
import tempfile

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


def test_run_archive_summary_smoke_test():
    filename = "summary.tbl"
    with tempfile.TemporaryDirectory() as tmp_dir:
        repo_dir = os.path.join(os.path.dirname(__file__), "data", "archive")
        target_file = os.path.join(repo_dir, filename)
        mocked_file = os.path.join(tmp_dir, filename)

        argv = ["archive", "summary", os.path.join(repo_dir, "project"), mocked_file]
        setup_argparse()

        # --- run tests
        res = main(argv)
        assert not res

        mocked = sorted([line.rstrip().split("\t") for line in open(mocked_file, "rt")][1:])
        target = sorted([line.rstrip().split("\t") for line in open(target_file, "rt")][1:])
        assert len(mocked) == len(target)
        j = target[0].index("ResolvedName")
        failed = []
        for (i, value) in enumerate(target):
            if mocked[i][-j] != value[-j]:
                failed.append(value)
        assert len(failed) == 0
