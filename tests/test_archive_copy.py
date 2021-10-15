"""Tests for ``cubi_tk.archive.copy``.

We only run some smoke tests here.
"""

import glob
import os

import filecmp
import pytest
import tempfile

from cubi_tk.__main__ import setup_argparse, main


def test_run_archive_summary_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "copy", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_archive_summary_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "copy"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_archive_summary_smoke_test(mocker, requests_mock):
    with tempfile.TemporaryDirectory() as tmp_dir:
        repo_dir = os.path.join(os.path.dirname(__file__), "data", "archive")

        argv = [
            "archive",
            "copy",
            "--audit-file",
            os.path.join(tmp_dir, "audit.orig"),
            "--audit-result",
            os.path.join(tmp_dir, "audit.copy"),
            os.path.join(repo_dir, "temp_dest"),
            os.path.join(tmp_dir, "final_dest"),
        ]
        parser, _subparsers = setup_argparse()

        # --- run tests
        res = main(argv)
        assert not res

        prefix = os.path.join(repo_dir, "final_dest_verif")
        fns = [
            x.replace(prefix + "/", "", 1)
            for x in filter(
                lambda y: os.path.isfile(y), glob.glob(prefix + "/**/*", recursive=True)
            )
        ]
        prefix = os.path.join(tmp_dir, "final_dest")
        fns = fns + [
            x.replace(prefix + "/", "", 1)
            for x in filter(
                lambda y: os.path.isfile(y), glob.glob(prefix + "/**/*", recursive=True)
            )
        ]
        fns = list(set(fns))

        assert filecmp.cmp(
            os.path.join(repo_dir, "audit.orig"), os.path.join(tmp_dir, "audit.orig")
        )
        assert filecmp.cmp(
            os.path.join(repo_dir, "audit.copy"), os.path.join(tmp_dir, "audit.copy")
        )
        matches, mismatches, errors = filecmp.cmpfiles(
            os.path.join(repo_dir, "final_dest_verif"),
            os.path.join(tmp_dir, "final_dest"),
            common=fns,
            shallow=False,
        )
        assert len(errors) == 0
        assert len(mismatches) == 0
