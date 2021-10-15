"""Tests for ``cubi_tk.archive.prepare``.

We only run some smoke tests here.
"""

import glob
import os
import tempfile

import filecmp
import pytest

from cubi_tk.__main__ import setup_argparse, main


def test_run_archive_prepare_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "summary", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_archive_prepare_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "summary"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_archive_prepare_smoke_test(mocker, requests_mock):
    with tempfile.TemporaryDirectory() as tmp_dir:
        repo_dir = os.path.join(os.path.dirname(__file__), "data", "archive")
        project_name = "2021-10-15_project"

        argv = [
            "archive",
            "prepare",
            "--rules",
            os.path.join(repo_dir, "rules.yaml"),
            "--no-readme",
            os.path.join(repo_dir, project_name),
            os.path.join(tmp_dir, "temp_dest"),
        ]
        parser, _subparsers = setup_argparse()

        # --- run tests
        res = main(argv)
        assert not res

        # --- collect files producted, and those which should have been produced
        prefix = os.path.join(repo_dir, "temp_dest_verif")
        fns = [
            x.replace(prefix + "/", "", 1)
            for x in filter(
                lambda y: os.path.isfile(y), glob.glob(prefix + "/**/*", recursive=True)
            )
        ]
        prefix = os.path.join(tmp_dir, "temp_dest")
        fns = fns + [
            x.replace(prefix + "/", "", 1)
            for x in filter(
                lambda y: os.path.isfile(y), glob.glob(prefix + "/**/*", recursive=True)
            )
        ]
        fns = list(set(fns))

        matches, mismatches, errors = filecmp.cmpfiles(
            os.path.join(repo_dir, "temp_dest_verif"),
            os.path.join(tmp_dir, "temp_dest"),
            common=fns,
            shallow=False,
        )
        assert len(errors) == 0
        assert len(mismatches) == 0
