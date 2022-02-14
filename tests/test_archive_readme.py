"""Tests for ``cubi_tk.archive.prepare``.

We only run some smoke tests here.
"""

import os
import tempfile

import pytest

from cubi_tk.__main__ import setup_argparse, main

import cubi_tk.archive.readme


def test_run_archive_readme_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "readme", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_archive_readme_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "readme"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_archive_readme_smoke_test():
    cubi_tk.archive.readme.NO_INPUT = True

    with tempfile.TemporaryDirectory() as tmp_dir:
        project_name = "project"
        project_dir = os.path.join(os.path.dirname(__file__), "data", "archive", project_name)

        readme_path = os.path.join(tmp_dir, project_name, "README.md")

        argv = [
            "--sodar-server-url",
            "https://sodar.bihealth.,org",
            "archive",
            "readme",
            "--var-PI-name",
            "Maxene Musterfrau",
            "--var-archiver-name",
            "Eric Blanc",
            "--var-client-name",
            "Max Mustermann",
            "--var-SODAR-UUID",
            "00000000-0000-0000-0000-000000000000",
            "--var-Gitlab-URL",
            "https://cubi-gitlab.bihealth.org",
            "--var-start-date",
            "1970-01-01",
            project_dir,
            readme_path,
        ]
        setup_argparse()

        # --- run tests
        res = main(argv)
        assert not res

        assert cubi_tk.archive.readme.is_readme_valid(readme_path)
