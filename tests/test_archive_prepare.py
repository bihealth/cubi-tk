"""Tests for ``cubi_tk.archive.prepare``.

We only run some smoke tests here.
"""

import datetime
import glob
import os
import re
import tempfile

import filecmp
import pytest

from cubi_tk.__main__ import setup_argparse, main
from .test_archive_copy import sort_hashdeep_title_and_body


SNAKEMAKE = re.compile("^.*\\.snakemake\\.tar\\.gz$")
HASHDEEP = re.compile("^(([0-9]{4})-([0-9]{2})-([0-9]{2}))_hashdeep_report\\.txt$")


def test_run_archive_prepare_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "prepare", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_archive_prepare_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "prepare"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_archive_prepare_smoke_test():
    with tempfile.TemporaryDirectory() as tmp_dir:
        repo_dir = os.path.join(os.path.dirname(__file__), "data", "archive")
        project_name = "project"

        argv = [
            "archive",
            "prepare",
            "--rules",
            os.path.join(repo_dir, "rules.yaml"),
            "--readme",
            os.path.join(repo_dir, "temp_dest_verif", "README.md"),
            os.path.join(repo_dir, project_name),
            os.path.join(tmp_dir, "temp_dest"),
        ]
        setup_argparse()

        # --- run tests
        res = main(argv)
        assert not res

        # --- remove hashdeep report filename timestamp
        os.rename(
            os.path.join(
                tmp_dir, "temp_dest", datetime.date.today().strftime("%Y-%m-%d_hashdeep_report.txt")
            ),
            os.path.join(tmp_dir, "temp_dest", "1970-01-01_hashdeep_report.txt"),
        )

        # --- compare hashdeep report with reference
        (repo_titles, repo_body) = sort_hashdeep_title_and_body(
            os.path.join(repo_dir, "temp_dest_verif", "1970-01-01_hashdeep_report.txt")
        )
        (tmp_titles, tmp_body) = sort_hashdeep_title_and_body(
            os.path.join(tmp_dir, "temp_dest", "1970-01-01_hashdeep_report.txt")
        )
        # No test on gzipped files, timestamp stored on gzip format could be different
        assert repo_body == tmp_body

        prefix = os.path.join(repo_dir, "temp_dest_verif")
        ref_fns = [
            os.path.relpath(x, start=prefix)
            for x in filter(
                lambda x: os.path.isfile(x) or os.path.islink(x),
                glob.glob(prefix + "/**/*", recursive=True),
            )
        ]
        prefix = os.path.join(tmp_dir, "temp_dest")
        test_fns = [
            os.path.relpath(x, start=prefix)
            for x in filter(
                lambda x: os.path.isfile(x) or os.path.islink(x),
                glob.glob(prefix + "/**/*", recursive=True),
            )
        ]
        assert sorted(ref_fns) == sorted(test_fns)

        matches, mismatches, errors = filecmp.cmpfiles(
            os.path.join(repo_dir, "temp_dest_verif"),
            os.path.join(tmp_dir, "temp_dest"),
            common=ref_fns,
            shallow=False,
        )
        assert len(matches) > 0
        assert sorted(errors) == ["extra_data/to_ignored_dir", "extra_data/to_ignored_file"]
        assert sorted(mismatches) == [
            "1970-01-01_hashdeep_report.txt",
            "README.md",
            "pipeline/output/sample2",
        ]
