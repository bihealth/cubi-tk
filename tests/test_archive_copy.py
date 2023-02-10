"""Tests for ``cubi_tk.archive.copy``.

We only run some smoke tests here.
"""

import datetime
import filecmp
import glob
import os
import re
import tempfile

import pytest

from cubi_tk.__main__ import main, setup_argparse

HASHDEEP_TITLES_PATTERN = re.compile("^(%|#).*$")
IGNORE_FILES_PATTERN = re.compile("^(.*/)?(hashdeep|workdir)_(report|audit)\\.txt$")
IGNORE_LINES_PATTERN = re.compile(
    "^.+,(.*/)?(\\.snakemake\\.tar\\.gz|1970-01-01_hashdeep_report\\.txt)$"
)


def test_run_archive_copy_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "copy", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_archive_copy_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["archive", "copy"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def sort_hashdeep_title_and_body(filename):
    titles = []
    body = []
    with open(filename, "rt") as f:
        lines = [x.rstrip() for x in f.readlines()]
    for line in lines:
        line.rstrip()
        if HASHDEEP_TITLES_PATTERN.match(line):
            titles.append(line)
        else:
            if not IGNORE_LINES_PATTERN.match(line):
                body.append(line)
    return (sorted(titles), sorted(body))


def test_run_archive_copy_smoke_test(mocker):
    with tempfile.TemporaryDirectory() as tmp_dir:
        repo_dir = os.path.join(os.path.dirname(__file__), "data", "archive")

        argv = [
            "archive",
            "copy",
            "--keep-workdir-hashdeep",
            os.path.join(repo_dir, "temp_dest_verif"),
            os.path.join(tmp_dir, "final_dest"),
        ]
        setup_argparse()

        # --- run tests
        res = main(argv)
        assert res == 0

        # --- remove timestamps on all hashdeep reports & audits
        now = datetime.date.today().strftime("%Y-%m-%d")
        prefix = os.path.join(tmp_dir, "final_dest")
        for fn in ["hashdeep_audit", "workdir_report", "workdir_audit"]:
            from_fn = "{}_{}.txt".format(now, fn)
            to_fn = "{}.txt".format(fn)
            os.rename(os.path.join(prefix, from_fn), os.path.join(prefix, to_fn))

        # --- check report
        (repo_titles, repo_body) = sort_hashdeep_title_and_body(
            os.path.join(repo_dir, "final_dest_verif", "workdir_report.txt")
        )
        (tmp_titles, tmp_body) = sort_hashdeep_title_and_body(
            os.path.join(tmp_dir, "final_dest", "workdir_report.txt")
        )

        # --- check audits
        for fn in ["hashdeep_audit", "workdir_audit"]:
            with open(os.path.join(repo_dir, "final_dest_verif", fn + ".txt"), "r") as f:
                repo = sorted(f.readlines())
            with open(os.path.join(tmp_dir, "final_dest", fn + ".txt"), "r") as f:
                tmp = sorted(f.readlines())
            assert repo == tmp

        # --- test all copied files, except the hashdeep report & audit, that can differ by line order
        prefix = os.path.join(repo_dir, "final_dest_verif")
        ref_fns = [
            os.path.relpath(x, start=prefix)
            for x in filter(
                lambda x: os.path.isfile(x) or os.path.islink(x),
                glob.glob(prefix + "/**/*", recursive=True),
            )
        ]
        ref_fns = filter(lambda x: not IGNORE_FILES_PATTERN.match(x), ref_fns)
        prefix = os.path.join(tmp_dir, "final_dest")
        test_fns = [
            os.path.relpath(x, start=prefix)
            for x in filter(
                lambda x: os.path.isfile(x) or os.path.islink(x),
                glob.glob(prefix + "/**/*", recursive=True),
            )
        ]
        test_fns = filter(lambda x: not IGNORE_FILES_PATTERN.match(x), test_fns)

        matches, mismatches, errors = filecmp.cmpfiles(
            os.path.join(repo_dir, "final_dest_verif"),
            os.path.join(tmp_dir, "final_dest"),
            common=ref_fns,
            shallow=False,
        )
        assert len(matches) > 0
        assert sorted(errors) == ["extra_data/to_ignored_dir", "extra_data/to_ignored_file"]
        assert sorted(mismatches) == ["pipeline/output/sample2"]

        assert os.path.exists(os.path.join(tmp_dir, "final_dest", "archive_copy_complete"))
