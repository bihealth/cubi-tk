"""Tests for ``cubi_tk.archive.copy``.

We only run some smoke tests here.
"""

import glob
import os
import re

import filecmp
import pytest
import tempfile

from cubi_tk.__main__ import setup_argparse, main


ORIG_PATTERN = re.compile("^(%|#).*$")
COPY_PATTERN = re.compile("^(hashdeep| ).*$")


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


def _sort_hashdeep_title_and_body(filename, title):
    titles = []
    body = []
    with open(filename, "rt") as f:
        lines = [x.rstrip() for x in f.readlines()]
    for line in lines:
        line.rstrip()
        if title.match(line):
            titles.append(line)
        else:
            body.append(line)
    return (sorted(titles), sorted(body))


def test_run_archive_copy_smoke_test():
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
        setup_argparse()

        # --- run tests
        res = main(argv)
        assert not res

        prefix = os.path.join(repo_dir, "final_dest_verif")
        fns = [
            x.replace(prefix + "/", "", 1)
            for x in filter(os.path.isfile, glob.glob(prefix + "/**/*", recursive=True))
        ]
        prefix = os.path.join(tmp_dir, "final_dest")
        fns = fns + [
            x.replace(prefix + "/", "", 1)
            for x in filter(os.path.isfile, glob.glob(prefix + "/**/*", recursive=True))
        ]
        fns = list(set(fns))

        (repo_titles, repo_body) = _sort_hashdeep_title_and_body(
            os.path.join(repo_dir, "audit.orig"), ORIG_PATTERN
        )
        (tmp_titles, tmp_body) = _sort_hashdeep_title_and_body(
            os.path.join(tmp_dir, "audit.orig"), ORIG_PATTERN
        )
        assert repo_body == tmp_body

        (repo_titles, repo_body) = _sort_hashdeep_title_and_body(
            os.path.join(repo_dir, "audit.copy"), COPY_PATTERN
        )
        (tmp_titles, tmp_body) = _sort_hashdeep_title_and_body(
            os.path.join(tmp_dir, "audit.copy"), COPY_PATTERN
        )
        assert repo_titles == tmp_titles and repo_body == tmp_body

        _, mismatches, errors = filecmp.cmpfiles(
            os.path.join(repo_dir, "final_dest_verif"),
            os.path.join(tmp_dir, "final_dest"),
            common=fns,
            shallow=False,
        )
        assert len(errors) == 0
        assert len(mismatches) == 0
