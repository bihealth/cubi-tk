"""Tests for ``cubi_tk.sea_snap.working_dir``.

We only run some smoke tests here.
"""

import os

import pytest
import time
from pathlib import Path

from cubi_tk.__main__ import setup_argparse, main


def test_run_seasnap_working_dir_help(capsys):
    parser, subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "working-dir", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_seasnap_working_dir_smoke_test(capsys, fs):
    # --- setup arguments
    seasnap_dir = "fake_seasnap"
    seasnap_files = [
        "mapping_config.yaml",
        "DE_config.yaml",
        "cluster_config.json",
        "mapping_pipeline.snake",
        "sea-snap.py",
    ]

    argv = ["sea-snap", "working-dir", seasnap_dir]

    parser, subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # --- add test files
    fs.create_dir(seasnap_dir)
    for f in seasnap_files:
        fs.create_file(os.path.join(seasnap_dir, f))

    # --- run tests
    res = main(argv)
    assert not res

    # test dir created
    wd = time.strftime(args.dirname)
    assert Path(wd).is_dir()

    # test files copied
    seasnap_files = seasnap_files[:3]
    for f in seasnap_files:
        p = os.path.join(wd, f)
        assert Path(p).is_file()

    # test symlink created
    p = os.path.join(wd, "sea-snap")
    assert Path(p).is_symlink()

    res = capsys.readouterr()
    assert not res.err
