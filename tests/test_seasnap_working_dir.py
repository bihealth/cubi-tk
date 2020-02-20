"""Tests for ``cubi_sak.sea_snap.working_dir``.

We only run some smoke tests here.
"""

import os

import pytest
import linecache
import tokenize
import time
from pyfakefs import fake_pathlib, fake_filesystem_shutil
from pyfakefs.fake_filesystem_unittest import Patcher

from cubi_sak.__main__ import setup_argparse, main


def test_run_seasnap_working_dir_help(capsys):
    parser, subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "working-dir", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


@pytest.fixture
def fs_reload_sut():
    patcher = Patcher(modules_to_reload=[setup_argparse, main])
    patcher.setUp()
    linecache.open = patcher.original_open
    tokenize._builtin_open = patcher.original_open
    yield patcher.fs
    patcher.tearDown()


def test_run_seasnap_working_dir_smoke_test(tmp_path, requests_mock, capsys, mocker, fs_reload_sut):
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
    fs = fs_reload_sut

    fs.create_dir(seasnap_dir)
    for f in seasnap_files:
        fs.create_file(os.path.join(seasnap_dir, f))

    # --- setup mocks
    fake_pathl = fake_pathlib.FakePathlibModule(fs)
    mocker.patch("pathlib.Path", fake_pathl.Path)

    fake_shutil = fake_filesystem_shutil.FakeShutilModule(fs)
    mocker.patch("cubi_sak.sea_snap.working_dir.shutil", fake_shutil)

    # --- run tests
    res = main(argv)
    assert not res

    # test dir created
    wd = time.strftime(args.dirname)
    assert fake_pathl.Path(wd).is_dir()

    # test files copied
    seasnap_files = seasnap_files[:3]
    for f in seasnap_files:
        p = os.path.join(wd, f)
        assert fake_pathl.Path(p).is_file()

    # test symlink created
    p = os.path.join(wd, "sea-snap")
    assert fake_pathl.Path(p).is_symlink()

    res = capsys.readouterr()
    assert not res.err
