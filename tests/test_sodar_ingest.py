"""Tests for ``cubi_tk.sodar.ingest``."""

from argparse import ArgumentParser
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from cubi_tk.__main__ import setup_argparse
from cubi_tk.sodar.ingest import SodarIngest


def test_run_sodar_ingest_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_sodar_ingest_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


@pytest.fixture
def fake_filesystem(fs):
    yield fs


@pytest.fixture
def ingest(fs):
    fs.create_dir(Path.home().joinpath(".irods"))
    fs.create_file(Path.home().joinpath(".irods", "irods_environment.json"))

    argv = ["--recursive", "testdir", "target"]

    parser = ArgumentParser()
    SodarIngest.setup_argparse(parser)
    args = parser.parse_args(argv)

    obj = SodarIngest(args)
    obj.lz_irods_path = "/irodsZone"
    obj.target_coll = "targetCollection"
    return obj


def test_sodar_ingest_build_file_list(fs, caplog):
    class DummyArgs(object):
        pass

    fs.create_symlink("/not_existing", "/broken_link")
    fs.create_symlink("/loop_src", "/loop_src2")
    fs.create_symlink("/loop_src2", "/loop_src")

    args = DummyArgs()
    args.sources = ["broken_link", "not_here", "loop_src", "testdir"]
    args.recursive = True
    dummy = MagicMock()
    args_mock = PropertyMock(return_value=args)
    type(dummy).args = args_mock

    fs.create_dir("/testdir/subdir")
    fs.create_file("/testdir/file1")
    fs.create_file("/testdir/file1.md5")
    fs.create_file("/testdir/subdir/file2")
    fs.create_file("/file3")
    fs.create_symlink("/testdir/file3", "/file3")

    paths = SodarIngest.build_file_list(dummy)

    # Sources
    assert "File not found: broken_link" in caplog.messages
    assert "File not found: not_here" in caplog.messages
    assert "Symlink loop: loop_src" in caplog.messages

    # Files
    assert {"spath": Path("/testdir/file1"), "ipath": Path("file1")} in paths
    assert {"spath": Path("/testdir/file1.md5"), "ipath": Path("file1.md5")} not in paths
    assert {"spath": Path("/testdir/subdir/file2"), "ipath": Path("subdir/file2")} in paths
    assert {"spath": Path("/testdir/file3"), "ipath": Path("file3")} in paths

    # Re-run without recursive search
    args.recursive = False
    paths = SodarIngest.build_file_list(dummy)
    assert {"spath": Path("/testdir/file1"), "ipath": Path("file1")} in paths
    assert {"spath": Path("/testdir/file1.md5"), "ipath": Path("file1.md5")} not in paths
    assert {"spath": Path("/testdir/subdir/file2"), "ipath": Path("subdir/file2")} not in paths
    assert {"spath": Path("/testdir/file3"), "ipath": Path("file3")} in paths


@patch("cubi_tk.sodar.ingest.sorted")
@patch("cubi_tk.sodar.ingest.compute_md5_checksum", return_value="5555")
@patch("pathlib.Path.stat")
@patch("cubi_tk.sodar.ingest.TransferJob")
def test_sodar_ingest_build_jobs(mockjob, mockstats, mockmd5, mocksorted, ingest):
    paths = [
        {"spath": Path("myfile.csv"), "ipath": Path("dest_dir/myfile.csv")},
        {"spath": Path("folder/file.csv"), "ipath": Path("dest_dir/folder/file.csv")},
    ]
    mockstats().st_size = 1024

    ingest.build_jobs(paths)
    for p in paths:
        mockjob.assert_any_call(
            path_src=str(p["spath"]),
            path_dest=f"{ingest.lz_irods_path}/{ingest.target_coll}/{str(p['ipath'])}",
            bytes=1024,
            md5="5555",
        )
