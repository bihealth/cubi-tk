"""Tests for ``cubi_tk.sodar.ingest``."""

from pathlib import Path
from unittest.mock import Mock, patch

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
def ingest():
    obj = SodarIngest(args={"sources": "testfolder", "recursive": True})
    obj.lz_irods_path = "/irodsZone"
    obj.target_coll = "targetCollection"
    return obj


@patch("cubi_tk.sodar.ingest.sorted")
@patch("cubi_tk.sodar.ingest.compute_md5_checksum", return_value="5555")
@patch("cubi_tk.sodar.ingest.Path.stat")
@patch("cubi_tk.sodar.ingest.TransferJob")
def test_sodar_ingest_build_jobs(mockjob, mockstats, mockmd5, mocksorted, ingest):
    paths = [
        {"spath": Path("myfile.csv"), "ipath": Path("dest_dir/myfile.csv")},
        {"spath": Path("folder/file.csv"), "ipath": Path("dest_dir/folder/file.csv")},
    ]
    mockstats.return_value = Mock(st_size=1024)

    ingest.build_jobs(paths)
    print(mockjob.call_args_list)
    for p in paths:
        mockjob.assert_any_call(
            path_src=str(p["spath"]),
            path_dest=f"{ingest.lz_irods_path}/{ingest.target_coll}/{str(p['ipath'])}",
            bytes=1024,
            md5="5555",
        )

