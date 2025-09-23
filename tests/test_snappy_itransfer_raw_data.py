"""Tests for ``cubi_tk.snappy.itransfer_raw_data``.

We only run some smoke tests here.
"""

import datetime
import os
import re
from unittest.mock import MagicMock, patch

from pyfakefs import fake_filesystem
import pytest

from cubi_tk.__main__ import main, setup_argparse
from cubi_tk.irods_common import TransferJob

from .conftest import my_get_lz_info, my_iRODS_transfer, setup_snappy_itransfer_mocks


def test_run_snappy_itransfer_raw_data_help(capsys):
    parser, subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-raw-data", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_snappy_itransfer_raw_data_nothing(capsys):
    parser, subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-raw-data"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


@patch("cubi_tk.snappy.itransfer_raw_data.SnappyItransferRawDataCommand._no_files_found_warning")
@patch(
    "cubi_tk.snappy.itransfer_raw_data.SnappyItransferRawDataCommand._get_lz_info", my_get_lz_info
)
@patch("cubi_tk.sodar_common.iRODSTransfer")
def test_run_snappy_itransfer_raw_data_smoke_test(
    mock_transfer, mock_filecheck, mocker, minimal_config, germline_trio_sheet_tsv
):
    # Setup transfer mock, for assertion
    mock_transfer_obj = my_iRODS_transfer()
    mock_transfer.return_value = mock_transfer_obj
    # Set up mock for _no_files_found_warning, allows asserting it was called with properly built transfer_job list
    mock_filecheck.return_value = 0

    # Mock check_call for md5sum creation, allows assertion of call count
    mock_check_call = MagicMock(return_value=0)
    mocker.patch("cubi_tk.common.check_call", mock_check_call)

    # Set up command line arguments
    fake_base_path = "/base/path"
    sodar_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    argv = [
        "snappy",
        "itransfer-raw-data",
        "--base-path",
        fake_base_path,
        "--sodar-server-url",
        "https://sodar-staging.bihealth.org/",
        "--sodar-api-token",
        "XXXX",
        "--parallel-checksum-jobs",
        "0",
        "--yes",
        sodar_uuid,
    ]

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break both biomedsheets and multiprocessing.
    fs = fake_filesystem.FakeFilesystem()

    fake_file_paths = []
    for member in ("index", "father", "mother"):
        for ext in ("", ".md5"):
            fake_file_paths.append(
                "%s/ngs_mapping/work/input_links/%s-N1-DNA1-WES1/%s-N1-DNA1-WES1.fastq.gz%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])

    # Create sample sheet in fake file system
    sample_sheet_path = fake_base_path + "/.snappy_pipeline/sheet.tsv"
    fs.create_file(sample_sheet_path, contents=germline_trio_sheet_tsv)
    # Create config in fake file system
    config_path = fake_base_path + "/.snappy_pipeline/config.yaml"
    fs.create_file(config_path, contents=minimal_config)

    # Create expected transfer jobs
    today = datetime.date.today().strftime("%Y-%m-%d")
    sample_name_pattern = re.compile("[^-./]+-N1-DNA1-WES1")
    expected_tfj = [
        TransferJob(
            path_local=f,
            path_remote=os.path.join(
                "/irods/dest",
                re.findall(sample_name_pattern, f)[0],
                "raw_data",
                today,
                f.split("-WES1/")[1],
            ),
        )
        for f in fake_file_paths
    ]
    expected_tfj = sorted(expected_tfj, key=lambda x: x.path_local)

    # Set Mocker
    setup_snappy_itransfer_mocks(mocker, fs, "raw_data")

    # Actually exercise code and perform test.
    parser, subparsers = setup_argparse()
    args = parser.parse_args(argv)

    res = main(argv)
    assert not res
    mock_filecheck.assert_called_with(expected_tfj)
    mock_transfer_obj.put.assert_called_with(recursive=True, overwrite=args.overwrite)
