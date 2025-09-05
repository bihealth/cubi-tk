"""Tests for ``cubi_tk.sea_snap.itransfer_results``.

We only run some smoke tests here.
"""

import datetime
import os
from unittest.mock import ANY, patch, MagicMock

import pytest

from cubi_tk.__main__ import main, setup_argparse
from cubi_tk.irods_common import TransferJob

from .conftest import my_get_lz_info, my_iRODS_transfer

def test_run_seasnap_itransfer_results_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "itransfer-results", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_seasnap_itransfer_results_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "itransfer-results"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


@patch("cubi_tk.common.check_call")
@patch('cubi_tk.sea_snap.itransfer_results.SeasnapItransferMappingResultsCommand._no_files_found_warning')
@patch("cubi_tk.sea_snap.itransfer_results.SeasnapItransferMappingResultsCommand._get_lz_info", my_get_lz_info)
@patch("cubi_tk.sodar_common.iRODSTransfer")
@patch("cubi_tk.common.Value", MagicMock())
def test_run_seasnap_itransfer_results_smoke_test(mock_transfer, mock_filecheck, mock_check_call, fs):
    # Setup transfer mock, for assertion
    mock_transfer_obj = my_iRODS_transfer()
    mock_transfer.return_value = mock_transfer_obj
    # Set up mock for _no_files_found_warning, allows asserting it was called with properly built transfer_job list
    mock_filecheck.return_value = 0
    # Mock check_call for md5sum creation, allows assertion of call count
    mock_check_call.return_value = 0

    # --- setup arguments
    dest_path = "/irods/dest"
    sodar_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    fake_base_path = "/base/path"
    blueprint_path = os.path.join(os.path.dirname(__file__), "data", "test_blueprint.txt")

    argv = [
        "sea-snap",
        "itransfer-results",
        "--verbose",
        "--parallel-checksum-jobs",
        "0",
        "--sodar-server-url",
        "https://sodar-staging.bihealth.org/",
        "--sodar-api-token",
        "XXXX",
        blueprint_path,
        sodar_uuid,
    ]

    # --- add test files
    fake_file_paths = []
    for member in ("sample1", "sample2", "sample3"):
        for ext in ("", ".md5"):
            fake_file_paths.append(
                "%s/mapping/star/%s/out/star.%s-N1-RNA1-RNA-Seq1.bam%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_file_paths.append(
                "%s/mapping/star/%s/report/star.%s-N1-RNA1-RNA-Seq1.log%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])

    # Add blueprint file, and update the (copied) mtime, so it is always "newer" than the fake test files
    bp_file = fs.add_real_file(blueprint_path)
    bp_file.st_mtime = datetime.datetime.now().timestamp()

    # Remove index's log MD5 file again so it is recreated.
    fs.remove(fake_file_paths[3])

    # Create expected transfer jobs
    expected_tfj = [
        TransferJob(
            path_local=f,
            path_remote=os.path.join(
                dest_path,
                # test_blueprint.txt has always the same destination path
                'fakedest' + ('.md5' if f.endswith('.md5') else ''),
            ),

        )
        for f in fake_file_paths
    ]
    expected_tfj = sorted(expected_tfj, key=lambda x: x.path_local)

    # --- run tests
    res = main(argv)
    assert not res

    # Expected jobs (itransfer_common will always add the md5 jobs as well)
    mock_filecheck.assert_called_with(expected_tfj)
    mock_transfer_obj.put.assert_called_with(recursive=True, sync=False)

    # Check that the missing md5 file was created
    assert fs.exists(fake_file_paths[3])
    assert mock_check_call.call_count == 1
    mock_check_call.assert_called_once_with(
        ["md5sum", "star.sample1-N1-RNA1-RNA-Seq1.log"],
        cwd=os.path.dirname(fake_file_paths[3]),
        stdout=ANY,
    )

    assert fs.exists(fake_file_paths[3])

