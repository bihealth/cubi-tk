"""Tests for ``cubi_tk.snappy.itransfer_ngs_mapping``.

We only run some smoke tests here.
"""

import datetime
import os
import re
from unittest import mock
from unittest.mock import ANY, MagicMock, patch

from pyfakefs import fake_filesystem
import pytest

from cubi_tk.__main__ import main, setup_argparse
from cubi_tk.irods_common import TransferJob

from .conftest import my_exists, my_get_sodar_info


def test_run_snappy_itransfer_ngs_mapping_help(capsys):
    parser, subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-ngs-mapping", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_snappy_itransfer_ngs_mapping_nothing(capsys):
    parser, subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-ngs-mapping"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


@patch("cubi_tk.snappy.itransfer_common.iRODSTransfer")
def test_run_snappy_itransfer_ngs_mapping_smoke_test(
    mock_transfer, mocker, germline_trio_sheet_tsv, minimal_config
):
    mock_transfer_obj = MagicMock()
    mock_transfer_obj.size = 1000
    mock_transfer_obj.put = MagicMock()
    mock_transfer.return_value = mock_transfer_obj

    fake_base_path = "/base/path"
    sodar_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    argv = [
        "--verbose",
        "snappy",
        "itransfer-ngs-mapping",
        "--base-path",
        fake_base_path,
        "--sodar-server-url",
        "https://sodar.bihealth.org/",
        "--sodar-api-token",
        "XXXX",
        sodar_uuid,
    ]

    parser, subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break biomedsheets.
    fs = fake_filesystem.FakeFilesystem()

    fake_file_paths = []
    for member in ("index", "father", "mother"):
        for ext in ("", ".md5"):
            fake_file_paths.append(
                "%s/ngs_mapping/output/bwa.%s-N1-DNA1-WES1/out/%s-N1-DNA1-WES1.bam%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_file_paths.append(
                "%s/ngs_mapping/output/bwa.%s-N1-DNA1-WES1/log/bwa.%s-N1-DNA1-WES1.log%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])

    # Create sample sheet in fake file system
    sample_sheet_path = fake_base_path + "/.snappy_pipeline/sheet.tsv"
    fs.create_file(sample_sheet_path, contents=germline_trio_sheet_tsv, create_missing_dirs=True)
    # Create config in fake file system
    config_path = fake_base_path + "/.snappy_pipeline/config.yaml"
    fs.create_file(config_path, contents=minimal_config, create_missing_dirs=True)

    # Print path to all created files
    print("\n".join(fake_file_paths + [sample_sheet_path, config_path]))

    # Create expected transfer jobs
    today = datetime.date.today().strftime("%Y-%m-%d")
    sample_name_pattern = re.compile("[^-./]+-N1-DNA1-WES1")
    expected_tfj = [
        TransferJob(
            path_local=f,
            path_remote=os.path.join(
                "/irods/dest",
                re.findall(sample_name_pattern, f)[0],
                "ngs_mapping",
                today,
                f.split("-WES1/")[1],
            ),
        )
        for f in fake_file_paths
    ]
    expected_tfj = sorted(expected_tfj, key=lambda x: x.path_local)

    # Remove index's log MD5 file again so it is recreated.
    fs.remove(fake_file_paths[3])

    # Set Mocker
    mocker.patch("pathlib.Path.exists", my_exists)
    mocker.patch(
        "cubi_tk.snappy.itransfer_common.SnappyItransferCommandBase.get_sodar_info",
        my_get_sodar_info,
    )

    fake_os = fake_filesystem.FakeOsModule(fs)
    mocker.patch("glob.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_common.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_ngs_mapping.os", fake_os)

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_tk.snappy.itransfer_common.open", fake_open)
    mocker.patch("cubi_tk.snappy.common.open", fake_open)

    mock_check_call = mock.mock_open()
    mocker.patch("cubi_tk.snappy.itransfer_common.check_call", mock_check_call)

    # Actually exercise code and perform test.
    res = main(argv)
    assert not res
    mock_transfer.assert_called_with(expected_tfj, ask=not args.yes)
    mock_transfer_obj.put.assert_called_with(recursive=True, sync=args.overwrite_remote)

    assert fs.exists(fake_file_paths[3])
    assert mock_check_call.call_count == 1
    mock_check_call.assert_called_once_with(
        ["md5sum", "bwa.index-N1-DNA1-WES1.log"],
        cwd=os.path.dirname(fake_file_paths[3]),
        stdout=ANY,
    )
