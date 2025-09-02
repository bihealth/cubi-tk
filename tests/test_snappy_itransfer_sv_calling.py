"""Tests for ``cubi_tk.snappy.itransfer_variant_calling``.

We only run some smoke tests here.
"""

import datetime
import os
import re
import textwrap
from unittest import mock
from unittest.mock import ANY, MagicMock, patch

from pyfakefs import fake_filesystem
import pytest

from cubi_tk.__main__ import main, setup_argparse
from cubi_tk.snappy.itransfer_sv_calling import (
    SnappyItransferSvCallingCommand,
    SnappyStepNotFoundException,
)
from cubi_tk.irods_common import TransferJob

from .conftest import my_exists, my_get_lz_info


def fake_config(n_tools=1):
    """Return configuration text"""
    head = textwrap.dedent(
        r"""
        static_data_config: {}

        step_config:
        """
    ).lstrip()

    tool1 = textwrap.dedent(
        r"""
          sv_calling_targeted:
            tools:
              - gcnv
              - manta
        dummy_line
        """
    ).removesuffix("dummy_line\n")

    tool2 = textwrap.dedent(
        r"""
          sv_calling_wgs:
            dna:
              tools:
                - gcnv
                - manta
        dummy_line
        """
    ).removesuffix("dummy_line\n")

    tail = textwrap.dedent(
        r"""
        data_sets:
          first_batch:
            sodar_uuid: 466ab946-ce6a-4c78-9981-19b79e7bbe86
            file: sheet.tsv
            search_patterns:
            - {'left': '*/*/*_R1.fastq.gz', 'right': '*/*/*_R2.fastq.gz'}
            search_paths: ['/path']
            type: germline_variants
            naming_scheme: only_secondary_id
        """
    )

    if n_tools == 0:
        return head.rstrip() + " {}\n" + tail
    if n_tools == 1:
        return head + tool1 + tail
    if n_tools == 2:
        return head + tool1 + tool2 + tail


def test_run_snappy_itransfer_sv_calling_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-sv-calling", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_snappy_itransfer_sv_calling_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-sv-calling"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


@patch("cubi_tk.snappy.itransfer_sv_calling.SnappyItransferSvCallingCommand._get_lz_info", my_get_lz_info)
def test_run_snappy_itransfer_sv_calling_no_sv_step(fs):
    fake_base_path = "/base/path"
    sodar_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    argv = [
        "--verbose",
        "snappy",
        "itransfer-sv-calling",
        "--base-path",
        fake_base_path,
        "--sodar-server-url",
        "https://sodar-staging.bihealth.org/",
        "--sodar-api-token",
        "XXXX",
        sodar_uuid,
    ]

    no_sv_config = fake_config(0)
    print(no_sv_config)
    fs.create_file(
        os.path.join(fake_base_path, ".snappy_pipeline/config.yaml"),
        contents=no_sv_config,
        create_missing_dirs=True,
    )

    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)
    with pytest.raises(SnappyStepNotFoundException):
        SnappyItransferSvCallingCommand(args)


@patch("cubi_tk.snappy.itransfer_sv_calling.SnappyItransferSvCallingCommand._get_lz_info", my_get_lz_info)
def test_run_snappy_itransfer_sv_calling_two_sv_steps(fs):
    fake_base_path = "/base/path"
    sodar_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    argv = [
        "--verbose",
        "snappy",
        "itransfer-sv-calling",
        "--base-path",
        fake_base_path,
        "--sodar-server-url",
        "https://sodar-staging.bihealth.org/",
        "--sodar-api-token",
        "XXXX",
        sodar_uuid,
    ]

    no_sv_config = fake_config(2)
    print(no_sv_config)
    fs.create_file(
        os.path.join(fake_base_path, ".snappy_pipeline/config.yaml"),
        contents=no_sv_config,
        create_missing_dirs=True,
    )

    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)
    with pytest.raises(SnappyStepNotFoundException):
        SnappyItransferSvCallingCommand(args)


@patch("cubi_tk.snappy.itransfer_common.iRODSTransfer")
def test_run_snappy_itransfer_sv_calling_smoke_test(mock_transfer, mocker, germline_trio_sheet_tsv):
    mock_transfer_obj = MagicMock()
    mock_transfer_obj.size = 1000
    mock_transfer_obj.put = MagicMock()
    mock_transfer.return_value = mock_transfer_obj

    fake_base_path = "/base/path"
    sodar_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    argv = [
        "--verbose",
        "snappy",
        "itransfer-sv-calling",
        "--base-path",
        fake_base_path,
        "--sodar-server-url",
        "https://sodar-staging.bihealth.org/",
        "--sodar-api-token",
        "XXXX",
        "--yes",
        # tsv_path,
        sodar_uuid,
    ]

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break both biomedsheets and multiprocessing.
    fs = fake_filesystem.FakeFilesystem()

    fake_file_paths = []
    for member in ("index",):
        for ext in ("", ".md5"):
            fake_file_paths.append(
                "%s/sv_calling_targeted/output/bwa_mem2.gcnv.%s-N1-DNA1-WES1/out/bwa_mem2.gcnv.%s-N1-DNA1-WES1.vcf.gz%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_file_paths.append(
                "%s/sv_calling_targeted/output/bwa_mem2.manta.%s-N1-DNA1-WES1/out/bwa_mem2.manta.%s-N1-DNA1-WES1.vcf.gz%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_file_paths.append(
                "%s/sv_calling_targeted/output/bwa_mem2.gcnv.%s-N1-DNA1-WES1/log/bwa_mem2.gcnv.%s-N1-DNA1-WES1.log%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
    # Create sample sheet in fake file system
    sample_sheet_path = fake_base_path + "/.snappy_pipeline/sheet.tsv"
    fs.create_file(sample_sheet_path, contents=germline_trio_sheet_tsv, create_missing_dirs=True)
    # Create config in fake file system
    config_path = fake_base_path + "/.snappy_pipeline/config.yaml"
    fs.create_file(config_path, contents=fake_config(), create_missing_dirs=True)

    # Print path to all created files
    print(fake_config())
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
                "sv_calling_targeted",
                today,
                f.split("-WES1/")[1],
            ),
        )
        for f in fake_file_paths
    ]
    expected_manta = tuple(sorted([t for t in expected_tfj if "manta" in t.path_local], key=lambda x: x.path_local))
    # expected_gcnv = list(
    #     sorted([t for t in expected_tfj if "gcnv" in t.path_local], key=lambda x: x.path_local)
    # )

    # Remove index's log MD5 file again so it is recreated.
    fs.remove(fake_file_paths[3])

    # Set Mocker
    mocker.patch("pathlib.Path.exists", my_exists)
    mocker.patch(
        "cubi_tk.sodar_common.SodarIngestBase._get_lz_info",
        my_get_lz_info,
    )

    fake_os = fake_filesystem.FakeOsModule(fs)
    mocker.patch("glob.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_common.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_variant_calling.os", fake_os)
    mocker.patch("cubi_tk.common.os", fake_os)

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_tk.snappy.itransfer_sv_calling.open", fake_open)
    mocker.patch("cubi_tk.snappy.itransfer_common.open", fake_open)
    mocker.patch("cubi_tk.snappy.common.open", fake_open)
    mocker.patch("cubi_tk.common.open", fake_open)

    mock_check_call = mock.mock_open()
    mocker.patch("cubi_tk.common.check_call", mock_check_call)

    mocker.patch("cubi_tk.snappy.itransfer_common.iRODSCommon.irods_hash_scheme", mock.MagicMock(return_value="MD5"))

    # Actually exercise code and perform test.
    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)
    res = main(argv)
    assert not res
    assert mock_transfer.call_count == 2
    # No easy way to check two calls
    # mock_transfer.assert_called_with(expected_gcnv, ask=not args.yes)
    mock_transfer.assert_called_with(expected_manta, ask=not args.yes, sodar_profile = "global")
    assert mock_transfer_obj.put.call_count == 2
    mock_transfer_obj.put.assert_called_with(recursive=True, sync=args.sync)

    assert fs.exists(fake_file_paths[3])
    assert mock_check_call.call_count == 1
    mock_check_call.assert_called_once_with(
        ["md5sum", "bwa_mem2.gcnv.index-N1-DNA1-WES1.vcf.gz"],
        cwd=os.path.dirname(fake_file_paths[3]),
        stdout=ANY,
    )
