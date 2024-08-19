"""Tests for ``cubi_tk.sodar.ingest_fastq``.

We only run some smoke tests here.
"""

import json
import os
import re
import unittest
from unittest import mock
from unittest.mock import patch

from pyfakefs import fake_filesystem, fake_pathlib
import pytest

from cubi_tk.__main__ import main, setup_argparse
from cubi_tk.exceptions import ParameterException
from cubi_tk.sodar.ingest_fastq import SodarIngestFastq

from .conftest import my_get_sodar_info, my_sodar_api_export
from .factories import InvestigationFactory


def test_run_sodar_ingest_fastq_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest-fastq", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_sodar_ingest_fastq_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest-fastq"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_sodar_ingest_fastq_preset_definitions():
    from cubi_tk.sodar.ingest_fastq import DEST_PATTERN_PRESETS, SRC_REGEX_PRESETS

    regexes = SRC_REGEX_PRESETS.keys()
    patterns = DEST_PATTERN_PRESETS.keys()

    # Check that all presets exits for both regex & pattern
    assert sorted(regexes) == sorted(patterns)

    # Check that all presets are not empty
    for preset in regexes:
        assert SRC_REGEX_PRESETS[preset]
        assert DEST_PATTERN_PRESETS[preset]


def test_run_sodar_ingest_fastq_default_preset_regex():
    from cubi_tk.sodar.ingest_fastq import SRC_REGEX_PRESETS

    ## Test default regex
    # Collection of example filenames and the expected {sample} value the regex should capture
    test_filenames = {
        "Sample1-N1-RNA1-RNA_seq1.fastq.gz": "Sample1-N1-RNA1-RNA_seq1",
        "P1234_Samplename_S14_L006_R2_001.fastq.gz": "P1234_Samplename",
        "P1234_Samplename2_R1.fastq.gz": "P1234_Samplename2",
    }
    for test_filename, expected_sample in test_filenames.items():
        res = re.match(SRC_REGEX_PRESETS["default"], test_filename)
        assert res is not None
        assert res.groupdict()["sample"] == expected_sample


def test_run_sodar_ingest_fastq_ont_preset_regex():
    from cubi_tk.sodar.ingest_fastq import SRC_REGEX_PRESETS

    test_filenames = {
        "fake_base_path/20240101_A0000_sample1/20240101_0000_A1_AB12345_000xyz/bam_fail/"
        "AB12345_000xyz_pass_1c1234_0.bam": (
            "A0000_sample1",
            "20240101_0000_A1_AB12345_000xyz",
            "bam_fail/",
        ),
        "fake_base_path/20240101_A0000_sample1/20240101_0000_A1_AB12345_000xyz/"
        "final_summary_AB12345_000xyz_1c1234_0.txt": (
            "A0000_sample1",
            "20240101_0000_A1_AB12345_000xyz",
            None,
        ),
    }
    for test_filename, expected_res in test_filenames.items():
        res = re.match(SRC_REGEX_PRESETS["ONT"], test_filename)
        assert res is not None
        groups = res.groupdict()
        assert groups["sample"] == expected_res[0]
        assert groups["RunID"] == expected_res[1]
        if expected_res[2]:
            assert groups["subfolder"] == expected_res[2]
        else:
            assert groups["subfolder"] is None


@patch("cubi_tk.sodar.ingest_fastq.api.samplesheet.retrieve")
@patch("cubi_tk.sodar.ingest_fastq.api.samplesheet.export")
def test_run_sodar_ingest_fastq_get_match_to_collection_mapping(mock_api_export, mock_api_retrieve):
    # Patched sodar API call
    mock_api_export.return_value = my_sodar_api_export()

    # Instantiate SodarIngestFastq (seems to require args?)
    landing_zone_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    project_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    fake_base_path = "/base/path"
    argv = [
        "--verbose",
        "sodar",
        "ingest-fastq",
        "--num-parallel-transfers",
        "0",
        "--sodar-api-token",
        "XXXX",
        "--yes",
        fake_base_path,
        landing_zone_uuid,
    ]

    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)
    ingestfastq = SodarIngestFastq(args)

    # test to get expected dict
    expected = {
        "Folder1": "Sample1-N1-DNA1-WES1",
        "Folder2": "Sample2-N1-DNA1-WES1",
        "Folder3": "Sample3-N1-DNA1-WES1",
    }

    assert expected == ingestfastq.get_match_to_collection_mapping(project_uuid, "Folder name")
    assert expected == ingestfastq.get_match_to_collection_mapping(
        project_uuid, "Folder name", "Library Name"
    )

    # Test for alternative collection column
    expected2 = {
        "Folder1": "Sample1-N1-DNA1",
        "Folder2": "Sample2-N1-DNA1",
        "Folder3": "Sample3-N1-DNA1",
    }
    assert expected2 == ingestfastq.get_match_to_collection_mapping(
        project_uuid, "Folder name", "Extract Name"
    )

    # Test for missing column
    with unittest.TestCase.assertRaises(unittest.TestCase, ParameterException):
        ingestfastq.get_match_to_collection_mapping(project_uuid, "Typo-Column")

    # Test with additional assay
    mock_api_export.return_value = my_sodar_api_export(2)
    mock_api_retrieve.return_value = InvestigationFactory()
    assay_uuid = list(mock_api_retrieve.return_value.studies["s_Study_0"].assays.keys())[0]
    ingestfastq.args.assay = assay_uuid

    assert expected == ingestfastq.get_match_to_collection_mapping(project_uuid, "Folder name")


def test_run_sodar_ingest_fastq_smoke_test(mocker, requests_mock):
    # --- setup arguments
    irods_path = "/irods/dest"
    landing_zone_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    # dest_path = "target/folder/"
    fake_base_path = "/base/path"
    argv = [
        "--verbose",
        "sodar",
        "ingest-fastq",
        "--num-parallel-transfers",
        "0",
        "--sodar-api-token",
        "XXXX",
        "--yes",
        # "--remote-dir-pattern",
        # dest_path,
        fake_base_path,
        landing_zone_uuid,
    ]

    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break biomedsheets.
    fs = fake_filesystem.FakeFilesystem()
    fake_os = fake_filesystem.FakeOsModule(fs)
    fake_pl = fake_pathlib.FakePathlibModule(fs)

    # --- add test files
    fake_file_paths = []
    for member in ("sample1", "sample2", "sample3"):
        for ext in ("", ".md5"):
            fake_file_paths.append(
                "%s/%s/%s-N1-RNA1-RNA_seq1.fastq.gz%s" % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_file_paths.append(
                "%s/%s/%s-N1-DNA1-WES1.fq.gz%s" % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])

    # Remove index's log MD5 file again so it is recreated.
    fs.remove(fake_file_paths[3])

    # --- mock modules
    mocker.patch("glob.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_common.os", fake_os)
    mocker.patch(
        "cubi_tk.snappy.itransfer_common.SnappyItransferCommandBase.get_sodar_info",
        my_get_sodar_info,
    )

    mock_check_output = mock.MagicMock(return_value=0)
    mocker.patch("cubi_tk.irods_common.iRODSTransfer.put", mock_check_output)

    mock_check_call = mock.MagicMock(return_value=0)
    mocker.patch("cubi_tk.snappy.itransfer_common.check_call", mock_check_call)
    mocker.patch("cubi_tk.sodar.ingest_fastq.check_call", mock_check_call)

    mocker.patch("cubi_tk.sodar.ingest_fastq.pathlib", fake_pl)
    mocker.patch("cubi_tk.sodar.ingest_fastq.os", fake_os)

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_tk.snappy.itransfer_common.open", fake_open)
    mocker.patch("cubi_tk.sodar.ingest_fastq.open", fake_open)

    # necessary because independent test fail
    mock_value = mock.MagicMock()
    mocker.patch("cubi_tk.sodar.ingest_fastq.Value", mock_value)
    mocker.patch("cubi_tk.snappy.itransfer_common.Value", mock_value)

    # requests mock
    return_value = dict(
        assay="",
        config_data="",
        configuration="",
        date_modified="",
        description="",
        irods_path=irods_path,
        project="",
        sodar_uuid="",
        status="",
        status_info="",
        title="",
        user=dict(sodar_uuid="", username="", name="", email=""),
    )
    url = os.path.join(args.sodar_url, "landingzones", "api", "retrieve", args.destination)
    requests_mock.register_uri("GET", url, text=json.dumps(return_value))

    # --- run tests
    res = main(argv)

    assert not res

    assert mock_check_call.call_count == 1
    assert mock_check_call.call_args[0] == (["md5sum", "sample1-N1-DNA1-WES1.fq.gz"],)

    # The upload logic for multiple transfers/files has been moved into the iRODScommon classes
    # We just need one call to that here
    assert mock_check_output.call_count == 1

    # Test that the TransferJob contain all files (setting the remote_dest with this mock setup does not work)
    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)
    ingestfastq = SodarIngestFastq(args)
    lz, actual = ingestfastq.build_jobs()
    assert len(actual) == len(fake_file_paths)


def test_run_sodar_ingest_fastq_smoke_test_ont_preset(mocker, requests_mock):
    # --- setup arguments
    irods_path = "/irods/dest"
    landing_zone_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    # dest_path = "target/folder/"
    fake_base_path = "/base/path"
    argv = [
        "--verbose",
        "sodar",
        "ingest-fastq",
        "--num-parallel-transfers",
        "0",
        "--sodar-api-token",
        "XXXX",
        "--yes",
        "--preset",
        "ONT",
        # "--remote-dir-pattern",
        # dest_path,
        fake_base_path,
        landing_zone_uuid,
    ]

    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break biomedsheets.
    fs = fake_filesystem.FakeFilesystem()
    fake_os = fake_filesystem.FakeOsModule(fs)
    fake_pl = fake_pathlib.FakePathlibModule(fs)

    # --- add test files
    fake_file_paths = []
    date = "20240101"
    project_sample_id = "A0000_sample{n}"
    for sample_n in (1, 2, 3):
        sample_path = project_sample_id.format(n=sample_n)
        # date _ time _ positions _ ID _ hash
        flowcell_id_hash = "AB12345_000xyz"
        flowcellrun = f"{date}_0000_1A_{flowcell_id_hash}"
        for file_pattern in (
            f"{fake_base_path}/{date}_{sample_path}/{flowcellrun}/bam_fail/{flowcell_id_hash}_pass_{sample_n}c1234_0.bam",
            f"{fake_base_path}/{date}_{sample_path}/{flowcellrun}/bam_pass/{flowcell_id_hash}_fail_{sample_n}c1234_0.bam",
            f"{fake_base_path}/{date}_{sample_path}/{flowcellrun}/final_summary_{flowcell_id_hash}_{sample_n}c1234_0.txt",
            f"{fake_base_path}/{date}_{sample_path}/{flowcellrun}/pod5/{flowcell_id_hash}_{sample_n}c1234_0.pod5",
            f"{fake_base_path}/{date}_{sample_path}/{flowcellrun}/report_{flowcell_id_hash}.html",
            f"{fake_base_path}/{date}_{sample_path}/{flowcellrun}/report_{flowcell_id_hash}.json",
            f"{fake_base_path}/{date}_{sample_path}/{flowcellrun}/sequencing_summary_{flowcell_id_hash}_{sample_n}c1234_0.txt",
        ):
            for ext in ("", ".md5"):
                fake_file_paths.append(file_pattern + ext)
                fs.create_file(fake_file_paths[-1])

    # Remove MD5 file for sample 1 fail bam, so it is recreated.
    fs.remove(fake_file_paths[3])

    # --- mock modules
    mocker.patch("glob.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_common.os", fake_os)
    mocker.patch(
        "cubi_tk.snappy.itransfer_common.SnappyItransferCommandBase.get_sodar_info",
        my_get_sodar_info,
    )

    mock_check_output = mock.MagicMock(return_value=0)
    mocker.patch("cubi_tk.irods_common.iRODSTransfer.put", mock_check_output)

    mock_check_call = mock.MagicMock(return_value=0)
    mocker.patch("cubi_tk.snappy.itransfer_common.check_call", mock_check_call)
    mocker.patch("cubi_tk.sodar.ingest_fastq.check_call", mock_check_call)

    mocker.patch("cubi_tk.sodar.ingest_fastq.pathlib", fake_pl)
    mocker.patch("cubi_tk.sodar.ingest_fastq.os", fake_os)

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_tk.snappy.itransfer_common.open", fake_open)
    mocker.patch("cubi_tk.sodar.ingest_fastq.open", fake_open)

    # necessary because independent test fail
    mock_value = mock.MagicMock()
    mocker.patch("cubi_tk.sodar.ingest_fastq.Value", mock_value)
    mocker.patch("cubi_tk.snappy.itransfer_common.Value", mock_value)

    # requests mock
    return_value = dict(
        assay="",
        config_data="",
        configuration="",
        date_modified="",
        description="",
        irods_path=irods_path,
        project="",
        sodar_uuid="",
        status="",
        status_info="",
        title="",
        user=dict(sodar_uuid="", username="", name="", email=""),
    )
    url = os.path.join(args.sodar_url, "landingzones", "api", "retrieve", args.destination)
    requests_mock.register_uri("GET", url, text=json.dumps(return_value))

    # --- run tests
    res = main(argv)

    assert not res

    assert mock_check_call.call_count == 1
    assert mock_check_call.call_args[0] == (["md5sum", "AB12345_000xyz_fail_1c1234_0.bam"],)

    # The upload logic for multiple transfers/files has been moved into the iRODScommon classes
    # We just need one call to that here
    assert mock_check_output.call_count == 1

    # Test that the TransferJob contain all files, except html (3x2 for md5s)
    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)
    ingestfastq = SodarIngestFastq(args)
    lz, actual = ingestfastq.build_jobs()
    assert len(actual) == len(fake_file_paths) - 6
