"""Tests for ``cubi_tk.sodar.ingest_data``.

We only run some smoke tests here.
"""

import datetime
import json
import os
import re
import unittest
from unittest import mock

import cattr

from pyfakefs import fake_filesystem, fake_pathlib
import pytest

from cubi_tk.__main__ import main, setup_argparse
from cubi_tk.exceptions import ParameterException
from cubi_tk.irods_common import TransferJob
from cubi_tk.sodar.ingest_data import (
    DEST_PATTERN_PRESETS,
    SRC_REGEX_PRESETS,
    SodarIngestData,
)

from .conftest import my_get_lz_info, my_sodar_api_export
from .factories import InvestigationFactory


def test_run_sodar_ingest_fastq_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest-data", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_sodar_ingest_fastq_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest-data"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_sodar_ingest_fastq_preset_definitions():
    regexes = SRC_REGEX_PRESETS.keys()
    patterns = DEST_PATTERN_PRESETS.keys()

    # Check that all presets exits for both regex & pattern
    assert sorted(regexes) == sorted(patterns)

    # Check that all presets are not empty
    for preset in regexes:
        assert SRC_REGEX_PRESETS[preset]
        assert DEST_PATTERN_PRESETS[preset]


def test_run_sodar_ingest_fastq_default_preset_regex():
    ## Test default regex
    # Collection of example filenames and the expected {sample} value the regex should capture
    test_filenames = {
        "Sample1-N1-RNA1-RNA_seq1.fastq.gz": "Sample1-N1-RNA1-RNA_seq1",
        "P1234_Samplename_S14_L006_R2_001.fastq.gz": "P1234_Samplename",
        "P1234_Samplename2_R1.fastq.gz": "P1234_Samplename2",
    }
    for test_filename, expected_sample in test_filenames.items():
        res = re.match(SRC_REGEX_PRESETS["fastq"], test_filename)
        assert res is not None
        assert res.groupdict()["sample"] == expected_sample


def test_run_sodar_ingest_fastq_digestiflow_preset_regex():
    ## Test default regex
    # Collection of example filenames and the expected {sample} value the regex should capture
    pattern = "240101_XY01234_0000_B{flowcell}/A0000_{sample}/{flowcell}/{lane}/A0000_{sample}_S1_{lane}_R1_001.fastq.gz"
    samples = ("sample1", "sample2")
    flowcells = ("AB123XY456", "CD678LT000")
    lanes = ("L001", "L002")
    test_filenames = {}
    for flowcell in flowcells:
        for sample in samples:
            for lane in lanes:
                test_filename = pattern.format(flowcell=flowcell, sample=sample, lane=lane)
                test_filenames[test_filename] = (sample, flowcell)

    for test_filename, expected in test_filenames.items():
        res = re.match(SRC_REGEX_PRESETS["digestiflow"], test_filename)
        assert res is not None
        assert res.groupdict()["sample"] == expected[0]
        assert res.groupdict()["flowcell"] == expected[1]


def test_run_sodar_ingest_fastq_ont_preset_regex():
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


def test_run_sodar_ingest_fastq_get_match_to_collection_mapping(requests_mock):
    # Patched sodar API call
    requests_mock.register_uri("GET", "https://sodar.bihealth.org/samplesheets/api/export/json/466ab946-ce6a-4c78-9981-19b79e7bbe86", json=my_sodar_api_export(), status_code= 200)

    # Instantiate SodarIngestData (seems to require args?)
    landing_zone_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    project_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    fake_base_path = "/base/path"
    argv = [
        "--verbose",
        "sodar",
        "ingest-data",
        "--num-parallel-transfers",
        "0",
        "--sodar-server-url",
        "https://sodar.bihealth.org/",
        "--sodar-api-token",
        "XXXX",
        "--yes",
        landing_zone_uuid,
        fake_base_path,
    ]

    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)
    ingestfastq = SodarIngestData(args)

    # test to get expected dict
    expected = {
        "Folder1": "Sample1-N1-DNA1-WES1",
        "Folder2": "Sample2-N1-DNA1-WES1",
        "Folder3": "Sample3-N1-DNA1-WES1",
    }
    args.project_uuid = project_uuid
    assert expected == ingestfastq.get_match_to_collection_mapping("Folder name")
    assert expected == ingestfastq.get_match_to_collection_mapping(
        "Folder name", "Library Name"
    )

    # Test for alternative collection column
    expected2 = {
        "Folder1": "Sample1-N1-DNA1",
        "Folder2": "Sample2-N1-DNA1",
        "Folder3": "Sample3-N1-DNA1",
    }
    assert expected2 == ingestfastq.get_match_to_collection_mapping(
        "Folder name", "Extract Name"
    )

    # Test for missing column
    with unittest.TestCase.assertRaises(unittest.TestCase, ParameterException):
        ingestfastq.get_match_to_collection_mapping("Typo-Column")

    # Test with additional assay
    requests_mock.register_uri("GET", "https://sodar.bihealth.org/samplesheets/api/export/json/466ab946-ce6a-4c78-9981-19b79e7bbe86", json=my_sodar_api_export(2, offset=1), status_code= 200)
    retval = InvestigationFactory()
    requests_mock.register_uri("GET", "https://sodar.bihealth.org/samplesheets/api/investigation/retrieve/466ab946-ce6a-4c78-9981-19b79e7bbe86", json= cattr.unstructure(retval), status_code= 200)
    study_key = list(retval.studies.keys())[0]
    assay_uuid = list(retval.studies[study_key].assays.keys())[0]
    ingestfastq.args.assay_uuid = assay_uuid

    assert expected == ingestfastq.get_match_to_collection_mapping("Folder name")


def test_run_sodar_ingest_fastq_smoke_test(mocker, requests_mock, fs):
    # --- setup arguments
    irods_path = "/irods/dest"
    landing_zone_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    fake_base_path = "/base/path"
    argv = [
        "--verbose",
        "sodar",
        "ingest-data",
        "--num-parallel-transfers",
        "0",
        "--sodar-server-url",
        "https://sodar.bihealth.org/",
        "--sodar-api-token",
        "XXXX",
        "--yes",
        landing_zone_uuid,
        fake_base_path,

    ]

    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break biomedsheets.
    fake_os = fake_filesystem.FakeOsModule(fs)
    fake_pl = fake_pathlib.FakePathlibModule(fs)

    # --- add test files
    fake_file_paths = []
    fake_dest_paths = []
    date = datetime.date.today().strftime("%Y-%m-%d")
    for member in ("sample1", "sample2", "sample3"):
        for ext in ("", ".md5"):
            fake_file_paths.append(
                "%s/%s/%s-N1-RNA1-RNA_seq1.fastq.gz%s" % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_dest_paths.append(
                TransferJob(
                    path_local=fake_file_paths[-1],
                    path_remote=f"/irods/dest/{member}-N1-RNA1-RNA_seq1/raw_data/{date}/{member}-N1-RNA1-RNA_seq1.fastq.gz{ext}",
                )
            )

            fake_file_paths.append(
                "%s/%s/%s-N1-DNA1-WES1.fq.gz%s" % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_dest_paths.append(
                TransferJob(
                    path_local=fake_file_paths[-1],
                    path_remote=f"/irods/dest/{member}-N1-DNA1-WES1/raw_data/{date}/{member}-N1-DNA1-WES1.fq.gz{ext}",
                )
            )

    # Remove index's log MD5 file again so it is recreated.
    fs.remove(fake_file_paths[3])

    # --- mock modules
    mocker.patch("cubi_tk.snappy.itransfer_common.os", fake_os)
    mocker.patch(
        "cubi_tk.snappy.itransfer_common.SnappyItransferCommandBase.get_lz_info",
        my_get_lz_info,
    )
    mock_check_output = mock.MagicMock(return_value=0)
    mocker.patch("cubi_tk.irods_common.iRODSTransfer.put", mock_check_output)

    mock_check_call = mock.MagicMock(return_value=0)
    mocker.patch("cubi_tk.common.check_call", mock_check_call)

    mocker.patch("cubi_tk.sodar.ingest_data.pathlib", fake_pl)
    mocker.patch("cubi_tk.sodar.ingest_data.os", fake_os)

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_tk.snappy.itransfer_common.open", fake_open)
    mocker.patch("cubi_tk.sodar.ingest_data.open", fake_open)

    # necessary because independent test fail
    mock_value = mock.MagicMock()
    mocker.patch("cubi_tk.sodar.ingest_data.Value", mock_value)
    mocker.patch("cubi_tk.common.Value", mock_value)

    mocker.patch("cubi_tk.sodar.ingest_data.iRODSTransfer.irods_hash_scheme", mock.MagicMock(return_value="MD5"))

    # requests mock
    return_value = {
        "assay": "",
        "config_data": "",
        "configuration": "",
        "date_modified": "",
        "description": "",
        "irods_path": irods_path,
        "project": "",
        "sodar_uuid": "",
        "status": "",
        "status_info": "",
        "status_locked" : "",
        "title": "",
        "user":  "",
    }

    url = os.path.join("https://sodar.bihealth.org/", "landingzones", "api", "retrieve", landing_zone_uuid)
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
    ingestdata = SodarIngestData(args)
    ingestdata.check_args(args)
    assert ingestdata.remote_dir_pattern == DEST_PATTERN_PRESETS["fastq"]
    lz, actual = ingestdata.build_jobs(".md5")
    assert sorted(actual, key=lambda x: x.path_remote) == sorted(
        fake_dest_paths, key=lambda x: x.path_remote
    )

    remote_pattern = "{collection_name}/target/folder/{filename}"
    argv[-2:] = [
        "--remote-dir-pattern",
        remote_pattern,
        landing_zone_uuid,
        fake_base_path,
    ]
    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)
    ingestdata = SodarIngestData(args)
    assert ingestdata.remote_dir_pattern == remote_pattern


def test_run_sodar_ingest_fastq_smoke_test_ont_preset(mocker, requests_mock, fs):
    # --- setup arguments
    irods_path = "/irods/dest"
    landing_zone_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    fake_base_path = "/base/path"
    argv = [
        "--verbose",
        "sodar",
        "ingest-data",
        "--num-parallel-transfers",
        "0",
        "--sodar-server-url",
        "https://sodar.bihealth.org/",
        "--sodar-api-token",
        "XXXX",
        "--yes",
        "--preset",
        "ONT",
        landing_zone_uuid,
        fake_base_path,
    ]

    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break biomedsheets.
    fake_os = fake_filesystem.FakeOsModule(fs)
    fake_pl = fake_pathlib.FakePathlibModule(fs)

    # --- add test files
    fake_file_paths = []
    date = "20240101"
    project_sample_id = "A0000_sample{n}"
    fake_dest_paths = []
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
                # html files will NOT be recognised by the preset
                if "html" in fake_file_paths[-1]:
                    continue
                fake_dest_paths.append(
                    TransferJob(
                        path_local=fake_file_paths[-1],
                        path_remote=os.path.join(
                            f"/irods/dest/{sample_path}/raw_data/{flowcellrun}/",
                            os.path.relpath(
                                fake_file_paths[-1],
                                f"{fake_base_path}/{date}_{sample_path}/{flowcellrun}",
                            ),
                        ),
                    )
                )

    # Remove MD5 file for sample 1 fail bam, so it is recreated.
    fs.remove(fake_file_paths[3])

    # --- mock modules
    mocker.patch("cubi_tk.snappy.itransfer_common.os", fake_os)
    mocker.patch(
        "cubi_tk.snappy.itransfer_common.SnappyItransferCommandBase.get_lz_info",
        my_get_lz_info,
    )


    mock_check_output = mock.MagicMock(return_value=0)
    mocker.patch("cubi_tk.irods_common.iRODSTransfer.put", mock_check_output)

    mock_check_call = mock.MagicMock(return_value=0)
    mocker.patch("cubi_tk.common.check_call", mock_check_call)

    mocker.patch("cubi_tk.sodar.ingest_data.pathlib", fake_pl)
    mocker.patch("cubi_tk.sodar.ingest_data.os", fake_os)

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_tk.snappy.itransfer_common.open", fake_open)
    mocker.patch("cubi_tk.sodar.ingest_data.open", fake_open)

    # necessary because independent test fail
    mock_value = mock.MagicMock()
    mocker.patch("cubi_tk.sodar.ingest_data.Value", mock_value)
    mocker.patch("cubi_tk.common.Value", mock_value)

    mocker.patch("cubi_tk.sodar.ingest_data.iRODSTransfer.irods_hash_scheme", mock.MagicMock(return_value="MD5"))


    # requests mock
    return_value = {
        "assay": "",
        "config_data": "",
        "configuration": "",
        "date_modified": "",
        "description": "",
        "irods_path": irods_path,
        "project": "",
        "sodar_uuid": "",
        "status": "",
        "status_info": "",
        "status_locked" : "",
        "title": "",
        "user": "",
    }
    url = os.path.join("https://sodar.bihealth.org/", "landingzones", "api", "retrieve", landing_zone_uuid)
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
    ingestdata = SodarIngestData(args)
    lz, actual = ingestdata.build_jobs(".md5")
    assert sorted(actual, key=lambda x: x.path_remote) == sorted(
        fake_dest_paths, key=lambda x: x.path_remote
    )
    assert len(actual) == len(fake_file_paths) - 6
