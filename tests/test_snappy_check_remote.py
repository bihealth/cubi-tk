"""Tests for ``cubi_tk.snappy.check_remote``."""

import pathlib

import pytest

from cubi_tk.snappy.check_remote import Checker, FindLocalFiles, FindLocalRawdataFiles

from .helpers import createIrodsDataObject

# from cubi_tk.snappy.retrieve_irods_collection import IrodsDataObject


# Tests FindLocalFiles =================================================================================================


def test_findlocal_run(germline_trio_sheet_object):
    """Tests FindLocalFiles.run()"""
    # Define expected
    expected = {
        "tests/data/find_snappy/output/index-N1-DNA1-WES1/out": [
            "bwa.P001-N1-DNA1-WES1.bam",
            "bwa.P001-N1-DNA1-WES1.bam.bai",
        ],
        "tests/data/find_snappy/output/index-N1-DNA1-WES1/report/bam_qc": [
            "bwa.P001-N1-DNA1-WES1.bam.bamstats.html.md5",
            "bwa.P001-N1-DNA1-WES1.bam.bamstats.html",
        ],
    }

    # Define input
    test_dir_path = pathlib.Path(__file__).resolve().parent / "data" / "find_snappy"

    # Get actual
    actual = FindLocalFiles(
        sheet=germline_trio_sheet_object, base_path=str(test_dir_path), step_list=["ngs_mapping"]
    ).run()
    assert len(actual) == 1, "Expects a single key for check 'output'."
    for actual_dir in actual["output"]["index-N1-DNA1-WES1"]:
        for expected_dir in expected:
            if expected_dir in actual_dir:
                actual_sorted = sorted(actual["output"]["index-N1-DNA1-WES1"][actual_dir])
                expected_sorted = sorted(expected.get(expected_dir))
                assert actual_sorted == expected_sorted

    # Test exceptions on `step_list` parameter
    with pytest.raises(ValueError):
        FindLocalFiles(sheet=germline_trio_sheet_object, base_path=str(test_dir_path))
    with pytest.raises(ValueError):
        FindLocalFiles(sheet=germline_trio_sheet_object, base_path=str(test_dir_path), step_list=[])


# Tests FindLocalRawdataFiles ==========================================================================================


def test_findrawdata_run(germline_trio_sheet_object):
    """Tests FindLocalRawdataFiles.run()"""
    # Define expected
    expected = {
        "tests/data/find_snappy/ngs_mapping/work/input_links/index-N1-DNA1-WES1": [
            "P001-tn-Exome_S1_R2_001.fastq.gz",
            "P001-tn-Exome_S1_R1_001.fastq.gz",
        ]
    }

    # Define input
    test_dir_path = pathlib.Path(__file__).resolve().parent / "data" / "find_snappy"

    # Get actual and assert
    actual = FindLocalRawdataFiles(
        sheet=germline_trio_sheet_object, base_path=str(test_dir_path)
    ).run()
    assert len(actual) == 1, "Expects a single key for library 'index-N1-DNA1-WES1'."
    for actual_dir in actual["index-N1-DNA1-WES1"]:
        for expected_dir in expected:
            if expected_dir in actual_dir:
                actual_sorted = sorted(actual["index-N1-DNA1-WES1"][actual_dir])
                expected_sorted = sorted(expected.get(expected_dir))
                assert actual_sorted == expected_sorted


# Tests Checker ========================================================================================================


def test_compare_local_and_remote_files():
    """Tests Checker.compare_local_and_remote_files()"""
    # Create checker object
    checker = Checker(local_files_dict=None, remote_files_dict=None)

    # Define input
    file_md5sum = "d41d8cd98f00b204e9800998ecf8427e"
    replicas_md5sum = [file_md5sum] * 3
    in_remote_dict = {
        "bwa.P001-N1-DNA1-WES1.bam": [
            createIrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.bam",
                irods_path="/sodar_path/bwa.P001-N1-DNA1-WES1.bam",
                file_md5sum=file_md5sum,
                replicas_md5sum=replicas_md5sum,
            )
        ],
        "bwa.P001-N1-DNA1-WES1.bam.bai": [
            createIrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.bam.bai",
                irods_path="/sodar_path/bwa.P001-N1-DNA1-WES1.bam.bai",
                file_md5sum=file_md5sum,
                replicas_md5sum=replicas_md5sum,
            )
        ],
        "bwa.P002-N1-DNA1-WES1.bam": [
            createIrodsDataObject(
                file_name="bwa.P002-N1-DNA1-WES1.bam",
                irods_path="/sodar_path/bwa.P002-N1-DNA1-WES1.bam",
                file_md5sum=file_md5sum,
                replicas_md5sum=replicas_md5sum,
            )
        ],
        "bwa.P002-N1-DNA1-WES1.bam.bai": [
            createIrodsDataObject(
                file_name="bwa.P002-N1-DNA1-WES1.bam.bai",
                irods_path="/sodar_path/bwa.P002-N1-DNA1-WES1.bam.bai",
                file_md5sum=file_md5sum,
                replicas_md5sum=replicas_md5sum,
            )
        ],
    }
    local_path = "/local/path/P001-N1-DNA1-WES1/GRCh37/2019-07-11/ngs_mapping/output/bwa.P001-N1-DNA1-WES1/out"
    in_local_dict = {
        local_path: [
            "bwa.P001-N1-DNA1-WES1.bam",
            "bwa.P001-N1-DNA1-WES1.bam.bai",
            "bwa.P001-N1-DNA1-WES1.fastq",
        ]
    }

    # Define expected
    expected_both = [
        local_path + "/" + file
        for file in ["bwa.P001-N1-DNA1-WES1.bam", "bwa.P001-N1-DNA1-WES1.bam.bai"]
    ]
    expected_only_remote = [
        "/sodar_path/" + file
        for file in ["bwa.P002-N1-DNA1-WES1.bam.bai", "bwa.P002-N1-DNA1-WES1.bam"]
    ]
    expected_only_local = [local_path + "/" + file for file in ["bwa.P001-N1-DNA1-WES1.fastq"]]

    # Run and assert
    actual_both, actual_remote, actual_local = checker.compare_local_and_remote_files(
        local_dict=in_local_dict, remote_dict=in_remote_dict
    )

    assert actual_both == set(expected_both)
    assert actual_remote == set(expected_only_remote)
    assert actual_local == set(expected_only_local)

    # ================= #
    # Test empty remote #
    # ================= #
    # Run and assert
    actual_both, actual_remote, actual_local = checker.compare_local_and_remote_files(
        local_dict=in_local_dict, remote_dict={}
    )
    assert actual_both == set()
    assert actual_remote == set()
    assert actual_local == set(expected_only_local + expected_both)

    # ========================= #
    # Test extra path in remote #
    # ========================= #
    # Update input and expected results
    extra_remote_files_dict = {
        "bwa.P001-N1-DNA1-WES1.conda_info.txt": [
            createIrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.conda_info.txt",
                irods_path="/sodar_path/bwa.P001-N1-DNA1-WES1.conda_info.txt",
                file_md5sum=file_md5sum,
                replicas_md5sum=replicas_md5sum,
            )
        ]
    }

    in_remote_dict.update(extra_remote_files_dict)
    expected_only_remote += ["/sodar_path/bwa.P001-N1-DNA1-WES1.conda_info.txt"]
    # Run and assert
    actual_both, actual_remote, actual_local = checker.compare_local_and_remote_files(
        local_dict=in_local_dict, remote_dict=in_remote_dict
    )
    assert actual_both == set(expected_both)
    assert actual_remote == set(expected_only_remote)
    assert actual_local == set(expected_only_local)

    # ======================== #
    # Test extra path in local #
    # ======================== #
    # Update input and expected results
    extra_local_path = (
        "/local/path/P001-N1-DNA1-WES1/GRCh37/2019-07-11/ngs_mapping/bwa.P001-N1-DNA1-WES1/report"
    )
    in_local_dict.update({extra_local_path: ["bwa.P001-N1-DNA1-WES1.bam.bamstats.html"]})
    expected_only_local += [extra_local_path + "/" + "bwa.P001-N1-DNA1-WES1.bam.bamstats.html"]
    # Run and assert
    actual_both, actual_remote, actual_local = checker.compare_local_and_remote_files(
        local_dict=in_local_dict, remote_dict=in_remote_dict
    )
    assert actual_both == set(expected_both)
    assert actual_remote == set(expected_only_remote)
    assert actual_local == set(expected_only_local)
