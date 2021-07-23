"""Tests for ``cubi_tk.snappy.check_remote``."""
import pathlib

import pytest

from cubi_tk.snappy.check_remote import Checker, FindLocalFiles, FindRemoteFiles


# Tests FindLocalFiles =================================================================================================


def test_findlocal_run(germline_trio_sheet_object):
    """Tests FindLocalFiles::run()"""
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


# Tests FindRemoteFiles ================================================================================================


def test_parse_sample_sheet(germline_trio_sheet_object):
    """Tests FindRemoteFiles::parse_sample_sheet()"""
    # Initialise object
    find_obj = FindRemoteFiles(
        sheet=germline_trio_sheet_object, sodar_url="", sodar_api_token="", project_uuid=""
    )  # noqa: B106
    # Define expected
    expected = ["index-N1-DNA1-WES1", "father-N1-DNA1-WES1", "mother-N1-DNA1-WES1"]
    # Get actual
    actual = find_obj.parse_sample_sheet()
    assert actual == expected


def test_parse_ils_stdout():
    """Tests FindRemoteFiles::parse_ils_stdout()"""
    # Initialise object
    find_obj = FindRemoteFiles(
        sheet=None, sodar_url="", sodar_api_token="", project_uuid=""
    )  # noqa: B106

    # Define expected number of files per directory
    expected = {
        "ngs_mapping/bwa.P001-N1-DNA1-WES1/log": 6,
        "ngs_mapping/bwa.P001-N1-DNA1-WES1/out": 4,
        "ngs_mapping/bwa.P001-N1-DNA1-WES1/report/bam_qc": 8,
        "variant_calling/bwa.gatk_hc.P001-N1-DNA1-WES1/report/bcftools_stats": 6,
    }

    # Load test file (bytes)
    test_file_path = pathlib.Path(__file__).resolve().parent / "data" / "ils_out_str.txt"
    test_file = open(test_file_path, "r")
    data = test_file.read()
    test_file.close()
    data_bytes = str.encode(data)

    # Run and assert
    result = find_obj.parse_ils_stdout(data_bytes)
    for directory in result:
        for test_dir in expected:
            if directory.endswith(test_dir):
                expected_count = expected.get(test_dir)
                msg = "Directory '{dir}' should contain {count} files.".format(
                    dir=directory, count=expected_count
                )
                assert len(result.get(directory)) == expected_count, msg


# Tests Checker ========================================================================================================


def test_compare_local_and_remote_files():
    """Tests Checker::compare_local_and_remote_files()"""
    # Create checker object
    checker = Checker(local_files_dict=None, remote_files_dict=None)

    # Define input
    remote_path = (
        "/remote/path/P001-N1-DNA1-WES1/GRCh37/2019-07-11/ngs_mapping/bwa.P001-N1-DNA1-WES1/out"
    )
    in_remote_dict = {
        remote_path: [
            "bwa.P001-N1-DNA1-WES1.bam",
            "bwa.P001-N1-DNA1-WES1.bam.bai",
            "bwa.P001-N1-DNA1-WES1.bam.bai.md5",
            "bwa.P001-N1-DNA1-WES1.bam.md5",
        ]
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
        remote_path + "/" + file
        for file in ["bwa.P001-N1-DNA1-WES1.bam.bai.md5", "bwa.P001-N1-DNA1-WES1.bam.md5"]
    ]
    expected_only_local = [local_path + "/" + file for file in ["bwa.P001-N1-DNA1-WES1.fastq"]]

    # Run and assert
    actual_both, actual_remote, actual_local = checker.compare_local_and_remote_files(
        local_dict=in_local_dict,
        remote_dict=in_remote_dict,
        check_name="ngs_mapping",
        library_name="P001-N1-DNA1-WES1",
    )
    assert actual_both == set(expected_both)
    assert actual_remote == set(expected_only_remote)
    assert actual_local == set(expected_only_local)

    # ================= #
    # Test empty remote #
    # ================= #
    # Run and assert
    actual_both, actual_remote, actual_local = checker.compare_local_and_remote_files(
        local_dict=in_local_dict,
        remote_dict={},
        check_name="ngs_mapping",
        library_name="P001-N1-DNA1-WES1",
    )
    assert actual_both == set()
    assert actual_remote == set()
    assert actual_local == set(expected_only_local + expected_both)

    # ========================= #
    # Test extra path in remote #
    # ========================= #
    # Update input and expected results
    extra_remote_path = (
        "/remote/path/P001-N1-DNA1-WES1/GRCh37/2019-07-11/ngs_mapping/bwa.P001-N1-DNA1-WES1/log"
    )
    extra_remote_files = [
        "bwa.P001-N1-DNA1-WES1.conda_info.txt",
        "bwa.P001-N1-DNA1-WES1.conda_info.txt.md5",
    ]
    in_remote_dict.update({extra_remote_path: extra_remote_files})
    expected_only_remote += [extra_remote_path + "/" + file for file in extra_remote_files]
    # Run and assert
    actual_both, actual_remote, actual_local = checker.compare_local_and_remote_files(
        local_dict=in_local_dict,
        remote_dict=in_remote_dict,
        check_name="ngs_mapping",
        library_name="P001-N1-DNA1-WES1",
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
    extra_local_files = [
        "bwa.P001-N1-DNA1-WES1.bam.bamstats.html",
        "bwa.P001-N1-DNA1-WES1.bam.bamstats.html.md5",
    ]
    in_local_dict.update({extra_local_path: extra_local_files})
    expected_only_local += [extra_local_path + "/" + file for file in extra_local_files]
    # Run and assert
    actual_both, actual_remote, actual_local = checker.compare_local_and_remote_files(
        local_dict=in_local_dict,
        remote_dict=in_remote_dict,
        check_name="ngs_mapping",
        library_name="P001-N1-DNA1-WES1",
    )
    assert actual_both == set(expected_both)
    assert actual_remote == set(expected_only_remote)
    assert actual_local == set(expected_only_local)

    # =========================== #
    # Test path without step name #
    # =========================== #
    # Update input and expected results
    invalid_local_path = "/local/path/without/step/name"
    in_local_dict.update({invalid_local_path: ["temp.txt"]})
    # Expects IndexError because input dictionary wasn't prefiltered
    with pytest.raises(IndexError):
        checker.compare_local_and_remote_files(
            local_dict=in_local_dict,
            remote_dict=in_remote_dict,
            check_name="ngs_mapping",
            library_name="P001-N1-DNA1-WES1",
        )
