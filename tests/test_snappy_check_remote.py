"""Tests for ``cubi_tk.snappy.check_remote``."""
import pathlib

from cubi_tk.snappy.check_remote import Checker, FindRemoteFiles


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


def test_find_local_files():
    """Tests Checker::find_local_files()"""
    # Create checker object
    checker = Checker(remote_files_dict=None, base_path=None)

    # Define input
    test_dir_path = pathlib.Path(__file__).resolve().parent / "data" / "find_snappy" / "output"

    # Define expected
    expected = {
        "data/find_snappy/output/P001-N1-DNA1-WES1/out": [
            "bwa.P001-N1-DNA1-WES1.bam",
            "bwa.P001-N1-DNA1-WES1.bam.bai",
        ],
        "data/find_snappy/output/P001-N1-DNA1-WES1/report/bam_qc": [
            "bwa.P001-N1-DNA1-WES1.bam.bamstats.html.md5",
            "bwa.P001-N1-DNA1-WES1.bam.bamstats.html",
        ],
    }

    # Run and assert
    result = checker.find_local_files(library_name="P001-N1-DNA1-WES1", path=test_dir_path)
    for result_dir in result:
        for expected_dir in expected:
            if result_dir.endswith(expected_dir):
                files_str = ", ".join(expected.get(expected_dir))
                msg = "Directory {dir} should return files: {files}".format(
                    dir=result_dir, files=files_str
                )
                assert sorted(result.get(result_dir)) == sorted(expected.get(expected_dir)), msg
