"""Tests for ``cubi_tk.snappy.check_remote``."""
import pathlib

from cubi_tk.snappy.check_remote import FindRemoteFiles


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
    find_obj = FindRemoteFiles(sheet=None, sodar_url="", sodar_api_token="", project_uuid="")  # noqa: B106

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
