"""Tests for ``cubi_tk.snappy.pull_processed_data``.
"""

import pytest

from cubi_tk.snappy.pull_processed_data import PullProcessedDataCommand
from cubi_tk.snappy.retrieve_irods_collection import IrodsDataObject
from cubi_tk.__main__ import setup_argparse

# Empty file MD5 checksum
FILE_MD5SUM = "d41d8cd98f00b204e9800998ecf8427e"

# Arbitrary replicas MD5 checksum value
REPLICAS_MD5SUM = [FILE_MD5SUM] * 3


@pytest.fixture
def pull_processed_data():
    return PullProcessedDataCommand(args=None)


@pytest.fixture
def remote_files_bam():
    """Returns iRODS collection example for BAM files and two samples, P001 and P002"""
    p0001_sodar_path = "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P001-N1-DNA1-WES1/%s"
    p0002_sodar_path = "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P002-N1-DNA1-WES1/%s"
    return {
        "bwa.P001-N1-DNA1-WES1.bam": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.bam",
                irods_path=f"{p0001_sodar_path % '1975-01-04'}/ngs_mapping/bwa.P001-N1-DNA1-WES1.bam",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.bam",
                irods_path=f"{p0001_sodar_path % '1999-09-09'}/ngs_mapping/bwa.P001-N1-DNA1-WES1.bam",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
        ],
        "bwa.P001-N1-DNA1-WES1.bam.bai": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.bam.bai",
                irods_path=f"{p0001_sodar_path % '1975-01-04'}/ngs_mapping/bwa.P001-N1-DNA1-WES1.bam.bai",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.bam.bai",
                irods_path=f"{p0001_sodar_path % '1999-09-09'}/ngs_mapping/bwa.P001-N1-DNA1-WES1.bam.bai",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
        ],
        "bwa.P002-N1-DNA1-WES1.bam": [
            IrodsDataObject(
                file_name="bwa.P002-N1-DNA1-WES1.bam",
                irods_path=f"{p0002_sodar_path % '1999-09-09'}/ngs_mapping/bwa.P002-N1-DNA1-WES1.bam",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
            IrodsDataObject(
                file_name="bwa.P002-N1-DNA1-WES1.bam",
                irods_path=f"{p0002_sodar_path % '1975-01-04'}/ngs_mapping/bwa.P002-N1-DNA1-WES1.bam",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
        ],
        "bwa.P002-N1-DNA1-WES1.bam.bai": [
            IrodsDataObject(
                file_name="bwa.P002-N1-DNA1-WES1.bam.bai",
                irods_path=f"{p0002_sodar_path % '1999-09-09'}/ngs_mapping/bwa.P002-N1-DNA1-WES1.bam.bai",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
            IrodsDataObject(
                file_name="bwa.P002-N1-DNA1-WES1.bam.bai",
                irods_path=f"{p0002_sodar_path % '1975-01-04'}/ngs_mapping/bwa.P002-N1-DNA1-WES1.bam.bai",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
        ],
    }


@pytest.fixture
def remote_files_vcf():
    """Returns iRODS collection example for VCF files and two samples, P001 and P002"""
    p0001_sodar_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P001-N1-DNA1-WES1/1999-09-09"
    )
    p0002_sodar_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P002-N1-DNA1-WES1/1999-09-09"
    )
    return {
        "bwa.P001-N1-DNA1-WES1.vcf.gz": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.vcf.gz",
                irods_path=f"{p0001_sodar_path}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
        "bwa.P001-N1-DNA1-WES1.vcf.gz.tbi": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.vcf.gz.tbi",
                irods_path=f"{p0001_sodar_path}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz.tbi",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
        "bwa.P002-N1-DNA1-WES1.vcf.gz": [
            IrodsDataObject(
                file_name="bwa.P002-N1-DNA1-WES1.vcf.gz",
                irods_path=f"{p0002_sodar_path}/variant_calling/bwa.P002-N1-DNA1-WES1.vcf.gz",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
        "bwa.P002-N1-DNA1-WES1.vcf.gz.tbi": [
            IrodsDataObject(
                file_name="bwa.P002-N1-DNA1-WES1.vcf.gz.tbi",
                irods_path=f"{p0002_sodar_path}/variant_calling/bwa.P002-N1-DNA1-WES1.vcf.gz.tbi",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
    }


@pytest.fixture
def remote_files_log():
    """Returns iRODS collection example for LOG files and one samples, P001."""
    p0001_sodar_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P001-N1-DNA1-WES1/1999-09-09"
    )
    return {
        "bwa.P001-N1-DNA1-WES1.conda_info.txt": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.conda_info.txt",
                irods_path=f"{p0001_sodar_path}/variant_calling/bwa.P001-N1-DNA1-WES1.conda_info.txt",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
        "bwa.P001-N1-DNA1-WES1.conda_list.txt": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.conda_list.txt",
                irods_path=f"{p0001_sodar_path}/variant_calling/bwa.P001-N1-DNA1-WES1.conda_list.txt",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
        "bwa.P001-N1-DNA1-WES1.log": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.log",
                irods_path=f"{p0001_sodar_path}/variant_calling/bwa.P001-N1-DNA1-WES1.log",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
    }


@pytest.fixture
def remote_files_txt():
    """Returns iRODS collection example for TXT files and one samples, P001."""
    p0001_sodar_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P001-N1-DNA1-WES1/1999-09-09"
    )
    return {
        "bwa.P001-N1-DNA1-WES1.txt": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.txt",
                irods_path=f"{p0001_sodar_path}/variant_calling/bwa.P001-N1-DNA1-WES1.txt",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ]
    }


@pytest.fixture
def remote_files_csv():
    """Returns iRODS collection example for CSV files and one samples, P001."""
    p0001_sodar_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P001-N1-DNA1-WES1/1999-09-09"
    )
    return {
        "bwa.P001-N1-DNA1-WES1.csv": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.csv",
                irods_path=f"{p0001_sodar_path}/variant_calling/bwa.P001-N1-DNA1-WES1.csv",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ]
    }


@pytest.fixture
def remote_files_common_links_txt():
    """Returns iRODS collection example for TXT files and one samples, P001.
    All files are stored in common links, i.e., 'ResultsReports', 'MiscFiles', and 'TrackHubs'.
    """
    sodar_path = "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999"
    return {
        "bwa.P001-N1-DNA1-WES1_MiscFiles.txt": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1.txt",
                irods_path=f"{sodar_path}/MiscFiles/bwa.P001-N1-DNA1-WES1_MiscFiles.txt",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
        "bwa.P001-N1-DNA1-WES1_ResultsReports.txt": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1_ResultsReports.txt",
                irods_path=f"{sodar_path}/ResultsReports/bwa.P001-N1-DNA1-WES1_ResultsReports.txt",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
        "bwa.P001-N1-DNA1-WES1_TrackHubs.txt": [
            IrodsDataObject(
                file_name="bwa.P001-N1-DNA1-WES1_TrackHubs.txt",
                irods_path=f"{sodar_path}/TrackHubs/bwa.P001-N1-DNA1-WES1_TrackHubs.txt",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
    }


@pytest.fixture
def remote_files_all(
    remote_files_bam, remote_files_csv, remote_files_log, remote_files_txt, remote_files_vcf
):
    """Returns full example of iRODS collection: BAM, CSV, LOG, TXT and VCF files"""
    return {
        **remote_files_bam,
        **remote_files_csv,
        **remote_files_log,
        **remote_files_txt,
        **remote_files_vcf,
    }


def test_run_snappy_pull_processed_help(capsys):
    """Test ``cubi-tk snappy pull-processed-data --help``"""
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "pull-processed-data", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_snappy_pull_processed_nothing(capsys):
    """Test ``cubi-tk snappy pull-processed-data``"""
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "pull-processed-data"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_pull_processed_data_filter_irods_collection_bam(
    pull_processed_data, remote_files_bam, remote_files_txt, remote_files_all
):
    """Tests PullProcessedDataCommand.filter_irods_collection() - BAM files"""
    # Define input
    absent_sample_list = ["P098", "P099"]
    samples_list = ["P001", "P002"]
    library_name_list = ["P001-N1-DNA1-WES1", "P002-N1-DNA1-WES1"]
    file_type = "bam"

    # Call with samples id as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=samples_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert actual == remote_files_bam

    # Call with library names as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=library_name_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert actual == remote_files_bam

    # Sanity check - should return empty dictionary, samples aren't present
    actual = pull_processed_data.filter_irods_collection(
        identifiers=absent_sample_list, remote_files_dict=remote_files_bam, file_type=file_type
    )
    assert len(actual) == 0

    # Sanity check - should return empty dictionary, file extension is not present
    actual = pull_processed_data.filter_irods_collection(
        identifiers=library_name_list, remote_files_dict=remote_files_txt, file_type=file_type
    )
    assert len(actual) == 0


def test_pull_processed_data_filter_irods_collection_vcf(
    pull_processed_data, remote_files_vcf, remote_files_txt, remote_files_all
):
    """Tests PullProcessedDataCommand.filter_irods_collection() - VCF files"""
    # Define input
    absent_sample_list = ["P098", "P099"]
    samples_list = ["P001", "P002"]
    library_name_list = ["P001-N1-DNA1-WES1", "P002-N1-DNA1-WES1"]
    file_type = "vcf"

    # Call with samples id as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=samples_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert actual == remote_files_vcf

    # Call with library names as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=library_name_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert actual == remote_files_vcf

    # Sanity check - should return empty dictionary, samples aren't present
    actual = pull_processed_data.filter_irods_collection(
        identifiers=absent_sample_list, remote_files_dict=remote_files_vcf, file_type=file_type
    )
    assert len(actual) == 0

    # Sanity check - should return empty dictionary, file extension is not present
    actual = pull_processed_data.filter_irods_collection(
        identifiers=library_name_list, remote_files_dict=remote_files_txt, file_type=file_type
    )
    assert len(actual) == 0


def test_pull_processed_data_filter_irods_collection_log(
    pull_processed_data, remote_files_log, remote_files_txt, remote_files_all
):
    """Tests PullProcessedDataCommand.filter_irods_collection() - LOG files"""
    # Define input
    absent_sample_list = ["P098", "P099"]
    samples_list = ["P001", "P002"]
    library_name_list = ["P001-N1-DNA1-WES1", "P002-N1-DNA1-WES1"]
    file_type = "log"

    # Call with samples id as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=samples_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert actual == remote_files_log

    # Call with library names as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=library_name_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert actual == remote_files_log

    # Sanity check - should return empty dictionary, samples aren't present
    actual = pull_processed_data.filter_irods_collection(
        identifiers=absent_sample_list, remote_files_dict=remote_files_log, file_type=file_type
    )
    assert len(actual) == 0

    # Sanity check - should return empty dictionary, file extension is not present
    actual = pull_processed_data.filter_irods_collection(
        identifiers=library_name_list, remote_files_dict=remote_files_txt, file_type=file_type
    )
    assert len(actual) == 0


def test_pull_processed_data_filter_irods_collection_csv(
    pull_processed_data, remote_files_csv, remote_files_txt, remote_files_all
):
    """Tests PullProcessedDataCommand.filter_irods_collection() - CSV files"""
    # Define input
    absent_sample_list = ["P098", "P099"]
    samples_list = ["P001", "P002"]
    library_name_list = ["P001-N1-DNA1-WES1", "P002-N1-DNA1-WES1"]
    file_type = "csv"

    # Call with samples id as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=samples_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert actual == remote_files_csv

    # Call with library names as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=library_name_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert actual == remote_files_csv

    # Sanity check - should return empty dictionary, samples aren't present
    actual = pull_processed_data.filter_irods_collection(
        identifiers=absent_sample_list, remote_files_dict=remote_files_csv, file_type=file_type
    )
    assert len(actual) == 0

    # Sanity check - should return empty dictionary, file extension is not present
    actual = pull_processed_data.filter_irods_collection(
        identifiers=library_name_list, remote_files_dict=remote_files_txt, file_type=file_type
    )
    assert len(actual) == 0


def test_pull_processed_data_filter_irods_collection_txt(pull_processed_data, remote_files_all):
    """Tests PullProcessedDataCommand.filter_irods_collection() - TXT files"""
    # Define input
    samples_list = ["P001", "P002"]
    library_name_list = ["P001-N1-DNA1-WES1", "P002-N1-DNA1-WES1"]
    file_type = "txt"

    # Define expected
    expected_keys = [
        "bwa.P001-N1-DNA1-WES1.conda_info.txt",
        "bwa.P001-N1-DNA1-WES1.conda_list.txt",
        "bwa.P001-N1-DNA1-WES1.txt",
    ]

    # Call with samples id as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=samples_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert all([key in expected_keys for key in actual.keys()])

    # Call with library names as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=library_name_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert all([key in expected_keys for key in actual.keys()])


def test_pull_processed_data_filter_irods_collection_txt_ignore_common_links(
    pull_processed_data, remote_files_txt, remote_files_common_links_txt
):
    """Tests PullProcessedDataCommand.filter_irods_collection() - TXT files, igore common links"""
    # Define input
    samples_list = ["P001"]
    library_name_list = ["P001-N1-DNA1-WES1"]
    file_type = "txt"
    remote_files_combined = {**remote_files_txt, **remote_files_common_links_txt}

    # Call with samples id as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=samples_list, remote_files_dict=remote_files_combined, file_type=file_type
    )
    assert actual == remote_files_txt

    # Call with library names as identifiers
    actual = pull_processed_data.filter_irods_collection(
        identifiers=library_name_list, remote_files_dict=remote_files_combined, file_type=file_type
    )
    assert actual == remote_files_txt


def test_pull_processed_data_pair_ipath_with_outdir_bam(pull_processed_data, remote_files_bam):
    """Tests PullProcessedDataCommand.pull_processed_data - BAM files"""
    # Define input
    out_dir = "out_dir"
    assay_uuid = "99999999-aaa-bbbb-cccc-99999999"
    wrong_assay_uuid = "11111111-aaa-bbbb-cccc-11111111"

    # Define expected
    irods_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P00{i}-N1-DNA1-WES1/1999-09-09/ngs_mapping/"
        "bwa.P00{i}-N1-DNA1-WES1.{ext}"
    )
    full_out_dir = (
        "out_dir/P00{i}-N1-DNA1-WES1/1999-09-09/ngs_mapping/bwa.P00{i}-N1-DNA1-WES1.{ext}"
    )
    irods_files_list = [
        irods_path.format(i=i, ext=ext)
        for i in (1, 2)
        for ext in ("bam", "bam.bai", "bam.md5", "bam.bai.md5")
    ]
    correct_uuid_output_dir_list = [
        full_out_dir.format(i=i, ext=ext)
        for i in (1, 2)
        for ext in ("bam", "bam.bai", "bam.md5", "bam.bai.md5")
    ]
    wrong_uuid_output_dir_list = [
        "out_dir/bwa.P00{i}-N1-DNA1-WES1.{ext}".format(i=i, ext=ext)
        for i in (1, 2)
        for ext in ("bam", "bam.bai", "bam.md5", "bam.bai.md5")
    ]
    correct_uuid_expected = []
    for _irods_path, _out_path in zip(irods_files_list, correct_uuid_output_dir_list):
        correct_uuid_expected.append((_irods_path, _out_path))
    wrong_uuid_expected = []
    for _irods_path, _out_path in zip(irods_files_list, wrong_uuid_output_dir_list):
        wrong_uuid_expected.append((_irods_path, _out_path))

    # Test with correct assay UUID - directory structure same as in SODAR
    actual = pull_processed_data.pair_ipath_with_outdir(
        remote_files_dict=remote_files_bam, output_dir=out_dir, assay_uuid=assay_uuid
    )
    assert sorted(actual) == sorted(correct_uuid_expected)

    # Test with wrong assay UUID - all files copied to root of output directory
    actual = pull_processed_data.pair_ipath_with_outdir(
        remote_files_dict=remote_files_bam, output_dir=out_dir, assay_uuid=wrong_assay_uuid
    )
    assert sorted(actual) == sorted(wrong_uuid_expected)


def test_pull_processed_data_pair_ipath_with_outdir_bam_retrieve_all(
    pull_processed_data, remote_files_bam
):
    """Tests PullProcessedDataCommand.pull_processed_data - all versions of BAM files."""
    # Define input
    out_dir = "out_dir"
    assay_uuid = "99999999-aaa-bbbb-cccc-99999999"

    # Define expected
    irods_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P00{i}-N1-DNA1-WES1/{date}/ngs_mapping/"
        "bwa.P00{i}-N1-DNA1-WES1.{ext}"
    )
    full_out_dir = "out_dir/P00{i}-N1-DNA1-WES1/{date}/ngs_mapping/bwa.P00{i}-N1-DNA1-WES1.{ext}"
    irods_files_list = [
        irods_path.format(i=i, date=date, ext=ext)
        for i in (1, 2)
        for date in ("1999-09-09", "1975-01-04")
        for ext in ("bam", "bam.bai", "bam.md5", "bam.bai.md5")
    ]
    correct_uuid_output_dir_list = [
        full_out_dir.format(i=i, date=date, ext=ext)
        for i in (1, 2)
        for date in ("1999-09-09", "1975-01-04")
        for ext in ("bam", "bam.bai", "bam.md5", "bam.bai.md5")
    ]
    correct_uuid_expected = []
    for _irods_path, _out_path in zip(irods_files_list, correct_uuid_output_dir_list):
        correct_uuid_expected.append((_irods_path, _out_path))

    # Test with correct assay UUID - directory structure same as in SODAR
    actual = pull_processed_data.pair_ipath_with_outdir(
        remote_files_dict=remote_files_bam,
        output_dir=out_dir,
        assay_uuid=assay_uuid,
        retrieve_all=True,
    )
    assert sorted(actual) == sorted(correct_uuid_expected)


def test_pull_processed_data_pair_ipath_with_outdir_vcf(pull_processed_data, remote_files_vcf):
    """Tests PullProcessedDataCommand.pull_processed_data - VCF files"""
    # Define input
    out_dir = "out_dir"
    assay_uuid = "99999999-aaa-bbbb-cccc-99999999"
    wrong_assay_uuid = "11111111-aaa-bbbb-cccc-11111111"

    # Define expected
    irods_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P00{i}-N1-DNA1-WES1/1999-09-09/variant_calling/"
        "bwa.P00{i}-N1-DNA1-WES1.{ext}"
    )
    full_out_dir = (
        "out_dir/P00{i}-N1-DNA1-WES1/1999-09-09/variant_calling/bwa.P00{i}-N1-DNA1-WES1.{ext}"
    )
    irods_files_list = [
        irods_path.format(i=i, ext=ext)
        for i in (1, 2)
        for ext in ("vcf.gz", "vcf.gz.tbi", "vcf.gz.md5", "vcf.gz.tbi.md5")
    ]
    correct_uuid_output_dir_list = [
        full_out_dir.format(i=i, ext=ext)
        for i in (1, 2)
        for ext in ("vcf.gz", "vcf.gz.tbi", "vcf.gz.md5", "vcf.gz.tbi.md5")
    ]
    wrong_uuid_output_dir_list = [
        "out_dir/bwa.P00{i}-N1-DNA1-WES1.{ext}".format(i=i, ext=ext)
        for i in (1, 2)
        for ext in ("vcf.gz", "vcf.gz.tbi", "vcf.gz.md5", "vcf.gz.tbi.md5")
    ]
    correct_uuid_expected = []
    for _irods_path, _out_path in zip(irods_files_list, correct_uuid_output_dir_list):
        correct_uuid_expected.append((_irods_path, _out_path))
    wrong_uuid_expected = []
    for _irods_path, _out_path in zip(irods_files_list, wrong_uuid_output_dir_list):
        wrong_uuid_expected.append((_irods_path, _out_path))

    # Test with correct assay UUID - directory structure same as in SODAR
    actual = pull_processed_data.pair_ipath_with_outdir(
        remote_files_dict=remote_files_vcf, output_dir=out_dir, assay_uuid=assay_uuid
    )
    assert sorted(actual) == sorted(correct_uuid_expected)

    # Test with wrong assay UUID - all files copied to root of output directory
    actual = pull_processed_data.pair_ipath_with_outdir(
        remote_files_dict=remote_files_vcf, output_dir=out_dir, assay_uuid=wrong_assay_uuid
    )
    assert sorted(actual) == sorted(wrong_uuid_expected)


def test_pull_processed_data_pair_ipath_with_outdir_empty(pull_processed_data):
    """Tests PullProcessedDataCommand.pull_processed_data - no files"""
    # Define input
    out_dir = "out_dir"
    assay_uuid = "99999999-aaa-bbbb-cccc-99999999"

    # Test with correct assay UUID
    actual = pull_processed_data.pair_ipath_with_outdir(
        remote_files_dict={}, output_dir=out_dir, assay_uuid=assay_uuid
    )
    assert len(actual) == 0
