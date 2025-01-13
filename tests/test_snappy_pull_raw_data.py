"""Tests for ``cubi_tk.snappy.pull_raw_data``."""

import pytest

from cubi_tk.__main__ import setup_argparse
from cubi_tk.snappy.pull_raw_data import Config, PullRawDataCommand

from .helpers import createIrodsDataObject as IrodsDataObject

# Empty file MD5 checksum
FILE_MD5SUM = "d41d8cd98f00b204e9800998ecf8427e"

# Arbitrary replicas MD5 checksum value
REPLICAS_MD5SUM = [FILE_MD5SUM] * 3


@pytest.fixture
def pull_raw_data():
    """Returns instantiated PullRawDataCommand"""
    args_dict = {
        "verbose": False,
        "sodar_server_url": "https://sodar.bihealth.org/",
        "sodar_api_token": "__secret__",
        "base_path": ".",
        "sodar_url": "https://sodar.bihealth.org/",
        "dry_run": False,
        "overwrite": False,
        "use_library_name": False,
        "tsv_shortcut": "germline",
        "first_batch": 0,
        "last_batch": None,
        "samples": None,
        "assay_uuid": None,
        "project_uuid": "99999999-aaaa-bbbb-cccc-99999999",
    }
    return PullRawDataCommand(Config(**args_dict))


@pytest.fixture
def remote_files_fastq():
    """Returns iRODS collection example for BAM files and two samples, P001 and P002"""
    p0001_sodar_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P001-N1-DNA1-WES1/1999-09-09"
    )
    p0002_sodar_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P002-N1-DNA1-WES1/1999-09-09"
    )
    return {
        "P001_R1_001.fastq.gz": [
            IrodsDataObject(
                file_name="P001_R1_001.fastq.gz",
                irods_path=f"{p0001_sodar_path}/raw_data/P001_R1_001.fastq.gz",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
        "P001_R2_001.fastq.gz": [
            IrodsDataObject(
                file_name="P001_R2_001.fastq.gz",
                irods_path=f"{p0001_sodar_path}/raw_data/P001_R2_001.fastq.gz",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
        "P002_R1_001.fastq.gz": [
            IrodsDataObject(
                file_name="P002_R1_001.fastq.gz",
                irods_path=f"{p0002_sodar_path}/raw_data/P002_R1_001.fastq.gz",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
        ],
        "P002_R2_001.fastq.gz": [
            IrodsDataObject(
                file_name="P002_R2_001.fastq.gz",
                irods_path=f"{p0002_sodar_path}/raw_data/P002_R2_001.fastq.gz",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            )
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
def remote_files_all(remote_files_fastq, remote_files_vcf):
    """Returns full example of iRODS collection: FASTQ and VCF files"""
    return {**remote_files_fastq, **remote_files_vcf}


@pytest.fixture
def sample_to_irods_dict():
    """Returns example of output from PullRawDataCommand.pair_ipath_with_folder_name() based on sample names."""
    p0001_sodar_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P001-N1-DNA1-WES1/1999-09-09"
    )
    p0002_sodar_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P002-N1-DNA1-WES1/1999-09-09"
    )
    return {
        "P001": [
            IrodsDataObject(
                file_name="P001_R1_001.fastq.gz",
                irods_path=f"{p0001_sodar_path}/raw_data/P001_R1_001.fastq.gz",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
            IrodsDataObject(
                file_name="P001_R2_001.fastq.gz",
                irods_path=f"{p0001_sodar_path}/raw_data/P001_R2_001.fastq.gz",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
        ],
        "P002": [
            IrodsDataObject(
                file_name="P002_R1_001.fastq.gz",
                irods_path=f"{p0002_sodar_path}/raw_data/P002_R1_001.fastq.gz",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
            IrodsDataObject(
                file_name="P002_R2_001.fastq.gz",
                irods_path=f"{p0002_sodar_path}/raw_data/P002_R2_001.fastq.gz",
                file_md5sum=FILE_MD5SUM,
                replicas_md5sum=REPLICAS_MD5SUM,
            ),
        ],
    }


@pytest.fixture
def library_to_irods_dict(sample_to_irods_dict):
    """Returns example of output from PullRawDataCommand.pair_ipath_with_folder_name() based on library names"""
    output_dict = {}
    for key, value in sample_to_irods_dict.items():
        new_key = key + "-N1-DNA1-WES1"
        output_dict[new_key] = value
    return output_dict


def test_run_snappy_pull_raw_help(capsys):
    """Test ``cubi-tk snappy pull-raw-data --help``"""
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "pull-raw-data", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_snappy_pull_raw_nothing(capsys):
    """Test ``cubi-tk snappy pull-raw-data``"""
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "pull-raw-data"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_pull_raw_data_filter_irods_collection(pull_raw_data, remote_files_fastq, remote_files_all):
    """Tests PullRawDataCommand.filter_irods_collection() - FASTQ files"""
    # Define input
    absent_sample_list = ["P098", "P099"]
    samples_list = ["P001", "P002"]
    file_type = "fastq"

    # Call with samples id as identifiers
    actual = pull_raw_data.filter_irods_collection(
        identifiers=samples_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert actual == remote_files_fastq

    # Sanity check - should return empty dictionary, samples aren't present
    actual = pull_raw_data.filter_irods_collection(
        identifiers=absent_sample_list, remote_files_dict=remote_files_fastq, file_type=file_type
    )
    assert len(actual) == 0


def test_pull_raw_data_filter_irods_collection_plus_dir_name(
    pull_raw_data, remote_files_fastq, remote_files_all, library_to_irods_dict
):
    """Tests PullRawDataCommand.filter_irods_collection_plus_dir_name() - FASTQ files"""
    # Define input
    absent_sample_list = ["P098-N1-DNA1-WES1", "P099-N1-DNA1-WES1"]
    samples_list = ["P001-N1-DNA1-WES1", "P002-N1-DNA1-WES1"]
    file_type = "fastq"

    # Call with samples id as identifiers
    actual = pull_raw_data.filter_irods_collection_by_library_name_in_path(
        identifiers=samples_list, remote_files_dict=remote_files_all, file_type=file_type
    )
    assert actual == library_to_irods_dict

    # Sanity check - should return empty dictionary, samples aren't present
    actual = pull_raw_data.filter_irods_collection_by_library_name_in_path(
        identifiers=absent_sample_list, remote_files_dict=remote_files_fastq, file_type=file_type
    )
    assert len(actual) == 0


def test_pull_raw_data_get_library_to_irods_dict(pull_raw_data, remote_files_fastq):
    """Tests PullRawDataCommand.get_library_to_irods_dict()"""
    samples_list = ["P001", "P002"]
    actual = pull_raw_data.get_library_to_irods_dict(
        identifiers=samples_list, remote_files_dict=remote_files_fastq
    )
    for id_ in samples_list:
        assert all(str(irods.name).startswith(id_) for irods in actual.get(id_))


def test_pull_raw_data_pair_ipath_with_folder_name(pull_raw_data, sample_to_irods_dict):
    """Tests PullRawDataCommand.pair_ipath_with_folder_name()"""
    # Define input
    out_dir = "out_dir"
    assay_uuid = "99999999-aaa-bbbb-cccc-99999999"
    wrong_assay_uuid = "11111111-aaa-bbbb-cccc-11111111"
    identifiers_tup = [("P00{i}".format(i=i), "P00{i}".format(i=i)) for i in (1, 2)]

    # Define expected
    irods_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P00{i}-N1-DNA1-WES1/1999-09-09/raw_data/"
        "P00{i}_R{r}_001.{ext}"
    )
    full_out_dir = "out_dir/P00{i}/P00{i}-N1-DNA1-WES1/1999-09-09/raw_data/P00{i}_R{r}_001.{ext}"
    irods_files_list = [
        irods_path.format(i=i, r=r, ext=ext)
        for i in (1, 2)
        for r in (1, 2)
        for ext in ("fastq.gz",)
    ]
    correct_uuid_output_dir_list = [
        full_out_dir.format(i=i, r=r, ext=ext)
        for i in (1, 2)
        for r in (1, 2)
        for ext in ("fastq.gz",)
    ]
    wrong_uuid_output_dir_list = [
        "out_dir/P00{i}/P00{i}_R{r}_001.{ext}".format(i=i, r=r, ext=ext)
        for i in (1, 2)
        for r in (1, 2)
        for ext in ("fastq.gz",)
    ]
    correct_uuid_expected = []
    for _irods_path, _out_path in zip(irods_files_list, correct_uuid_output_dir_list, strict=True):
        correct_uuid_expected.append((_irods_path, _out_path))
    wrong_uuid_expected = []
    for _irods_path, _out_path in zip(irods_files_list, wrong_uuid_output_dir_list, strict=True):
        wrong_uuid_expected.append((_irods_path, _out_path))

    # Test with correct assay UUID - directory structure same as in SODAR
    actual = pull_raw_data.pair_ipath_with_outdir(
        library_to_irods_dict=sample_to_irods_dict,
        identifiers_tuples=identifiers_tup,
        output_dir=out_dir,
        assay_uuid=assay_uuid,
    )
    assert sorted(actual) == sorted(correct_uuid_expected)

    # Test with wrong assay UUID - all files copied to root of output directory
    actual = pull_raw_data.pair_ipath_with_outdir(
        library_to_irods_dict=sample_to_irods_dict,
        identifiers_tuples=identifiers_tup,
        output_dir=out_dir,
        assay_uuid=wrong_assay_uuid,
    )
    assert sorted(actual) == sorted(wrong_uuid_expected)


def test_pull_raw_data_pair_ipath_with_folder_name_empty(pull_raw_data):
    """Tests PullRawDataCommand.pair_ipath_with_folder_name() - no files"""
    # Define input
    out_dir = "out_dir"
    assay_uuid = "99999999-aaa-bbbb-cccc-99999999"
    identifiers_tup = [("P00{i}".format(i=i), "P00{i}".format(i=i)) for i in (1, 2)]
    # Test with correct assay UUID
    actual = pull_raw_data.pair_ipath_with_outdir(
        library_to_irods_dict={},
        identifiers_tuples=identifiers_tup,
        output_dir=out_dir,
        assay_uuid=assay_uuid,
    )
    assert len(actual) == 0
