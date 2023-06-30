"""Tests for ``cubi_tk.snappy.pull_data_common``.
"""

import pytest

from cubi_tk.snappy.pull_data_common import PullDataCommon
from .helpers import createIrodsDataObject as IrodsDataObject

# Empty file MD5 checksum
FILE_MD5SUM = "d41d8cd98f00b204e9800998ecf8427e"

# Arbitrary replicas MD5 checksum value
REPLICAS_MD5SUM = [FILE_MD5SUM] * 3


@pytest.fixture
def irods_objects_list():
    """Returns list of iRODS objects for VCF file with three different dates. Format: '%Y-%m-%d'."""
    p0001_sodar_path = "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P001-N1-DNA1-WES1/%s"
    return [
        IrodsDataObject(
            file_name="bwa.P001-N1-DNA1-WES1.vcf.gz",
            irods_path=f"{p0001_sodar_path % '1999-09-09'}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz",
            file_md5sum=FILE_MD5SUM,
            replicas_md5sum=REPLICAS_MD5SUM,
        ),
        IrodsDataObject(
            file_name="bwa.P001-N1-DNA1-WES1.vcf.gz",
            irods_path=f"{p0001_sodar_path % '2038-01-19'}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz",
            file_md5sum=FILE_MD5SUM,
            replicas_md5sum=REPLICAS_MD5SUM,
        ),
        IrodsDataObject(
            file_name="bwa.P001-N1-DNA1-WES1.vcf.gz",
            irods_path=f"{p0001_sodar_path % '2000-01-01'}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz",
            file_md5sum=FILE_MD5SUM,
            replicas_md5sum=REPLICAS_MD5SUM,
        ),
    ]


@pytest.fixture
def irods_objects_list_format_mixed():
    """Returns list of iRODS objects for VCF file with three different dates. Multiple date formats."""
    p0001_sodar_path = "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P001-N1-DNA1-WES1/%s"
    return [
        IrodsDataObject(
            file_name="bwa.P001-N1-DNA1-WES1.vcf.gz",
            irods_path=f"{p0001_sodar_path % '1999-09-09'}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz",
            file_md5sum=FILE_MD5SUM,
            replicas_md5sum=REPLICAS_MD5SUM,
        ),
        IrodsDataObject(
            file_name="bwa.P001-N1-DNA1-WES1.vcf.gz",
            irods_path=f"{p0001_sodar_path % '2038_01_19'}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz",
            file_md5sum=FILE_MD5SUM,
            replicas_md5sum=REPLICAS_MD5SUM,
        ),
        IrodsDataObject(
            file_name="bwa.P001-N1-DNA1-WES1.vcf.gz",
            irods_path=f"{p0001_sodar_path % '20000101'}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz",
            file_md5sum=FILE_MD5SUM,
            replicas_md5sum=REPLICAS_MD5SUM,
        ),
    ]


@pytest.fixture
def irods_objects_list_missing_date():
    """Returns list of iRODS objects for VCF file with three different dates."""
    p0001_sodar_path = (
        "/sodar_path/.../assay_99999999-aaa-bbbb-cccc-99999999/P001-N1-DNA1-WES1/NO_DATE"
    )
    return [
        IrodsDataObject(
            file_name="bwa.P001-N1-DNA1-WES1.vcf.gz",
            irods_path=f"{p0001_sodar_path}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz",
            file_md5sum=FILE_MD5SUM,
            replicas_md5sum=REPLICAS_MD5SUM,
        ),
        IrodsDataObject(
            file_name="bwa.P001-N1-DNA1-WES1.vcf.gz",
            irods_path=f"{p0001_sodar_path}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz",
            file_md5sum=FILE_MD5SUM,
            replicas_md5sum=REPLICAS_MD5SUM,
        ),
        IrodsDataObject(
            file_name="bwa.P001-N1-DNA1-WES1.vcf.gz",
            irods_path=f"{p0001_sodar_path}/variant_calling/bwa.P001-N1-DNA1-WES1.vcf.gz",
            file_md5sum=FILE_MD5SUM,
            replicas_md5sum=REPLICAS_MD5SUM,
        ),
    ]


def test_pull_data_common_sort_irods_object_by_date_in_path(irods_objects_list):
    """Tests PullDataCommon.sort_irods_object_by_date_in_path() - format '%Y-%m-%d'"""
    raw_data_class = PullDataCommon()
    expected = ("2038-01-19", "2000-01-01", "1999-09-09")
    actual = raw_data_class.sort_irods_object_by_date_in_path(irods_obj_list=irods_objects_list)
    for count, irods_obj in enumerate(actual):
        assert expected[count] in irods_obj.path


def test_pull_data_common_sort_irods_object_by_date_in_path_mixed(irods_objects_list_format_mixed):
    """Tests PullDataCommon.sort_irods_object_by_date_in_path() - mixed dates formats"""
    raw_data_class = PullDataCommon()
    expected = ("2038_01_19", "20000101", "1999-09-09")
    actual = raw_data_class.sort_irods_object_by_date_in_path(
        irods_obj_list=irods_objects_list_format_mixed
    )
    for count, irods_obj in enumerate(actual):
        assert expected[count] in irods_obj.path


def test_pull_data_common_sort_irods_object_by_date_in_path_missing_date(
    irods_objects_list_missing_date,
):
    """Tests PullDataCommon.sort_irods_object_by_date_in_path() - missing date in path"""
    raw_data_class = PullDataCommon()
    with pytest.raises(ValueError):
        raw_data_class.sort_irods_object_by_date_in_path(
            irods_obj_list=irods_objects_list_missing_date
        )
