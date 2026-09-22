import unittest
from argparse import Namespace
from copy import deepcopy
import io
from typing import Callable

import pytest
import os
import re
from unittest.mock import patch, MagicMock
from pathlib import Path

from cubi_tk.sodar_common import SodarIngestBase, SodarPullBase
from cubi_tk.irods_common import TransferJob
from cubi_tk.api_models import LandingZone, IrodsDataObject
from cubi_tk.exceptions import ParameterException

#############################
# Tests for SodarIngestBase #
#############################
def get_SodarIngestBase(dest="123e4567-e89b-12d3-a456-426655440000", select_lz=None, yes=False):
    args = Namespace(
        config_profile="global",
        sodar_server_url="https://sodar-dummy.bihealth.org/",
        sodar_api_token="token123",
        dry_run=False,
        overwrite=False,
        remote_checksums=False,
        yes=yes,
        validate_and_move=False,
        parallel_checksum_jobs=1,
        recompute_checksums=False,
        select_lz=select_lz,
        destination=dest,
    )
    return SodarIngestBase(args)


def test_sodar_ingest_base_check_args():
    args_ok = Namespace(
        dry_run=False,
        overwrite=False,
        remote_checksums=False,
        yes=False,
        validate_and_move=False,
        parallel_checksum_jobs=1,
        recompute_checksums=False,
        select_lz=None,
        destination="123e4567-e89b-12d3-a456-426655440000",
    )

    # All required args present
    assert SodarIngestBase.check_args(MagicMock(), args_ok) == 0

    # Missing dest
    args_missing = Namespace(
        dry_run=False,
        overwrite=False,
        remote_checksums=False,
        yes=False,
        validate_and_move=False,
        parallel_checksum_jobs=1,
        recompute_checksums=False,
        select_lz=None,
    )
    assert SodarIngestBase.check_args(MagicMock(), args_missing) == 1

    # --yes incompatible with --select-lz manual.
    args_invalid = Namespace(
        dry_run=False,
        overwrite=False,
        remote_checksums=False,
        yes=True,
        validate_and_move=False,
        parallel_checksum_jobs=1,
        recompute_checksums=False,
        select_lz="manual",
        destination="123e4567-e89b-12d3-a456-426655440000",
    )
    with pytest.raises(ValueError, match="can not be combined"):
        SodarIngestBase.check_args(MagicMock(), args_invalid)


@patch("cubi_tk.sodar_common.iRODSTransfer", return_value=MagicMock())
@patch("cubi_tk.sodar_common.SodarIngestBase._create_lz")
@patch("cubi_tk.sodar_common.SodarApi.get_landingzone_list")
@patch("cubi_tk.sodar_common.SodarApi.get_landingzone_retrieve")
@patch("cubi_tk.sodar_common.SodarIngestBase._get_lz_info")
def test_sodar_ingest_base__get_landing_zone(
    mock_lz_info,
    mock_get_landingzone_retrieve,
    mock_api,
    mock_create_lz,
    irods_mock,
    caplog,
    monkeypatch,
):
    new_lz = ("123e4567-e89b-12d3-a456-426655440000", "/sodar/dummy/path/created")
    mock_create_lz.return_value = new_lz
    mock_lz_info.return_value = new_lz
    lzs = [
        LandingZone(
            sodar_uuid=f"123e4567-e89b-12d3-a456-426655440000{i + 1}",
            date_modified=date,
            status="ACTIVE",
            status_locked=False,
            project="123e4567-e89b-12d3-a456-426655440000",
            title="20200101_1{i}0000",
            description="",
            user="",
            assay="",
            status_info="",
            irods_path=f"/sodar/dummy/path/lz{i + 1}",
        )
        for i, date in enumerate(
            (
                "2020-01-01T10:00:00.0000+01:00",
                "2020-01-01T14:00:00.0000+01:00",
                "2020-01-01T12:00:00.0000+01:00",
            )
        )
    ]
    lz1 = ("123e4567-e89b-12d3-a456-4266554400001", "/sodar/dummy/path/lz1")
    lz2 = ("123e4567-e89b-12d3-a456-4266554400002", "/sodar/dummy/path/lz2")
    lz3 = ("123e4567-e89b-12d3-a456-4266554400003", "/sodar/dummy/path/lz3")
    mock_get_landingzone_retrieve.return_value = None
    # No existing LZ
    mock_api.return_value = []
    SIB = get_SodarIngestBase()
    assert new_lz == SIB._get_landing_zone()
    mock_create_lz.assert_called_once_with(noninteractive_override=False)
    assert (
        caplog.messages[-1]
        == "No active landing zones found or selection mode is 'create', creating a new one..."
    )
    mock_create_lz.reset_mock()

    # With --yes or --select-lz != manual a single existing LZ is always used (unless select_mode is "manual" or "create")
    mock_api.return_value = [lzs[0]]
    SIB = get_SodarIngestBase(yes=True)
    assert lz1 == SIB._get_landing_zone()
    SIB = get_SodarIngestBase(select_lz="oldest")
    assert lz1 == SIB._get_landing_zone(select_mode=SIB.args.select_lz)
    assert caplog.messages[-1] == f"Single active landingzone with UUID {lz1[0]} will be used"
    SIB = get_SodarIngestBase(select_lz="last_used")
    assert lz1 == SIB._get_landing_zone(select_mode=SIB.args.select_lz)
    assert caplog.messages[-1] == f"Single active landingzone with UUID {lz1[0]} will be used"

    # Several existing LZ (oldest to newest creation, middle once modified last)
    mock_api.return_value = sorted(lzs, key=lambda lz: lz.date_modified)
    # - 'last_used' here is the middle LZ
    assert lz2 == SIB._get_landing_zone(select_mode=SIB.args.select_lz)
    assert (
        caplog.messages[-1]
        == f"Newest active landingzone (by modification) with UUID {lz2[0]} will be used"
    )
    # - With 'newest' or --yes and no --select-lz get the last_created LZ:
    mock_api.return_value = lzs
    SIB = get_SodarIngestBase(select_lz="newest")
    assert lz3 == SIB._get_landing_zone(select_mode=SIB.args.select_lz)
    SIB = get_SodarIngestBase(yes=True)
    assert lz3 == SIB._get_landing_zone()
    assert (
        caplog.messages[-1]
        == f"Newest active landingzone (by creation) with UUID {lz3[0]} will be used"
    )
    # - without '--yes', also asks for user input without --select-lz
    # dont use existing one, create new one
    SIB = get_SodarIngestBase()
    monkeypatch.setattr("sys.stdin", io.StringIO("n\n"))
    assert new_lz == SIB._get_landing_zone()
    assert caplog.messages[-1] == "Creating a new landing zone..."
    # - 'oldest'
    SIB = get_SodarIngestBase(select_lz="oldest")
    assert lz1 == SIB._get_landing_zone(select_mode=SIB.args.select_lz)
    assert caplog.messages[-1] == f"Oldest active landingzone with UUID {lz1[0]} will be used"

    # - 'manual' asks for user input
    SIB = get_SodarIngestBase(select_lz="manual", yes=False)
    monkeypatch.setattr("sys.stdin", io.StringIO("a\n100\n1\n"))
    assert lz1 == SIB._get_landing_zone(select_mode=SIB.args.select_lz)
    # - 'manual'  allows selecting a new lz via 0 (no second question for creation confirmation)
    monkeypatch.setattr("sys.stdin", io.StringIO("0\n"))
    mock_create_lz.reset_mock()
    assert new_lz == SIB._get_landing_zone(select_mode=SIB.args.select_lz)
    mock_create_lz.assert_called_once_with(noninteractive_override=True)


@patch("cubi_tk.sodar_common.iRODSTransfer", return_value=MagicMock())
@patch("cubi_tk.sodar_common.SodarApi.get_landingzone_list")
@patch("cubi_tk.sodar_common.SodarApi.get_landingzone_retrieve")
def test_sodar_ingest_base__get_lz_info_lz_path(
    mock_get_landingzone_retrieve, mock_lz_list, mock_api
):
    SIB = get_SodarIngestBase()
    SIB.sodar_api.lz_path = "/sodar/dummy/path/lz1"
    SIB.sodar_api.assay_uuid = None
    lz = LandingZone(
        sodar_uuid="123e4567-e89b-12d3-a456-426655440001",
        date_modified="2020-01-01T10:00:00.0000+01:00",
        status="ACTIVE",
        status_locked=False,
        project="123e4567-e89b-12d3-a456-426655440000",
        title="20200101_10000",
        description="",
        user="",
        assay="assay-uuid-1",
        status_info="",
        irods_path="/sodar/dummy/path/lz1",
    )
    mock_lz_list.return_value = [lz]
    assert SIB._get_lz_info() == (lz.sodar_uuid, lz.irods_path)
    assert SIB.sodar_api.assay_uuid == lz.assay


@patch("cubi_tk.sodar_common.iRODSTransfer", return_value=MagicMock())
@patch("cubi_tk.sodar_common.SodarApi.get_landingzone_retrieve")
def test_sodar_ingest_base__get_lz_info_project_uuid_uses_lz(
    mock_get_landingzone_retrieve, mock_api
):
    SIB = get_SodarIngestBase()
    SIB.sodar_api.project_uuid = "123e4567-e89b-12d3-a456-426655440000"
    SIB.sodar_api.lz_path = None
    lz = LandingZone(
        sodar_uuid="123e4567-e89b-12d3-a456-426655440001",
        date_modified="2020-01-01T10:00:00.0000+01:00",
        status="ACTIVE",
        status_locked=False,
        project="123e4567-e89b-12d3-a456-426655440000",
        title="20200101_10000",
        description="",
        user="",
        assay="assay-uuid-1",
        status_info="",
        irods_path="/sodar/dummy/path/lz1",
    )
    mock_get_landingzone_retrieve.return_value = lz
    assert SIB._get_lz_info() == (lz.sodar_uuid, lz.irods_path)


@patch("cubi_tk.sodar_common.iRODSTransfer", return_value=MagicMock())
@patch("cubi_tk.sodar_common.SodarIngestBase._get_landing_zone")
@patch(
    "cubi_tk.sodar_common.SodarApi.get_samplesheet_investigation_retrieve", return_value=object()
)
@patch("cubi_tk.sodar_common.SodarApi.get_landingzone_retrieve")
def test_sodar_ingest_base__get_lz_info_project_uuid_selects_new_lz(
    mock_get_landingzone_retrieve, mock_samplesheet_investigation_retrieve, mock_get_lz, mock_api
):
    SIB = get_SodarIngestBase()
    SIB.sodar_api.project_uuid = "123e4567-e89b-12d3-a456-426655440000"
    SIB.sodar_api.lz_path = None
    target = ("123e4567-e89b-12d3-a456-426655440777", "/sodar/dummy/path/selected")
    mock_get_lz.return_value = target
    mock_get_landingzone_retrieve.return_value = None
    assert SIB._get_lz_info() == target


@patch("cubi_tk.sodar_common.iRODSTransfer", return_value=MagicMock())
@patch("cubi_tk.sodar_common.SodarApi.get_samplesheet_investigation_retrieve", return_value=None)
@patch("cubi_tk.sodar_common.SodarApi.get_landingzone_retrieve")
def test_sodar_ingest_base__get_lz_info_invalid_project_uuid(
    mock_get_landingzone_retrieve, mock_samplesheet_investigation_retrieve, mock_api
):
    SIB = get_SodarIngestBase()
    SIB.sodar_api.project_uuid = "123e4567-e89b-12d3-a456-426655440000"
    SIB.sodar_api.lz_path = None
    mock_get_landingzone_retrieve.return_value = None
    with pytest.raises(ParameterException, match="could neither be associated with a project"):
        SIB._get_lz_info()


def test_sodar_ingest_base_warnings():
    raise NotImplementedError

    _no_files_found_warning

    _select_lz_warning


def test_sodar_ingest_base_execute():
    raise NotImplementedError


###########################
# Tests for SodarPullBase #
###########################
def make_data_obj(path):
    return IrodsDataObject(
        name=Path(path).name, type="file", path=path, size=0, modify_time="", checksum=""
    )


@pytest.fixture
def filtered_data_objects():
    return {
        "coll1-N1-DNA1": [
            make_data_obj(path="/irods/project/coll1-N1-DNA1/subcol1/file1.vcf.gz"),
            make_data_obj(path="/irods/project/coll1-N1-DNA1/subcol2/file1.vcf.gz"),
            make_data_obj(path="/irods/project/coll1-N1-DNA1/subcol1/miscFile.txt"),
        ],
        "coll2-N1-DNA1": [
            make_data_obj(path="/irods/project/coll2-N1-DNA1/subcol1/file2.vcf.gz"),
            make_data_obj(path="/irods/project/coll2-N1-DNA1/subcol1/file2.bam"),
            make_data_obj(path="/irods/project/coll2-N1-DNA1/subcol1/miscFile.txt"),
        ],
    }


def test_sodarpullbase_filter_irods_collection(filtered_data_objects):
    fake_irods_data_dict = {
        "file1.vcf.gz": [
            make_data_obj(path="/irods/project/coll1-N1-DNA1/subcol1/file1.vcf.gz"),
            make_data_obj(path="/irods/project/coll1-N1-DNA1/subcol2/file1.vcf.gz"),
        ],
        "file2.vcf.gz": [
            make_data_obj(path="/irods/project/coll2-N1-DNA1/subcol1/file2.vcf.gz"),
        ],
        "file2.bam": [
            make_data_obj(path="/irods/project/coll2-N1-DNA1/subcol1/file2.bam"),
        ],
        "miscFile.txt": [
            make_data_obj(path="/irods/project/coll1-N1-DNA1/subcol1/miscFile.txt"),
            make_data_obj(path="/irods/project/coll2-N1-DNA1/subcol1/miscFile.txt"),
        ],
    }

    kwarg_list = [
        # No filters at all -> all files
        {"file_patterns": [], "samples": [], "substring_match": False},
        # Test filepattern filter works
        {"file_patterns": ["*.vcf.gz"], "samples": [], "substring_match": False},
        # Test file pattern with mutiple patterns, also **/*.X & *.Y
        {"file_patterns": ["*.vcf.gz", "**/*.txt"], "samples": [], "substring_match": False},
        # Test Sample/Collection filter works
        {"file_patterns": [], "samples": ["coll1-N1-DNA1"], "substring_match": False},
        # Test substring matching works
        {"file_patterns": [], "samples": ["coll1"], "substring_match": True},
    ]

    expected_results = [
        deepcopy(filtered_data_objects),
        {
            k: [v for v in l if v.path.endswith("vcf.gz")]
            for k, l in deepcopy(filtered_data_objects).items()
        },
        {
            k: [v for v in l if not v.path.endswith("bam")]
            for k, l in deepcopy(filtered_data_objects).items()
        },
        {k: l for k, l in deepcopy(filtered_data_objects).items() if k == "coll1-N1-DNA1"},
        {k: l for k, l in deepcopy(filtered_data_objects).items() if k == "coll1-N1-DNA1"},
    ]

    for kwargs, expected in zip(kwarg_list, expected_results, strict=True):
        result = SodarPullBase.filter_irods_file_list(
            fake_irods_data_dict, "/irods/project", **kwargs
        )
        assert result == expected

    # TODO: add tests for `common_assay_path` (different assays in project)


def get_SodarPullBase(yes=False, output_dir='/path/to/output'):
    args = Namespace(
        config_profile="global",
        sodar_server_url="https://sodar-dummy.bihealth.org/",
        sodar_api_token="token123",
        dry_run=False,
        overwrite=False,
        yes=yes,
        project_uuid="123e4567-e89b-12d3-a456-426655440000",
        output_dir = output_dir
    )
    return SodarPullBase(args)

@patch("cubi_tk.sodar_common.iRODSTransfer", return_value=MagicMock())
@patch("cubi_tk.sodar_common.RetrieveSodarCollection")
def test_sodarpullbase_build_jobs(mock_retrieve, filtered_data_objects, fs):

    testinstance = get_SodarPullBase()
    mock_retrieve.yes = testinstance.args.yes

    id_func = lambda i: i
    def get_expected_out(base: str = os.getcwd(), file_mod_func: Callable = id_func):
        return [
            TransferJob(
                path_remote=obj.path, path_local=file_mod_func(obj.path.replace("/irods/project", base))
            )
            for k, l in filtered_data_objects.items()
            for obj in l
        ]
    # base functionality: write to CWD and replicate irods file structure
    assert testinstance.build_jobs(filtered_data_objects, "/irods/project") == get_expected_out()

    # Test with modified output basepath
    with patch("cubi_tk.sodar_common.SodarPullBase.get_output_basepath", return_value='/path/to/data'):
        assert testinstance.build_jobs(filtered_data_objects, "/irods/project") == get_expected_out('/path/to/data')

    # Test with modified output filepath (from irods)
    expected_out = get_expected_out(file_mod_func = lambda s: re.sub(r'/subcol[12]/', '/', s))
    with patch("cubi_tk.sodar_common.SodarPullBase.get_output_filepath", lambda d: "{collection}/{filename}".format(**d) ):
        assert testinstance.build_jobs(filtered_data_objects, "/irods/project") == expected_out


def test_sodarpullbase_parse_sample_tsv():
    # Test on Biomedsheet
    samples = SodarPullBase.parse_sample_tsv(
        Path(__file__).resolve().parent / "data" / "pull_sheets" / "sheet_germline.tsv",
        sample_col=2,
        skip_rows=12,
    )
    assert samples == {"index", "mother", "father"}


def test_sodarpullbase_report_no_files(caplog):
    assert SodarPullBase._no_files_found_warning([1,2,3]) == 0
    assert len(caplog.messages) == 0
    assert SodarPullBase._no_files_found_warning([]) == 1
    assert caplog.messages[0] == "No files for download were found!"


def test_sodarpullbase_get_functions():
    raise NotImplementedError

    # def get_sample_list(self) -> set[str]:
    #     """Function to get samples to filter downloadable files by collection"""
    #     logger.debug(
    #         f"`cubi-tk {self.cubitk_section} {self.command_name}` does not implement it's own `get_sample_list` function, using all samples by default."
    #     )
    #     return set()
    #
    # def get_file_patterns(self) -> list[str]:
    #     """Function to get samples to filter downloadable files by collection"""
    #     logger.debug(
    #         f"`cubi-tk {self.cubitk_section} {self.command_name}` does not implement it's own `get_file_patterns` function, using all files by default."
    #     )
    #     return []
    #
    # def get_substring_match(self) -> bool:
    #     """Function to get samples to filter downloadable files by collection"""
    #     logger.debug(
    #         f"`cubi-tk {self.cubitk_section} {self.command_name}` does not implement it's own `get_substring_match` function, not using substring_match by default."
    #     )
    #     return False

    # def get_output_basepath(self) -> str:
    #     """Abstract method for output_path"""
    #     logger.debug(
    #         f"`cubi-tk {self.cubitk_section} {self.command_name}` does not implement it's own `get_output_basepath` function, using CWD by default."
    #     )
    #     return os.getcwd()
    #
    # def get_output_filepath(self, out_parts: FilePathParts) -> str:
    #     """Abstract method for output_path"""
    #     logger.debug(
    #         f"`cubi-tk {self.cubitk_section} {self.command_name}` does not implement it's own `get_output_filepath` function, using pattern from iRODs by default."
    #     )
    #     return "{collection}/{subcollections}/{filename}".format(**out_parts)


# TODO: Tests for RetrieveSodarCollection

def test_sodarpullbase_execute():
    raise NotImplementedError

def test_sodar_common_RetrieveSodarCollection():
    raise NotImplementedError

    test: RetrieveSodarCollection.perform()
