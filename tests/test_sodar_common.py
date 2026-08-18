from argparse import Namespace
import io
import pytest
from unittest.mock import patch, MagicMock

from cubi_tk.sodar_common import SodarIngestBase
from cubi_tk.api_models import LandingZone
from cubi_tk.exceptions import ParameterException


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
