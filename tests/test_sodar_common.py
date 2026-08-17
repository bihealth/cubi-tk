from argparse import Namespace
import pytest
from unittest.mock import patch, MagicMock

from cubi_tk.sodar_common import SodarIngestBase
from cubi_tk.api_models import LandingZone


def get_SodarIngestBase(dest="123e4567-e89b-12d3-a456-426655440000", select_lz=None, yes=False):
    args = Namespace(
        # config = None,
        config_profile=None,
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
    raise NotImplementedError


@patch("cubi_tk.sodar_common.iRODSTransfer", return_value=MagicMock())
@patch("cubi_tk.sodar_common.SodarIngestBase._create_lz")
@patch("cubi_tk.sodar_common.SodarApi.get_landingzone_list")
def test_sodar_ingest_base__get_landing_zone(mock_api, mock_create_lz, caplog):
    new_lz = ("123e4567-e89b-12d3-a456-426655440000", "/sodar/dummy/path/created")
    mock_create_lz.return_value = new_lz

    lzs = [
        LandingZone(
            sodar_uuid=f"123e4567-e89b-12d3-a456-42665544000{i + 1}",
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

    # No existing LZ
    mock_api.return_value = []
    SIB = get_SodarIngestBase()
    assert new_lz == SIB._get_landing_zone()
    mock_create_lz.assert_called_once()
    assert caplog.messages[-1] == "No active Landing Zone available, creating new one."
    mock_create_lz.reset_mock()

    # With --yes or --select-lz != manual a single existing LZ is always used (unless select_mode is "manual" or "create")
    mock_api.return_value = [lzs[0]]
    SIB = get_SodarIngestBase(yes=True)
    assert lz1 == SIB._get_landing_zone()
    SIB = get_SodarIngestBase(select_lz="oldest")
    assert lz1 == SIB._get_landing_zone()
    assert caplog.messages[-1] == f"Single active landingzone with UUID {lz1[0]} will be used"
    SIB = get_SodarIngestBase(select_lz="last_used")
    assert lz1 == SIB._get_landing_zone()
    assert caplog.messages[-1] == f"Single active landingzone with UUID {lz1[0]} will be used"

    # Several existing LZ (oldest to newest creation, middle once modified last)
    mock_api.return_value = lzs
    # - 'last_used' here is the middle LZ
    assert lz2 == SIB._get_landing_zone()
    assert (
        caplog.messages[-1]
        == f"Newest active landingzone (by modification) with UUID {lz2[0]} will be used"
    )
    # - With 'newest' or --yes and no --select-lz get the last_created LZ:
    SIB = get_SodarIngestBase(select_lz="newest")
    assert lz3 == SIB._get_landing_zone()
    SIB = get_SodarIngestBase(yes=True)
    assert lz3 == SIB._get_landing_zone()
    assert (
        caplog.messages[-1]
        == f"Newest active landingzone (by creation) with UUID {lz3[0]} will be used"
    )
    # - without '--yes', also asks for user input without --select-lz
    SIB = get_SodarIngestBase()
    with patch("builtins.input", side_effect=["n"]):
        assert lz3 == SIB._get_landing_zone()
        assert (
            caplog.messages[-1]
            == f"Newest active landingzone (by creation) with UUID {lz3[0]} will be used"
        )
    # - 'oldest'
    SIB = get_SodarIngestBase(select_lz="oldest")
    assert lz1 == SIB._get_landing_zone()
    assert caplog.messages[-1] == f"Oldest active landingzone with UUID {lz1[0]} will be used"

    # - 'manual' asks for user input
    SIB = get_SodarIngestBase(select_lz="manual", yes=False)
    with patch("builtins.input", side_effect=["a", "100", "1"]):
        assert lz1 == SIB._get_landing_zone()
    # - 'manual'  allows selecting a new lz via 0 (no second question for creation confirmation)
    with patch("builtins.input", side_effect=["0"]):
        assert new_lz == SIB._get_landing_zone()
        mock_create_lz.assert_called_once_with(noninteractive_override=True)


def test_sodar_ingest_base__get_lz_info():
    raise NotImplementedError
