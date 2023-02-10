"""Tests for ``cubi_tk.snappy.pull_sheets``.
"""
import json
import pathlib
import pytest

from cubi_tk.common import CommonConfig
from cubi_tk.snappy.pull_sheets import PullSheetsConfig, build_sheet


@pytest.fixture
def pull_sheet_config():
    """Returns empty PullSheetsConfig object"""
    global_config = CommonConfig(
        **{"verbose": False, "sodar_api_token": "__secret__", "sodar_server_url": "url"}
    )
    args = {
        "global_config": global_config,
        "base_path": None,
        "yes": False,
        "dry_run": False,
        "show_diff": False,
        "show_diff_side_by_side": False,
        "library_types": ("WES", "RNA_seq"),
        "first_batch": 0,
        "last_batch": None,
        "tsv_shortcut": "germline",
    }
    return PullSheetsConfig(**args)


def load_germline_isa_dict():
    """Loads mock results from ``sodar_cli.api.samplesheet.export`` call for germline ISA tab."""
    path = (
        pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / "isa_dict_germline.txt"
    )
    with open(path, "r") as file:
        return json.load(file)


def test_build_sheet_germline(mocker, pull_sheet_config):
    """Tests ``build_sheet()`` - for germline ISA tab"""
    path = pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / "sheet.tsv"
    with open(path, "r") as file:
        expected = "".join(file.readlines())
    mocker.patch("sodar_cli.api.samplesheet.export", return_value=load_germline_isa_dict())
    actual = build_sheet(config=pull_sheet_config, project_uuid="")
    assert actual == expected
