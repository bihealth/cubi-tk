"""Tests for ``cubi_tk.snappy.pull_sheets``."""

from argparse import Namespace
import json
import pathlib
from unittest.mock import MagicMock, patch

import cattr


from cubi_tk.snappy.pull_sheets import build_sheet
from cubi_tk.sodar_api import SodarApi
from tests.factories import return_api_investigation_mock


def load_isa_dict(dictName):
    """Loads mock results from ``samplesheet.export`` call for germline ISA tab."""
    path = pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / dictName
    with open(path, "r") as file:
        return json.load(file)


@patch("cubi_tk.sodar_api.requests.get")
def test_build_sheet_germline(mocker):
    """Tests ``build_sheet()`` - for germline ISA tab"""
    args = Namespace( verbose = False,
            config = None,
            sodar_api_token= "****",
            sodar_server_url= None,
            base_path= None,
            yes= False,
            dry_run= False,
            show_diff= False,
            show_diff_side_by_side= False,
            library_types= ("WES", "RNA_seq"),
            first_batch= 0,
            last_batch= None,
            tsv_shortcut= "germline",
            project_uuid="",
            assay_uuid= None
    )
    path = pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / "sheet_germline.tsv"
    with open(path, "r") as file:
        expected = "".join(file.readlines())
    mocker.return_value.status_code = 200
    mocker.return_value.json = MagicMock(return_value=load_isa_dict("isa_dict_germline.txt"))
    actual = build_sheet(args=args, project_uuid="", sodar_api=SodarApi(args, set_default=True))
    assert actual == expected

@patch("cubi_tk.sodar_api.requests.get")
def test_build_sheet_cancer(mocker):
    """Tests ``build_sheet()`` - for cancer ISA tab"""
    args = Namespace( verbose = False,
            config = None,
            sodar_api_token= "****",
            sodar_server_url= None,
            base_path= None,
            yes= False,
            dry_run= False,
            show_diff= False,
            show_diff_side_by_side= False,
            library_types= ("WES", "RNA_seq"),
            first_batch= 0,
            last_batch= None,
            tsv_shortcut= "cancer",
            project_uuid="",
            assay_uuid= None
    )
    path = pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / "sheet_cancer.tsv"
    with open(path, "r") as file:
        expected = "".join(file.readlines())
    mocker.return_value.status_code = 200
    mocker.return_value.json = MagicMock(return_value=load_isa_dict("isa_dict_cancer.txt"))
    actual = build_sheet(args=args, project_uuid="", sodar_api=SodarApi(args, set_default=True))
    assert actual == expected


def test_build_sheet_cancer_multiassay(requests_mock):
    """Tests ``build_sheet()`` - for cancer ISA tab"""
    args = Namespace( verbose = False,
            config = None,
            sodar_api_token= "****",
            sodar_server_url= None,
            base_path= None,
            yes= False,
            dry_run= False,
            show_diff= False,
            show_diff_side_by_side= False,
            library_types= ("WES", "RNA_seq"),
            first_batch= 0,
            last_batch= None,
            project_uuid="1234",
            tsv_shortcut= "cancer",
            assay_uuid= "992dc872-0033-4c3b-817b-74b324327e7d"
    )
    path = pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / "sheet_cancer.tsv"
    with open(path, "r") as file:
        expected = "".join(file.readlines())
    requests_mock.register_uri("GET", "https://sodar-staging.bihealth.org/samplesheets/api/export/json/1234", json=load_isa_dict("isa_dict_cancer_multiassay.txt"), status_code= 200)
    requests_mock.register_uri("GET", "https://sodar-staging.bihealth.org/samplesheets/api/investigation/retrieve/1234", json= cattr.unstructure(return_api_investigation_mock()), status_code= 200)

    actual = build_sheet(args=args, project_uuid="", sodar_api=SodarApi(args, set_default=True))
    assert actual == expected
