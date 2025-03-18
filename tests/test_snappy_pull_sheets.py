"""Tests for ``cubi_tk.snappy.pull_sheets``."""

from argparse import Namespace
import json
import pathlib
from unittest.mock import MagicMock, patch


from cubi_tk.snappy.pull_sheets import build_sheet
from cubi_tk.sodar.models import Assay, Investigation, OntologyTermRef, Study
from cubi_tk.sodar_api import SodarApi


def load_isa_dict(dictName):
    """Loads mock results from ``samplesheet.export`` call for germline ISA tab."""
    path = pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / dictName
    with open(path, "r") as file:
        return json.load(file)


def return_api_investigation_mock():
    investigation = Investigation(
    sodar_uuid="c339b4de-23a9-4cc3-8801-5f65b4739680",
    archive_name="None",
    comments={"Created With Configuration": "/path/to/isa-configurations/bih_studies/bih_cancer", "Last Opened With Configuration": "bih_cancer"},
    description="",
    file_name="i_Investigation.txt",
    identifier="",
    irods_status=True,
    parser_version="0.2.9",
    project="ad002ac2-b06c-4012-9dc4-8c2ade3e7378",
    studies={
        "7b5f6a28-92d0-4871-8cba-8c74db8ee298":
        Study(
            sodar_uuid="7b5f6a28-92d0-4871-8cba-8c74db8ee298",
            identifier="investigation_title",
            file_name="s_investigation_title.txt",
            irods_path="/sodarZone/projects/ad/ad002ac2-b06c-4012-9dc4-8c2ade3e7378/sample_data/study_7b5f6a28-92d0-4871-8cba-8c74db8ee298",
            title="Investigation Title",
            description="",
            comments={"Study Grant Number": "", "Study Funding Agency": ""},
            assays={
                "992dc872-0033-4c3b-817b-74b324327e7d":
                Assay(
                    sodar_uuid="992dc872-0033-4c3b-817b-74b324327e7d",
                    file_name="a_investigation_title_exome_sequencing_second.txt",
                    irods_path="/sodarZone/projects/ad/ad002ac2-b06c-4012-9dc4-8c2ade3e7378/sample_data/study_7b5f6a28-92d0-4871-8cba-8c74db8ee298/assay_992dc872-0033-4c3b-817b-74b324327e7d",
                    technology_platform="Illumina",
                    technology_type=OntologyTermRef(name="nucleotide sequencing", accession="http://purl.obolibrary.org/obo/OBI_0000626", ontology_name="OBI"),
                    measurement_type=OntologyTermRef(name="exome sequencing", accession=None, ontology_name=None), comments={}
                    ),
                "bd3e98a0-e2a9-48ad-b2bc-d10d407307f2":
                Assay(
                    sodar_uuid="bd3e98a0-e2a9-48ad-b2bc-d10d407307f2",
                    file_name="a_investigation_title_exome_sequencing.txt",
                    irods_path="/sodarZone/projects/ad/ad002ac2-b06c-4012-9dc4-8c2ade3e7378/sample_data/study_7b5f6a28-92d0-4871-8cba-8c74db8ee298/assay_bd3e98a0-e2a9-48ad-b2bc-d10d407307f2",
                    technology_platform="Illumina", technology_type=OntologyTermRef(name="nucleotide sequencing", accession="http://purl.obolibrary.org/obo/OBI_0000626", ontology_name="OBI"),
                    measurement_type=OntologyTermRef(name="exome sequencing", accession=None, ontology_name=None), comments={}
                    )
                }
            )
        },
    title="Investigation Title"
    )
    return investigation


@patch("cubi_tk.sodar_api.requests.get")
def test_build_sheet_germline(mocker):
    """Tests ``build_sheet()`` - for germline ISA tab"""
    args = Namespace( verbose = False,
            config = None,
            sodar_api_token= None,
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
            sodar_api_token= None,
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

@patch("cubi_tk.sodar_api.requests.get")
@patch("sodar_cli.api.samplesheet.retrieve")
def test_build_sheet_cancer_multiassay(mocker, mocker_sodar_api):
    """Tests ``build_sheet()`` - for cancer ISA tab"""
    args = Namespace( verbose = False,
            config = None,
            sodar_api_token= None,
            sodar_server_url= None,
            base_path= None,
            yes= False,
            dry_run= False,
            show_diff= False,
            show_diff_side_by_side= False,
            library_types= ("WES", "RNA_seq"),
            first_batch= 0,
            last_batch= None,
            project_uuid="",
            tsv_shortcut= "cancer",
            assay_uuid= "992dc872-0033-4c3b-817b-74b324327e7d"
    )
    path = pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / "sheet_cancer.tsv"
    with open(path, "r") as file:
        expected = "".join(file.readlines())
    mocker_sodar_api.return_value.status_code = 200
    mocker_sodar_api.return_value.json = MagicMock(return_value=load_isa_dict("isa_dict_cancer_multiassay.txt"))
    mocker.return_value=return_api_investigation_mock()

    actual = build_sheet(args=args, project_uuid="", sodar_api=SodarApi(args, set_default=True))
    assert actual == expected
