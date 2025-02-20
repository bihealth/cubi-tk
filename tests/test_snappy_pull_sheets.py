"""Tests for ``cubi_tk.snappy.pull_sheets``."""

import json
import pathlib

import pytest

from cubi_tk.common import CommonConfig
from cubi_tk.snappy.pull_sheets import PullSheetsConfig, build_sheet
from cubi_tk.sodar.models import Assay, Investigation, OntologyTermRef, Study


def load_isa_dict(dictName):
    """Loads mock results from ``sodar_cli.api.samplesheet.export`` call for germline ISA tab."""
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
        "assay_uuid": None
    }
    return PullSheetsConfig(**args)


def test_build_sheet_germline(mocker, pull_sheet_config):
    """Tests ``build_sheet()`` - for germline ISA tab"""
    path = pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / "sheet_germline.tsv"
    with open(path, "r") as file:
        expected = "".join(file.readlines())
    mocker.patch(
        "sodar_cli.api.samplesheet.export", return_value=load_isa_dict("isa_dict_germline.txt")
    )
    actual = build_sheet(config=pull_sheet_config, project_uuid="")
    assert actual == expected


def test_build_sheet_cancer(mocker, pull_sheet_config):
    """Tests ``build_sheet()`` - for cancer ISA tab"""
    path = pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / "sheet_cancer.tsv"
    with open(path, "r") as file:
        expected = "".join(file.readlines())
    mocker.patch(
        "sodar_cli.api.samplesheet.export", return_value=load_isa_dict("isa_dict_cancer.txt")
    )
    actual = build_sheet(config=pull_sheet_config, tsv_shortcut="cancer", project_uuid="")
    assert actual == expected

def test_build_sheet_cancer_multiassay(mocker, pull_sheet_config):
    """Tests ``build_sheet()`` - for cancer ISA tab"""
    path = pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / "sheet_cancer.tsv"
    with open(path, "r") as file:
        expected = "".join(file.readlines())
    mocker.patch("sodar_cli.api.samplesheet.export", return_value=load_isa_dict("isa_dict_cancer_multiassay.txt"))
    mocker.patch("sodar_cli.api.samplesheet.retrieve", return_value=return_api_investigation_mock())
    actual = build_sheet(config=pull_sheet_config, assay_uuid= "992dc872-0033-4c3b-817b-74b324327e7d", tsv_shortcut="cancer", project_uuid="")
    assert actual == expected
