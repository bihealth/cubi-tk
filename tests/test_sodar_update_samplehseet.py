import argparse
from collections import OrderedDict
from io import StringIO
import json
import pathlib
import re
from unittest.mock import patch

import pandas as pd
import pytest

from cubi_tk.sodar.update_samplehseet import UpdateSamplesheetCommand
from cubi_tk.sodar_api import SodarAPI


@pytest.fixture
def MV_isa_json():
    with open(pathlib.Path(__file__).resolve().parent / "data" / "isa_mv.json") as f:
        return json.load(f)


@pytest.fixture
def MV_ped_extra_sample():
    return """FAM_03\tInd_04\t0\t0\t1\t2\n"""


@pytest.fixture
def MV_ped_samples():
    return """FAM_01\tInd_01\t0\t0\t1\t2\nFAM_02\tInd_02\t0\tInd_03\t2\t2\nFAM_02\tInd_03\t0\t0\t2\t2\n"""


@pytest.fixture
@patch("cubi_tk.sodar_api.SodarAPI._api_call")
def mock_isa_data(API_call, MV_isa_json):
    API_call.return_value = MV_isa_json
    api = SodarAPI("https://sodar.bihealth.org/", "1234", "dummy-project-UUID")
    isa_data = api.get_ISA_samplesheet()
    investigation = isa_data["investigation"][1]
    study = pd.read_csv(StringIO(isa_data["study"][1]), sep="\t")
    assay = pd.read_csv(StringIO(isa_data["assay"][1]), sep="\t")
    return investigation, study, assay


@pytest.fixture
def UCS_class_object():
    parser = argparse.ArgumentParser()
    UpdateSamplesheetCommand.setup_argparse(parser)
    args = parser.parse_args(["dummy-project-UUID"])
    UCS = UpdateSamplesheetCommand(args)
    return UCS


def helper_update_UCS(arg_list, UCS):
    parser = argparse.ArgumentParser()
    UpdateSamplesheetCommand.setup_argparse(parser)
    args = parser.parse_args(arg_list)
    UCS.args = args

    return UCS


def test_gather_ISA_column_names(mock_isa_data, UCS_class_object):
    from cubi_tk.sodar.update_samplehseet import ISA_NON_SETTABLE, REQUIRED_COLUMNS

    study = mock_isa_data[1]
    assay = mock_isa_data[2]

    isa_names = UCS_class_object.gather_ISA_column_names(study, assay)

    assert not all(col in isa_names for col in ISA_NON_SETTABLE)
    assert all(col in isa_names for col in REQUIRED_COLUMNS)

    isa_regex = re.compile(r"(Characteristics|Parameter Value|Comment)\[(.*?)]")
    multi_colname_regex = re.compile(r"\.[0-9]+$")
    for col in study.columns.tolist() + assay.columns.tolist():
        colname_long = multi_colname_regex.sub("", col)
        colname_short = isa_regex.sub(r"\2", colname_long)
        if colname_long in ISA_NON_SETTABLE:
            continue
        assert colname_long in isa_names
        assert colname_short in isa_names
        assert isa_names[colname_short] == isa_names[colname_long]


def test_parse_sampledata_args(mock_isa_data, UCS_class_object):
    isa_names = UCS_class_object.gather_ISA_column_names(mock_isa_data[1], mock_isa_data[2])

    # base mapping from default
    arg_list = [
        "-s",
        "Ind_01",
        "Probe_01",
        "Ana_01",
        "ATCG",
        "A1",
        "-s",
        "Ind_02",
        "Probe_02",
        "Ana_02",
        "ACTG",
        "A2",
        "-s",
        "Ind_03",
        "Probe_03",
        "Ana_03",
        "ATGC",
        "A3",
        "-d",
        "MV-ped",
        "dummy-project-UUID",
    ]
    expected = OrderedDict(
        [
            ("Family-ID", "Family"),
            ("Individual-ID", "Source Name"),
            ("Paternal-ID", "Father"),
            ("Maternal-ID", "Mother"),
            ("Sex", "Sex"),
            ("Phenotype", "Disease status"),
            ("Probe-ID", "Sample Name"),
            ("Analysis-ID", "Extract Name"),
            ("Barcode", "Barcode sequence"),
            ("Barcode-Name", "Barcode name"),
        ]
    )
    USC = helper_update_UCS(arg_list, UCS_class_object)
    assert USC.parse_sampledata_args(isa_names) == expected

    # manually defined mapping
    arg_list = [
        "--sample-fields",
        "Individual-ID=Source Name",
        "Sample Name",
        "Extract Name",
        "barcode=Barcode sequence",
        "-s",
        "Ind_01",
        "Probe_01",
        "Ana_01",
        "ATCG",
        "-s",
        "Ind_02",
        "Probe_02",
        "Ana_02",
        "ACTG",
        "-s",
        "Ind_03",
        "Probe_03",
        "Ana_03",
        "ATGC",
        "-d",
        "MV-ped",
        "dummy-project-UUID",
    ]
    expected["Sample Name"] = "Sample Name"
    expected["Extract Name"] = "Extract Name"
    expected["barcode"] = "Barcode sequence"

    USC = helper_update_UCS(arg_list, UCS_class_object)
    assert USC.parse_sampledata_args(isa_names) == expected

    # missing required fields (from default)
    arg_list = ["-s", "Ind_01", "Probe_01", "Ana_01", "ATCG", "-d", "MV-ped", "dummy-project-UUID"]
    USC = helper_update_UCS(arg_list, UCS_class_object)
    with pytest.raises(ValueError):
        USC.parse_sampledata_args(isa_names)

    # missing sample data
    arg_list = ["dummy-project-UUID"]
    USC = helper_update_UCS(arg_list, UCS_class_object)
    with pytest.raises(ValueError):
        USC.parse_sampledata_args(isa_names)

    # only base ped mapping
    arg_list = ["-p", "dummy-pedfile", "dummy-project-UUID"]
    expected = OrderedDict(
        [
            ("Family-ID", "Family"),
            ("Sample-ID", "Source Name"),
            ("Paternal-ID", "Father"),
            ("Maternal-ID", "Mother"),
            ("Sex", "Sex"),
            ("Phenotype", "Disease status"),
        ]
    )
    USC = helper_update_UCS(arg_list, UCS_class_object)
    assert USC.parse_sampledata_args(isa_names) == expected


def test_get_dynamic_columns(UCS_class_object):
    isa_names = {
        name: [("", "")]
        for name in (
            "Source Name",
            "Sample Name",
            "Extract Name",
            "Library Name",
            "Library Strategy",
            "Dummy",
        )
    }

    expected = OrderedDict(
        [
            ("Sample Name", "{Source Name}-N1"),
            ("Extract Name", "{Sample Name}-DNA1"),
            ("Library Name", "{Sample Name}-DNA1-{Library Strategy}1"),
        ]
    )
    assert UCS_class_object.get_dynamic_columns(isa_names) == expected

    arg_list = [
        "--dynamic-column",
        "Sample Name",
        "{Source Name}-N1",
        "--dynamic-column",
        "Extract Name",
        "{Sample Name}-DNA1",
        "--dynamic-column",
        "Dummy",
        "{Sample Name}-DNA1-{Library Strategy}1",
        "dummy-project-UUID",
    ]
    del expected["Library Name"]
    expected["Dummy"] = "{Sample Name}-DNA1-{Library Strategy}1"
    UCS = helper_update_UCS(arg_list, UCS_class_object)
    assert UCS.get_dynamic_columns(isa_names) == expected


def test_collect_sample_data(
    mock_isa_data, UCS_class_object, MV_ped_samples, MV_ped_extra_sample, fs
):
    fs.create_file("mv_samples.ped", contents=MV_ped_samples)
    fs.create_file("mv_extra_sample.ped", contents=MV_ped_extra_sample)

    expected = pd.DataFrame(
        [
            ["Ind_01", "FAM_01", "0", "0", "male", "affected", "Probe_01", "Ana_01", "ATCG", "A1"],
            [
                "Ind_02",
                "FAM_02",
                "0",
                "Ind_03",
                "female",
                "affected",
                "Probe_02",
                "Ana_02",
                "ACTG",
                "A2",
            ],
            [
                "Ind_03",
                "FAM_02",
                "0",
                "0",
                "female",
                "affected",
                "Probe_03",
                "Ana_03",
                "ATGC",
                "A3",
            ],
        ],
        columns=[
            "Individual-ID",
            "Family-ID",
            "Paternal-ID",
            "Maternal-ID",
            "Sex",
            "Phenotype",
            "Probe-ID",
            "Analysis-ID",
            "Barcode",
            "Barcode-Name",
        ],
    )
    arg_list = [
        "-s",
        "FAM_01",
        "Ind_01",
        "0",
        "0",
        "male",
        "affected",
        "Probe_01",
        "Ana_01",
        "ATCG",
        "A1",
        "-s",
        "FAM_02",
        "Ind_02",
        "0",
        "Ind_03",
        "female",
        "affected",
        "Probe_02",
        "Ana_02",
        "ACTG",
        "A2",
        "-s",
        "FAM_02",
        "Ind_03",
        "0",
        "0",
        "female",
        "affected",
        "Probe_03",
        "Ana_03",
        "ATGC",
        "A3",
        "-d",
        "Modellvorhaben",
        "-p",
        "mv_samples.ped",
        "dummy-project-UUID",
    ]

    def run_usc_collect_sampledata(arg_list):
        USC = helper_update_UCS(arg_list, UCS_class_object)
        isa_names = USC.gather_ISA_column_names(mock_isa_data[1], mock_isa_data[2])
        sampledata_fields = USC.parse_sampledata_args(isa_names)
        return USC.collect_sample_data(isa_names, sampledata_fields)

    # test merginf of --ped & -s info (same samples)
    pd.testing.assert_frame_equal(run_usc_collect_sampledata(arg_list), expected)

    # incomplete info for sample only given via ped
    arg_list[36] = "mv_extra_sample.ped"
    with pytest.raises(ValueError):
        run_usc_collect_sampledata(arg_list)

    # Test 'MV-ped' default, to allow specifying only the info missing from ped file (+ one column for merging)
    arg_list = [
        "-s",
        "Ind_01",
        "Probe_01",
        "Ana_01",
        "ATCG",
        "A1",
        "-s",
        "Ind_02",
        "Probe_02",
        "Ana_02",
        "ACTG",
        "A2",
        "-s",
        "Ind_03",
        "Probe_03",
        "Ana_03",
        "ATGC",
        "A3",
        "-d",
        "MV-ped",
        "-p",
        "mv_samples.ped",
        "dummy-project-UUID",
    ]
    pd.testing.assert_frame_equal(run_usc_collect_sampledata(arg_list), expected)

    # Should still fail if ped has sample where additional info is missing
    arg_list[21] = "mv_extra_sample.ped"
    with pytest.raises(ValueError):
        run_usc_collect_sampledata(arg_list)

    # TODO: test germlinesheet default
    # - only --ped
    # - only -s
    # - --ped and -s (same samples)
    # - --ped and -s (different samples)
    # - --ped and -s (mismatch in sample-info)
    #   >> that one might only fail in ISA validation?


def test_match_sample_data_to_isa(mock_isa_data, UCS_class_object):
    arg_list = ["-s", "Ind", "Probe", "Ana", "ACTG", "A1", "-d", "MV-ped", "dummy-project-UUID"]
    UCS = helper_update_UCS(arg_list, UCS_class_object)
    isa_names = UCS.gather_ISA_column_names(mock_isa_data[1], mock_isa_data[2])
    sampledata_fields = UCS.parse_sampledata_args(isa_names)
    samples = pd.DataFrame(
        [
            ["Ind_01", "FAM_01", "0", "0", "male", "affected", "Probe_01", "Ana_01", "ATCG", "A1"],
            [
                "Ind_02",
                "FAM_02",
                "0",
                "Ind_03",
                "female",
                "affected",
                "Probe_02",
                "Ana_02",
                "ACTG",
                "A2",
            ],
            [
                "Ind_03",
                "FAM_02",
                "0",
                "0",
                "female",
                "affected",
                "Probe_03",
                "Ana_03",
                "ATGC",
                "A3",
            ],
        ],
        columns=[
            "Individual-ID",
            "Family-ID",
            "Paternal-ID",
            "Maternal-ID",
            "Sex",
            "Phenotype",
            "Probe-ID",
            "Analysis-ID",
            "Barcode",
            "Barcode-Name",
        ],
    )

    expected_study = pd.DataFrame(
        [
            ["Ind_01", "FAM_01", "0", "0", "male", "affected", "Probe_01"],
            ["Ind_02", "FAM_02", "0", "Ind_03", "female", "affected", "Probe_02"],
            ["Ind_03", "FAM_02", "0", "0", "female", "affected", "Probe_03"],
        ],
        columns=[
            "Source Name",
            "Characteristics[Family]",
            "Characteristics[Father]",
            "Characteristics[Mother]",
            "Characteristics[Sex]",
            "Characteristics[Disease status]",
            "Sample Name",
        ],
    )
    # All extract name columns are added at the same time, sorting comes later
    expected_assay = pd.DataFrame(
        [
            ["Probe_01", "Ana_01", "Ana_01", "ATCG", "A1"],
            ["Probe_02", "Ana_02", "Ana_02", "ACTG", "A2"],
            ["Probe_03", "Ana_03", "Ana_03", "ATGC", "A3"],
        ],
        columns=[
            "Sample Name",
            "Extract Name",
            "Extract Name.1",
            "Parameter Value[Barcode sequence]",
            "Parameter Value[Barcode name]",
        ],
    )

    study, assay = UCS.match_sample_data_to_isa(samples, isa_names, sampledata_fields)
    pd.testing.assert_frame_equal(study, expected_study)
    pd.testing.assert_frame_equal(assay, expected_assay)


def test_update_isa_table(UCS_class_object, caplog):
    orig_isa = pd.DataFrame(
        {
            "Sample Name": ["Probe_01", "Probe_02", "Probe_03"],
            "Extract Name": ["Ana_01", "Ana_02", "Ana_03"],
            "Protocol REF": ["DNA extraction", "DNA extraction", "DNA extraction"],
            "Parameter Value[Library layout]": ["paired", "paired", "paired"],
            "Parameter Value[Barcode sequence]": ["ATCG", "ACTG", ""],
            "Parameter Value[Barcode name]": ["A1", "A2", ""],
            "Extract Name.1": ["Ana_01", "Ana_02", "Ana_03"],
            "RawData File": ["", "", ""],
        }
    )
    parsed_assay = pd.DataFrame(
        {
            "Sample Name": ["Probe_02", "Probe_03", "Probe_04"],
            "Extract Name": ["Ana_02", "Ana_03", "Ana_04"],
            "Extract Name.1": ["Ana_02", "Ana_03", "Ana_04"],
            "Parameter Value[Barcode sequence]": ["XXXX", "ATTT", "UUUU"],
            "Parameter Value[Barcode name]": ["A2", "A3", "A4"],
        }
    )
    expected = pd.concat(
        [
            orig_isa.loc[[True, True, False]],
            pd.DataFrame(
                [
                    ["Probe_03", "Ana_03", "DNA extraction", "paired", "ATTT", "A3", "Ana_03", ""],
                    ["Probe_04", "Ana_04", "DNA extraction", "paired", "UUUU", "A4", "Ana_04", ""],
                ],
                columns=orig_isa.columns,
            ),
        ],
        ignore_index=True,
    )

    # Default case, no overwriting of non-empty fields, autofilling of columns with only 1 value
    actual = UCS_class_object.update_isa_table(orig_isa, parsed_assay)
    pd.testing.assert_frame_equal(actual, expected)

    # Check for warning message regarding non overwrite of for XXXX
    assert "XXXX" in caplog.records[-1].message and "Barcode sequence" in caplog.records[-1].message

    # no auto-filling
    expected["Parameter Value[Library layout]"] = ["paired", "paired", "paired", ""]
    actual = UCS_class_object.update_isa_table(orig_isa, parsed_assay, no_autofill=True)
    pd.testing.assert_frame_equal(actual, expected)

    # allow overwriting
    expected["Parameter Value[Barcode sequence]"] = ["ATCG", "XXXX", "ATTT", "UUUU"]
    actual = UCS_class_object.update_isa_table(
        orig_isa, parsed_assay, overwrite=True, no_autofill=True
    )
    pd.testing.assert_frame_equal(actual, expected)


# FIXME: smoke test for execute
