import argparse
from io import StringIO
import json
import pathlib
import re
from unittest.mock import patch

import pandas as pd
from cubi_tk.parsers import get_sodar_parser
import pytest

from cubi_tk.exceptions import ParameterException
from cubi_tk.sodar.update_samplesheet import UpdateSamplesheetCommand
from cubi_tk.sodar_api import SodarAPI


@pytest.fixture
def MV_isa_json():
    with open(pathlib.Path(__file__).resolve().parent / "data" / "isa_mv.json") as f:
        return json.load(f)


@pytest.fixture
def MV_ped_extra_sample():
    return """FAM_03\tAna_04\t0\t0\t1\t2\n"""


@pytest.fixture
def MV_ped_samples():
    return """FAM_01\tAna_01\t0\t0\t1\t2\nFAM_02\tAna_02\t0\tAna_03\t2\t2\nFAM_02\tAna_03\t0\t0\t2\t2\n"""


@pytest.fixture
@patch("cubi_tk.sodar_api.SodarAPI._api_call")
def mock_isa_data(API_call, MV_isa_json):
    API_call.return_value = MV_isa_json
    api = SodarAPI("https://sodar.bihealth.org/", "1234", "123e4567-e89b-12d3-a456-426655440000")
    isa_data = api.get_ISA_samplesheet()
    investigation = isa_data["investigation"]["content"]
    study = pd.read_csv(StringIO(isa_data["study"]["content"]), sep="\t")
    assay = pd.read_csv(StringIO(isa_data["assay"]["content"]), sep="\t")
    return investigation, study, assay


@pytest.fixture
def UCS_class_object():
    parser = argparse.ArgumentParser()
    UpdateSamplesheetCommand.setup_argparse(parser)
    args = parser.parse_args(["123e4567-e89b-12d3-a456-426655440000"])
    UCS = UpdateSamplesheetCommand(args)
    return UCS


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        [
            [
                "Ana_01",
                "FAM_01",
                "0",
                "0",
                "male",
                "affected",
                "Ind_01",
                "Probe_01",
                "ATCG",
                "A1",
                "Ana_01-N1",
                "Ana_01-N1-DNA1",
                "Ana_01-N1-DNA1-WGS1",
            ],
            [
                "Ana_02",
                "FAM_02",
                "0",
                "Ana_03",
                "female",
                "affected",
                "Ind_02",
                "Probe_02",
                "ACTG",
                "A2",
                "Ana_02-N1",
                "Ana_02-N1-DNA1",
                "Ana_02-N1-DNA1-WGS1",
            ],
            [
                "Ana_03",
                "FAM_02",
                "0",
                "0",
                "female",
                "affected",
                "Ind_03",
                "Probe_03",
                "ATGC",
                "A3",
                "Ana_03-N1",
                "Ana_03-N1-DNA1",
                "Ana_03-N1-DNA1-WGS1",
            ],
        ],
        columns=[
            "Analysis-ID",
            "Family-ID",
            "Paternal-ID",
            "Maternal-ID",
            "Sex",
            "Phenotype",
            "Individual-ID",
            "Probe-ID",
            "Barcode",
            "Barcode-Name",
            "Sample Name",
            "Extract Name",
            "Library Name",
        ],
    )


def helper_update_UCS(arg_list, UCS):
    parser = argparse.ArgumentParser()
    UpdateSamplesheetCommand.setup_argparse(parser)
    args = parser.parse_args(arg_list)
    UCS.args = args

    return UCS


def test_gather_ISA_column_names(mock_isa_data, UCS_class_object):
    from cubi_tk.sodar.update_samplesheet import ISA_NON_SETTABLE, REQUIRED_COLUMNS

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
        "MV-barcodes",
        "123e4567-e89b-12d3-a456-426655440000",
    ]
    expected = {
        "Family-ID": "Family",
        "Analysis-ID": "Source Name",
        "Paternal-ID": "Father",
        "Maternal-ID": "Mother",
        "Sex": "Sex",
        "Phenotype": "Disease status",
        "Individual-ID": "Individual-ID",
        "Probe-ID": "Probe-ID",
        "Barcode": "Barcode sequence",
        "Barcode-Name": "Barcode name",
    }
    USC = helper_update_UCS(arg_list, UCS_class_object)
    assert USC.parse_sampledata_args(isa_names) == expected

    # manually defined mapping
    arg_list = [
        "--sample-fields",
        "Dummy-ID=Source Name",
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
        "MV-barcodes",
        "123e4567-e89b-12d3-a456-426655440000",
    ]
    expected["Dummy-ID"] = "Source Name"
    expected["Sample Name"] = "Sample Name"
    expected["Extract Name"] = "Extract Name"
    expected["barcode"] = "Barcode sequence"

    USC = helper_update_UCS(arg_list, UCS_class_object)
    assert USC.parse_sampledata_args(isa_names) == expected

    # missing required fields (from default)
    arg_list = [
        "-s",
        "Ind_01",
        "Probe_01",
        "Ana_01",
        "ATCG",
        "-d",
        "MV-barcodes",
        "123e4567-e89b-12d3-a456-426655440000",
    ]
    USC = helper_update_UCS(arg_list, UCS_class_object)
    with pytest.raises(ValueError):
        USC.parse_sampledata_args(isa_names)

    # missing sample data
    arg_list = ["123e4567-e89b-12d3-a456-426655440000"]
    USC = helper_update_UCS(arg_list, UCS_class_object)
    with pytest.raises(ValueError):
        USC.parse_sampledata_args(isa_names)

    # only base ped mapping
    arg_list = ["-p", "dummy-pedfile", "123e4567-e89b-12d3-a456-426655440000"]
    expected = {
        "Family-ID": "Family",
        "Sample-ID": "Source Name",
        "Paternal-ID": "Father",
        "Maternal-ID": "Mother",
        "Sex": "Sex",
        "Phenotype": "Disease status",
    }
    USC = helper_update_UCS(arg_list, UCS_class_object)
    assert USC.parse_sampledata_args(isa_names) == expected


def test_get_dynamic_columns(UCS_class_object):
    # Use this both for exisiting (sampledata) and for allowed (isa) columns
    existing_names = (
        "Sample-ID",
        "Source Name",
        "Sample Name",
        "Extract Name",
        "Library Name",
        "Library Strategy",
        "Dummy",
    )

    expected = {
        "Sample Name": "{Sample-ID}-N1",
        "Extract Name": "{Sample-ID}-N1-DNA1",
        "Library Name": "{Sample-ID}-N1-DNA1-WGS1",
    }
    assert UCS_class_object.get_dynamic_columns(existing_names, existing_names) == expected

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
        "123e4567-e89b-12d3-a456-426655440000",
    ]
    expected = {
        "Sample Name": "{Source Name}-N1",
        "Extract Name": "{Sample Name}-DNA1",
        "Dummy": "{Sample Name}-DNA1-{Library Strategy}1",
    }
    UCS = helper_update_UCS(arg_list, UCS_class_object)
    assert UCS.get_dynamic_columns(existing_names, existing_names) == expected

    # FIXME: test auto-exclusion of Library name from defaults if not in existing_names


def test_collect_sample_data(
    mock_isa_data, UCS_class_object, MV_ped_samples, MV_ped_extra_sample, sample_df, fs, caplog
):
    fs.create_file("mv_samples.ped", contents=MV_ped_samples)
    fs.create_file("mv_extra_sample.ped", contents=MV_ped_extra_sample)

    expected = sample_df
    arg_list = [
        "-s",
        "FAM_01",
        "Ana_01",
        "0",
        "0",
        "male",
        "affected",
        "Ind_01",
        "Probe_01",
        "ATCG",
        "A1",
        "-s",
        "FAM_02",
        "Ana_02",
        "0",
        "Ana_03",
        "female",
        "affected",
        "Ind_02",
        "Probe_02",
        "ACTG",
        "A2",
        "-s",
        "FAM_02",
        "Ana_03",
        "0",
        "0",
        "female",
        "affected",
        "Ind_03",
        "Probe_03",
        "ATGC",
        "A3",
        "-d",
        "Modellvorhaben",
        "-p",
        "mv_samples.ped",
        "123e4567-e89b-12d3-a456-426655440000",
    ]

    def run_usc_collect_sampledata(arg_list, **kwargs):
        USC = helper_update_UCS(arg_list, UCS_class_object)
        isa_names = USC.gather_ISA_column_names(mock_isa_data[1], mock_isa_data[2])
        sampledata_fields = USC.parse_sampledata_args(isa_names)
        return USC.collect_sample_data(isa_names, sampledata_fields, **kwargs)

    # test merging of --ped & -s info (same samples)
    pd.testing.assert_frame_equal(run_usc_collect_sampledata(arg_list), expected)

    # incomplete info for sample only given via ped
    arg_list[36] = "mv_extra_sample.ped"
    with pytest.raises(ParameterException):
        run_usc_collect_sampledata(arg_list)
        assert "Combination of ped and sample data has missing values" in caplog.records[-1].message

    # Test 'MV-barcodes' default, to allow specifying only the info missing from ped file (+ one column for merging)
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
        "MV-barcodes",
        "-p",
        "mv_samples.ped",
        "123e4567-e89b-12d3-a456-426655440000",
    ]
    pd.testing.assert_frame_equal(run_usc_collect_sampledata(arg_list), expected)

    # Should still fail if ped has sample where additional info is missing
    arg_list[21] = "mv_extra_sample.ped"
    with pytest.raises(ParameterException):
        run_usc_collect_sampledata(arg_list)
        assert "Combination of ped and sample data has missing values" in caplog.records[-1].message

    # test germlinesheet default
    # - only -ped
    expected_cols = [
        "Family-ID",
        "Analysis-ID",
        "Paternal-ID",
        "Maternal-ID",
        "Sex",
        "Phenotype",
        "Sample Name",
        "Extract Name",
        "Library Name",
    ]
    expected = expected.loc[:, expected_cols]
    expected_cols[1] = "Sample-ID"
    expected.columns = expected_cols
    arg_list = [
        "-p",
        "mv_samples.ped",
        "123e4567-e89b-12d3-a456-426655440000",
    ]
    pd.testing.assert_frame_equal(run_usc_collect_sampledata(arg_list), expected)

    # - only -s
    arg_list = [
        "123e4567-e89b-12d3-a456-426655440000",
        "-s",
        "FAM_01",
        "Ana_01",
        "0",
        "0",
        "male",
        "affected",
        "-s",
        "FAM_02",
        "Ana_02",
        "0",
        "Ana_03",
        "female",
        "affected",
        "-s",
        "FAM_02",
        "Ana_03",
        "0",
        "0",
        "female",
        "affected",
    ]
    pd.testing.assert_frame_equal(run_usc_collect_sampledata(arg_list), expected)

    # Test --snappy-compatible
    arg_list2 = arg_list[:]
    for i in [3, 10, 12, 17]:
        arg_list2[i] = arg_list2[i].replace("_", "-")
    arg_list2 += ["--snappy-compatible"]
    pd.testing.assert_frame_equal(
        run_usc_collect_sampledata(arg_list, snappy_compatible=True), expected
    )

    # - --ped and -s (same samples)
    arg_list += ["-p", "mv_samples.ped"]
    pd.testing.assert_frame_equal(run_usc_collect_sampledata(arg_list), expected)

    # - --ped and -s (different samples)
    arg_list[-1] = "mv_extra_sample.ped"
    expected2 = pd.concat(
        [
            expected,
            pd.DataFrame(
                [
                    [
                        "FAM_03",
                        "Ana_04",
                        "0",
                        "0",
                        "male",
                        "affected",
                        "Ana_04-N1",
                        "Ana_04-N1-DNA1",
                        "Ana_04-N1-DNA1-WGS1",
                    ]
                ],
                columns=expected.columns,
            ),
        ],
        ignore_index=True,
    )
    pd.testing.assert_frame_equal(run_usc_collect_sampledata(arg_list), expected2)

    # - --ped and -s (mismatch in sample-info)
    arg_list[-1] = "mv_samples.ped"
    arg_list[6] = "female"
    with pytest.raises(ParameterException):
        run_usc_collect_sampledata(arg_list)
        assert (
            "Sample with different values found in combined sample data:"
            in caplog.records[-1].message
        )
    #   >> that one might only fail in ISA validation?


def test_match_sample_data_to_isa(mock_isa_data, UCS_class_object, sample_df):
    arg_list = [
        "-s",
        "Ind",
        "Probe",
        "Ana",
        "ACTG",
        "A1",
        "-d",
        "MV-barcodes",
        "123e4567-e89b-12d3-a456-426655440000",
    ]
    UCS = helper_update_UCS(arg_list, UCS_class_object)
    isa_names = UCS.gather_ISA_column_names(mock_isa_data[1], mock_isa_data[2])
    sampledata_fields = UCS.parse_sampledata_args(isa_names)
    samples = sample_df

    expected_study = pd.DataFrame(
        [
            ["Ana_01", "FAM_01", "0", "0", "male", "affected", "Ind_01", "Probe_01", "Ana_01-N1"],
            [
                "Ana_02",
                "FAM_02",
                "0",
                "Ana_03",
                "female",
                "affected",
                "Ind_02",
                "Probe_02",
                "Ana_02-N1",
            ],
            ["Ana_03", "FAM_02", "0", "0", "female", "affected", "Ind_03", "Probe_03", "Ana_03-N1"],
        ],
        columns=[
            "Source Name",
            "Characteristics[Family]",
            "Characteristics[Father]",
            "Characteristics[Mother]",
            "Characteristics[Sex]",
            "Characteristics[Disease status]",
            "Characteristics[Individual-ID]",
            "Characteristics[Probe-ID]",
            "Sample Name",
        ],
    )
    # Order of columns here does not matter yet
    expected_assay = pd.DataFrame(
        [
            ["ATCG", "A1", "Ana_01-N1", "Ana_01-N1-DNA1", "Ana_01-N1-DNA1-WGS1"],
            ["ACTG", "A2", "Ana_02-N1", "Ana_02-N1-DNA1", "Ana_02-N1-DNA1-WGS1"],
            ["ATGC", "A3", "Ana_03-N1", "Ana_03-N1-DNA1", "Ana_03-N1-DNA1-WGS1"],
        ],
        columns=[
            "Parameter Value[Barcode sequence]",
            "Parameter Value[Barcode name]",
            "Sample Name",
            "Extract Name",
            "Library Name",
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


@patch("cubi_tk.sodar.update_samplesheet.SodarAPI.upload_ISA_samplesheet")
@patch("cubi_tk.sodar.update_samplesheet.SodarAPI._api_call")
def test_execute(mock_api_call, mock_upload_isa, MV_isa_json, sample_df):
    # restrict to 1 sample, match cols to ISA
    sample_df = sample_df.iloc[0:1, :]
    sample_df.columns = [
        "Source Name",
        "Characteristics[Family]",
        "Characteristics[Father]",
        "Characteristics[Mother]",
        "Characteristics[Sex]",
        "Characteristics[Disease status]",
        "Characteristics[Individual-ID]",
        "Characteristics[Probe-ID]",
        "Parameter Value[Barcode sequence]",
        "Parameter Value[Barcode name]",
        "Sample Name",
        "Extract Name",
        "Library Name",
    ]

    sodar_parser = get_sodar_parser()
    parser = argparse.ArgumentParser(parents=[sodar_parser])
    UpdateSamplesheetCommand.setup_argparse(parser)

    mock_api_call.return_value = MV_isa_json
    mock_upload_isa.return_value = 0

    # Build expected content of to-be-uploaded files
    expected_i = MV_isa_json["investigation"]["tsv"]
    study_tsv = MV_isa_json["studies"]["s_modellvorhaben_rare_diseases.txt"]["tsv"]
    assay_tsv = MV_isa_json["assays"]["a_modellvorhaben_rare_diseases_genome_sequencing.txt"]["tsv"]
    start_s = pd.read_csv(StringIO(study_tsv), sep="\t", dtype=str)
    start_a = pd.read_csv(StringIO(assay_tsv), sep="\t", dtype=str)

    expected_s = pd.concat([start_s, sample_df.iloc[:, [0, 1, 2, 3, 4, 5, 10]]], ignore_index=True)
    expected_s["Protocol REF"] = "Sample collection"
    expected_s = expected_s.to_csv(
        sep="\t", index=False, header=study_tsv.split("\n")[0].split("\t")
    )

    expected_a = pd.concat([start_a, sample_df.iloc[:, [10, 11, 12]]], ignore_index=True)
    expected_a["Protocol REF"] = "Nucleic acid extraction WGS"
    expected_a["Protocol REF.1"] = "Library construction WGS"
    expected_a["Protocol REF.2"] = "Nucleic acid sequencing WGS"
    expected_a = expected_a.to_csv(
        sep="\t", index=False, header=assay_tsv.split("\n")[0].split("\t")
    )

    # Test germlinesheet default
    args = parser.parse_args(
        [
            "--sodar-api-token",
            "1234",
            "--sodar-server-url",
            "https://sodar.bihealth.org/",
            "-s",
            "FAM_01",
            "Ana_01",
            "0",
            "0",
            "male",
            "affected",
            "--no-autofill",
            "123e4567-e89b-12d3-a456-426655440000",
        ]
    )
    UpdateSamplesheetCommand(args).execute()
    mock_upload_isa.assert_called_with(
        ("i_Investigation.txt", expected_i),
        ("s_modellvorhaben_rare_diseases.txt", expected_s),
        ("a_modellvorhaben_rare_diseases_genome_sequencing.txt", expected_a),
    )

    # Test MV default
    expected_s = pd.concat(
        [start_s, sample_df.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7, 10]]], ignore_index=True
    )
    expected_s["Protocol REF"] = "Sample collection"
    expected_s = expected_s.to_csv(
        sep="\t", index=False, header=study_tsv.split("\n")[0].split("\t")
    )

    expected_a = pd.concat([start_a, sample_df.iloc[:, [8, 9, 10, 11, 12]]], ignore_index=True)
    expected_a["Protocol REF"] = "Nucleic acid extraction WGS"
    expected_a["Protocol REF.1"] = "Library construction WGS"
    expected_a["Protocol REF.2"] = "Nucleic acid sequencing WGS"
    expected_a = expected_a.to_csv(
        sep="\t", index=False, header=assay_tsv.split("\n")[0].split("\t")
    )

    args = parser.parse_args(
        [
            "--sodar-api-token",
            "1234",
            "--sodar-server-url",
            "https://sodar.bihealth.org/",
            "-d",
            "MV",
            "-s",
            "FAM_01",
            "Ana_01",
            "0",
            "0",
            "male",
            "affected",
            "Ind_01",
            "Probe_01",
            "ATCG",
            "A1",
            "--no-autofill",
            "123e4567-e89b-12d3-a456-426655440000",
        ]
    )
    UpdateSamplesheetCommand(args).execute()
    mock_upload_isa.assert_called_with(
        ("i_Investigation.txt", expected_i),
        ("s_modellvorhaben_rare_diseases.txt", expected_s),
        ("a_modellvorhaben_rare_diseases_genome_sequencing.txt", expected_a),
    )
