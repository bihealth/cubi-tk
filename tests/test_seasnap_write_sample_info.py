"""Tests for ``cubi_tk.sea_snap.write_sample_info``.

We only run some smoke tests here.
"""

import os

import pytest

from cubi_tk.__main__ import setup_argparse, main


def test_run_seasnap_write_sample_info_help(capsys):
    parser, subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "write-sample-info", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_seasnap_write_sample_info_nothing(capsys):
    parser, subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "write-sample-info"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_seasnap_write_sample_info_smoke_test(capsys, mocker, fs):
    # --- setup arguments
    in_path_pattern = os.path.join(
        os.path.dirname(__file__), "data", "fastq_test", "{sample}_{mate,R1|R2}"
    )
    path_isa_test = os.path.join(
        os.path.dirname(__file__),
        "data",
        "ISA_files_test",
        "a_isatest_transcriptome_profiling_nucleotide_sequencing.txt",
    )

    argv = ["sea-snap", "write-sample-info", "--isa-assay", path_isa_test, in_path_pattern, "-"]

    parser, subparsers = setup_argparse()

    # --- add test files
    fs.add_real_file(path_isa_test)

    path_fastq_test = os.path.join(os.path.dirname(__file__), "data", "fastq_test")
    fs.add_real_directory(path_fastq_test)

    target_file = os.path.join(os.path.dirname(__file__), "data", "sample_info_test.yaml")
    fs.add_real_file(target_file)

    # --- run as end-to-end test
    res = main(argv)
    assert not res

    with open(target_file, "r") as f:
        expected_result = f.read()

    res = capsys.readouterr()
    assert not res.err

    assert expected_result == res.out
