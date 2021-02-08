"""Tests for ``cubi_tk.sea_snap.write_sample_info``.

We only run some smoke tests here.
"""

import os

import pytest
import filecmp
import glob

from cubi_tk.__main__ import setup_argparse, main
from cubi_tk.sea_snap.pull_isa import URL_TPL


def test_run_seasnap_write_sample_info_help(capsys):
    parser, _subparsers = setup_argparse()
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


def test_run_seasnap_write_sample_info_smoke_test(capsys, requests_mock, fs):
    # --- setup arguments
    project_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    in_path_pattern = os.path.join(
        os.path.dirname(__file__), "data", "fastq_test", "{sample}_{mate,R1|R2}"
    )

    argv = [
        "sea-snap",
        "write-sample-info",
        "--sodar-auth-token",
        "XXX",
        "--project_uuid",
        project_uuid,
        in_path_pattern,
        "-",
    ]

    parser, subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # --- add test content and files
    path_json = os.path.join(os.path.dirname(__file__), "data", "isa_test.json")
    fs.add_real_file(path_json)
    with open(path_json, "rt") as inputf:
        json_text = inputf.read()

    path_fastq_test = os.path.join(os.path.dirname(__file__), "data", "fastq_test")
    fs.add_real_directory(path_fastq_test)

    target_file = os.path.join(os.path.dirname(__file__), "data", "sample_info_test.yaml")
    fs.add_real_file(target_file)

    # --- mock modules
    url = URL_TPL % {"sodar_url": args.sodar_url, "project_uuid": project_uuid, "api_key": "XXX"}
    requests_mock.get(url, text=json_text)

    # --- run as end-to-end test
    res = main(argv)
    assert not res

    # test content of generated file
    with open(target_file, "r") as f:
        expected_result = f.read()

    res = capsys.readouterr()
    assert not res.err

    assert expected_result == res.out

    # test whether ISA files were pulled correctly
    test_dir = os.path.join(os.path.dirname(__file__), "data", "ISA_files_test")
    fs.add_real_directory(test_dir)
    files = glob.glob(os.path.join(test_dir, "*"))

    match, mismatch, errors = filecmp.cmpfiles(
        "ISA_files", test_dir, (os.path.basename(f) for f in files), shallow=False
    )
    print([match, mismatch, errors])
    assert len(mismatch) == 0
    assert len(errors) == 0
