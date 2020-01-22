"""Tests for ``cubi_sak.snappy.pull_sheet``.

We only run some smoke tests here.
"""

import os

import pytest

from cubi_sak.snappy.pull_sheet import URL_TPL
from cubi_sak.snappy import pull_sheet
from cubi_sak.__main__ import setup_argparse, main


def test_run_snappy_pull_sheet_help(capsys):
    parser, subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "pull-sheet", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_snappy_pull_sheet_nothing(capsys):
    parser, subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "pull-sheet"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_strip():
    assert pull_sheet.strip(1) == 1
    assert pull_sheet.strip(" X ") == "X"


def test_run_snappy_pull_sheet_smoke_test(tmp_path, requests_mock, capsys):
    project_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    argv = ["snappy", "pull-sheet", "--sodar-auth-token", "XXX", project_uuid]

    parser, subparsers = setup_argparse()
    args = parser.parse_args(argv)

    path_json = os.path.join(os.path.dirname(__file__), "data", "germline.json")
    with open(path_json, "rt") as inputf:
        json_text = inputf.read()

    url = URL_TPL % {"sodar_url": args.sodar_url, "project_uuid": project_uuid, "api_key": "XXX"}
    requests_mock.get(url, text=json_text)

    res = main(argv)  # run as end-to-end test
    assert not res

    path_out = os.path.join(os.path.dirname(__file__), "data", "germline.out")
    with open(path_out, "rt") as inputf:
        expected_out = inputf.read()

    res = capsys.readouterr()
    assert not res.err
    assert res.out == expected_out
