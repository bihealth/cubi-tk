"""Tests for ``cubi_tk.sea_snap.pull_isa``.

We only run some smoke tests here.
"""

import os

import pytest
import filecmp
import glob

from cubi_tk.sea_snap.pull_isa import URL_TPL
from cubi_tk.__main__ import setup_argparse, main


def test_run_seasnap_pull_isa_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "pull-isa", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_seasnap_pull_isa_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "pull-isa"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_seasnap_pull_isa_smoke_test(requests_mock, capsys, fs):
    # --- setup arguments
    project_uuid = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    argv = ["sea-snap", "pull-isa", "--sodar-api-token", "XXX", project_uuid]

    parser, subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # --- add test content
    path_json = os.path.join(os.path.dirname(__file__), "data", "isa_test.json")
    fs.add_real_file(path_json)
    with open(path_json, "rt") as inputf:
        json_text = inputf.read()

    # --- mock modules
    url = URL_TPL % {"sodar_url": args.sodar_url, "project_uuid": project_uuid, "api_key": "XXX"}
    requests_mock.get(url, text=json_text)

    # --- run tests
    res = main(argv)
    assert not res

    test_dir = os.path.join(os.path.dirname(__file__), "data", "ISA_files_test")
    fs.add_real_directory(test_dir)
    files = glob.glob(os.path.join(test_dir, "*"))

    match, mismatch, errors = filecmp.cmpfiles(
        "ISA_files", test_dir, (os.path.basename(f) for f in files), shallow=False
    )
    print([match, mismatch, errors])
    assert len(mismatch) == 0
    assert len(errors) == 0

    res = capsys.readouterr()
    assert not res.err
