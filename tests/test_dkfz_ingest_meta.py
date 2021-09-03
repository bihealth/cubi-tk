"""Tests for ``cubi_tk.dkfz.ingest_meta``.

We only run some smoke tests here.
"""

import datetime
import glob
import os

from unittest import mock

import json
import pytest
from pyfakefs import fake_filesystem

from cubi_tk.__main__ import setup_argparse, main


def test_run_dkfz_ingest_meta_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["dkfz", "ingest-meta", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_dkfz_ingest_meta_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["dkfz", "ingest-meta"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_dkfz_ingest_meta_smoke_test(mocker, requests_mock):
    # --- setup arguments
    irods_path = "/irods/dest"
    landing_zone_uuid = "landing_zone_uuid"
    meta_path = os.path.join(os.path.dirname(__file__), "data", "dkfz", "mocks", "1_meta.tsv")
    report_path = os.path.join(os.path.dirname(meta_path), "download_report.html")
    argv = [
        "--verbose",
        "dkfz",
        "ingest-meta",
        "--sodar-api-token",
        "XXXX",
        "--extra-files",
        report_path,
        "--assay-type",
        "EXON",
        meta_path,
        landing_zone_uuid,
    ]

    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break biomedsheets.
    fs = fake_filesystem.FakeFilesystem()

    # --- add test files
    theDate = datetime.date.today().strftime("%Y-%m-%d")
    test_files = {
        "DKFZ_meta": glob.glob(os.path.join(os.path.dirname(meta_path), "1*"), recursive=False),
        "DKFZ_upload/{}".format(theDate): glob.glob(
            os.path.join(os.path.dirname(meta_path), "*.html"), recursive=False
        ),
    }
    for filenames in test_files.values():
        for filename in filenames:
            fs.add_real_file(os.path.join(os.path.dirname(meta_path), filename))

    # requests mock
    return_value = dict(
        assay="",
        config_data="",
        configuration="",
        date_modified="",
        description="",
        irods_path=irods_path,
        project="",
        sodar_uuid="",
        status="",
        status_info="",
        title="",
        user=dict(sodar_uuid="", username="", name="", email=""),
    )
    url = os.path.join(args.sodar_url, "landingzones", "api", "retrieve", args.destination)
    requests_mock.register_uri("GET", url, text=json.dumps(return_value))

    # Mock context manager tempfile.TemoraryDirectory()
    mocker.patch("tempfile.mkdtemp").return_value = "/mock_tmp_dir"
    fs.create_dir("/mock_tmp_dir")
    mocker.patch("cubi_tk.dkfz.ingest_meta.open", fake_filesystem.FakeFileOpen(fs))
    mocker.patch("pandas.io.common.open", fake_filesystem.FakeFileOpen(fs))

    mock_check_output = mock.MagicMock(return_value=0)
    mocker.patch("cubi_tk.dkfz.ingest_meta.check_output", mock_check_output)

    # --- run tests
    res = main(argv)

    assert not res

    assert (
        mock_check_output.call_count
        == len(test_files) + 2 * sum([len(x) for x in test_files.values()]) + 2
    )
    for (d, filenames) in test_files.items():
        target_dir = os.path.join(irods_path, "MiscFiles", d)
        expected_argv = ["imkdir", "-p", target_dir]
        assert ((expected_argv,),) in mock_check_output.call_args_list
        for filename in filenames:
            expected_argv = [
                "iput",
                "-aK",
                filename,
                os.path.join(target_dir, os.path.basename(filename)),
            ]
            assert ((expected_argv,),) in mock_check_output.call_args_list
