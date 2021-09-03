"""Tests for ``cubi_tk.dkfz.ingest_fastq``.

We only run some smoke tests here.
"""

import datetime
import glob
import json
import os
import re
import typing

from unittest import mock

import pytest
from pyfakefs import fake_filesystem

from cubi_tk.__main__ import setup_argparse, main


def test_run_dkfz_ingest_fastq_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["dkfz", "ingest-fastq", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_dkfz_ingest_fastq_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["dkfz", "ingest-fastq"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def grep(regex, filename, grepl=True):
    if not isinstance(regex, typing.Pattern):
        regex = re.compile(str(regex))
    matched_lines = []
    with open(filename, "r") as f:
        for line in f:
            if regex.search(line):
                if grepl:
                    return 0
                matched_lines.append(line.strip())
    if grepl:
        return 1
    return matched_lines


CUBI_ID = re.compile("^CUBI ID: ([^ ]+) *$")


def test_run_dkfz_ingest_fastq_smoke_test(mocker, requests_mock):
    # --- setup arguments
    irods_path = "/irods/dest"
    landing_zone_uuid = "landing_zone_uuid"
    assay_type = "EXON"
    meta_path = os.path.join(os.path.dirname(__file__), "data", "dkfz", "mocks", "1_meta.tsv")
    argv = [
        "--verbose",
        "dkfz",
        "ingest-fastq",
        "--num-parallel-transfers",
        "1",
        "--md5-check",
        "--assay-type",
        assay_type,
        "--sodar-api-token",
        "XXXX",
        meta_path,
        landing_zone_uuid,
    ]

    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break biomedsheets.
    fs = fake_filesystem.FakeFilesystem()

    # --- add real files
    filenames = glob.glob(
        os.path.join(os.path.dirname(__file__), "data", "dkfz", "mocks", "**", "*.fastq.gz"),
        recursive=True,
    )
    files_to_upload = []
    for fn in filenames:
        fs.add_real_file(fn)
        fs.add_real_file(fn + ".md5sum")
        lines = grep(CUBI_ID, fn, grepl=False)
        if len(lines) > 0 and grep(assay_type, fn) == 0:
            files_to_upload.append((fn, CUBI_ID.match(lines[0]).group(1)))

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_tk.dkfz.ingest_fastq.open", fake_open)

    mock_check_output = mock.MagicMock(return_value=0)
    mocker.patch("cubi_tk.dkfz.ingest_fastq.check_output", mock_check_output)

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

    # --- run tests
    res = main(argv)

    assert not res

    assert mock_check_output.call_count == 3 * len(files_to_upload)

    theDate = datetime.date.today().strftime("%Y-%m-%d")
    for (fn, cubi_id) in files_to_upload:
        target_dir = os.path.join(irods_path, cubi_id, "raw_data", theDate)
        expected_imkdir_argv = ["imkdir", "-p", target_dir]
        assert ((expected_imkdir_argv,),) in mock_check_output.call_args_list
        expected_fastq_argv = ["iput", "-aK", fn, os.path.join(target_dir, os.path.basename(fn))]
        assert ((expected_fastq_argv,),) in mock_check_output.call_args_list
        expected_md5_argv = [
            "iput",
            "-aK",
            fn + ".md5sum",
            os.path.join(target_dir, os.path.basename(fn)) + ".md5",
        ]
        assert ((expected_md5_argv,),) in mock_check_output.call_args_list
