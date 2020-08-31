"""Tests for ``cubi_tk.snappy.itransfer_raw_data``.

We only run some smoke tests here.
"""

import os
from unittest import mock

import pytest
from pyfakefs import fake_filesystem

from cubi_tk.__main__ import setup_argparse, main


def test_run_snappy_itransfer_raw_data_help(capsys):
    parser, subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-raw-data", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_snappy_itransfer_raw_data_nothing(capsys):
    parser, subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-raw-data"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_snappy_itransfer_raw_data_smoke_test(mocker):
    fake_base_path = "/base/path"
    dest_path = "/irods/dest"
    tsv_path = os.path.join(os.path.dirname(__file__), "data", "germline.out")
    argv = [
        "snappy",
        "itransfer-raw-data",
        "--num-parallel-transfers",
        "1",
        "--base-path",
        fake_base_path,
        "--sodar-api-token",
        "XXXX",
        tsv_path,
        dest_path,
    ]

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break both biomedsheets and multiprocessing.
    fs = fake_filesystem.FakeFilesystem()

    fake_file_paths = []
    for member in ("index", "father", "mother"):
        for ext in ("", ".md5"):
            fake_file_paths.append(
                "%s/ngs_mapping/work/input_links/%s-N1-DNA1-WES1/%s-N1-DNA1-WES1.fastq.gz%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])

    fake_os = fake_filesystem.FakeOsModule(fs)
    mocker.patch("glob.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_common.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_raw_data.os", fake_os)

    mock_check_output = mock.mock_open()
    mocker.patch("cubi_tk.snappy.itransfer_common.check_output", mock_check_output)

    # Actually exercise code and perform test.
    parser, subparsers = setup_argparse()
    args = parser.parse_args(argv)
    res = main(argv)

    assert not res
    # We do not care about call order but simply test call count and then assert that all files are there which would
    # be equivalent of comparing sets of files.
    assert mock_check_output.call_count == len(fake_file_paths) * 3
    for path in fake_file_paths:
        index, rel_path = os.path.relpath(
            path, os.path.join(fake_base_path, "ngs_mapping/work/input_links")
        ).split("/", 1)
        remote_path = os.path.join(dest_path, index, "raw_data", args.remote_dir_date, rel_path)
        expected_mkdir_argv = ["imkdir", "-p", os.path.dirname(remote_path)]
        expected_irsync_argv = ["irsync", "-a", "-K", path, "i:%s" % remote_path]
        expected_ils_argv = ["ils", os.path.dirname(remote_path)]
        mock_check_output.assert_any_call(expected_mkdir_argv)
        mock_check_output.assert_any_call(expected_irsync_argv)
        mock_check_output.assert_any_call(expected_ils_argv, stderr=-2)
