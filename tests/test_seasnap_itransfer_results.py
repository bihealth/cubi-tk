"""Tests for ``cubi_tk.sea_snap.itransfer_results``.

We only run some smoke tests here.
"""

import os
from unittest import mock

import pytest
from pyfakefs import fake_filesystem, fake_pathlib

from cubi_tk.__main__ import setup_argparse, main


def test_run_seasnap_itransfer_results_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "itransfer-results", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_seasnap_itransfer_results_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "itransfer-results"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_seasnap_itransfer_results_smoke_test(mocker):
    # --- setup arguments
    dest_path = "/irods/dest"
    fake_base_path = "/base/path"
    blueprint_path = os.path.join(os.path.dirname(__file__), "data", "test_blueprint.txt")

    argv = [
        "--verbose",
        "sea-snap",
        "itransfer-results",
        blueprint_path,
        dest_path,
        "--num-parallel-transfers",
        "0",
    ]

    parser, subparsers = setup_argparse()

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break biomedsheets.
    fs = fake_filesystem.FakeFilesystem()
    fake_os = fake_filesystem.FakeOsModule(fs)
    fake_pl = fake_pathlib.FakePathlibModule(fs)

    # --- add test files
    fake_file_paths = []
    for member in ("sample1", "sample2", "sample3"):
        for ext in ("", ".md5"):
            fake_file_paths.append(
                "%s/mapping/star/%s/out/star.%s-N1-RNA1-RNA-Seq1.bam%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_file_paths.append(
                "%s/mapping/star/%s/report/star.%s-N1-RNA1-RNA-Seq1.log%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])

    fs.add_real_file(blueprint_path)
    fake_pl.Path(blueprint_path).touch()

    # Remove index's log MD5 file again so it is recreated.
    fs.remove(fake_file_paths[3])

    # --- mock modules
    mocker.patch("glob.os", fake_os)
    mocker.patch("cubi_tk.sea_snap.itransfer_results.pathlib", fake_pl)
    mocker.patch("cubi_tk.sea_snap.itransfer_results.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_common.os", fake_os)

    mock_check_output = mock.mock_open()
    mocker.patch("cubi_tk.sea_snap.itransfer_results.check_output", mock_check_output)
    mocker.patch("cubi_tk.snappy.itransfer_common.check_output", mock_check_output)

    mock_check_call = mock.mock_open()
    mocker.patch("cubi_tk.snappy.itransfer_common.check_call", mock_check_call)

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_tk.snappy.itransfer_common.open", fake_open)

    # necessary because independent test fail
    mock_value = mock.MagicMock()
    mocker.patch("cubi_tk.sea_snap.itransfer_results.Value", mock_value)
    mocker.patch("cubi_tk.snappy.itransfer_common.Value", mock_value)

    # --- run tests
    res = main(argv)

    print(mock_check_output.call_args_list)

    assert not res

    assert fs.exists(fake_file_paths[3])

    assert mock_check_call.call_count == 1
    assert mock_check_call.call_args[0] == (["md5sum", "star.sample1-N1-RNA1-RNA-Seq1.log"],)

    assert mock_check_output.call_count == len(fake_file_paths) * 2
    remote_path = os.path.join(dest_path, "fakedest")
    for path in fake_file_paths:
        expected_mkdir_argv = f"imkdir -p $(dirname {remote_path} )"
        ext = ".md5" if path.split(".")[-1] == "md5" else ""
        expected_irsync_argv = f"irsync -a -K {path} {('i:%s' + ext) % remote_path}"

        assert ((expected_mkdir_argv,), {"shell": True}) in mock_check_output.call_args_list
        assert ((expected_irsync_argv,), {"shell": True}) in mock_check_output.call_args_list
