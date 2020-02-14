"""Tests for ``cubi_sak.sea_snap.itransfer_results``.

We only run some smoke tests here.
"""

import os
from unittest import mock
from pathlib import Path

import pytest
import linecache
import tokenize
from pyfakefs import fake_filesystem, fake_pathlib
from pyfakefs.fake_filesystem_unittest import Patcher

from cubi_sak.__main__ import setup_argparse, main


def test_run_seasnap_itransfer_results_help(capsys):
    parser, subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "itransfer-results", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_seasnap_itransfer_results_nothing(capsys):
    parser, subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sea-snap", "itransfer-results"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


@pytest.fixture
def fs_reload_sut():
    patcher = Patcher(modules_to_reload=[setup_argparse, main])
    patcher.setUp()
    linecache.open = patcher.original_open
    tokenize._builtin_open = patcher.original_open
    yield patcher.fs
    patcher.tearDown()


def test_run_seasnap_itransfer_results_smoke_test(mocker, fs_reload_sut):
    dest_path = "/irods/dest"
    fake_base_path = "/base/path"
    blueprint_path = os.path.join(os.path.dirname(__file__), "data", "test_blueprint.txt")
    argv = ["--verbose", "sea-snap", "itransfer-results", blueprint_path, dest_path]

    # Setup fake file system but only patch selected modules.
    # We cannot use the Patcher approach here as this would
    # break both biomedsheets and multiprocessing.
    # fs = fake_filesystem.FakeFilesystem()
    fs = fs_reload_sut

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
    Path(blueprint_path).touch()

    # Remove index's log MD5 file again so it is recreated.
    fs.remove(fake_file_paths[3])
    fake_os = fake_filesystem.FakeOsModule(fs)

    fake_pathl = fake_pathlib.FakePathlibModule(fs)

    mocker.patch("pathlib.Path", fake_pathl.Path)
    mocker.patch("cubi_sak.sea_snap.itransfer_results.os", fake_os)

    mock_check_output = mock.mock_open()
    mocker.patch("cubi_sak.sea_snap.itransfer_results.check_output", mock_check_output)

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_sak.sea_snap.itransfer_results.open", fake_open)

    mock_check_call = mock.mock_open()
    mocker.patch("cubi_sak.sea_snap.itransfer_results.check_call", mock_check_call)

    # necessary because independent test fail
    mock_value = mock.mock_open()
    mocker.patch("cubi_sak.sea_snap.itransfer_results.Value", mock_value)

    # Actually exercise code and perform test.
    parser, subparsers = setup_argparse()
    res = main(argv)

    assert not res

    # We do not care about call order but simply test call count
    # and then assert that all files are there which would
    # be equivalent of comparing sets of files.

    assert fs.exists(fake_file_paths[3])

    assert mock_check_call.call_count == 1
    assert mock_check_call.call_args[0] == (["md5sum", "star.sample1-N1-RNA1-RNA-Seq1.log"],)

    assert mock_check_output.call_count == len(fake_file_paths) * 2
    remote_path = os.path.join(dest_path, "fakedest")
    for path in fake_file_paths:
        expected_mkdir_argv = ["imkdir", "-p", "$(dirname", remote_path, ")"]
        ext = ".md5" if path.split(".")[-1] == "md5" else ""
        expected_irsync_argv = ["irsync", "-a", "-K", path, ("i:%s" + ext) % remote_path]

        mock_check_output.assert_any_call(expected_mkdir_argv)
        mock_check_output.assert_any_call(expected_irsync_argv)
