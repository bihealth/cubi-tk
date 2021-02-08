"""Tests for ``cubi_tk.snappy.itransfer_variant_calling``.

We only run some smoke tests here.
"""

import os
from unittest import mock
from unittest.mock import ANY

import pytest
from pyfakefs import fake_filesystem

from cubi_tk.__main__ import setup_argparse, main


def test_run_snappy_itransfer_variant_calling_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-variant-calling", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_snappy_itransfer_variant_calling_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-variant-calling"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_snappy_itransfer_variant_calling_smoke_test(mocker):
    fake_base_path = "/base/path"
    dest_path = "/irods/dest"
    tsv_path = os.path.join(os.path.dirname(__file__), "data", "germline.out")
    argv = [
        "--verbose",
        "snappy",
        "itransfer-variant-calling",
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
    for member in ("index",):
        for ext in ("", ".md5"):
            fake_file_paths.append(
                "%s/variant_calling/output/bwa.gatk_hc.%s-N1-DNA1-WES1/out/bwa.gatk_hc.%s-N1-DNA1-WES1.vcf.gz%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_file_paths.append(
                "%s/variant_calling/output/bwa.gatk_hc.%s-N1-DNA1-WES1/out/bwa.gatk_hc.%s-N1-DNA1-WES1.vcf.gz.tbi%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_file_paths.append(
                "%s/variant_calling/output/bwa.gatk_hc.%s-N1-DNA1-WES1/log/bwa.gatk_hc.%s-N1-DNA1-WES1.log%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])

    print("\n".join(fake_file_paths))

    # Remove index's log MD5 file again so it is recreated.
    fs.remove(fake_file_paths[3])

    fake_os = fake_filesystem.FakeOsModule(fs)
    mocker.patch("glob.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_common.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_variant_calling.os", fake_os)

    mock_check_output = mock.mock_open()
    mocker.patch("cubi_tk.snappy.itransfer_common.check_output", mock_check_output)

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_tk.snappy.itransfer_common.open", fake_open)

    mock_check_call = mock.mock_open()
    mocker.patch("cubi_tk.snappy.itransfer_common.check_call", mock_check_call)

    # Actually exercise code and perform test.
    parser, _subparsers = setup_argparse()
    args = parser.parse_args(argv)
    res = main(argv)

    assert not res

    # We do not care about call order but simply test call count and then assert that all files are there which would
    # be equivalent of comparing sets of files.

    assert fs.exists(fake_file_paths[3])

    assert mock_check_call.call_count == 1
    mock_check_call.assert_called_once_with(
        ["md5sum", "bwa.gatk_hc.index-N1-DNA1-WES1.vcf.gz"],
        cwd=os.path.dirname(fake_file_paths[3]),
        stdout=ANY,
    )

    assert mock_check_output.call_count == len(fake_file_paths) * 3
    for path in fake_file_paths:
        mapper_index, rel_path = os.path.relpath(
            path, os.path.join(fake_base_path, "variant_calling/output")
        ).split("/", 1)
        _mapper, index = mapper_index.rsplit(".", 1)
        remote_path = os.path.join(
            dest_path, index, "variant_calling", args.remote_dir_date, rel_path
        )
        expected_mkdir_argv = ["imkdir", "-p", os.path.dirname(remote_path)]
        expected_irsync_argv = ["irsync", "-a", "-K", path, "i:%s" % remote_path]
        expected_ils_argv = ["ils", os.path.dirname(remote_path)]
        mock_check_output.assert_any_call(expected_mkdir_argv)
        mock_check_output.assert_any_call(expected_irsync_argv)
        mock_check_output.assert_any_call(expected_ils_argv, stderr=-2)
