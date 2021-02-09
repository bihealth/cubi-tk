"""Tests for ``cubi_tk.snappy.itransfer_ngs_mapping``.

We only run some smoke tests here.
"""

import os
from unittest import mock
from unittest.mock import ANY

import pytest
from pyfakefs import fake_filesystem

from cubi_tk.__main__ import setup_argparse, main


def test_run_snappy_itransfer_ngs_mapping_help(capsys):
    parser, subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-ngs-mapping", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_snappy_itransfer_ngs_mapping_nothing(capsys):
    parser, subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["snappy", "itransfer-ngs-mapping"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_snappy_itransfer_ngs_mapping_smoke_test(mocker):
    fake_base_path = "/base/path"
    sodar_path = "sodar/path/to/landing/zone/landing_zone_uuid"
    tsv_path = os.path.join(os.path.dirname(__file__), "data", "germline.out")
    argv = [
        "--verbose",
        "snappy",
        "itransfer-ngs-mapping",
        "--base-path",
        fake_base_path,
        "--sodar-api-token",
        "XXXX",
        tsv_path,
        sodar_path,
    ]

    parser, subparsers = setup_argparse()
    args = parser.parse_args(argv)

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break both biomedsheets and multiprocessing.
    fs = fake_filesystem.FakeFilesystem()

    fake_file_paths = []
    for member in ("index", "father", "mother"):
        for ext in ("", ".md5"):
            fake_file_paths.append(
                "%s/ngs_mapping/output/bwa.%s-N1-DNA1-WES1/out/%s-N1-DNA1-WES1.bam%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])
            fake_file_paths.append(
                "%s/ngs_mapping/output/bwa.%s-N1-DNA1-WES1/log/bwa.%s-N1-DNA1-WES1.log%s"
                % (fake_base_path, member, member, ext)
            )
            fs.create_file(fake_file_paths[-1])

    # Remove index's log MD5 file again so it is recreated.
    fs.remove(fake_file_paths[3])

    fake_os = fake_filesystem.FakeOsModule(fs)
    mocker.patch("glob.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_common.os", fake_os)
    mocker.patch("cubi_tk.snappy.itransfer_ngs_mapping.os", fake_os)

    mock_check_output = mock.mock_open()
    mocker.patch("cubi_tk.snappy.itransfer_common.check_output", mock_check_output)

    fake_open = fake_filesystem.FakeFileOpen(fs)
    mocker.patch("cubi_tk.snappy.itransfer_common.open", fake_open)

    mock_check_call = mock.mock_open()
    mocker.patch("cubi_tk.snappy.itransfer_common.check_call", mock_check_call)

    # # requests mock
    # return_value = dict(assay="", config_data="", configuration="", date_modified="", description="", irods_path=sodar_path, project="", sodar_uuid="", status="", status_info="", title="", user="")
    # url_tpl = "%(sodar_url)s/landingzones/api/retrieve/%(landing_zone_uuid)s"
    # url = url_tpl % {"sodar_url": args.sodar_url, "landing_zone_uuid": args.landing_zone_uuid}
    # requests_mock.get(url, text=json.dumps(return_value))
    # #requests_mock.get("resource://biomedsheets//data/std_fields.json", text="dummy")
    # #requests_mock.get("resource://biomedsheets/data/std_fields.json#/extraInfoDefs/template/ncbiTaxon", text="dummy")

    # Actually exercise code and perform test.
    res = main(argv)

    assert not res

    # We do not care about call order but simply test call count and then assert that all files are there which would
    # be equivalent of comparing sets of files.

    assert fs.exists(fake_file_paths[3])

    assert mock_check_call.call_count == 1
    mock_check_call.assert_called_once_with(
        ["md5sum", "bwa.index-N1-DNA1-WES1.log"],
        cwd=os.path.dirname(fake_file_paths[3]),
        stdout=ANY,
    )

    assert mock_check_output.call_count == len(fake_file_paths) * 3
    for path in fake_file_paths:
        mapper_index, rel_path = os.path.relpath(
            path, os.path.join(fake_base_path, "ngs_mapping/output")
        ).split("/", 1)
        _mapper, index = mapper_index.rsplit(".", 1)
        remote_path = os.path.join(sodar_path, index, "ngs_mapping", args.remote_dir_date, rel_path)
        expected_mkdir_argv = ["imkdir", "-p", os.path.dirname(remote_path)]
        expected_irsync_argv = ["irsync", "-a", "-K", path, "i:%s" % remote_path]
        expected_ils_argv = ["ils", os.path.dirname(remote_path)]
        mock_check_output.assert_any_call(expected_mkdir_argv)
        mock_check_output.assert_any_call(expected_irsync_argv)
        mock_check_output.assert_any_call(expected_ils_argv, stderr=-2)
