"""Tests for ``cubi_tk.snappy.itransfer_variant_calling``.

We only run some smoke tests here.
"""

import os
import textwrap
from unittest import mock
from unittest.mock import ANY

import pytest
from pyfakefs import fake_filesystem

from cubi_tk.__main__ import setup_argparse, main


@pytest.fixture
def minimal_config():
    """Return configuration text"""
    return textwrap.dedent(
        r"""
        static_data_config: {}

        step_config: {}

        data_sets:
          first_batch:
            file: sheet.tsv
            search_patterns:
            - {'left': '*/*/*_R1.fastq.gz', 'right': '*/*/*_R2.fastq.gz'}
            search_paths: ['/path']
            type: germline_variants
            naming_scheme: only_secondary_id
        """
    ).lstrip()


@pytest.fixture
def germline_trio_plus_sheet_tsv():
    """Return contents for germline trio plus TSV file"""
    return textwrap.dedent(
        """
        [Custom Fields]
        key\tannotatedEntity\tdocs\ttype\tminimum\tmaximum\tunit\tchoices\tpattern
        familyId\tbioEntity\tFamily\tstring\t.\t.\t.\t.\t.
        libraryKit\tngsLibrary\tEnrichment kit\tstring\t.\t.\t.\t.\t.

        [Data]
        familyId\tpatientName\tfatherName\tmotherName\tsex\tisAffected\tlibraryType\tlibraryKit\tfolderName\thpoTerms
        family1\tP001\tP002\tP003\tF\tY\tWGS\tAgilent SureSelect Human All Exon V6\tP011\t.
        family1\tP002\t.\t.\tM\tN\tWGS\tAgilent SureSelect Human All Exon V6\tP002\t.
        family1\tP003\t.\t.\tF\tN\tWGS\tAgilent SureSelect Human All Exon V6\tP003\t.
        family2\tP004\tP005\tP006\tM\tY\tWGS\tAgilent SureSelect Human All Exon V6\tP004\t.
        family2\tP005\t.\t.\tM\tN\tWGS\tAgilent SureSelect Human All Exon V6\tP005\t.
        family2\tP006\t.\t.\tF\tY\tWGS\tAgilent SureSelect Human All Exon V6\tP006\t.
        family2\tP007\t.\t.\tF\tY\tWGS\tAgilent SureSelect Human All Exon V6\tP007\t.
        """
    ).lstrip()


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


def test_run_snappy_itransfer_variant_calling_smoke_test(
    mocker, minimal_config, germline_trio_plus_sheet_tsv
):
    fake_base_path = "/base/path"
    dest_path = "466ab946-ce6a-4c78-9981-19b79e7bbe86"
    argv = [
        "--verbose",
        "snappy",
        "itransfer-variant-calling",
        "--base-path",
        fake_base_path,
        "--sodar-api-token",
        "XXXX",
        # tsv_path,
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
    # Create sample sheet in fake file system
    sample_sheet_path = fake_base_path + "/.snappy_pipeline/sheet.tsv"
    fs.create_file(
        sample_sheet_path, contents=germline_trio_plus_sheet_tsv, create_missing_dirs=True
    )
    # Create config in fake file system
    config_path = fake_base_path + "/.snappy_pipeline/config.yaml"
    fs.create_file(config_path, contents=minimal_config, create_missing_dirs=True)

    # Print path to all created files
    print("\n".join(fake_file_paths + [sample_sheet_path, config_path]))

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
