"""Tests for ``cubi_tk.dkfz.prepare_isatab``.

We only run some smoke tests here.
"""

import glob
import os

from pathlib import Path

import pytest
from pyfakefs import fake_filesystem, fake_pathlib

from cubi_tk.__main__ import setup_argparse, main


def test_run_dkfz_prepare_isatab_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["dkfz", "prepare-isatab", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_dkfz_prepare_isatab_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["dkfz", "prepare-isatab"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_dkfz_prepare_isatab_smoke_test(mocker, requests_mock):
    # --- setup arguments
    isatab_dir = "/isatab_directory"
    meta_path = os.path.join(os.path.dirname(__file__), "data", "dkfz", "mocks", "1_meta.tsv")
    argv = [
        "--verbose",
        "dkfz",
        "prepare-isatab",
        "--study-title",
        "test",
        "--mapping-table",
        os.path.join(isatab_dir, "mapping_table.txt"),
        meta_path,
        isatab_dir,
    ]

    parser, _subparsers = setup_argparse()

    # Setup fake file system but only patch selected modules.  We cannot use the Patcher approach here as this would
    # break biomedsheets.
    fs = fake_filesystem.FakeFilesystem()
    fake_pl = fake_pathlib.FakePathlibModule(fs)
    fake_open = fake_filesystem.FakeFileOpen(fs)

    # --- add test files
    fs.add_real_file(meta_path)
    fs.add_real_directory(
        str(
            Path(
                os.path.join(os.path.dirname(__file__), "..", "cubi_tk", "isa_tpl", "isatab-dkfz")
            ).resolve()
        )
    )

    # --- mock modules
    mocker.patch("cubi_tk.dkfz.prepare_isatab.Path", fake_pl.Path)
    mocker.patch("cubi_tk.dkfz.prepare_isatab.open", fake_open)
    mocker.patch("pandas.io.common.open", fake_open)

    # --- pyfakefs.FakeDirectory object needed for the target directory
    isatab_dir = fs.create_dir(isatab_dir)

    # --- run tests
    res = main(argv)

    assert not res

    # test whether ISA files were created OK
    regression_dir = str(
        Path(os.path.join(os.path.dirname(__file__), "data", "dkfz", "regression")).resolve()
    )
    fs.add_real_directory(regression_dir)
    filenames = glob.glob(os.path.join(regression_dir, "*"))

    # filecmp & pyfakefs don't play nice together
    for filename in filenames:
        f = os.path.basename(filename)
        assert f in isatab_dir.ordered_dirs
        fake_file = isatab_dir.get_entry(f)
        assert fake_file.contents == Path(filename).read_text()
