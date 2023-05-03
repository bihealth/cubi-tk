"""Tests for ``cubi_tk.sodar.ingest``."""

import pytest
from unittest import mock
from pyfakefs import fake_filesystem, fake_pathlib
from cubi_tk.__main__ import main, setup_argparse


def test_run_sodar_ingest_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_sodar_ingest_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err
