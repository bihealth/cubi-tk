"""Tests for ``cubi_tk.sodar.ingest``."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from cubi_tk.__main__ import setup_argparse
from cubi_tk.sodar.ingest import SodarIngest


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


@pytest.fixture
def ingest():
    obj = SodarIngest(args={"sources": "testfolder", "recursive": True})
    obj.lz_irods_path = "/irodsZone"
    obj.target_coll = "targetCollection"
    return obj
