"""Tests for the main command."""

import pytest

from cubi_tk.__main__ import main


def test_main_prints_help_to_stdout(capsys):
    with pytest.raises(SystemExit) as e:
        main([])

    assert e.value.code != 0
    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_main_help_prints_help_to_stdout(capsys):
    with pytest.raises(SystemExit) as e:
        main(["--help"])

    assert e.value.code == 0
    res = capsys.readouterr()
    assert res.out
    assert not res.err
