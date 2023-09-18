"""Tests for ``cubi_tk.isa_tpl``."""

from argparse import ArgumentParser
from unittest.mock import patch

from cubi_isa_templates import TEMPLATES

from cubi_tk.isa_tpl.__init__ import run_cookiecutter, validate_output_directory


def test_run_cookiecutter(tmp_path):
    tpl = TEMPLATES["generic"]
    args = type("test", (), {})()
    path = tmp_path / "dir"
    args.output_dir = str(path)
    args.verbose = False

    run_cookiecutter(tpl, args, no_input=True)


@patch.object(ArgumentParser, "error")
def test_validate_output_directory(mockerror, tmp_path):
    parser = ArgumentParser()
    d = tmp_path / "dir"
    d.mkdir()
    d2 = tmp_path / "no_dir" / "subdir"
    validate_output_directory(parser, d)
    validate_output_directory(parser, d2)
    validate_output_directory(parser, d2.parent)
    assert mockerror.call_count == 2
