"""Tests for ``cubi_tk.isa_tpl``.

We run cookiecutter for each template once for smoke-testing but don't actually validate the results behind the
output directory being created.
"""

from argparse import ArgumentParser
from unittest.mock import patch

from cubi_tk.isa_tpl.__init__ import validate_output_directory


@patch.object(ArgumentParser, "error")
def test_validate_output_directory(test_error, tmp_path):
    parser = ArgumentParser()
    d = tmp_path / "dir"
    d.mkdir()
    d2 = tmp_path / "no_dir" / "subdir"
    validate_output_directory(parser, d)
    validate_output_directory(parser, d2)
    validate_output_directory(parser, d2.parent)
    assert parser.error.call_count == 2
