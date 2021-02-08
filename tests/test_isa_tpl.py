"""Tests for ``cubi_tk.isa_tpl``.

We run cookiecutter for each template once for smoke-testing but don't actually validate the results behind the
output directory being created.
"""

from cubi_tk.isa_tpl import run_cookiecutter, TEMPLATES
from cubi_tk.__main__ import setup_argparse


def test_run_cookiecutter_isatab_generic(tmp_path):
    output_path = tmp_path / "output_dir"
    parser, subparsers = setup_argparse()
    args = parser.parse_args(["isa-tpl", "generic", str(output_path)])

    run_isatab_generic = run_cookiecutter(TEMPLATES["generic"], no_input=True)
    run_isatab_generic(args, parser, subparsers.choices[args.cmd])

    assert output_path.exists()
    assert (output_path / "i_Investigation.txt").exists()
    assert (output_path / "a_output_dir_transcriptome_profiling_nucleotide_sequencing.txt").exists()
    assert (output_path / "s_output_dir.txt").exists()


def test_run_cookiecutter_isatab_germline(tmp_path):
    output_path = tmp_path / "output_dir"
    parser, _subparsers = setup_argparse()
    args = parser.parse_args(["isa-tpl", "germline", str(output_path)])

    run_isatab_germline = run_cookiecutter(TEMPLATES["germline"], no_input=True)
    run_isatab_germline(args, parser, subparsers.choices[args.cmd])

    assert output_path.exists()
    assert (output_path / "i_Investigation.txt").exists()
    assert (output_path / "a_output_dir_exome_sequencing_nucleotide_sequencing.txt").exists()
    assert (output_path / "s_output_dir.txt").exists()
