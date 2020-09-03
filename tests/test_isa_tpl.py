"""Tests for ``cubi_tk.isa_tpl``.

We run cookiecutter for each template once for smoke-testing but don't actually validate the results behind the
output directory being created.
"""

import filecmp
import glob
import os

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
    parser, subparsers = setup_argparse()
    args = parser.parse_args(["isa-tpl", "germline", str(output_path)])

    run_isatab_germline = run_cookiecutter(TEMPLATES["germline"], no_input=True)
    run_isatab_germline(args, parser, subparsers.choices[args.cmd])

    assert output_path.exists()
    assert (output_path / "i_Investigation.txt").exists()
    assert (output_path / "a_output_dir_exome_sequencing_nucleotide_sequencing.txt").exists()
    assert (output_path / "s_output_dir.txt").exists()


def test_run_cookiecutter_isatab_ms_meta_biocrates(tmp_path):
    # Setup parameters
    output_path = tmp_path / "output_dir"
    parser, subparsers = setup_argparse()
    args = parser.parse_args(["isa-tpl", "ms_meta_biocrates", str(output_path)])

    # Create templates
    run_isatpl = run_cookiecutter(TEMPLATES["ms_meta_biocrates"], no_input=True)
    run_isatpl(args, parser, subparsers.choices[args.cmd])

    # Check output files
    assert output_path.exists()
    assert (output_path / "i_Investigation.txt").exists()
    assert (output_path / "a_investigation_title_Biocrates_MxP_Quant_500_Kit_FIA.txt").exists()
    assert (output_path / "a_investigation_title_Biocrates_MxP_Quant_500_Kit_LC.txt").exists()
    assert (output_path / "s_investigation_title.txt").exists()

    # Run altamisa validate here? I.e. it shouldn't throw exceptions or critical warnings.

    # Test against reference files
    path_test = os.path.join(os.path.dirname(__file__), "data", "isa_tpl", "ms_meta_biocrates_01")
    files = glob.glob(os.path.join(path_test, "*"))
    match, mismatch, errors = filecmp.cmpfiles(
        path_test, output_path, (os.path.basename(f) for f in files), shallow=False
    )
    print([match, mismatch, errors])
    assert len(mismatch) == 0
    assert len(errors) == 0
