"""Tests for ``cubi_tk.isa_tab``.

"""
import filecmp
import glob
import os
from shutil import copytree

from cubi_tk.__main__ import main


def test_run_isatab_annotate_case1_default(tmp_path):
    # Testing under default settings
    # i.e. first study, first assay, no forced overwrite of annotations

    # Input isa-tab files
    path_input = os.path.join(os.path.dirname(__file__), "data", "isa_tab", "annotate_input")
    path_input = copytree(path_input, os.path.join(tmp_path, "isa_tab_annotate"))

    # Input annotation
    path_input_annotation = os.path.join(
        os.path.dirname(__file__), "data", "isa_tab", "annotate_input", "isa_tab_annotation.csv"
    )

    # Run annotation
    argv = [
        "isa-tab",
        "annotate",
        "--yes",
        os.path.join(path_input, "i_Investigation.txt"),
        path_input_annotation,
    ]

    res = main(argv)
    assert not res

    # Reference files
    path_test = os.path.join(os.path.dirname(__file__), "data", "isa_tab", "annotate_result1")

    # Tests
    files = glob.glob(os.path.join(path_test, "*"))

    match, mismatch, errors = filecmp.cmpfiles(
        path_test, path_input, (os.path.basename(f) for f in files), shallow=False
    )
    print([match, mismatch, errors])
    assert len(mismatch) == 0
    assert len(errors) == 0


def test_run_isatab_annotate_case2_specific(tmp_path):
    # Testing more specific settings
    # i.e. selecting a specific study and assay and forced overwrite of annotations

    # Input isa-tab files
    path_input = os.path.join(os.path.dirname(__file__), "data", "isa_tab", "annotate_input")
    path_input = copytree(path_input, os.path.join(tmp_path, "isa_tab_annotate"))

    # Input annotation
    path_input_annotation = os.path.join(
        os.path.dirname(__file__), "data", "isa_tab", "annotate_input", "isa_tab_annotation.csv"
    )

    # Run annotation
    argv = [
        "isa-tab",
        "annotate",
        "--force-update",
        "--yes",
        os.path.join(path_input, "i_Investigation.txt"),
        path_input_annotation,
        "--target-study",
        "s_isatest.txt",
        "--target-assay",
        "a_isatest_selecting_assay_2.txt",
    ]

    res = main(argv)
    assert not res

    # Reference files
    path_test = os.path.join(os.path.dirname(__file__), "data", "isa_tab", "annotate_result2")

    # Tests
    files = glob.glob(os.path.join(path_test, "*"))

    match, mismatch, errors = filecmp.cmpfiles(
        path_test, path_input, (os.path.basename(f) for f in files), shallow=False
    )
    print([match, mismatch, errors])
    assert len(mismatch) == 0
    assert len(errors) == 0
