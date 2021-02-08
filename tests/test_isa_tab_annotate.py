"""Tests for ``cubi_tk.isa_tab``.

"""
import os
import glob
import filecmp

from shutil import copytree

import pytest

from cubi_tk.__main__ import main


def test_run_isatab_annotate_case1(tmp_path):
    # test input isa-tab files
    path_input = os.path.join(os.path.dirname(__file__), "data", "isa_tab", "annotate_input")
    path_input = copytree(path_input, os.path.join(tmp_path, "isa_tab_annotate"))

    # test input annotation
    path_input_annotation = os.path.join(
        os.path.dirname(__file__), "data", "isa_tab", "annotate_input", "isa_tab_annotation.csv"
    )

    # run annotation
    argv = [
        "isa-tab",
        "annotate",
        "--yes",
        os.path.join(path_input, "i_Investigation.txt"),
        path_input_annotation,
    ]

    res = main(argv)
    assert not res

    # test reference files
    path_test = os.path.join(os.path.dirname(__file__), "data", "isa_tab", "annotate_result1")

    # tests
    files = glob.glob(os.path.join(path_test, "*"))

    match, mismatch, errors = filecmp.cmpfiles(
        path_test, path_input, (os.path.basename(f) for f in files), shallow=False
    )
    print([match, mismatch, errors])
    assert len(mismatch) == 0
    assert len(errors) == 0


def test_run_isatab_annotate_case2(tmp_path):
    # test input isa-tab files
    path_input = os.path.join(os.path.dirname(__file__), "data", "isa_tab", "annotate_input")
    path_input = copytree(path_input, os.path.join(tmp_path, "isa_tab_annotate"))

    # test input annotation
    path_input_annotation = os.path.join(
        os.path.dirname(__file__), "data", "isa_tab", "annotate_input", "isa_tab_annotation.csv"
    )

    # run annotation
    argv = [
        "isa-tab",
        "annotate",
        "--force-update",
        "--yes",
        os.path.join(path_input, "i_Investigation.txt"),
        path_input_annotation,
    ]

    res = main(argv)
    assert not res

    # test reference files
    path_test = os.path.join(os.path.dirname(__file__), "data", "isa_tab", "annotate_result2")

    # tests
    files = glob.glob(os.path.join(path_test, "*"))

    match, mismatch, errors = filecmp.cmpfiles(
        path_test, path_input, (os.path.basename(f) for f in files), shallow=False
    )
    print([match, mismatch, errors])
    assert len(mismatch) == 0
    assert len(errors) == 0


def test_run_isatab_annotate_case_sciex(tmp_path):
    # test input isa-tab files
    path_input = os.path.join(os.path.dirname(__file__), "data", "isa_tpl", "ms_meta_biocrates_01")
    path_input = copytree(path_input, os.path.join(tmp_path, "isa_tab_annotate"))

    # test input annotation
    path_input_annotation = os.path.join(
        os.path.dirname(__file__),
        "data",
        "isa_tab",
        "annotate_sciex1_anno",
        "sciex_sequence_lc_batch2.csv",
    )

    # run annotation
    argv = [
        "isa-tab",
        "annotate",
        "--yes",
        "--use-case",
        "sciex_analyst_sequence",
        "--target-assay",
        "1",
        os.path.join(path_input, "i_Investigation.txt"),
        path_input_annotation,
    ]

    res = main(argv)
    assert not res

    # test reference files
    path_test = os.path.join(
        os.path.dirname(__file__), "data", "isa_tab", "annotate_sciex1_results"
    )

    # tests
    files = glob.glob(os.path.join(path_test, "*"))

    match, mismatch, errors = filecmp.cmpfiles(
        path_test, path_input, (os.path.basename(f) for f in files), shallow=False
    )
    print([match, mismatch, errors])
    assert len(mismatch) == 0
    assert len(errors) == 0


@pytest.mark.skip(reason="to be implemented")
def test_run_isatab_annotate_case_sciex2(tmp_path):
    # test input isa-tab files
    path_input = os.path.join(os.path.dirname(__file__), "data", "isa_tab", "annotate_sciex2_input")
    path_input = copytree(path_input, os.path.join(tmp_path, "isa_tab_annotate"))

    # test input annotation
    path_input_annotation = os.path.join(
        os.path.dirname(__file__),
        "data",
        "isa_tab",
        "annotate_sciex2_anno",
        "sciex_sequence_lc_batch2.csv",
    )

    # run annotation
    argv = [
        "isa-tab",
        "annotate",
        "--yes",
        "--use-case",
        "sciex_analyst_sequence",
        "--target-assay",
        "1",
        os.path.join(path_input, "i_Investigation.txt"),
        path_input_annotation,
    ]

    res = main(argv)
    assert not res

    # test reference files
    path_test = os.path.join(
        os.path.dirname(__file__), "data", "isa_tab", "annotate_sciex2_results"
    )

    # tests
    files = glob.glob(os.path.join(path_test, "*"))

    match, mismatch, errors = filecmp.cmpfiles(
        path_test, path_input, (os.path.basename(f) for f in files), shallow=False
    )
    print([match, mismatch, errors])
    assert len(mismatch) == 0
    assert len(errors) == 0
