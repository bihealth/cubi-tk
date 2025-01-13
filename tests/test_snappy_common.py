"""Tests for ``cubi_tk.snappy.common``."""

import pathlib

from biomedsheets import models, shortcuts
import pytest

from cubi_tk.snappy.common import (
    CouldNotFindBioMedSheet,
    CouldNotFindPipelineRoot,
    find_snappy_root_dir,
    get_biomedsheet_path,
    load_sheet_tsv,
)


def test_could_not_find_pipeline_root_exception():
    """Tests CouldNotFindPipelineRoot raise"""
    with pytest.raises(CouldNotFindPipelineRoot):
        raise CouldNotFindPipelineRoot()


def test_could_not_find_biomedsheet_exception():
    """Tests CouldNotFindPipelineRoot raise"""
    with pytest.raises(CouldNotFindBioMedSheet):
        raise CouldNotFindBioMedSheet()


def test_find_snappy_root_dir():
    """Tests find_snappy_root_dir()"""
    # Define input
    in_root_dir_present = pathlib.Path(__file__).resolve().parent / "data" / "find_snappy"
    in_root_dir_absent = pathlib.Path(__file__).resolve().parent / "data" / "fastq_test"
    # Positive test - directory contains '.snappy_pipeline'
    actual = find_snappy_root_dir(start_path=in_root_dir_present)
    assert actual is not None
    # Negative test - directory should not be find
    with pytest.raises(CouldNotFindPipelineRoot):
        find_snappy_root_dir(start_path=in_root_dir_absent)


def test_load_sheet_tsv():
    """Tests load_sheet_tsv()"""
    # Define expected
    expected_ngs_library_name_list = ["P001-N1-DNA1-WGS1", "P004-N1-DNA1-WGS1", "P007-N1-DNA1-WGS1"]

    # Define input
    sheet_path = pathlib.Path(__file__).resolve().parent / "data" / "germline_sheet.tsv"

    # Get actual
    sheet = load_sheet_tsv(path_tsv=sheet_path)
    assert isinstance(sheet, models.Sheet)

    # Convert to manageable format
    shortcut_sheet = shortcuts.GermlineCaseSheet(sheet)
    for pedigree in shortcut_sheet.cohort.pedigrees:
        assert pedigree.index.dna_ngs_library.name in expected_ngs_library_name_list


def test_get_biomedsheet_path():
    """Tests get_biomedsheet_path()"""
    # Define input
    uuid = "99999999-aaaa-bbbb-cccc-999999999999"
    config_path = (
        pathlib.Path(__file__).resolve().parent
        / "data"
        / "find_snappy"
        / ".snappy_pipeline"
        / "config.yaml"
    )
    # Positive test - UUID in config file
    actual = get_biomedsheet_path(config_path, uuid)
    assert actual is not None
    # Negative test - should raise exception as UUID doesn't exist
    with pytest.raises(CouldNotFindBioMedSheet):
        get_biomedsheet_path(config_path, "123456")
