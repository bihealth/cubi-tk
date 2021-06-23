"""Tests for ``cubi_tk.snappy.varfish_upload``."""
import pathlib

from biomedsheets import models, shortcuts
from biomedsheets.io_tsv import read_germline_tsv_sheet
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID


from cubi_tk.snappy.varfish_upload import load_sheet_tsv, yield_ngs_library_names


def test_load_sheet_tsv():
    """Tests varfish_upload.load_sheet_tsv()"""
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


def test_yield_ngs_library_names():
    """Tests varfish_upload.yield_ngs_library_names()"""
    # Define expected
    expected_batch_one = ["P001-N1-DNA1-WGS1"]
    expected_batch_two = ["P004-N1-DNA1-WGS1"]
    expected_batch_three = ["P007-N1-DNA1-WGS1"]
    expected_ped_field_defined = expected_batch_one + expected_batch_two
    expected_ped_field_none = expected_batch_one + expected_batch_two + expected_batch_three

    # Define input
    sheet_path = pathlib.Path(__file__).resolve().parent / "data" / "germline_sheet.tsv"
    with open(sheet_path, "rt") as f_sheet:
        sheet = read_germline_tsv_sheet(f=f_sheet, naming_scheme=NAMING_ONLY_SECONDARY_ID)

    # Test `pedigree_field` = 'familyId'
    actual = yield_ngs_library_names(sheet=sheet, pedigree_field="familyId")
    for name_ in actual:
        assert name_ in expected_ped_field_defined

    # Test `pedigree_field` is None
    actual = yield_ngs_library_names(sheet=sheet, pedigree_field=None)
    for name_ in actual:
        assert name_ in expected_ped_field_none

    # Test `pedigree_field` is None and min batch = 2
    actual = yield_ngs_library_names(sheet=sheet, pedigree_field=None, min_batch=2)
    for name_ in actual:
        expected_list = expected_batch_two + expected_batch_three
        assert name_ in expected_list

    # Test `pedigree_field` is None and min batch = 3
    actual = yield_ngs_library_names(sheet=sheet, pedigree_field=None, min_batch=3)
    for name_ in actual:
        assert name_ in expected_batch_three
