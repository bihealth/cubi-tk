"""Tests for ``cubi_tk.snappy.varfish_upload``."""
import pathlib

from biomedsheets.io_tsv import read_germline_tsv_sheet
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID

from cubi_tk.snappy.varfish_upload import yield_ngs_library_names


def test_yield_ngs_library_names():
    """Tests yield_ngs_library_names()"""
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
