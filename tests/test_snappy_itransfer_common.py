"""Tests for ``cubi_tk.snappy.itransfer_common``."""
import pathlib

import pytest

from biomedsheets.io_tsv import read_germline_tsv_sheet
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID

from cubi_tk.snappy.itransfer_common import SnappyItransferCommandBase


@pytest.fixture
def snappy_itransfer_command_base():
    """Returns SnappyItransferCommandBase object"""
    return SnappyItransferCommandBase(args=None)


def test_yield_ngs_library_names(snappy_itransfer_command_base):
    """Tests varfish_upload.yield_ngs_library_names()"""
    # Define expected
    expected_batch_one = ["P001-N1-DNA1-WGS1", "P002-N1-DNA1-WGS1", "P003-N1-DNA1-WGS1"]
    expected_batch_two = ["P004-N1-DNA1-WGS1", "P005-N1-DNA1-WGS1", "P006-N1-DNA1-WGS1"]
    expected_batch_three = ["P007-N1-DNA1-WGS1", "P008-N1-DNA1-WGS1", "P009-N1-DNA1-WGS1"]
    expected_batch_four = ["P010-N1-DNA1-WGS1", "P011-N1-DNA1-WGS1", "P012-N1-DNA1-WGS1"]
    expected_batch_five = ["P013-N1-DNA1-WGS1", "P014-N1-DNA1-WGS1"]
    expected_batch_six = ["P015-N1-DNA1-WGS1"]

    # Define input
    sheet_path = pathlib.Path(__file__).resolve().parent / "data" / "germline_sheet_multi_batch.tsv"
    with open(sheet_path, "rt") as f_sheet:
        sheet = read_germline_tsv_sheet(f=f_sheet, naming_scheme=NAMING_ONLY_SECONDARY_ID)

    # Sanity test - no constraints
    actual = snappy_itransfer_command_base.yield_ngs_library_names(
        sheet=sheet, min_batch=None, max_batch=None
    )
    expected_list = (
        expected_batch_one
        + expected_batch_two
        + expected_batch_three
        + expected_batch_four
        + expected_batch_five
        + expected_batch_six
    )
    for name_ in actual:
        assert name_ in expected_list

    # Test min batch = 2, max batch = None
    actual = snappy_itransfer_command_base.yield_ngs_library_names(
        sheet=sheet, min_batch=2, max_batch=None
    )
    expected_list = (
        expected_batch_two
        + expected_batch_three
        + expected_batch_four
        + expected_batch_five
        + expected_batch_six
    )
    for name_ in actual:
        assert name_ in expected_list
    # Test min batch = 2, max batch = 3
    actual = snappy_itransfer_command_base.yield_ngs_library_names(
        sheet=sheet, min_batch=2, max_batch=3
    )
    expected_list = expected_batch_two + expected_batch_three
    for name_ in actual:
        assert name_ in expected_list

    # Test min batch = 3, max batch = 5
    actual = snappy_itransfer_command_base.yield_ngs_library_names(
        sheet=sheet, min_batch=3, max_batch=5
    )
    expected_list = expected_batch_three + expected_batch_four + expected_batch_five
    for name_ in actual:
        assert name_ in expected_list

    # Test min batch = 5, max batch = 5
    actual = snappy_itransfer_command_base.yield_ngs_library_names(
        sheet=sheet, min_batch=5, max_batch=5
    )
    expected_list = expected_batch_five
    for name_ in actual:
        assert name_ in expected_list

    # Test min batch = 6, max batch = 6
    actual = snappy_itransfer_command_base.yield_ngs_library_names(
        sheet=sheet, min_batch=6, max_batch=6
    )
    expected_list = expected_batch_six
    for name_ in actual:
        assert name_ in expected_list
