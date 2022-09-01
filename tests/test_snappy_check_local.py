"""Tests for ``cubi_tk.snappy.check_local``."""
import io
import textwrap
import pathlib

import pytest
from biomedsheets.io_tsv import read_germline_tsv_sheet
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
from biomedsheets.shortcuts import GermlineCaseSheet

from cubi_tk.snappy.check_local import GermlineSheetChecker, PedFileCheck, VcfFileCheck


@pytest.fixture
def header_sheet_tsv():
    """Returns TSV file header"""
    return """
        [Metadata]
        schema\tgermline_variants
        schema_version\tv1

        [Custom Fields]
        key\tannotatedEntity\tdocs\ttype\tminimum\tmaximum\tunit\tchoices\tpattern
        batchNo\tbioEntity\tBatch No.\tinteger\t.\t.\t.\t.\t.
        familyId\tbioEntity\tFamily\tstring\t.\t.\t.\t.\t.
        projectUuid\tbioEntity\tProject UUID\tstring\t.\t.\t.\t.\t.
        libraryKit\tngsLibrary\tEnrichment kit\tstring\t.\t.\t.\t.\t.

        [Data]
        """


@pytest.fixture
def sheet_tsv_inconsistent_parent_sex(header_sheet_tsv):
    """Return contents for germline TSV file"""
    return textwrap.dedent(
        f"""
        {header_sheet_tsv}familyId\tpatientName\tfatherName\tmotherName\tsex\tisAffected\tlibraryType\tfolderName\tbatchNo\thpoTerms\tprojectUuid\tseqPlatform\tlibraryKit
        FAM_index\tindex\tfather\tmother\tM\tY\tWES\tindex\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index\tfather\t0\t0\tU\tN\tWES\tfather\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index\tmother\t0\t0\tU\tN\tWES\tmother\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        """
    ).lstrip()


@pytest.fixture
def sheet_tsv_same_family_id_different_pedigrees(header_sheet_tsv):
    """Return contents for germline TSV file"""
    return textwrap.dedent(
        f"""
        {header_sheet_tsv}familyId\tpatientName\tfatherName\tmotherName\tsex\tisAffected\tlibraryType\tfolderName\tbatchNo\thpoTerms\tprojectUuid\tseqPlatform\tlibraryKit
        FAM_index\tindex\tfather\tmother\tM\tY\tWES\tindex\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index\tfather\t0\t0\tM\tN\tWES\tfather\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index\tmother\t0\t0\tF\tN\tWES\tmother\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index2\tindex2\tfather2\tmother2\tM\tY\tWES\tindex\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index2\tfather2\t0\t0\tM\tN\tWES\tfather\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index\tmother2\t0\t0\tF\tN\tWES\tmother\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        """
    ).lstrip()


@pytest.fixture
def sheet_tsv_father_wrong_family_id(header_sheet_tsv):
    """Return contents for germline TSV file"""
    return textwrap.dedent(
        f"""
        {header_sheet_tsv}familyId\tpatientName\tfatherName\tmotherName\tsex\tisAffected\tlibraryType\tfolderName\tbatchNo\thpoTerms\tprojectUuid\tseqPlatform\tlibraryKit
        FAM_index\tindex\tfather\tmother\tM\tY\tWES\tindex\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_wrong\tfather\t0\t0\tM\tN\tWES\tfather\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index\tmother\t0\t0\tF\tN\tWES\tmother\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        """
    ).lstrip()


@pytest.fixture
def sheet_tsv_mother_wrong_family_id(header_sheet_tsv):
    """Return contents for germline TSV file"""
    return textwrap.dedent(
        f"""
        {header_sheet_tsv}familyId\tpatientName\tfatherName\tmotherName\tsex\tisAffected\tlibraryType\tfolderName\tbatchNo\thpoTerms\tprojectUuid\tseqPlatform\tlibraryKit
        FAM_index\tindex\tfather\tmother\tM\tY\tWES\tindex\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index\tfather\t0\t0\tM\tN\tWES\tfather\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_wrong\tmother\t0\t0\tF\tN\tWES\tmother\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        """
    ).lstrip()


def create_germline_sheet_object(sheet_tsv):
    """Create Germline Sheet

    :param sheet_tsv: TSV text for sample sheet.
    :type sheet_tsv: str

    :return: Returns GermlineCaseSheet object with provided sheet tsv text.
    """
    # Create dna sample sheet based on germline sheet
    germline_sheet_io = io.StringIO(sheet_tsv)
    return GermlineCaseSheet(
        sheet=read_germline_tsv_sheet(germline_sheet_io, naming_scheme=NAMING_ONLY_SECONDARY_ID)
    )


# Tests GermlineSheetChecker ===========================================================================================


def test_germline_sheet_checker_sanity_check(germline_trio_sheet_object):
    """Tests GermlineSheetChecker.run_checks() - sanity check, sheet correctly set"""
    assert GermlineSheetChecker([germline_trio_sheet_object]).run_checks()


def test_germline_sheet_checker_parent_sex_consistency(sheet_tsv_inconsistent_parent_sex):
    """Tests GermlineSheetChecker.run_checks() - parent sex consistency, set as 'U'"""
    sheet = create_germline_sheet_object(sheet_tsv=sheet_tsv_inconsistent_parent_sex)
    assert not GermlineSheetChecker([sheet]).run_checks()


def test_germline_sheet_checker_father_wrong_family_id(sheet_tsv_father_wrong_family_id):
    """Tests GermlineSheetChecker.run_checks() - father with wrong family id"""
    sheet = create_germline_sheet_object(sheet_tsv=sheet_tsv_father_wrong_family_id)
    assert not GermlineSheetChecker([sheet]).run_checks()


def test_germline_sheet_checker_mother_wrong_family_id(sheet_tsv_mother_wrong_family_id):
    """Tests GermlineSheetChecker.run_checks() - mother with wrong family id"""
    sheet = create_germline_sheet_object(sheet_tsv=sheet_tsv_mother_wrong_family_id)
    assert not GermlineSheetChecker([sheet]).run_checks()


def test_germline_sheet_checker_same_family_id_different_pedigrees(
    sheet_tsv_same_family_id_different_pedigrees
):
    """Tests GermlineSheetChecker.run_checks() - different pedigrees with same family id"""
    sheet = create_germline_sheet_object(sheet_tsv=sheet_tsv_same_family_id_different_pedigrees)
    assert not GermlineSheetChecker([sheet]).run_checks()


# Tests PedFileCheck ===================================================================================================


def test_ped_file_check_sanity_check(germline_trio_sheet_object):
    """Tests PedFileCheck.run_checks() - sanity check, ped file correctly set"""
    path = pathlib.Path(__file__).resolve().parent / "data" / "check_remote" / "correct"
    assert PedFileCheck(sheets=[germline_trio_sheet_object], base_dir=path).run_checks()


def test_ped_file_check_inconsistency_sex(germline_trio_sheet_object):
    """Tests PedFileCheck.run_checks() - index with wrong sex in ped file"""
    path = (
        pathlib.Path(__file__).resolve().parent
        / "data"
        / "check_remote"
        / "ped_inconsistency_sex_index"
    )
    assert not PedFileCheck(sheets=[germline_trio_sheet_object], base_dir=path).run_checks()


def test_ped_file_check_inconsistency_disease(germline_trio_sheet_object):
    """Tests PedFileCheck.run_checks() - index with wrong disease status in ped file"""
    path = (
        pathlib.Path(__file__).resolve().parent
        / "data"
        / "check_remote"
        / "ped_inconsistency_disease_status"
    )
    assert not PedFileCheck(sheets=[germline_trio_sheet_object], base_dir=path).run_checks()


def test_ped_file_check_inconsistency_parents(germline_trio_sheet_object):
    """Tests PedFileCheck.run_checks() - father and mother inverted in ped file"""
    path = (
        pathlib.Path(__file__).resolve().parent
        / "data"
        / "check_remote"
        / "ped_inconsistency_inverted_parents"
    )
    assert not PedFileCheck(sheets=[germline_trio_sheet_object], base_dir=path).run_checks()


def test_ped_file_check_empty(germline_trio_sheet_object):
    """Tests PedFileCheck.run_checks() - ped file is empty"""
    path = pathlib.Path(__file__).resolve().parent / "data" / "check_remote" / "ped_empty"
    assert not PedFileCheck(sheets=[germline_trio_sheet_object], base_dir=path).run_checks()


# Tests VcfFileCheck ===================================================================================================


def test_vcf_file_check_sanity_check(germline_trio_sheet_object):
    """Tests VcfFileCheck.run_checks() - sanity check, VCF file correctly set"""
    path = pathlib.Path(__file__).resolve().parent / "data" / "check_remote" / "correct"
    assert VcfFileCheck(sheets=[germline_trio_sheet_object], base_dir=path).run_checks()


def test_vcf_file_check_missing_parent(germline_trio_sheet_object):
    """Tests VcfFileCheck.run_checks() - missing mother in VCF file"""
    path = pathlib.Path(__file__).resolve().parent / "data" / "check_remote" / "vcf_missing_parent"
    assert not VcfFileCheck(sheets=[germline_trio_sheet_object], base_dir=path).run_checks()


def test_vcf_file_check_extra_member(germline_trio_sheet_object):
    """Tests VcfFileCheck.run_checks() - sanity check, VCF file contains extra member, 'sister'"""
    path = pathlib.Path(__file__).resolve().parent / "data" / "check_remote" / "vcf_extra_member"
    assert not VcfFileCheck(sheets=[germline_trio_sheet_object], base_dir=path).run_checks()


def test_vcf_file_check_broken_symlink(germline_trio_sheet_object):
    """Tests VcfFileCheck.run_checks() - broken symlink to VCF"""
    path = pathlib.Path(__file__).resolve().parent / "data" / "check_remote" / "vcf_broken_symlink"
    assert not VcfFileCheck(sheets=[germline_trio_sheet_object], base_dir=path).run_checks()
