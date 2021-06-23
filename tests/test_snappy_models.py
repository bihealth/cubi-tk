"""Tests for ``cubi_tk.snappy.models``."""
import pathlib

import pytest

from cubi_tk.snappy.models import DataSet, SearchPattern, load_datasets


@pytest.fixture
def dataset_w_pedigree_field():
    """
    :return: Return model Dataset example with `pedigree_field` defined.
    """
    search_pattern = SearchPattern(left="*/*/*_R1.fastq.gz", right="*/*/*_R2.fastq.gz")
    dataset = DataSet(
        sheet_file="sheet.tsv",
        sheet_type="germline_variants",
        search_paths=("/path",),
        search_patterns=(search_pattern,),
        naming_scheme="only_secondary_id",
        sodar_uuid="99999999-aaaa-bbbb-cccc-999999999999",
        pedigree_field="familyId",
    )
    return dataset


@pytest.fixture
def dataset_wo_pedigree_field():
    """
    :return: Return model Dataset example without a defined `pedigree_field`.
    """
    search_pattern = SearchPattern(left="*/*/*_R1.fastq.gz", right="*/*/*_R2.fastq.gz")
    dataset = DataSet(
        sheet_file="sheet_wo.tsv",
        sheet_type="germline_variants",
        search_paths=("/path",),
        search_patterns=(search_pattern,),
        naming_scheme="only_secondary_id",
        sodar_uuid="99999999-dddd-eeee-ffff-999999999999",
    )
    return dataset


def test_dataset_w_fixture(dataset_w_pedigree_field):
    """Testes dataset_w_pedigree_field()"""
    ds = dataset_w_pedigree_field
    assert isinstance(ds, DataSet)
    assert ds.sheet_file == "sheet.tsv"
    assert ds.naming_scheme == "only_secondary_id"
    assert ds.sodar_uuid == "99999999-aaaa-bbbb-cccc-999999999999"
    assert ds.pedigree_field == "familyId"


def test_dataset_wo_fixture(dataset_wo_pedigree_field):
    """Testes dataset_wo_pedigree_field()"""
    ds = dataset_wo_pedigree_field
    assert isinstance(ds, DataSet)
    assert ds.sheet_file == "sheet_wo.tsv"
    assert ds.naming_scheme == "only_secondary_id"
    assert ds.sodar_uuid == "99999999-dddd-eeee-ffff-999999999999"
    assert ds.pedigree_field is None


def test_load_datasets(dataset_w_pedigree_field, dataset_wo_pedigree_field):
    """Tests models.load_datasets()"""
    # Define expected
    expected = {"first_batch": dataset_w_pedigree_field, "second_batch": dataset_wo_pedigree_field}
    # Define input
    config_path = pathlib.Path(__file__).resolve().parent / "data" / "test_config.yaml"
    # Get actual
    actual = load_datasets(config_path)
    assert len(actual) == 2, "Should return two datasets: 'first_batch' and 'second_batch'."
    assert actual == expected
