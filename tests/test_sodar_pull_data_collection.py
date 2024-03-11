from copy import deepcopy
import pathlib
import re
from unittest.mock import MagicMock

import pytest

from cubi_tk.irods_common import TransferJob
from cubi_tk.sodar.pull_data_collection import PullDataCollection


class MockDataObject:
    def __init__(self, path):
        self.path = path

    def __eq__(self, other):
        return self.path == other.path

    def __repr__(self):
        return f"MockDataObject(path={self.path})"


@pytest.fixture
def filtered_data_objects():
    return {
        "coll1-N1-DNA1": [
            MockDataObject(path="/irods/project/coll1-N1-DNA1/subcol1/file1.vcf.gz"),
            MockDataObject(path="/irods/project/coll1-N1-DNA1/subcol2/file1.vcf.gz"),
            MockDataObject(path="/irods/project/coll1-N1-DNA1/subcol1/miscFile.txt"),
        ],
        "coll2-N1-DNA1": [
            MockDataObject(path="/irods/project/coll2-N1-DNA1/subcol1/file2.vcf.gz"),
            MockDataObject(path="/irods/project/coll2-N1-DNA1/subcol1/file2.bam"),
            MockDataObject(path="/irods/project/coll2-N1-DNA1/subcol1/miscFile.txt"),
        ],
    }


def test_filter_irods_collection(filtered_data_objects):
    fake_irods_data_dict = {
        "file1.vcf.gz": [
            MockDataObject(path="/irods/project/coll1-N1-DNA1/subcol1/file1.vcf.gz"),
            MockDataObject(path="/irods/project/coll1-N1-DNA1/subcol2/file1.vcf.gz"),
        ],
        "file2.vcf.gz": [
            MockDataObject(path="/irods/project/coll2-N1-DNA1/subcol1/file2.vcf.gz"),
        ],
        "file2.bam": [
            MockDataObject(path="/irods/project/coll2-N1-DNA1/subcol1/file2.bam"),
        ],
        "miscFile.txt": [
            MockDataObject(path="/irods/project/coll1-N1-DNA1/subcol1/miscFile.txt"),
            MockDataObject(path="/irods/project/coll2-N1-DNA1/subcol1/miscFile.txt"),
        ],
    }

    kwarg_list = [
        # No filters at all -> all files
        {"file_patterns": [], "samples": [], "substring_match": False},
        # Test filepattern filter works
        {"file_patterns": ["*.vcf.gz"], "samples": [], "substring_match": False},
        # Test file pattern with mutiple patterns, also **/*.X & *.Y
        {"file_patterns": ["*.vcf.gz", "**/*.txt"], "samples": [], "substring_match": False},
        # Test Sample/Collection filter works
        {"file_patterns": [], "samples": ["coll1-N1-DNA1"], "substring_match": False},
        # Test substring matching works
        {"file_patterns": [], "samples": ["coll1"], "substring_match": True},
    ]

    expected_results = [
        deepcopy(filtered_data_objects),
        {
            k: [v for v in l if v.path.endswith("vcf.gz")]
            for k, l in deepcopy(filtered_data_objects).items()
        },
        {
            k: [v for v in l if not v.path.endswith("bam")]
            for k, l in deepcopy(filtered_data_objects).items()
        },
        {k: l for k, l in deepcopy(filtered_data_objects).items() if k == "coll1-N1-DNA1"},
        {k: l for k, l in deepcopy(filtered_data_objects).items() if k == "coll1-N1-DNA1"},
    ]

    for kwargs, expected in zip(kwarg_list, expected_results):
        result = PullDataCollection.filter_irods_file_list(
            fake_irods_data_dict, "/irods/project", **kwargs
        )
        assert result == expected


def test_build_download_jobs(filtered_data_objects):
    mockargs = MagicMock()
    mockargs.output_dir = "/path/to/output"
    mockargs.output_regex = []  # ['', '', '']
    mockargs.output_pattern = "{collection}/{subcollections}/{filename}"

    testinstance = PullDataCollection(mockargs)

    expected_out = [
        TransferJob(
            path_remote=obj.path, path_local=obj.path.replace("/irods/project", "/path/to/output")
        )
        for k, l in filtered_data_objects.items()
        for obj in l
    ]
    out = testinstance.build_download_jobs(filtered_data_objects, "/irods/project")
    assert out == expected_out

    # Test with different output pattern
    mockargs.output_pattern = "{collection}/{filename}"
    expected_out = [
        TransferJob(
            path_remote=obj.path,
            path_local=re.sub(
                "/subcol[12]", "", obj.path.replace("/irods/project", "/path/to/output")
            ),
        )
        for k, l in filtered_data_objects.items()
        for obj in l
    ]
    out = testinstance.build_download_jobs(filtered_data_objects, "/irods/project")
    assert out == expected_out

    # Test with regex
    mockargs.output_regex = [
        ["subcollections", "subcol", "subcollection"],
        ["collection", "-N1-DNA1", ""],
    ]
    mockargs.output_pattern = "{collection}/{subcollections}/{filename}"
    expected_out = [
        TransferJob(
            path_remote=obj.path,
            path_local=obj.path.replace("/irods/project", "/path/to/output")
            .replace("subcol", "subcollection")
            .replace("-N1-DNA1", ""),
        )
        for k, l in filtered_data_objects.items()
        for obj in l
    ]
    out = testinstance.build_download_jobs(filtered_data_objects, "/irods/project")
    assert out == expected_out


def test_parse_samplesheet():
    # Test on Biomedsheet
    samples = PullDataCollection.parse_sample_tsv(
        pathlib.Path(__file__).resolve().parent / "data" / "pull_sheets" / "sheet.tsv",
        sample_col=2,
        n_header_cols=13,
    )

    assert samples == ["index", "mother", "father"]
