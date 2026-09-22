from copy import deepcopy
import pathlib
import re
from unittest.mock import MagicMock

import pytest

from cubi_tk.__main__ import setup_argparse
from cubi_tk.sodar.pull_data import PullDataCommand


def test_sodar_pull_data_get_functions():
    raise NotImplementedError

    # def get_output_basepath(self):
    #     return self.args.output_dir
    #
    # def get_output_filepath(self, out_parts: FilePathParts):
    #     #TODO: add typeguard? (ensured by check_args)
    #     # apply regexes
    #     for filepart, m_pat, r_pat in self.args.output_regex:
    #         out_parts[filepart] = re.sub(m_pat, r_pat, out_parts[filepart])
    #     return self.args.output_pattern.format(**out_parts)


    # Test with regex
    # TODO move to test_sodar_pull_data
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
    out = testinstance.build_jobs(filtered_data_objects, "/irods/project")
    assert out == expected_out

    # def get_sample_list(self) -> set[str]:
    #     # Get list of sample ids
    #     if self.args.sample_list:
    #         samples = set(self.args.sample_list)
    #     elif self.args.biomedsheet:
    #         samples = self.parse_sample_tsv(self.args.biomedsheet, sample_col=2, skip_rows=12)
    #     elif self.args.tsv:
    #         samples = self.parse_sample_tsv(
    #             self.args.tsv, sample_col=self.args.tsv_column, skip_rows=self.args.tsv_skip_rows
    #         )
    #     else:
    #         samples = set()
    #
    #     return samples
    #
    # def get_file_patterns(self) -> list[str]:
    #     """Function to get samples to filter downloadable files by collection"""
    #     if self.args.all_files:
    #         file_patterns = []
    #     elif self.args.preset:
    #         file_patterns = self.presets[self.args.preset]
    #     else:  # self.args.file_pattern
    #         file_patterns = self.args.file_pattern
    #     return file_patterns
    #
    # def get_substring_match(self) -> bool:
    #     return self.args.substring_match


def test_sodar_pull_data_check_args():
    raise NotImplementedError

    # test check that --output-dir can be created or is writable

    # test that --output-regex first var is correct


def test_run_sodar_pull_data_collection_help(capsys):
    """Test ``cubi-tk sodar pull-data --help``"""
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "pull-data", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_sodar_pull_data_collection_nothing(capsys):
    """Test ``cubi-tk sodar pull-data``"""
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "pull-data"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


def test_run_sodar_pull_data_collection_smoketest(capsys):
    raise NotImplementedError
