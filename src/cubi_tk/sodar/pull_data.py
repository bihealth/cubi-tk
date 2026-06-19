"""``cubi-tk sodar pull-data``: download data from iRODS via SODAR."""

import argparse
import os
import re

from ..sodar_common import SodarPullBase
from loguru import logger

class PullDataCommand(SodarPullBase):
    """Implementation of pull data command."""

    command_name = "pull-data"

    presets = {
        "dragen": [
            "**/*_FAM_dragen.fam.hard-filtered.vcf.gz**/*_FAM_dragen.fam.hard-filtered.vcf.gz.tbi",
            "**/*_FAM_dragen.fam.cnv.vcf.gz",
            "**/*_FAM_dragen.fam.cnv.vcf.gz.tbi",
            "**/*_FAM_dragen.fam.sv.vcf.gz",
            "**/*_FAM_dragen.fam.sv.vcf.gz.tbi",
            "**/*.qc-coverage*.csv",
            "**/*.ped",
            "**/*.mapping_metrics.csv",
        ],
    }

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments for ``pull-data`` command."""
        parser.add_argument(
            "--hidden-cmd", dest="sodar_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        group_files = parser.add_mutually_exclusive_group(required=True)

        group_files.add_argument(
            "-p", "--preset", help="Preset to use for file selection.", choices=cls.presets.keys()
        )
        group_files.add_argument(
            "-f", "--file-pattern", help="File pattern to use for file selection.", nargs="+"
        )
        group_files.add_argument(
            "-a",
            "--all-files",
            action="store_true",
            help="Do not filter files, download everything.",
        )

        group_samples = parser.add_argument_group("Sample Filters")
        group_samples.add_argument(
            "--substring-match",
            action="store_true",
            help="Defined samples do not need to match collections exactly, a substring match is enough",
        )

        group_samples.add_argument(
            "-s",
            "--sample-list",
            nargs="+",
            help="Sample list used for filtering collections."
            "Takes precedence over --tsv and --biomedsheet.",
        )
        group_samples.add_argument(
            "--biomedsheet",
            help="Biomedsheet file for filtering collections. Sets tsv-column to 2 and "
            "tsv-skip-rows to 12. Takes precedence over --tsv.",
        )
        group_samples.add_argument(
            "--tsv", help="Tabular file with sample names to use for filtering collections."
        )
        group_samples.add_argument(
            "--tsv-column",
            default=1,
            help="Column index for sample entries in tsv file. Default: 1.",
        )
        group_samples.add_argument(
            "--tsv-skip-rows", default=0, help="Number of header lines in tsv file. Default: 0."
        )

        parser.add_argument(
            "-o", "--output-dir", help="Output directory. Default: $PWD", default=os.getcwd()
        )
        parser.add_argument(
            "--output-pattern",
            default="{collection}/{subcollections}/{filename}",
            help="Pattern for output files. Default: '{collection}/{subcollections}/{filename}'",
        )
        parser.add_argument(
            "--output-regex",
            nargs=3,
            action="append",
            metavar=("FILEPART", "MATCH", "REPL"),
            default=[],
            type=str,
            help="Regular expression to change parts from iRODS path for output pattern. "
            "Syntax: 'collection|subcollections|filename' 'regex' 'replacement'"
            "Can be given multiple times, Default: None",
        )

    def check_args(self, args) -> int | None:
        """Called for checking arguments, override to change behaviour."""
        res = super().check_args(args)
        # Validate output directory path
        if not os.path.exists(args.output_dir):
            try:
                os.makedirs(args.output_dir)
            except Exception as e:
                logger.error(
                    f"Output directory path does not exist and cannot be created: {args.output_dir}"
                )
                logger.debug(e)
                res = 1
        elif not os.access(args.output_dir, os.W_OK):
            logger.error(
                f"Output directory path either does not exist or it is not writable: {args.base_path}"
            )
            res = 1
        return res

    def get_output_basepath(self):
        return self.args.output_dir

    def get_output_filepath(self, out_parts: dict[str, str]):
        # apply regexes
        for filepart, m_pat, r_pat in self.args.output_regex:
            out_parts[filepart] = re.sub(m_pat, r_pat, out_parts[filepart])
        return self.args.output_pattern.format(**out_parts)

    def get_sample_list(self) -> set[str] | None:
        # Get list of sample ids
        if self.args.sample_list:
            samples = set(self.args.sample_list)
        elif self.args.biomedsheet:
            samples = self.parse_sample_tsv(self.args.biomedsheet, sample_col=2, skip_rows=12)
        elif self.args.tsv:
            samples = self.parse_sample_tsv(
                self.args.tsv, sample_col=self.args.tsv_column, skip_rows=self.args.tsv_skip_rows
            )
        else:
            samples = None

        return samples

    def get_file_patterns(self) -> list[str]:
        """Function to get samples to filter downloadable files by collection"""
        if self.args.all_files:
            file_patterns = []
        elif self.args.preset:
            file_patterns = self.presets[self.args.preset]
        else:  # self.args.file_pattern
            file_patterns = self.args.file_pattern
        return file_patterns

    def get_substring_match(self) -> bool:
        return self.args.substring_match


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk org-raw check``."""
    return PullDataCommand.setup_argparse(parser)
