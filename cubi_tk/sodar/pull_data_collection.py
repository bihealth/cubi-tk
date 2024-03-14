"""``cubi-tk sodar pull-data-collection``: download raw data from iRODS via SODAR."""

import argparse
from collections import defaultdict
import os
from pathlib import PurePosixPath
import re
import typing
from typing import Dict, List

from irods.data_object import iRODSDataObject
from logzero import logger
import pandas as pd

from ..common import load_toml_config
from ..irods_common import TransferJob, iRODSTransfer
from ..snappy.pull_data_common import PullDataCommon
from ..sodar_common import RetrieveSodarCollection


class PullDataCollection(PullDataCommon):
    """Implementation of pull data collection command."""

    command_name = "pull-data-collection"

    presets = {
        "dragen": [
            "**/*_FAM_dragen.fam.hard-filtered.vcf.gz"
            "**/*_FAM_dragen.fam.hard-filtered.vcf.gz.tbi",
            "**/*_FAM_dragen.fam.cnv.vcf.gz",
            "**/*_FAM_dragen.fam.cnv.vcf.gz.tbi",
            "**/*_FAM_dragen.fam.sv.vcf.gz",
            "**/*_FAM_dragen.fam.sv.vcf.gz.tbi",
            "**/*.qc-coverage*.csv",
            "**/*.ped",
            "**/*.mapping_metrics.csv",
        ],
    }

    def __init__(self, args):
        """Constructor.

        :param args: argparse object with command line arguments.
        :type args: argparse.Namespace

        :param sodar_config_path: Path to SODAR configuration file.
        :type sodar_config_path: pathlib.Path

        :param irods_env_path: Path to irods_environment.json
        :type irods_env_path: pathlib.Path, optional
        """
        PullDataCommon.__init__(self)
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments for ``check-remote`` command."""
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )
        parser.add_argument(
            "--sodar-url",
            default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
            help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
        )
        parser.add_argument(
            "--sodar-api-token",
            default=os.environ.get("SODAR_API_TOKEN", None),
            help="Authentication token when talking to SODAR.  Defaults to SODAR_API_TOKEN environment variable.",
        )

        parser.add_argument(
            "--overwrite",
            default=False,
            action="store_true",
            help="Allow overwriting of local files.",
        )
        parser.add_argument(
            "--assay-uuid",
            default=None,
            type=str,
            help="UUID from Assay to check. Used to specify target while dealing with multi-assay projects.",
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

        parser.add_argument(
            "project_uuid",
            help="SODAR project UUID",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments."""
        res = 0

        # If SODAR info not provided, fetch from user's toml file
        toml_config = load_toml_config(args)
        args.sodar_url = args.sodar_url or toml_config.get("global", {}).get("sodar_server_url")
        args.sodar_api_token = args.sodar_api_token or toml_config.get("global", {}).get(
            "sodar_api_token"
        )

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
        elif not os.access(args.output_directory, os.W_OK):
            logger.error(
                f"Output directory path either does not exist or it is not writable: {args.base_path}"
            )
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk snappy pull-processed-data")
        logger.info("  args: %s", self.args)

        # Get list of sample ids
        if self.args.sample_list:
            samples = self.args.sample_list
        elif self.args.biomedsheet:
            samples = self.parse_sample_tsv(self.args.biomedsheet, sample_col=2, skip_rows=12)
        elif self.args.tsv:
            samples = self.parse_sample_tsv(
                self.args.tsv, sample_col=self.args.tsv_column, skip_rows=self.args.tsv_skip_rows
            )
        else:
            samples = None

        # Find all remote files (iRODS)
        filesearcher = RetrieveSodarCollection(
            self.args.sodar_url,
            self.args.sodar_api_token,
            self.args.assay_uuid,
            self.args.project_uuid,
        )

        remote_files_dict = filesearcher.perform()
        assay_path = filesearcher.get_assay_irods_path(self.args.assay_uuid)

        if self.args.all_files:
            file_patterns = []
        elif self.args.preset:
            file_patterns = self.presets[self.args.preset]
        else:  # self.args.file_pattern
            file_patterns = self.args.file_pattern

        filtered_remote_files_dict = self.filter_irods_file_list(
            file_patterns, samples, self.args.substring_match, assay_path
        )

        if len(filtered_remote_files_dict) == 0:
            self.report_no_file_found(available_files=[*remote_files_dict])
            return 0

        # Pair iRODS path with output path
        transfer_jobs = self.build_download_jobs(
            remote_files_dict=filtered_remote_files_dict,
            assay_path=assay_path,
        )

        # Retrieve files from iRODS
        iRODSTransfer(transfer_jobs).get(self.args.overwrite)

        logger.info("All done. Have a nice day!")
        return 0

    @staticmethod
    def parse_sample_tsv(tsv_path, sample_col=1, skip_rows=0, skip_comments=True) -> List[str]:
        extra_args = {"comment": "#"} if skip_comments else {}
        df = pd.read_csv(tsv_path, sep="\t", skiprows=skip_rows, **extra_args)
        try:
            samples = list(df.iloc[:, sample_col - 1])
        except IndexError:
            logger.error(
                f"Error extracting column no. {sample_col} from {tsv_path}, only {len(df.columns)} where detected."
            )
            raise

        return samples

    @staticmethod
    def filter_irods_file_list(
        remote_files_dict: Dict[str, List[iRODSDataObject]],
        common_assay_path: str,
        file_patterns: List[str],
        samples: List[str],
        substring_match: bool = False,
    ) -> Dict[str, List[iRODSDataObject]]:
        """Filter iRODS collection based on identifiers (sample id or library name) and file type/extension.

        :param remote_files_dict: Dictionary with iRODS collection information. Key: file name as string (e.g.,
        'P001-N1-DNA1-WES1.vcf.gz'); Value: iRODS data (``iRODSDataObject``).
        :type remote_files_dict: dict

        :param common_assay_path: Path common to all files. If provided, files in this path will be stripped.
        :type common_assay_path: str

        :param file_patterns: List of file patterns to use for file selection. Ignored if empty.
        :type file_patterns: list of strings

        :param samples: List of collection identifiers or substrings. Ignored if empty.
        :type samples: list

        :param substring_match: Fiter by extact collection matches or by substring matches.
        :type substring_match: bool

        :return: Returns dictionary: Key: sample (collection name [str]); Value: list of iRODS objects.
        """
        # Initialise variables
        filtered_dict = defaultdict(list)

        # Iterate
        for filename, irodsobjs in remote_files_dict.items():
            for irodsobj in irodsobjs:
                # Path needs to be stripped down to collections (=remove assay part & upwards)
                path = PurePosixPath(irodsobj.path).relative_to(PurePosixPath(common_assay_path))

                collection = path.parts[0]

                # Check if collection (=1st element of striped path) matches any of the samples
                if samples and not substring_match:
                    sample_match = any([s == collection for s in samples])
                elif samples:
                    sample_match = any([s in collection for s in samples])
                else:
                    sample_match = True

                if not sample_match:
                    continue

                if file_patterns:
                    file_pattern_match = any([p for p in file_patterns if path.match(p)])
                else:
                    file_pattern_match = True

                if not file_pattern_match:
                    continue

                filtered_dict[collection].append(irodsobj)

        return filtered_dict

    def build_download_jobs(
        self, remote_files_dict: Dict[str, List[iRODSDataObject]], assay_path: str
    ) -> List[TransferJob]:
        """Build list of download jobs for iRODS files."""
        # Initiate output
        output_list = []
        # Iterate over iRODS objects
        for collection, irods_objects in remote_files_dict.items():
            for irods_obj in irods_objects:
                relpath = PurePosixPath(irods_obj.path).relative_to(PurePosixPath(assay_path))
                coll, *subcolls, filename = relpath.parts
                assert coll == collection
                out_parts = {
                    "collection": coll,
                    "subcollections": "/".join(subcolls),
                    "filename": filename,
                }
                # apply regexes
                for filepart, m_pat, r_pat in self.args.output_regex:
                    out_parts[filepart] = re.sub(m_pat, r_pat, out_parts[filepart])

                job = TransferJob(
                    os.path.join(
                        self.args.output_dir, self.args.output_pattern.format(**out_parts)
                    ),
                    irods_obj.path,
                    # # Unclear if this is available or not
                    # irods_obj.size,
                )
                output_list.append(job)

        return output_list


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk org-raw check``."""
    return PullDataCollection.setup_argparse(parser)
