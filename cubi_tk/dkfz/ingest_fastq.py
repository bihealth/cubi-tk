"""``cubi-tk dkfz ingest-fastq``: transfer raw FASTQs into iRODS landing zone."""

import argparse
import os
import typing

from logzero import logger

from .parser import DkfzMetaParser
from .DkfzMeta import DkfzMeta

class DkfzIngestFastqCommand:
    """Implementation of dkfz ingest-fastq command for raw data."""

    command_name = "ingest-fastq"
    step_name = "raw_data"

    def __init__(self, args):
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="dkfz_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        group_sodar = parser.add_argument_group("SODAR-related")
        group_sodar.add_argument(
            "--sodar-url",
            default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
            help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
        )
        group_sodar.add_argument(
            "--sodar-api-token",
            default=os.environ.get("SODAR_API_TOKEN", None),
            help="Authentication token when talking to SODAR.  Defaults to SODAR_API_TOKEN environment variable.",
        )

        parser.add_argument(
            "--unless-exists",
            default=False,
            dest="unless_exists",
            action="store_true",
            help="If there already is a landing zone in the current project then use this one",
        )

        parser.add_argument(
            "--dry-run",
            "-n",
            default=False,
            action="store_true",
            help="Perform a dry run, i.e., don't change anything only display change, implies '--show-diff'.",
        )

        parser.add_argument(
            "--format",
            dest="format_string",
            default=None,
            help="Format string for printing, e.g. %%(uuid)s",
        )

        parser.add_argument(
            "--assay-type",
            dest="assay_type",
            choices = DkfzMetaParser.mappings["SEQUENCING_TYPE_to_Assays"].values(),
            default="exome",
            help="Assay type to upload to landing zone"
        )

        parser.add_argument(
            "--species",
            dest="species",
            default="human",
            help="Organism common name"
        )

        parser.add_argument(
            "--mapping",
            help=r"""
                File containing a table with the mapping between sample ids.
                The sample id stored in the DKFZ metafile is replaced by another id.
                The DKFZ sample id must be in column 'Sample Name', and the replacement id in column 'Sample Name CUBI'.
                When column 'Patient Name CUBI' is present in the mapping table, its contents is used for the 'Source Name' is the ISA-tab sample file.
                Additional columns are used to create 'Characteristics' columns for the sample material in the ISA-tab sample file.
            """
        )

        parser.add_argument("--dktk", default=False, action="store_true", help="Further heuristics to process DKTK Master-like samples")

        parser.add_argument("destination", help="UUID or iRods path of landing zone to move to.")

        parser.add_argument("meta", nargs="+", help="DKFZ meta file(s)")
            

    @classmethod
    def run(
            cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
        ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0
        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the upload to sodar."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk org-raw check")
        logger.info("  args: %s", self.args)

        dkfz_parser = DkfzMetaParser()
        all = None
        for filename in self.args.meta:
            meta = dkfz_parser.DkfzMeta(filename, species=self.args.species)
            if all is None:
                all = meta
            else:
                all.extend(meta)

        mapping = None
        if self.args.dktk:
            mapping = all.dktk(output_file=None)
        if self.args.mapping:
            mapping = pd.read_table(self.args.mapping)
        all.create_cubi_names(mapping=mapping)

        all.filename_mapping(assay_type=self.args.assay_type, sodar_path="/sodar/landing_zone")

        return 0

def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk dkfz ingest-fastq``."""
    return DkfzIngestFastqCommand.setup_argparse(parser)
