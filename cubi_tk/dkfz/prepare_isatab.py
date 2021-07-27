"""``cubi-tk dkfz ingest-fastq``: transfer raw FASTQs into iRODS landing zone."""

import argparse
import os
import textwrap
import typing

from pathlib import Path
from logzero import logger

from .parser import DkfzMetaParser
from .DkfzMeta import DkfzMeta

class DkfzPrepareIsatabCommand:
    """Implementation of dkfz prepare-isatab command for raw data."""

    command_name = "prepare-isatab"
    step_name = "raw_data"

    def __init__(self, args):
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="dkfz_cmd", default=cls.run, help=argparse.SUPPRESS
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

        parser.add_argument("--isatab-dir", help="Directory to store isatab files")

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
        """Execute the creation of isatab files."""
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
            mapping = all.dktk()
        if self.args.mapping:
            mapping = pd.read_table(self.args.mapping)
        if not mapping is None:
            mapping.to_csv(self.args.isatab_dir + "/" + "sample_ids_map.txt", sep="\t", index=False)
        all.create_cubi_names(mapping=mapping)

        Path(self.args.isatab_dir).mkdir(parents=True, exist_ok=True)
        for (k,v) in all.assays.items():
            filename = Path(self.args.isatab_dir + "/" + "a_" + k + ".txt")
            if filename.exists():
                logger.warning("ISAtab file {} already exists, not overwritten".format(filename.resolve()))
                continue
            all.get_assay(k).to_csv(filename.resolve(), sep="\t", na_rep="", index=False, header=True)
            
        filename = Path(self.args.isatab_dir + "/" + "s_sample.txt")
        if filename.exists():
            logger.warning("ISAtab file {} already exists, not overwritten".format(filename.resolve()))
        else:
            all.get_sample().to_csv(filename.resolve(), sep="\t", na_rep="", index=False, header=True)
        
        return 0

def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk dkfz prepare-isatab``."""
    return DkfzPrepareIsatabCommand.setup_argparse(parser)
