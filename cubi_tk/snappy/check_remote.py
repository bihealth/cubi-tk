"""``cubi-tk snappy check-remote``: check that files are present in remote SODAR/iRODS.

Only uses local information for checking that the linked-in RAW data files are correct in terms
of the MD5 sum.  Otherwise, just checks for presence of files (for now), the rationale being that

"""

import argparse
import os
import typing

from biomedsheets import shortcuts
from logzero import logger

from .common import get_biomedsheet_path, load_sheet_tsv


class RawDataChecker:
    """Check for raw data being present and equal as in local ``ngs_mapping`` directory."""

    def __init__(self, sheet, project_uuid):
        self.sheet = sheet
        self.project_uuid = project_uuid

    def run(self):
        logger.info("Starting raw data checks ...")
        logger.info("... done with raw data checks")
        return True


class NgsMappingChecker:
    """Check for mapping results being present without checking content."""

    def __init__(self, sheet, project_uuid):
        self.sheet = sheet
        self.project_uuid = project_uuid

    def run(self):
        logger.info("Starting ngs_mapping checks ...")
        logger.info("... done with ngs_mapping checks")
        return True


class VariantCallingChecker:
    """Check for variant calling results being present without checking content"""

    def __init__(self, germline_sheet, project_uuid):
        self.germline_sheet = germline_sheet
        self.project_uuid = project_uuid

    def run(self):
        logger.info("Starting variant_calling checks ...")
        logger.info("... done with variant_calling checks")
        return True


class SnappyCheckRemoteCommand:
    """Implementation of the ``check-remote`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args
        # Find biomedsheet file
        self.biomedsheet_tsv = get_biomedsheet_path(
            start_path=self.args.base_path, uuid=args.project_uuid
        )
        #: Raw sample sheet.
        self.sheet = load_sheet_tsv(self.biomedsheet_tsv, args.tsv_shortcut)
        #: Shortcut sample sheet.
        self.shortcut_sheet = shortcuts.GermlineCaseSheet(self.sheet)

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments for ``check-remote`` command."""
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--tsv-shortcut",
            default="germline",
            choices=("germline", "cancer"),
            help="The shortcut TSV schema to use.",
        )
        parser.add_argument(
            "--base-path",
            default=os.getcwd(),
            required=False,
            help=(
                "Base path of project (contains 'ngs_mapping/' etc.), spiders up from biomedsheet_tsv and falls "
                "back to current working directory by default."
            ),
        )
        parser.add_argument("project_uuid", type=str, help="UUID from project to check.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments."""
        res = 0

        if not os.path.exists(args.base_path):  # pragma: nocover
            logger.error("Base path %s does not exist", args.base_path)
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk snappy check-remote")
        logger.info("  args: %s", self.args)

        results = [
            RawDataChecker(self.sheet, self.args.project_uuid).run(),
            NgsMappingChecker(self.sheet, self.args.project_uuid).run(),
            VariantCallingChecker(self.shortcut_sheet, self.args.project_uuid).run(),
        ]

        logger.info("All done")
        return int(not all(results))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy check-local``."""
    return SnappyCheckRemoteCommand.setup_argparse(parser)
