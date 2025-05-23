"""``cubi-tk snappy itransfer-ngs-mapping``: transfer ngs_mapping results into iRODS landing zone."""

import argparse
import os

from .itransfer_common import SnappyItransferCommandBase

#: Template string for ngs_mapping results files.
TPL_INPUT_DIR = "ngs_mapping/output/%(mapper)s.%(library_name)s"


class SnappyItransferNgsMappingCommand(SnappyItransferCommandBase):
    """Implementation of snappy itransfer command for ngs_mapping results."""

    command_name = "itransfer-ngs-mapping"
    step_name = "ngs_mapping"

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )
        parser.add_argument(
            "--mapper", help="Name of the mapper to transfer for, defaults to bwa.", default="bwa"
        )

    def build_base_dir_glob_pattern(self, library_name: str) -> tuple[str, str]:
        return (
            os.path.join(
                self.args.base_path,
                TPL_INPUT_DIR % {"mapper": self.args.mapper, "library_name": library_name},
            ),
            "**",
        )


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy itransfer-raw-data``."""
    return SnappyItransferNgsMappingCommand.setup_argparse(parser)
