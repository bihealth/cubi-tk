"""``cubi-tk sea-snap itransfer-raw-data``: transfer raw FASTQs into iRODS landing zone."""

import argparse
import os

from ..snappy.itransfer_common import SnappyItransferCommandBase

#: Template string for raw data / input links file.
TPL_INPUT_LINK_DIR = "input_links/%(library_name)s"


class SeasnapItransferRawDataCommand(SnappyItransferCommandBase):
    """Implementation of sea-snap itransfer command for raw data."""

    command_name = "itransfer-raw-data"
    step_name = "raw_data"

    def build_base_dir_glob_pattern(self, library_name: str) -> tuple[str, str]:
        return (
            os.path.join(self.args.base_path, TPL_INPUT_LINK_DIR % {"library_name": library_name}),
            "**",
        )

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )



def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sea-snap itransfer-raw-data``."""
    return SeasnapItransferRawDataCommand.setup_argparse(parser)
