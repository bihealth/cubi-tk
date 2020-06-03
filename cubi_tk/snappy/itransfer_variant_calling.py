"""``cubi-tk snappy itransfer-variant-calling``: transfer variant_calling results into iRODS landing zone."""

import argparse
import os
import typing

from .itransfer_common import SnappyItransferCommandBase, IndexLibrariesOnlyMixin

#: Template string for variant_calling results files.
TPL_INPUT_DIR = "variant_calling/output/%(mapper)s.%(caller)s.%(library_name)s"


class SnappyItransferVariantCallingCommand(IndexLibrariesOnlyMixin, SnappyItransferCommandBase):
    """Implementation of snappy itransfer command for variant calling results."""

    fix_md5_files = True
    command_name = "itransfer-variant-calling"
    step_name = "variant_calling"
    start_batch_in_family = True

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        super().setup_argparse(parser)
        parser.add_argument(
            "--mapper", help="Name of the mapper to transfer for, defaults to bwa.", default="bwa"
        )
        parser.add_argument(
            "--caller",
            help="Name of the variant caller to transfer for, defaults to gatk_hc",
            default="gatk_hc",
        )

    def build_base_dir_glob_pattern(self, library_name: str) -> typing.Tuple[str, str]:
        return (
            os.path.join(
                self.args.base_path,
                TPL_INPUT_DIR
                % {
                    "mapper": self.args.mapper,
                    "caller": self.args.caller,
                    "library_name": library_name,
                },
            ),
            "**",
        )


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy itransfer-variant-calling``."""
    return SnappyItransferVariantCallingCommand.setup_argparse(parser)
