"""``cubi-tk snappy itransfer-step``: transfer step results into iRODS landing zone."""

import argparse
import os

from loguru import logger

from .itransfer_common import SnappyItransferCommandBase


class SnappyItransferStepCommand(SnappyItransferCommandBase):
    """Implementation of snappy itransfer command for results from any step."""

    command_name = "itransfer-step"
    step_name = None

    def __init__(self, args):
        if self.step_name is None:
            self.step_name = args.step
        super().__init__(args)

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )
        parser.add_argument(
            "--step",
            help=(
                "Name of the snappy pipeline step (step name must be identical to step directory)."
                "Steps names are available from the snappy command snappy-start-step --help"
            ),
            required=True,
        )
        parser.add_argument(
            "--tool",
            help=(
                "Name of the tool, for example bwa. Tools order in important:"
                "it must match the order used to generate filename prefix."
                "For example, the variant annotation step requires the mapper, caller and"
                "the annotator software. In that case, the snappy file prefix is:"
                "<mapper>.<caller>.<annotator>, so the command would be:"
                "--tool <mapper> <vcaller> <annotator>. Some steps add more information to their"
                "prefix, for example 'jannovar_somatic_vcf'"
            ),
            nargs="*",
        )

    def build_base_dir_glob_pattern(self, library_name: str) -> tuple[str, str]:
        prefix = ".".join(self.args.tool)
        logger.debug(f"Using step {self.args.step} and prefix {prefix}")
        return (
            os.path.join(
                self.args.base_path, self.args.step, "output", prefix + "." + library_name
            ),
            "**",
        )

    def check_args(self, args) -> int | None:
        """Called for checking arguments, override to change behaviour."""
        res = super().check_args(args)

        if self.step_name is None and self.args.step is None:
            logger.error("Snappy step is not defined")
            return 1

        return res


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy itransfer-step``."""
    return SnappyItransferStepCommand.setup_argparse(parser)
