"""``cubi-tk snappy itransfer-variant-calling``: transfer variant_calling results into iRODS landing zone."""

import argparse
import os
import typing

from logzero import logger
import yaml

from . import common
from .itransfer_common import IndexLibrariesOnlyMixin, SnappyItransferCommandBase

#: Template string for variant_calling results files.
TPL_INPUT_DIR = "%(step_name)s/output/%(mapper)s.%(caller)s.%(library_name)s"


class SnappyStepNotFoundException(Exception):
    """Raise when snappy-pipeline config does not define the expected steps this function needs."""


class SnappyItransferSvCallingCommand(IndexLibrariesOnlyMixin, SnappyItransferCommandBase):
    """Implementation of snappy itransfer command for variant calling results."""

    fix_md5_files = True
    command_name = "itransfer-sv-calling"
    step_names = ("sv_calling_wgs", "sv_calling_targeted")
    start_batch_in_family = True

    def __init__(self, args):
        super().__init__(args)

        path = common.find_snappy_root_dir(self.args.base_path or os.getcwd())
        with open(path / ".snappy_pipeline/config.yaml", "rt") as f:
            config = yaml.safe_load(f)
        self.step_name = None
        for step_name in self.__class__.step_names:
            if not self.step_name and step_name in config["step_config"]:
                self.step_name = step_name
            elif self.step_name and step_name in config["step_config"]:
                raise SnappyStepNotFoundException(
                    f"Found multiple sv-calling step names in config.yaml. Only one of {', '.join(self.__class__.step_names)} is allowed."
                )
        if not self.step_name:
            raise SnappyStepNotFoundException(
                f"Could not find any sv-calling step name in 'config.yaml'. Was looking for one of: {', '.join(self.__class__.step_names)}"
            )

        if self.step_name == 'sv-calling_targeted':
            self.defined_callers = config["step_config"][self.step_name]["tools"]
        else: #if self.step_name == 'sv-calling_wgs'
            # For WGS config looks like: sv-calling_wgs::tools::<dna>::[...]
            self.defined_callers = [tool for subcat in config["step_config"][self.step_name]["tools"] for tool in config["step_config"][self.step_name]["tools"][subcat]]

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        super().setup_argparse(parser)
        parser.add_argument(
            "--mapper",
            help="Name of the mapper to transfer for, defaults to bwa_mem2.",
            default="bwa_mem2",
        )
        parser.add_argument(
            "--caller",
            help="Name of the variant caller to transfer for. Defaults to all callers defined in config",
            default="all-defined",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute_multi()

    def execute_multi(self) -> typing.Optional[int]:
        """Execute the transfer."""
        ret = 0
        if self.args.caller == "all-defined":
            logger.info("Starting cubi-tk snappy sv-calling for multiple callers")
            for caller in self.defined_callers:
                self.args.caller = caller
                ret = self.execute() or ret
        else:
            ret = self.execute()

        return int(ret)

    def build_base_dir_glob_pattern(self, library_name: str) -> typing.Tuple[str, str]:
        return (
            os.path.join(
                self.args.base_path,
                TPL_INPUT_DIR
                % {
                    "step_name": self.step_name,
                    "mapper": self.args.mapper,
                    "caller": self.args.caller,
                    "library_name": library_name,
                },
            ),
            "**",
        )


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy itransfer-variant-calling``."""
    return SnappyItransferSvCallingCommand.setup_argparse(parser)
