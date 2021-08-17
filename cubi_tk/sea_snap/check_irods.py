"""``cubi-tk sea-snap check-irods``: Check target iRods collection (all sea-snap output files? all md5 files? metadata md5 consistent? enough replicas?)."""

import os
import argparse
from pathlib import Path
import yaml
import re

from logzero import logger

from ..irods.check import IrodsCheckCommand


MIN_NUM_REPLICAS = 2
NUM_PARALLEL_TESTS = 8


class SeasnapCheckIrodsCommand(IrodsCheckCommand):
    """Implementation of sea-snap check-irods command."""

    command_name = "check-irods"

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--hidden-cmd", dest="sea_snap_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--num-replicas",
            type=int,
            default=MIN_NUM_REPLICAS,
            help="Minimum number of replicas, defaults to %s" % MIN_NUM_REPLICAS,
        )

        parser.add_argument(
            "--num-parallel-tests",
            type=int,
            default=NUM_PARALLEL_TESTS,
            help="Number of parallel tests, defaults to %s" % NUM_PARALLEL_TESTS,
        )

        parser.add_argument(
            "--yes",
            default=False,
            action="store_true",
            help="Assume the answer to all prompts is 'yes'",
        )

        parser.add_argument(
            "--transfer-blueprint",
            default="SODAR_export_blueprint.txt",
            help=(
                "Filename of blueprint file for export to SODAR "
                "(created e.g. with './sea-snap sc l export'). "
                "Assumed to be in the results folder. "
                "Default: 'SODAR_export_blueprint.txt'"
            ),
        )

        parser.add_argument("results_folder", help="Path to a Sea-snap results folder.")

        parser.add_argument("irods_path", help="Path to an iRods collection.")

    def remote_path(self, dest):
        match_pattern = "".join(f"({p}/)?" for p in self.args.irods_path[1:].split("/"))
        return re.sub(match_pattern, "", dest)

    def execute(self):
        """Execute checks."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting sea-snap check-irods %s", self.command_name)
        logger.info("  args: %s", self.args)

        # --- get lists
        # files on SODAR
        files = self.get_file_paths()
        files_rel = [f.replace(self.args.irods_path + "/", "") for f in files["files"]]
        logger.info("Files on SODAR (first 20): %s", ", ".join(files_rel[:19]))

        # samples in project
        with open(os.path.join(self.args.results_folder, "sample_info.yaml"), "r") as stream:
            samples = list(yaml.safe_load(stream)["sample_info"])
        logger.info("Samples in sample_info.yaml: %s", ", ".join(samples))

        # destinations from blueprint file
        blueprint = Path(self.args.results_folder) / self.args.transfer_blueprint
        dests = [
            self.remote_path(dest)
            for dest in set(re.findall(r"i:__SODAR__/(\S+)", blueprint.read_text()))
        ]  # noqa: W605

        # --- run tests
        # all there?
        not_there = [d for d in dests if d not in files_rel and d[-4:] != ".md5"]
        if not_there:
            e_msg = "Some files have not been uploaded to SODAR: " + ", ".join(not_there)
            logger.error(e_msg)
            res = 1
            if not self.args.yes and not input("Continue? [yN] ").lower().startswith("y"):
                logger.error("OK, breaking at your request")
                return None
            # raise FileNotFoundError(e_msg)

        # samples covered?
        non_covered_samples = [s for s in samples if not any(s in f for f in files_rel)]
        if non_covered_samples:
            logger.warning(
                "These samples are in the sample sheet, but have no corresponding files on SODAR: %s",
                ", ".join(non_covered_samples),
            )
            if not self.args.yes and not input("Continue? [yN] ").lower().startswith("y"):
                logger.error("OK, breaking at your request")
                return None

        # generic tests (md5 sums, metadata, #replicas)
        self.run_checks(files)

        logger.info("All done")
        return res


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk irods check``."""
    return SeasnapCheckIrodsCommand.setup_argparse(parser)
