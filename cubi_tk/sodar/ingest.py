"""``cubi-tk sodar ingest``: add arbitrary files to SODAR"""

import argparse
import os
from pathlib import Path
import sys
import typing

import attr
import logzero
from logzero import logger
from sodar_cli import api

from cubi_tk.irods_utils import TransferJob, get_irods_error, init_irods, iRODSTransfer

from ..common import compute_md5_checksum, is_uuid, load_toml_config, sizeof_fmt

# for testing
logger.propagate = True

# no-frills logger
formatter = logzero.LogFormatter(fmt="%(message)s")
output_logger = logzero.setup_logger(formatter=formatter)


@attr.s(frozen=True, auto_attribs=True)
class Config:
    """Configuration for the ingest command."""

    config: str = attr.field(default=None)
    sodar_server_url: str = attr.field(default=None)
    sodar_api_token: str = attr.field(default=None, repr=lambda value: "***")  # type: ignore


class SodarIngest:
    """Implementation of sodar ingest command."""

    def __init__(self, args):
        # Command line arguments.
        self.args = args

        # Path to iRODS environment file
        self.irods_env_path = Path(Path.home(), ".irods", "irods_environment.json")
        if not self.irods_env_path.exists():
            logger.error("iRODS environment file is missing.")
            sys.exit(1)

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--hidden-cmd", dest="sodar_cmd", default=cls.run, help=argparse.SUPPRESS
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
            help="SODAR API token. Defaults to SODAR_API_TOKEN environment variable.",
        )
        parser.add_argument(
            "-r",
            "--recursive",
            default=False,
            action="store_true",
            help="Recursively match files in subdirectories. Creates iRODS sub-collections to match directory structure.",
        )
        parser.add_argument(
            "-K",
            "--remote-checksums",
            default=False,
            action="store_true",
            help="Trigger checksum computation on the iRODS side.",
        )
        parser.add_argument(
            "-y",
            "--yes",
            default=False,
            action="store_true",
            help="Don't ask for permission.",
        )
        parser.add_argument(
            "--collection", type=str, help="Target iRODS collection. Skips manual selection input."
        )
        parser.add_argument(
            "sources", help="One or multiple files/directories to ingest.", nargs="+"
        )
        parser.add_argument("destination", help="UUID or iRODS path of SODAR landing zone.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def execute(self):
        """Execute ingest."""
        # Get SODAR API info
        toml_config = load_toml_config(Config())

        if not self.args.sodar_url:
            self.args.sodar_url = toml_config.get("global", {}).get("sodar_server_url")
        if not self.args.sodar_api_token:
            self.args.sodar_api_token = toml_config.get("global", {}).get("sodar_api_token")

        # Retrieve iRODS path if destination is UUID
        if is_uuid(self.args.destination):
            try:
                lz_info = api.landingzone.retrieve(
                    sodar_url=self.args.sodar_url,
                    sodar_api_token=self.args.sodar_api_token,
                    landingzone_uuid=self.args.destination,
                )
            except Exception as e:
                logger.error("Failed to retrieve landing zone information.")
                logger.error(e)
                sys.exit(1)

            # TODO: Replace with status_locked check once implemented in sodar_cli
            if lz_info.status in ["ACTIVE", "FAILED"]:
                self.lz_irods_path = lz_info.irods_path
                logger.info(f"Target iRods path: {self.lz_irods_path}")
            else:
                logger.error("Target landing zone is not ACTIVE.")
                sys.exit(1)
        else:
            self.lz_irods_path = self.args.destination

        # Build file list and add missing md5 files
        source_paths = self.build_file_list()

        # Initiate iRODS session
        irods_session = init_irods(self.irods_env_path, ask=not self.args.yes)

        # Query target collection
        logger.info("Querying landing zone collections…")
        collections = []
        try:
            coll = irods_session.collections.get(self.lz_irods_path)
            for c in coll.subcollections:
                collections.append(c.name)
        except Exception as e:
            logger.error(f"Failed to query landing zone collections: {get_irods_error(e)}")
            sys.exit(1)
        finally:
            irods_session.cleanup()

        if self.args.collection is None:
            user_input = ""
            input_valid = False
            input_message = "####################\nPlease choose target collection:\n"
            for index, item in enumerate(collections):
                input_message += f"{index+1}) {item}\n"
            input_message += "Select by number: "

            while not input_valid:
                user_input = input(input_message)
                if user_input.isdigit():
                    user_input = int(user_input)
                    if 0 < user_input < len(collections):
                        input_valid = True

            self.target_coll = collections[user_input - 1]

        elif self.args.collection in collections:
            self.target_coll = self.args.collection
        else:
            logger.error("Selected target collection does not exist in landing zone.")
            sys.exit(1)

        # Create sub-collections for folders
        if self.args.recursive:
            dirs = sorted(
                {p["ipath"].parent for p in source_paths if not p["ipath"].parent == Path(".")}
            )

            logger.info("Planning to create the following sub-collections:")
            for d in dirs:
                output_logger.info(f"{self.target_coll}/{str(d)}")
            if not self.args.yes:
                if not input("Is this OK? [y/N] ").lower().startswith("y"):
                    logger.info("Aborting at your request.")
                    sys.exit(0)

            for d in dirs:
                coll_name = f"{self.lz_irods_path}/{self.target_coll}/{str(d)}"
                try:
                    irods_session.collections.create(coll_name)
                except Exception as e:
                    logger.error("Error creating sub-collection.")
                    logger.error(e)
                    sys.exit(1)
                finally:
                    irods_session.cleanup()
            logger.info("Sub-collections created.")

        # Build transfer jobs
        jobs = self.build_jobs(source_paths)

        # Final go from user & transfer
        itransfer = iRODSTransfer(irods_session, jobs)
        total_bytes = itransfer.total_bytes
        logger.info("Planning to transfer the following files:")
        for job in jobs:
            output_logger.info(job.path_src)
        logger.info(f"With a total size of {sizeof_fmt(total_bytes)}")
        logger.info("Into this iRODS collection:")
        output_logger.info(f"{self.lz_irods_path}/{self.target_coll}/")

        if not self.args.yes:
            if not input("Is this OK? [y/N] ").lower().startswith("y"):
                logger.info("Aborting at your request.")
                sys.exit(0)

        itransfer.put()
        logger.info("File transfer complete.")

        # Compute server-side checksums
        if self.args.remote_checksums:
            logger.info("Computing server-side checksums.")
            itransfer.chksum()

    def build_file_list(self):
        """Build list of source files to transfer. iRODS paths are relative to target collection."""

        source_paths = [Path(src) for src in self.args.sources]
        output_paths = list()

        for src in source_paths:
            try:
                abspath = src.resolve(strict=True)
            except FileNotFoundError:
                logger.warning(f"File not found: {src.name}")
                continue
            except RuntimeError:
                logger.warning(f"Symlink loop: {src.name}")
                continue

            if src.is_dir():
                paths = abspath.glob("**/*" if self.args.recursive else "*")
                for p in paths:
                    if p.is_file() and not p.suffix.lower() == ".md5":
                        output_paths.append({"spath": p, "ipath": p.relative_to(abspath)})
            else:
                output_paths.append({"spath": src, "ipath": Path(src.name)})
        return output_paths

    def build_jobs(self, source_paths) -> typing.Tuple[TransferJob, ...]:
        """Build file transfer jobs."""

        transfer_jobs = []

        for p in source_paths:
            transfer_jobs.append(
                TransferJob(
                    path_src=str(p["spath"]),
                    path_dest=f"{self.lz_irods_path}/{self.target_coll}/{str(p['ipath'])}",
                    bytes=p["spath"].stat().st_size,
                    md5=compute_md5_checksum(p["spath"]),
                )
            )

        return tuple(sorted(transfer_jobs))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar ingest``."""
    return SodarIngest.setup_argparse(parser)
