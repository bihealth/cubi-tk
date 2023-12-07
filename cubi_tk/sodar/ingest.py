"""``cubi-tk sodar ingest``: add arbitrary files to SODAR"""

import argparse
import os
from pathlib import Path
import sys
import typing

import attrs
import logzero
from logzero import logger
from sodar_cli import api

from ..common import compute_md5_checksum, is_uuid, load_toml_config, sizeof_fmt
from cubi_tk.irods_common import TransferJob, iRODSCommon, iRODSTransfer

# for testing
logger.propagate = True

# no-frills logger
formatter = logzero.LogFormatter(fmt="%(message)s")
output_logger = logzero.setup_logger(formatter=formatter)


@attrs.frozen(auto_attribs=True)
class Config:
    """Configuration for the ingest command."""

    config: str = attrs.field(default=None)
    sodar_server_url: str = attrs.field(default=None)
    sodar_api_token: str = attrs.field(default=None, repr=lambda value: "***")  # type: ignore


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

        # Get SODAR API info
        toml_config = load_toml_config(Config())
        if toml_config:
            config_url = toml_config.get("global", {}).get("sodar_server_url")
            if self.args.sodar_url == "https://sodar.bihealth.org/" and config_url:
                self.args.sodar_url = config_url
            if not self.args.sodar_api_token:
                self.args.sodar_api_token = toml_config.get("global", {}).get("sodar_api_token")
        if not self.args.sodar_api_token:
            logger.error("SODAR API token missing.")
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
            "-s",
            "--sync",
            default=False,
            action="store_true",
            help="Skip upload of files already present in remote collection.",
        )
        parser.add_argument(
            "-e",
            "--exclude",
            nargs="+",
            default="",
            type=str,
            help="Exclude files by defining one or multiple glob-style patterns.",
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
            "--collection",
            type=str,
            help="Target iRODS collection. Skips manual selection input.",
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
        # Retrieve iRODS path if destination is UUID
        if is_uuid(self.args.destination):
            try:
                lz_info = api.landingzone.retrieve(
                    sodar_url=self.args.sodar_url,
                    sodar_api_token=self.args.sodar_api_token,
                    landingzone_uuid=self.args.destination,
                )
            except Exception as e:  # pragma: no cover
                logger.error("Failed to retrieve landing zone information.")
                logger.exception(e)
                sys.exit(1)

            # TODO: Replace with status_locked check once implemented in sodar_cli
            if lz_info.status in ["ACTIVE", "FAILED"]:
                self.lz_irods_path = lz_info.irods_path
                logger.info(f"Target iRods path: {self.lz_irods_path}")
            else:
                logger.error("Target landing zone is not ACTIVE.")
                sys.exit(1)
        else:
            self.lz_irods_path = self.args.destination  # pragma: no cover

        # Build file list
        source_paths = self.build_file_list()
        if len(source_paths) == 0:
            logger.info("Nothing to do. Quitting.")
            sys.exit(0)

        # Initiate iRODS session
        irods_session = iRODSCommon().session

        # Query target collection
        logger.info("Querying landing zone collections…")
        collections = []
        try:
            with irods_session as i:
                coll = i.collections.get(self.lz_irods_path)
                for c in coll.subcollections:
                    collections.append(c.name)
        except Exception as e:  # pragma: no cover
            logger.error(
                f"Failed to query landing zone collections: {iRODSCommon().get_irods_error(e)}"
            )
            sys.exit(1)

        # Query user for target sub-collection
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
                    if 0 < user_input <= len(collections):
                        input_valid = True

            self.target_coll = collections[user_input - 1]

        elif self.args.collection in collections:
            self.target_coll = self.args.collection
        else:  # pragma: no cover
            logger.error("Selected target collection does not exist in landing zone.")
            sys.exit(1)

        # Build transfer jobs and add missing md5 files
        jobs = self.build_jobs(source_paths)
        jobs = sorted(jobs, key=lambda x: x.path_local)

        # Final go from user & transfer
        itransfer = iRODSTransfer(jobs, ask=not self.args.yes)
        logger.info("Planning to transfer the following files:")
        for job in jobs:
            output_logger.info(job.path_local)
        logger.info(f"With a total size of {sizeof_fmt(itransfer.size)}")
        logger.info("Into this iRODS collection:")
        output_logger.info(f"{self.lz_irods_path}/{self.target_coll}/")

        if not self.args.yes:
            if not input("Is this OK? [y/N] ").lower().startswith("y"):  # pragma: no cover
                logger.info("Aborting at your request.")
                sys.exit(0)

        itransfer.put(recursive=self.args.recursive, sync=self.args.sync)
        logger.info("File transfer complete.")

        # Compute server-side checksums
        if self.args.remote_checksums:  # pragma: no cover
            logger.info("Computing server-side checksums.")
            itransfer.chksum()

    def build_file_list(self) -> typing.List[typing.Dict[Path, Path]]:
        """
        Build list of source files to transfer.
        iRODS paths are relative to target collection.
        """

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

            excludes = self.args.exclude
            if src.is_dir():
                paths = abspath.glob("**/*" if self.args.recursive else "*")
                for p in paths:
                    if excludes and any([p.match(e) for e in excludes]):
                        continue
                    if p.is_file() and not p.suffix.lower() == ".md5":
                        output_paths.append({"spath": p, "ipath": p.relative_to(abspath)})
            else:
                if not any([src.match(e) for e in excludes if e]):
                    output_paths.append({"spath": src, "ipath": Path(src.name)})
        return output_paths

    def build_jobs(self, source_paths: typing.Iterable[Path]) -> typing.Set[TransferJob]:
        """Build file transfer jobs."""

        transfer_jobs = []

        for p in source_paths:
            path_remote = f"{self.lz_irods_path}/{self.target_coll}/{str(p['ipath'])}"
            md5_path = p["spath"].parent / (p["spath"].name + ".md5")

            if md5_path.exists():
                logger.info(f"Found md5 hash on disk for {p['spath']}")
            else:
                md5sum = compute_md5_checksum(p["spath"])
                with md5_path.open("w", encoding="utf-8") as f:
                    f.write(f"{md5sum}  {p['spath'].name}")

            transfer_jobs.append(
                TransferJob(
                    path_local=str(p["spath"]),
                    path_remote=path_remote,
                )
            )

            transfer_jobs.append(
                TransferJob(
                    path_local=str(md5_path),
                    path_remote=path_remote + ".md5",
                )
            )

        return set(transfer_jobs)


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar ingest``."""
    return SodarIngest.setup_argparse(parser)
