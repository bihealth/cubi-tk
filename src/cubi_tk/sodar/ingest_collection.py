"""``cubi-tk sodar ingest-collection``: upload arbitrary files and folders into a specific SODAR landing zone collection"""

import argparse
from pathlib import Path
import sys
import typing

from loguru import logger

from cubi_tk.irods_common import TransferJob, iRODSCommon, iRODSTransfer
from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi

from ..common import compute_checksum, is_uuid, sizeof_fmt

# for testing
logger.propagate = True


class SodarIngestCollection:
    """Implementation of sodar ingest-collection command."""

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
            help="Don't ask for permission. Does not skip manual target collection selection.",
        )
        parser.add_argument(
            "--collection",
            type=str,
            help="Target iRODS collection. Skips manual target collection selection.",
        )
        parser.add_argument(
            "sources", help="One or multiple files/directories to ingest.", nargs="+"
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def execute(self):
        """Execute ingest."""
        self.lz_irods_path = self.args.destination
        logger.info("Starting cubi-tk sodar ingest-collection")
        print_args(self.args)
        # Retrieve iRODS path if destination is UUID
        if is_uuid(self.args.destination):

            sodar_api = SodarApi(self.args, with_dest=True, dest_string="destination")

            lz_info = sodar_api.get_landingzone_retrieve()
            if lz_info is None:  # pragma: no cover
                logger.error("Failed to retrieve landing zone information.")
                sys.exit(1)

            if lz_info.status in ["ACTIVE", "FAILED"]:
                self.lz_irods_path = lz_info.irods_path
                logger.info(f"Target iRods path: {self.lz_irods_path}")
            else:
                logger.error("Target landing zone is not ACTIVE.")
                sys.exit(1)

        irods_hash_scheme= iRODSCommon(sodar_profile=self.args.config_profile).irods_hash_scheme()
        hash_ending = "." + irods_hash_scheme.lower()

        # Build file list
        source_paths = self.build_file_list(hash_ending)
        if len(source_paths) == 0:
            logger.info("Nothing to do. Quitting.")
            sys.exit(0)

        # Query user for target sub-collection
        self.build_target_coll()

        # Build transfer jobs and add missing checksum files
        jobs = self.build_jobs(source_paths, irods_hash_scheme, hash_ending)
        jobs = sorted(jobs, key=lambda x: x.path_local)

        # Final go from user & transfer
        itransfer = iRODSTransfer(jobs, ask=not self.args.yes, sodar_profile=self.args.config_profile)
        logger.info("Planning to transfer the following files:")

        for job in jobs:
            logger.info(job.path_local)
        logger.info(f"With a total size of {sizeof_fmt(itransfer.size)}")
        logger.info("Into this iRODS collection:")
        logger.info(f"{self.target_coll}/")

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

    def build_target_coll(self):
        # Initiate iRODS session
        irods_session = iRODSCommon(sodar_profile=self.args.config_profile).session
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

        if not collections:
            self.target_coll = self.lz_irods_path
            logger.info("No subcollections found. Moving on.")
        elif self.args.collection is None:
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

            self.target_coll = f"{self.lz_irods_path}/{collections[user_input - 1]}"

        elif self.args.collection in collections:
            self.target_coll = f"{self.lz_irods_path}/{self.args.collection}"
        else:  # pragma: no cover
            logger.error("Selected target collection does not exist in landing zone.")
            sys.exit(1)

    def build_file_list(self, hash_ending) -> typing.List[typing.Dict[Path, Path]]:
        """
        Build list of source files to transfer.
        iRODS paths are relative to target collection.
        """

        source_paths = [Path(src) for src in self.args.sources]
        output_paths = []

        for src in source_paths:
            try:
                abspath = src.resolve(strict=True)
            except FileNotFoundError:
                logger.warning(f"File not found: {src.name}")
                continue
            except (RuntimeError, OSError):
                logger.warning(f"Symlink loop: {src.name}")
                continue

            excludes = self.args.exclude
            if src.is_dir():
                paths = abspath.glob("**/*" if self.args.recursive else "*")
                for p in paths:
                    if excludes and any(p.match(e) for e in excludes):
                        continue
                    if p.is_file() and not p.suffix.lower() == hash_ending:
                        output_paths.append({"spath": p, "ipath": p.relative_to(abspath)})
            else:
                if not any(src.match(e) for e in excludes if e):
                    output_paths.append({"spath": src, "ipath": Path(src.name)})
        return output_paths

    def build_jobs(self, source_paths: typing.Iterable[Path], irods_hash_scheme, hash_ending) -> typing.Tuple[TransferJob]:
        """Build file transfer jobs."""

        transfer_jobs = []

        for p in source_paths:
            path_remote = f"{self.target_coll}/{str(p['ipath'])}"

            hash_path = p["spath"].parent / (p["spath"].name + hash_ending)

            if hash_path.exists():
                logger.info(f"Found {irods_hash_scheme} hash on disk for {p['spath']}")
            else:
                checksum = compute_checksum(p["spath"], irods_hash_scheme)
                with hash_path.open("w", encoding="utf-8") as f:
                    f.write(f"{checksum}  {p['spath'].name}")

            transfer_jobs.append(
                TransferJob(
                    path_local=str(p["spath"]),
                    path_remote=path_remote,
                )
            )

            transfer_jobs.append(
                TransferJob(
                    path_local=str(hash_path),
                    path_remote=path_remote + hash_ending,
                )
            )

        return tuple(transfer_jobs)


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar ingest``."""
    return SodarIngestCollection.setup_argparse(parser)
