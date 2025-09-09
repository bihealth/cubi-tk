"""``cubi-tk sodar ingest-collection``: upload arbitrary files and folders into a specific SODAR landing zone collection"""

import argparse
from pathlib import Path
import sys

from loguru import logger

from cubi_tk.irods_common import TransferJob
from cubi_tk.sodar_common import SodarIngestBase

# for testing
logger.propagate = True


class SodarIngestCollection(SodarIngestBase):
    """Implementation of sodar ingest-collection command."""

    command_name = 'ingest-collection'

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
            "-e",
            "--exclude",
            nargs="+",
            default="",
            type=str,
            help="Exclude files by defining one or multiple glob-style patterns.",
        )
        parser.add_argument(
            "--collection",
            type=str,
            help="Target iRODS collection. Skips manual target collection selection.",
        )
        parser.add_argument(
            "sources", help="One or multiple files/directories to ingest.", nargs="+"
        )
        parser.add_argument(
            "destination", help="Sodar project UUID, landing-zone (irods) path or UUID to upload to."
        )

    def check_args(self, args) -> int | None:
        """Called for checking arguments, override to change behaviour."""
        if self.args.yes and not self.args.collection:
            logger.error("Can not skip user input without defined `--collection`.")
            sys.exit(1)


    def build_target_coll(self) -> str:
        # Initiate iRODS session
        irods_session = self.itransfer.session
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
                f"Failed to query landing zone collections: {self.itransfer.get_irods_error(e)}"
            )
            sys.exit(1)

        if not collections:
            logger.info("No subcollections found. Moving on.")
            return self.lz_irods_path
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

            return f"{self.lz_irods_path}/{collections[user_input - 1]}"

        elif self.args.collection in collections:
            return f"{self.lz_irods_path}/{self.args.collection}"
        else:  # pragma: no cover
            logger.error("Selected target collection does not exist in landing zone.")
            sys.exit(1)

    def build_file_list(self, hash_ending) -> list[dict[str, Path]]:
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

    def build_jobs(self, hash_ending: str) -> list[TransferJob]:
        """Build file transfer jobs."""

        target_coll = self.build_target_coll()
        source_paths = self.build_file_list(hash_ending)

        transfer_jobs = []

        for p in source_paths:
            path_remote = f"{target_coll}/{str(p['ipath'])}"

            hash_path = p["spath"].parent / (p["spath"].name + hash_ending)

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

        return sorted(transfer_jobs, key=lambda x: x.path_local)


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar ingest``."""
    return SodarIngestCollection.setup_argparse(parser)
