"""``cubi-sak snappy itransfer-raw-data``: transfer raw FASTQs into iRODS landing zone."""

import argparse
import datetime
import glob
import os
from ctypes import c_ulonglong
from multiprocessing.pool import ThreadPool
from multiprocessing import Value
import sys
import typing
from subprocess import check_output

import attr
from biomedsheets import io_tsv
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
from logzero import logger
import tqdm

from ..common import check_irods_icommands, sizeof_fmt
from ..exceptions import MissingFileException

#: Template string for raw data / input links file.
TPL_INPUT_LINK_DIR = "ngs_mapping/work/input_links/%(library_name)s"

#: Default number of parallel transfers.
DEFAULT_NUM_TRANSFERS = 8


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-sak snappy itransfer-raw-data``."""
    parser.add_argument("--hidden-cmd", dest="snappy_cmd", default=run, help=argparse.SUPPRESS)

    parser.add_argument(
        "--num-parallel-transfers",
        type=int,
        default=DEFAULT_NUM_TRANSFERS,
        help="Number of parallel transfers, defaults to %s" % DEFAULT_NUM_TRANSFERS,
    )
    parser.add_argument(
        "--tsv-shortcut",
        default="germline",
        choices=("germline", "cancer"),
        help="The shortcut TSV schema to use.",
    )
    parser.add_argument(
        "--start-batch", default=0, type=int, help="Batch to start the transfer at, defaults to 0."
    )
    parser.add_argument(
        "--base-path",
        default=os.getcwd(),
        required=False,
        help="Base path of project (contains 'ngs_mapping/' etc.), defaults to current path.",
    )
    parser.add_argument(
        "--remote-dir-pattern",
        default="{library_name}/raw_data/{date}",
        help="Pattern to use for constructing remote pattern",
    )
    parser.add_argument(
        "--remote-dir-date",
        default=datetime.date.today().strftime("%Y-%m-%d"),
        help="Date to use in remote directory, defaults to YYYY-MM-DD of today.",
    )

    parser.add_argument(
        "biomedsheet_tsv",
        type=argparse.FileType("rt"),
        help="Path to biomedsheets TSV file to load.",
    )
    parser.add_argument("irods_dest", help="path to iRODS collection to write to.")


def check_args(args):
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    # Check presence of icommands when not testing.
    if "pytest" not in sys.modules:  # pragma: nocover
        check_irods_icommands(warn_only=False)

    res = 0

    if not os.path.exists(args.base_path):  # pragma: nocover
        logger.error("Base path %s does not exist", args.base_path)
        res = 1

    return res


def load_sheet_tsv(args):
    """Load sample sheet."""
    logger.info(
        "Loading %s sample sheet from %s.",
        args.tsv_shortcut,
        getattr(args.biomedsheet_tsv, "name", "stdin"),
    )
    load_tsv = getattr(io_tsv, "read_%s_tsv_sheet" % args.tsv_shortcut)
    return load_tsv(args.biomedsheet_tsv, naming_scheme=NAMING_ONLY_SECONDARY_ID)
    # sheet_class = getattr(shortcuts, "%sCaseSheet" % args.tsv_shortcut.title())
    # shortcut_sheet = sheet_class(sheet)


def all_ngs_library_names(sheet):
    """Yield all NGS library names from sheet"""
    for donor in sheet.bio_entities.values():
        for bio_sample in donor.bio_samples.values():
            for test_sample in bio_sample.test_samples.values():
                for library in test_sample.ngs_libraries.values():
                    yield library.name


@attr.s(frozen=True, auto_attribs=True)
class TransferJob:
    """Encodes a transfer job from the local file system to the remote iRODS collection."""

    #: Source path.
    path_src: str

    #: Destination path.
    path_dest: str

    #: Number of bytes to transfer.
    bytes: int

    def to_oneline(self):
        return "%s -> %s (%s)" % (self.path_src, self.path_dest, self.bytes)


def build_jobs(args, library_names) -> typing.Tuple[TransferJob]:
    """Collect bulk data and MD5 files to transfer, construct jobs."""
    transfer_jobs = []
    for library_name in library_names:
        base_dir = os.path.join(args.base_path, TPL_INPUT_LINK_DIR % {"library_name": library_name})
        glob_pattern = base_dir + "/*"
        logger.debug("Glob pattern for library %s is %s", library_name, glob_pattern)
        for glob_result in sorted(glob.glob(glob_pattern)):
            rel_result = os.path.relpath(glob_result, base_dir)
            real_result = os.path.realpath(glob_result)
            if real_result.endswith(".md5"):
                continue  # skip, will be added automatically
            remote_dir = os.path.join(
                args.irods_dest,
                args.remote_dir_pattern.format(
                    library_name=library_name, date=args.remote_dir_date
                ),
            )
            if not os.path.exists(real_result):  # pragma: nocover
                raise MissingFileException("Missing file %s" % real_result)
            if not os.path.exists(real_result + ".md5"):  # pragma: nocover
                raise MissingFileException("Missing file %s" % (real_result + ".md5"))
            for ext in ("", ".md5"):
                transfer_jobs.append(
                    TransferJob(
                        path_src=real_result + ext,
                        path_dest=os.path.join(remote_dir, rel_result + ext),
                        bytes=os.path.getsize(real_result + ext),
                    )
                )
    return tuple(transfer_jobs)


def analyze_data(job: TransferJob, counter: Value, t: tqdm.tqdm):
    """Perform one piece of work and update the global counter."""
    mkdir_argv = ["imkdir", "-p", os.path.dirname(job.path_dest)]
    logger.debug("Creating directory when necessary: %s", " ".join(mkdir_argv))
    check_output(mkdir_argv)

    irsync_argv = ["irsync", "-a", "-K", job.path_src, "i:%s" % job.path_dest]
    logger.debug("Transferring file: %s", " ".join(irsync_argv))
    check_output(irsync_argv)

    with counter.get_lock():
        counter.value += job.bytes
        t.update(counter.value)


def run(args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser) -> None:
    res = check_args(args)
    if res:  # pragma: nocover
        return res

    logger.info("Starting cubi-sak snappy itransfer-raw-data")
    logger.info("  args: %s", args)

    sheet = load_sheet_tsv(args)
    library_names = list(all_ngs_library_names(sheet))
    logger.info("Libraries in sheet:\n%s", "\n".join(sorted(library_names)))

    transfer_jobs = build_jobs(args, library_names)
    total_bytes = sum([job.bytes for job in transfer_jobs])
    logger.debug("Transfer jobs:\n%s", "\n".join(map(lambda x: x.to_oneline(), transfer_jobs)))
    logger.info(
        "Transferring %d files with a total size of %s", len(transfer_jobs), sizeof_fmt(total_bytes)
    )

    counter = Value(c_ulonglong, 0)
    with tqdm.tqdm(total=total_bytes, unit="B", unit_scale=True) as t:
        pool = ThreadPool(processes=args.num_parallel_transfers)
        for job in transfer_jobs:
            pool.apply_async(analyze_data, args=(job, counter, t))
        pool.close()
        pool.join()

    logger.info("All done")
