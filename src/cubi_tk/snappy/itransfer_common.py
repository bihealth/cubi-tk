"""Common code for ``cubi-tk snappy itransfer-*`` commands."""

import argparse
from ctypes import c_ulonglong
import glob
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
import os
from subprocess import SubprocessError, check_call
import sys
import typing

from biomedsheets import shortcuts
from loguru import logger
import tqdm

from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi

from ..common import check_irods_icommands, is_uuid, sizeof_fmt
from ..irods_common import TransferJob, iRODSTransfer
from ..exceptions import CubiTkException, MissingFileException, ParameterException, UserCanceledException
from .common import get_biomedsheet_path, load_sheet_tsv
from .parse_sample_sheet import ParseSampleSheet

#: Default number of parallel transfers.
DEFAULT_NUM_TRANSFERS = 8


#TODO: remove/replace check_irods_icommands


def check_args(args):
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    _ = args


class SnappyItransferCommandBase(ParseSampleSheet):
    """Base class for itransfer commands."""

    #: The command name.
    command_name: typing.Optional[str] = None
    #: The step folder name to create.
    step_name: typing.Optional[str] = None
    #: Whether or not to fix .md5 files on the fly.
    fix_md5_files: bool = False
    #: Whether to look into largest start batch in family.
    start_batch_in_family: bool = False

    def __init__(self, args):
        #: Command line arguments.
        self.args = args
        self.step_name = self.__class__.step_name

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args) -> int | None:
        """Called for checking arguments, override to change behaviour."""
        # Check presence of icommands when not testing.#TODO: remove check_irods_icommands
        if "pytest" not in sys.modules:  # pragma: nocover
            check_irods_icommands(warn_only=False)
        res = 0
        if not os.path.exists(args.base_path):  # pragma: nocover
            logger.error("Base path {} does not exist", args.base_path)
            res = 1

        return res

    def build_base_dir_glob_pattern(
        self, library_name: str
    ) -> tuple[str, str]:  # pragma: nocover
        """Build base dir and glob pattern to append."""
        raise NotImplementedError("Abstract method called!")

    def build_jobs(self, library_names, sodar_api) -> tuple[str, list[TransferJob, ...]]:
        """Build file transfer jobs."""

        # Get path to iRODS directory
        try:
            lz_uuid, lz_irods_path = self.get_sodar_info(sodar_api)
        except CubiTkException as e:
            logger.error(f"couldn't find LZ UUID and LZ iRods Path: {e}")
            sys.exit(1)

        transfer_jobs = []
        for library_name in library_names:
            base_dir, glob_pattern = self.build_base_dir_glob_pattern(library_name)
            glob_pattern = os.path.join(base_dir, glob_pattern)
            logger.debug("Glob pattern for library {} is {}", library_name, glob_pattern)
            for glob_result in glob.glob(glob_pattern, recursive=True):
                rel_result = os.path.relpath(glob_result, base_dir)
                real_result = os.path.realpath(glob_result)
                if real_result.endswith(".md5"):
                    continue  # skip, will be added automatically
                if not os.path.isfile(real_result):
                    continue  # skip if did not resolve to file
                remote_dir = os.path.join(
                    lz_irods_path,
                    self.args.remote_dir_pattern.format(
                        library_name=library_name,
                        step=self.step_name,
                        date=self.args.remote_dir_date,
                    ),
                )
                if not os.path.exists(real_result):  # pragma: nocover
                    raise MissingFileException("Missing file %s" % real_result)
                if (
                    not os.path.exists(real_result + ".md5") and not self.fix_md5_files
                ):  # pragma: nocover
                    raise MissingFileException("Missing file %s" % (real_result + ".md5"))
                for ext in ("", ".md5"):
                    transfer_jobs.append(
                        TransferJob(
                            path_local=real_result + ext,
                            path_remote=str(os.path.join(remote_dir, rel_result + ext))
                        )
                    )
        return lz_uuid, sorted(transfer_jobs, key=lambda x: x.path_local)

    def get_sodar_info(self, sodar_api:SodarApi) -> tuple[str, str]:  #noqa: C901
        """Method evaluates user input to extract or create iRODS path. Use cases:

        1. User provides Landing Zone UUID: fetch path and use it.
        2. User provides Project UUID:
           i. If there are LZ associated with project, select the latest active and use it.
          ii. If there are no LZ associated with project, create a new one and use it.
        3. Data provided by user is neither an iRODS path nor a valid UUID. Report error and throw exception.

        :return: Returns landing zone UUID and path to iRODS directory.
        """
        # Initialise variables
        lz_irods_path = None
        lz_uuid = None
        not_project_uuid = False

        # Project UUID provided by user
        if is_uuid(self.args.destination):
            if self.args.yes:
                # Assume that provided UUID is associated with a Project and user wants a new LZ.
                # Behavior: search for available LZ; if none,create new LZ.

                lz_uuid, lz_irods_path = self.get_latest_landing_zone(sodar_api)
                if not lz_irods_path:
                    logger.info(
                        "No active Landing Zone available for project {}, a new one will be created...", lz_uuid
                    )
                    lz = sodar_api.post_landingzone_create()
                    if lz:
                        lz_uuid = lz.sodar_uuid
                        lz_irods_path = lz.irods_path
                    else:
                        msg = "Unable to create Landing Zone using UUID {0}.".format(self.args.destination)
                        raise ParameterException(msg)

            else:
                # Assume that provided UUID is associated with a Project.
                # Behaviour: get iRODS path from latest active Landing Zone.

                lz_uuid, lz_irods_path = self.get_latest_landing_zone(sodar_api)
                if lz_uuid is None or lz_irods_path is None:
                    not_project_uuid = True
                    logger.debug(
                        "Provided UUID may not be associated with a Project.")

                # Assume that provided UUID is associated with a LZ
                # Behaviour: get iRODS path from it.
                if not_project_uuid:
                    lz = sodar_api.get_landingzone_retrieve()
                    if lz:
                        lz_irods_path = lz.irods_path
                        lz_uuid = lz.sodar_uuid
                    else:
                        logger.debug(
                            "Provided UUID may not be associated with a Landing Zone.")

                # Request input from user.
                # Behaviour: depends on user reply to questions.
                if not not_project_uuid:
                    # Active lz available
                    # Ask user if should use latest available or create new one.
                    try:
                        lz_uuid, lz_irods_path =  self._get_user_input(lz_irods_path,lz_uuid, sodar_api)
                    except UserCanceledException as e :
                        raise e


        # Check if `in_destination` is a Landing zone path.
        elif self.args.destination.startswith("/"):
            lz_uuid = self.get_latest_landing_zone_uuid_by_path(lz_irods_path, sodar_api)
            if lz_uuid is None:
                msg = "Unable to identify UUID of given LZ {0}.".format(self.args.destination)
                raise ParameterException(msg)

        # Not able to process - raise exception.
        # UUID provided is not associated with project nor lz, or could not extract UUID from LZ path.
        if lz_irods_path is None:
            msg = "Data provided by user is not a valid UUID or LZ path. Please review input: {0}".format(
                self.args.destination
            )
            logger.error(msg)
            raise ParameterException(msg)

        # Log
        logger.info("Target iRODS path: {}", lz_irods_path)

        # Return
        return lz_uuid, lz_irods_path

    #possibly integrate in Sodar/transfer specific class/function
    def _get_user_input(self, lz_irods_path, lz_uuid, sodar_api):
        if lz_irods_path:
            logger.info("Found active Landing Zone: {} (uuid: {})", lz_irods_path, lz_uuid)
            if (
                not input("Can the process use this path? [yN] ")
                .lower()
                .startswith("y")
            ):
                logger.info(
                    "...an alternative is to create another Landing Zone using the UUID {}",
                    self.args.destination
                )
                if (
                    input("Can the process create a new landing zone? [yN] ")
                    .lower()
                    .startswith("y")
                ):
                    lz = sodar_api.post_landingzone_create()
                    if lz:
                        lz_uuid = lz.sodar_uuid
                        lz_irods_path = lz.irods_path
                else:
                    msg = "Not possible to continue the process without a landing zone path. Breaking..."
                    logger.info(msg)
                    raise UserCanceledException(msg)

        # No active lz available
        # As user if should create new new.
        else:
            logger.info("No active Landing Zone available for UUID {}", self.args.destination)
            if (
                input("Can the process create a new landing zone? [yN] ")
                .lower()
                .startswith("y")
            ):
                lz = sodar_api.post_landingzone_create()
                if lz:
                    lz_uuid = lz.sodar_uuid
                    lz_irods_path = lz.irods_path
            else:
                msg = "Not possible to continue the process without a landing zone path. Breaking..."
                logger.info(msg)
                raise UserCanceledException(msg)
        return lz_uuid, lz_irods_path

    def get_latest_landing_zone_uuid_by_path(self, lz_irods_path, sodar_api, ):
        """
        :return: Returns LZ UUID.
        """

        # List existing lzs
        # lzuuid from lz path already in sodar_api.projectUUID, doen't need to be in arguments
        existing_lzs = sodar_api.get_landingzone_list(sort_reverse = True, filter_for_state=["ACTIVE", "FAILED"])

        if existing_lzs:
            lz_uuid = existing_lzs[0].sodar_uuid
            logger.info(f"Latest active landingzone with UUID {lz_uuid} will be used")
        else:
            msg = (
                "Could not find an active LZ with the given path. Please review input: {0}".format(
                    lz_irods_path
                )
            )
            logger.error(msg)
            lz_uuid =  None

        return lz_uuid

    def get_latest_landing_zone(self, sodar_api):
        """
        :return: Returns landing zone UUID and iRODS path in latest active landing zone available.
        If none available, it returns None for both.
        """

        # Initialise variables
        lz_irods_path = None
        lz_uuid = None

        # List existing lzs
        existing_lzs = sodar_api.get_landingzone_list(sort_reverse = True, filter_for_state=["ACTIVE", "FAILED"])

        if existing_lzs:
            lz = existing_lzs[-1]
            lz_irods_path = lz.irods_path
            lz_uuid = lz.sodar_uuid
            logger.info(f"Latest active landingzone with UUID {lz_uuid} will be used")

        # Return
        return lz_uuid, lz_irods_path

    def _execute_md5_files_fix(
        self, transfer_jobs: list[TransferJob],
        parallel_jobs: int = 8
    ) -> list[TransferJob]:
        """Create missing MD5 files."""
        ok_jobs = []
        todo_jobs = []
        for job in transfer_jobs:
            if not os.path.exists(job.path_local):
                todo_jobs.append(job)
            else:
                ok_jobs.append(job)

        total_bytes = sum([os.path.getsize(j.path_local[: -len(".md5")]) for j in todo_jobs])
        logger.info(
            "Computing MD5 sums for {} files of {} with up to {} processes",
            len(todo_jobs),
            sizeof_fmt(total_bytes),
            parallel_jobs,
        )
        logger.info("Missing MD5 files:\n{}", "\n".join(j.path_local for j in todo_jobs))
        counter = Value(c_ulonglong, 0)
        with tqdm.tqdm(total=total_bytes, unit="B", unit_scale=True) as t:
            if parallel_jobs == 0:  # pragma: nocover
                for job in todo_jobs:
                    compute_md5sum(job, counter, t)
            else:
                pool = ThreadPool(processes=parallel_jobs)
                for job in todo_jobs:
                    pool.apply_async(compute_md5sum, args=(job, counter, t))
                pool.close()
                pool.join()

        # Finally, determine file sizes after done.
        done_jobs = [
            TransferJob(
                path_local=j.path_local,
                path_remote=j.path_remote,
            )
            for j in todo_jobs
        ]
        return sorted(done_jobs + ok_jobs, key=lambda x: x.path_local)

    def execute(self) -> int | None:
        """Execute the transfer."""
        # Validate arguments
        res = self.check_args(self.args)
        sodar_api = SodarApi(self.args, with_dest=True, dest_string="destination")
        if res:  # pragma: nocover
            return res

        # Logger
        logger.info("Starting cubi-tk snappy {}", self.command_name)
        print_args(self.args)

        # Fix for ngs_mapping & variant_calling vs step
        if self.step_name is None:
            self.step_name = self.args.step

        # Find biomedsheet file
        biomedsheet_tsv = get_biomedsheet_path(
            start_path=self.args.base_path, uuid=self.args.destination
        )

        # Extract library names from sample sheet
        sheet = load_sheet_tsv(biomedsheet_tsv, self.args.tsv_shortcut)
        library_names = list(
            self.yield_ngs_library_names(
                sheet=sheet, min_batch=self.args.first_batch, max_batch=self.args.last_batch
            )
        )
        logger.info("Libraries in sheet:\n{}", "\n".join(sorted(library_names)))

        lz_uuid, transfer_jobs = self.build_jobs(library_names, sodar_api)
        # logger.debug("Transfer jobs:\n{}", "\n".join(map(lambda x: x.to_oneline(), transfer_jobs)))

        if self.fix_md5_files:
            transfer_jobs = self._execute_md5_files_fix(transfer_jobs)

        # Final go from user & transfer
        itransfer = iRODSTransfer(transfer_jobs, ask=not self.args.yes)
        logger.info("Planning to transfer the following files:")
        for job in transfer_jobs:
            logger.info(job.path_local)
        logger.info(f"With a total size of {sizeof_fmt(itransfer.size)}")

        # This does support "num_parallel_transfers" (but it may autimatically use multiple transfer threads?)
        itransfer.put(recursive=True, sync=self.args.overwrite_remote)
        logger.info("File transfer complete.")

        # Validate and move transferred files
        # Behaviour: If flag is True and lz uuid is not None*,
        # it will ask SODAR to validate and move transferred files.
        # (*) It can be None if user provided path
        if lz_uuid and self.args.validate_and_move:
            logger.info(
                "Transferred files move to Landing Zone {} will be validated and moved in SODAR...",
                lz_uuid
            )
            uuid = sodar_api.post_landingzone_submit_move(lz_uuid)
            if uuid is None:
                logger.error("something went wrong during lz move")
                return
            logger.info("done.")
        else:
            logger.info("Transferred files will \033[1mnot\033[0m be automatically moved in SODAR.")

        logger.info("All done")
        return None


class IndexLibrariesOnlyMixin:
    """Mixin for ``SnappyItransferCommandBase`` that only considers libraries of indexes."""

    def yield_ngs_library_names(
        self, sheet, min_batch=None, max_batch=None, batch_key="batchNo", family_key="familyId"
    ):
        """Yield index only NGS library names from sheet.

        When ``min_batch`` is given then only the donors for which the ``extra_infos[batch_key]`` is greater than
        ``min_batch`` will be used.

        This function can be overloaded, for example to only consider the indexes.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param min_batch: Minimum batch number to be extracted from the sheet. All samples in batches below this values
        will be skipped.
        :type min_batch: int

        :param max_batch: Maximum batch number to be extracted from the sheet. All samples in batches above this values
        will be skipped.
        :type max_batch: int

        :param batch_key: Batch number key in sheet. Default: 'batchNo'.
        :type batch_key: str

        :param family_key: Family identifier key. Default: 'familyId'.
        :type family_key: str
        """
        family_max_batch = self._build_family_max_batch(sheet, batch_key, family_key)

        shortcut_sheet = shortcuts.GermlineCaseSheet(sheet)
        for pedigree in shortcut_sheet.cohort.pedigrees:
            donor = pedigree.index
            if min_batch is not None:
                batch = self._batch_of(donor, family_max_batch, batch_key, family_key)
                if batch < min_batch:
                    logger.debug(
                        "Skipping donor {} because {} = {} < min_batch = {}",
                        donor.name,
                        batch_key,
                        donor.extra_infos[batch_key],
                        min_batch,
                    )
                    continue
            if max_batch is not None:
                if batch > max_batch:
                    logger.debug(
                        "Skipping donor {} because {} = {} > max_batch = {}",
                        donor.name,
                        batch_key,
                        donor.extra_infos[batch_key],
                        max_batch,
                    )
                    continue
            logger.debug("Processing NGS library for donor {}", donor.name)
            yield donor.dna_ngs_library.name


def compute_md5sum(job: TransferJob, counter: Value, t: tqdm.tqdm) -> None:
    """Compute MD5 sum with ``md5sum`` command."""
    dirname = os.path.dirname(job.path_local)
    filename = os.path.basename(job.path_local)[: -len(".md5")]
    path_md5 = job.path_local

    md5sum_argv = ["md5sum", filename]
    logger.debug("Computing MD5sum {} > {}", " ".join(md5sum_argv), filename + ".md5")
    try:
        with open(path_md5, "wt") as md5f:
            check_call(md5sum_argv, cwd=dirname, stdout=md5f)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing md5sum: {}", e)
        logger.info("Removing file after error: {}", path_md5)
        try:
            os.remove(path_md5)
        except OSError as e_rm:  # pragma: nocover
            logger.error("Could not remove file: {}", e_rm)
        raise e

    with counter.get_lock():
        counter.value = os.path.getsize(job.path_local[: -len(".md5")])
        try:
            t.update(counter.value)
        except TypeError:
            pass  # swallow, pyfakefs and multiprocessing don't lik each other
