"""Common code for ``cubi-tk snappy itransfer-*`` commands."""

import argparse
import glob
import os
import sys
import typing

from biomedsheets import shortcuts
from loguru import logger

from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi

from ..common import execute_checksum_files_fix, sizeof_fmt
from ..irods_common import TransferJob, iRODSCommon, iRODSTransfer
from ..sodar_common import selectLandingzoneMixin
from ..exceptions import MissingFileException, ParameterException
from .common import get_biomedsheet_path, load_sheet_tsv
from .parse_sample_sheet import ParseSampleSheet

#: Default number of parallel transfers.
DEFAULT_NUM_TRANSFERS = 8


def check_args(args):
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    _ = args


class SnappyItransferCommandBase(selectLandingzoneMixin, ParseSampleSheet):
    """Base class for itransfer commands."""

    #: The command name.
    command_name: typing.Optional[str] = None
    #: The step folder name to create.
    step_name: typing.Optional[str] = None
    #: Whether to look into largest start batch in family.
    start_batch_in_family: bool = False

    def __init__(self, argparse_args, *args):
        #: Command line arguments.
        super(SnappyItransferCommandBase, self).__init__(argparse_args, *args)
        self.args = argparse_args
        self.step_name = self.__class__.step_name

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args) -> int | None:
        """Called for checking arguments, override to change behaviour."""
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

    def build_jobs(self, library_names, sodar_api, hash_ending) -> tuple[str, tuple[TransferJob, ...]]:
        """Build file transfer jobs."""

        # Get path to iRODS directory
        try:
            lz_uuid, lz_irods_path = self.get_lz_info(sodar_api)
        except ParameterException as e:
            logger.error(f"Couldn't find LZ UUID and LZ iRods Path: {e}")
            sys.exit(1)

        transfer_jobs = []
        for library_name in library_names:
            base_dir, glob_pattern = self.build_base_dir_glob_pattern(library_name)
            glob_pattern = os.path.join(base_dir, glob_pattern)
            logger.debug("Glob pattern for library {} is {}", library_name, glob_pattern)
            for glob_result in glob.glob(glob_pattern, recursive=True):
                rel_result = os.path.relpath(glob_result, base_dir)
                real_result = os.path.realpath(glob_result)
                if real_result.endswith(hash_ending):
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
                for ext in ("", hash_ending):
                    transfer_jobs.append(
                        TransferJob(
                            path_local=real_result + ext,
                            path_remote=str(os.path.join(remote_dir, rel_result + ext))
                        )
                    )
        return lz_uuid, tuple(sorted(transfer_jobs, key=lambda x: x.path_local))

    # def get_sodar_info(self, sodar_api: SodarApi) -> tuple[str, str]:  #noqa: C901
    #     """Method evaluates user input to extract or create iRODS path. Use cases:
    #
    #     1. User provides Landing Zone UUID: fetch path and use it.
    #     2. User provides Project UUID:
    #        i. If there are LZ associated with project, select the latest active and use it.
    #       ii. If there are no LZ associated with project, create a new one and use it.
    #     3. Data provided by user is neither an iRODS path nor a valid UUID. Report error and throw exception.
    #
    #     :return: Returns landing zone UUID and path to iRODS directory.
    #     """
    #     # Initialise variables
    #     lz_irods_path = None
    #     lz_uuid = None
    #     # Check if provided UUID is associated with a Project (we have access to)
    #     #FIXME: sodar_api.get_samplesheet_retrieve() should probably not log an error message here
    #     dest_is_project_uuid = is_uuid(self.args.destination) and sodar_api.get_samplesheet_retrieve() is not None
    #
    #     # Project UUID provided by user
    #     if dest_is_project_uuid:
    #         lz_uuid, lz_irods_path = self.get_landing_zone(sodar_api, latest=not self.args.select_lz)
    #     # Provided UUID is NOT associated with a project, assume it is LZ instead
    #     elif is_uuid(self.args.destination):
    #         lz = sodar_api.get_landingzone_retrieve(self.args.destination)
    #         if lz:
    #             lz_irods_path = lz.irods_path
    #             lz_uuid = lz.sodar_uuid
    #         else:
    #             msg = "Provided UUID ({}) could neither be associated with a project nor with a Landing Zone.".format(
    #                 self.args.destination
    #             )
    #             raise ParameterException(msg)
    #     # Check if `destination` is a Landing zone (irods) path.
    #     elif self.args.destination.startswith("/"):
    #         lz_uuid, lz_irods_path = self.get_landing_zone(sodar_api, latest=not self.args.select_lz)
    #         if lz_uuid is None:
    #             msg = "Unable to identify UUID of given LZ {0}.".format(self.args.destination)
    #             raise ParameterException(msg)
    #
    #     # Not able to process - raise exception.
    #     # UUID provided is not associated with project nor lz, or could not extract UUID from LZ path.
    #     if lz_irods_path is None:
    #         msg = "Data provided by user is not a valid UUID or LZ path. Please review input: {0}".format(
    #             self.args.destination
    #         )
    #         raise ParameterException(msg)
    #
    #     # Log
    #     logger.info("Target iRODS path: {}", lz_irods_path)
    #
    #     # Return
    #     return lz_uuid, lz_irods_path
    #
    # #possibly integrate these steps in Sodar/transfer specific class/function
    # def _create_lz(self, sodar_api) -> (str, str):
    #     """
    #     Create a new landing zone (asking for user confirmation unless --yes is given) and check that is usable.
    #     :param sodar_api: sodar_api object from cubi-tk.sodar_api
    #     :return: (lz_uuid, lz_irods_path)
    #     """
    #
    #     if self.args.yes or (
    #         input("Can the process create a new landing zone? [yN] ")
    #         .lower()
    #         .startswith("y")
    #     ):
    #         lz = sodar_api.post_landingzone_create()
    #         if lz:
    #             lz_uuid = lz.sodar_uuid
    #             lz_irods_path = lz.irods_path
    #             # check that async LZ creation task is done
    #             lz_usable = False
    #             while not lz_usable:
    #                 lz_check = sodar_api.get_landingzone_retrieve(lz_uuid)
    #                 if lz_check and lz_check.status == "ACTIVE":
    #                     lz_usable = True
    #                 logger.debug("Waiting 5 seconds for LZ {} to become usable...", lz_uuid)
    #                 time.sleep(5)  # wait and ask API again
    #             return lz_uuid, lz_irods_path
    #         else:
    #             raise CubiTkException("Something went wrong during Lz creation")
    #     else:
    #         msg = "Not possible to continue the process without a landing zone path. Breaking..."
    #         raise UserCanceledException(msg)
    #
    #
    # def get_landing_zone(self, sodar_api, latest=True) -> (str, str):
    #     """
    #     Selection of landing zone to use for transfer. If --yes is given will use latest active landing zone
    #     or create a new one. With latest=False will ask user to select one of the available landing zones.
    #     :param sodar_api: sodar_api object from cubi-tk.sodar_api
    #     :param latest: boolean
    #     :return: (lz_uuid, lz_irods_path)
    #     """
    #     # Get existing LZs from API
    #     existing_lzs = sodar_api.get_landingzone_list(sort_reverse=True, filter_for_state=["ACTIVE", "FAILED"])
    #     if existing_lzs:
    #         logger.info(
    #             "Found {} active landing zone{}.".format(len(existing_lzs), 's' if len(existing_lzs) > 1 else "")
    #         )
    #         if not self.args.yes and not latest and (
    #             not input("Should the process use an existing landing zone? [yN] ")
    #             .lower()
    #             .startswith("y")
    #         ):
    #             return self._create_lz(sodar_api)
    #
    #         if len(existing_lzs) == 1:
    #             lz = existing_lzs[0]
    #             logger.debug(f"Single active landingzone with UUID {lz.sodar_uuid} will be used")
    #         elif len(existing_lzs) > 1:
    #             if latest or self.args.yes:
    #                 # Get latest active landing zone
    #                 lz = existing_lzs[-1]
    #                 logger.info(f"Latest active landingzone with UUID {lz.sodar_uuid} will be used")
    #             else:
    #                 # Ask User which landing zone to use
    #                 user_input = ""
    #                 input_valid = False
    #                 input_message = "####################\nPlease choose target landing zone:\n"
    #                 for index, lz in enumerate(existing_lzs):
    #                     input_message += f"{index + 1}) {os.path.basename(lz.irods_path)} ({lz.sodar_uuid})\n"
    #                 input_message += "Select by number: "
    #
    #                 while not input_valid:
    #                     user_input = input(input_message)
    #                     if user_input.isdigit():
    #                         user_input = int(user_input)
    #                         if 0 < user_input <= len(existing_lzs):
    #                             input_valid = True
    #
    #                 lz = existing_lzs[user_input - 1]
    #     else:
    #         # No active landing zones available
    #         logger.info("No active Landing Zone available.")
    #         return self._create_lz(sodar_api)
    #
    #     lz_irods_path = lz.irods_path
    #     lz_uuid = lz.sodar_uuid
    #
    #     # Return
    #     return lz_uuid, lz_irods_path

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
        project_uuid = self.get_project_uuid(sodar_api)
        biomedsheet_tsv = get_biomedsheet_path(
            start_path=self.args.base_path, uuid=project_uuid
        )

        # Extract library names from sample sheet
        sheet = load_sheet_tsv(biomedsheet_tsv, self.args.tsv_shortcut)
        library_names = list(
            self.yield_ngs_library_names(
                sheet=sheet, min_batch=self.args.first_batch, max_batch=self.args.last_batch
            )
        )
        logger.info("Libraries in sheet:\n{}", "\n".join(sorted(library_names)))
        irods_hash_scheme = iRODSCommon(sodar_profile=self.args.config_profile).irods_hash_scheme()
        hash_ending = "."+irods_hash_scheme.lower()
        lz_uuid, transfer_jobs = self.build_jobs(library_names, sodar_api, hash_ending)
        # logger.debug("Transfer jobs:\n{}", "\n".join(map(lambda x: x.to_oneline(), transfer_jobs)))

        transfer_jobs = execute_checksum_files_fix(transfer_jobs, irods_hash_scheme)

        # Final go from user & transfer
        itransfer = iRODSTransfer(transfer_jobs, ask=not self.args.yes, sodar_profile=self.args.config_profile)
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

