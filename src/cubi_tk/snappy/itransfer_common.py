"""Common code for ``cubi-tk snappy itransfer-*`` commands."""

import argparse
import glob
import os
import sys
import typing

from biomedsheets import shortcuts
from loguru import logger

from cubi_tk.parsers import print_args

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

    def build_base_dir_glob_pattern(self, library_name: str) -> tuple[str, str]:  # pragma: nocover
        """Build base dir and glob pattern to append."""
        raise NotImplementedError("Abstract method called!")

    def build_jobs(self, library_names, hash_ending) -> tuple[str, tuple[TransferJob, ...]]:
        """Build file transfer jobs."""
        # Get path to iRODS directory
        try:
            lz_uuid, lz_irods_path = self.get_lz_info()
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

    def execute(self) -> int | None:
        """Execute the transfer."""
        # Validate arguments
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        # Logger
        logger.info("Starting cubi-tk snappy {}", self.command_name)
        print_args(self.args)

        # Fix for ngs_mapping & variant_calling vs step
        if self.step_name is None:
            self.step_name = self.args.step

        # Find biomedsheet file
        project_uuid = self.sodar_api.project_uuid
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
        lz_uuid, transfer_jobs = self.build_jobs(library_names, hash_ending)
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
            uuid = self.sodar_api.post_landingzone_submit_move(lz_uuid)
            if uuid is None:
                logger.error("something went wrong during lz move")
                return
            logger.info("done.")
        else:
            logger.info("Transferred files will not be automatically moved in SODAR.")

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

