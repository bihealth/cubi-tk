"""Common code for ``cubi-tk snappy itransfer-*`` commands."""

import glob
import os

from biomedsheets import shortcuts
from loguru import logger

from ..irods_common import TransferJob
from ..sodar_common import SodarIngestBase
from ..exceptions import MissingFileException
from .common import get_biomedsheet_path, load_sheet_tsv
from .parse_sample_sheet import ParseSampleSheet


def check_args(args):
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    _ = args


class SnappyItransferCommandBase(SodarIngestBase, ParseSampleSheet):
    """Base class for itransfer commands."""

    cubitk_section = "snappy"
    #: The step folder name to create.
    step_name: str | None = None
    #: Whether to look into the largest start batch in family.
    start_batch_in_family: bool = False

    def __init__(self, args):
        #: Command line arguments.
        super(SnappyItransferCommandBase, self).__init__(args)
        self.args = args
        # Allow setting this before running super.__init__ in subclasses
        if not self.step_name:
            self.step_name = self.__class__.step_name

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

    def get_sample_names(self) -> list[str]:
        # Find biomedsheet file
        project_uuid = self.sodar_api.project_uuid
        biomedsheet_tsv = get_biomedsheet_path(start_path=self.args.base_path, uuid=project_uuid)

        # Extract library names from sample sheet
        sheet = load_sheet_tsv(biomedsheet_tsv, self.args.tsv_shortcut)
        library_names = list(
            self.yield_ngs_library_names(
                sheet=sheet, min_batch=self.args.first_batch, max_batch=self.args.last_batch
            )
        )
        logger.info("Libraries in sheet:\n{}", "\n".join(sorted(library_names)))
        return library_names

    def build_jobs(self, hash_ending) -> list[TransferJob]:
        """Build file transfer jobs."""
        library_names = self.get_sample_names()

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
                    self.lz_irods_path,
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
                            path_remote=str(os.path.join(remote_dir, rel_result + ext)),
                        )
                    )
        return sorted(transfer_jobs, key=lambda x: x.path_local)


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
