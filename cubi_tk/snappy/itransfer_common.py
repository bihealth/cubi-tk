"""Common code for ``cubi-tk snappy itransfer-*`` commands."""

import argparse
import datetime
import glob
import os
import typing
from ctypes import c_ulonglong
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
from subprocess import check_output, SubprocessError, check_call, STDOUT
import sys

import attr
from biomedsheets import io_tsv, shortcuts
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
from logzero import logger
import requests
from retrying import retry
import tqdm

from ..exceptions import MissingFileException, ParameterException, UserCanceledException
from ..common import check_irods_icommands, is_uuid, load_toml_config, sizeof_fmt

#: Default number of parallel transfers.
DEFAULT_NUM_TRANSFERS = 8


@attr.s(frozen=True, auto_attribs=True)
class TransferJob:
    """Encodes a transfer job from the local file system to the remote iRODS collection."""

    #: Source path.
    path_src: str

    #: Destination path.
    path_dest: str

    #: Number of bytes to transfer.
    bytes: int

    command: typing.Optional[str] = None

    def to_oneline(self):
        return "%s -> %s (%s) [%s]" % (self.path_src, self.path_dest, self.bytes, self.command)


@retry(wait_fixed=1000, stop_max_attempt_number=5)
def _wait_until_ils_succeeds(path):
    check_output(["ils", path], stderr=STDOUT)


@retry(wait_fixed=1000, stop_max_attempt_number=5)
def irsync_transfer(job: TransferJob, counter: Value, t: tqdm.tqdm):
    """Perform one piece of work and update the global counter."""
    mkdir_argv = ["imkdir", "-p", os.path.dirname(job.path_dest)]
    logger.debug("Creating directory when necessary: %s", " ".join(mkdir_argv))
    try:
        check_output(mkdir_argv)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing imkdir: %s (probably retrying)", e)
        raise

    _wait_until_ils_succeeds(os.path.dirname(job.path_dest))

    irsync_argv = ["irsync", "-a", "-K", job.path_src, "i:%s" % job.path_dest]
    logger.debug("Transferring file: %s", " ".join(irsync_argv))
    try:
        check_output(irsync_argv)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing irsync: %s (probably retrying)", e)
        raise

    with counter.get_lock():
        counter.value += job.bytes
        try:
            t.update(counter.value)
        except TypeError:
            pass  # swallow, pyfakefs and multiprocessing don't lik each other


def check_args(args):
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    _ = args


def load_sheet_tsv(args):
    """Load sample sheet."""
    logger.info(
        "Loading %s sample sheet from %s.",
        args.tsv_shortcut,
        getattr(args.biomedsheet_tsv, "name", "stdin"),
    )
    load_tsv = getattr(io_tsv, "read_%s_tsv_sheet" % args.tsv_shortcut)
    return load_tsv(args.biomedsheet_tsv, naming_scheme=NAMING_ONLY_SECONDARY_ID)


def load_sheets_tsv(args):
    """Load multiple sample sheets."""
    result = []

    for path in args.biomedsheet_tsv:
        logger.info(
            "Loading %s sample sheet from %s.",
            args.tsv_shortcut,
            getattr(args.biomedsheet_tsv, "name", "stdin"),
        )
        load_tsv = getattr(io_tsv, "read_%s_tsv_sheet" % args.tsv_shortcut)
        result.append(load_tsv(path, naming_scheme=NAMING_ONLY_SECONDARY_ID))

    return result


class SnappyItransferCommandBase:
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

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup common arguments for itransfer commands."""

        group_sodar = parser.add_argument_group("SODAR-related")
        group_sodar.add_argument(
            "--sodar-url",
            default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
            help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
        )
        group_sodar.add_argument(
            "--sodar-api-token",
            default=os.environ.get("SODAR_API_TOKEN", None),
            help="Authentication token when talking to SODAR.  Defaults to SODAR_API_TOKEN environment variable.",
        )

        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )

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
            "--start-batch",
            default=0,
            type=int,
            help="Batch to start the transfer at, defaults to 0.",
        )
        parser.add_argument(
            "--base-path",
            default=os.getcwd(),
            required=False,
            help="Base path of project (contains 'ngs_mapping/' etc.), defaults to current path.",
        )
        parser.add_argument(
            "--remote-dir-date",
            default=datetime.date.today().strftime("%Y-%m-%d"),
            help="Date to use in remote directory, defaults to YYYY-MM-DD of today.",
        )
        parser.add_argument(
            "--remote-dir-pattern",
            default="{library_name}/%s/{date}" % cls.step_name,
            help="Pattern to use for constructing remote pattern",
        )

        parser.add_argument(
            "biomedsheet_tsv",
            type=argparse.FileType("rt"),
            help="Path to biomedsheets TSV file to load.",
        )

        parser.add_argument(
            "--yes",
            default=False,
            action="store_true",
            help="Assume all answers are yes, e.g., will create or use "
            "existing available landing zones without asking.",
        )

        parser.add_argument(
            "--validate-and-move",
            default=False,
            action="store_true",
            help="After files are transferred to SODAR, it will proceed with validation and move.",
        )

        parser.add_argument("destination", help="UUID or iRods path of landing zone to move to.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        # Check presence of icommands when not testing.
        if "pytest" not in sys.modules:  # pragma: nocover
            check_irods_icommands(warn_only=False)
        res = 0

        toml_config = load_toml_config(args)
        if not args.sodar_url:
            if toml_config:
                args.sodar_url = toml_config.get("global", {}).get("sodar_server_url")
            else:
                logger.error("SODAR URL not found in config files. Please specify on command line.")
                res = 1
        if not args.sodar_api_token:
            if toml_config:
                args.sodar_api_token = toml_config.get("global", {}).get("sodar_api_token")
            else:
                logger.error(
                    "SODAR API token not found in config files. Please specify on command line."
                )
                res = 1

        if not os.path.exists(args.base_path):  # pragma: nocover
            logger.error("Base path %s does not exist", args.base_path)
            res = 1

        return res

    def _build_family_max_batch(self, sheet, batch_key, family_key):
        family_max_batch = {}
        for donor in sheet.bio_entities.values():
            if batch_key in donor.extra_infos and family_key in donor.extra_infos:
                family_id = donor.extra_infos[family_key]
                batch_no = donor.extra_infos[batch_key]
                family_max_batch[family_id] = max(family_max_batch.get(family_id, 0), batch_no)
        return family_max_batch

    def _batch_of(self, donor, family_max_batch, batch_key, family_key):
        if batch_key in donor.extra_infos:
            batch = donor.extra_infos[batch_key]
        else:
            batch = 0
        if self.start_batch_in_family and family_key in donor.extra_infos:
            family_id = donor.extra_infos[family_key]
            batch = max(batch, family_max_batch.get(family_id, 0))
        return batch

    def yield_ngs_library_names(
        self, sheet, min_batch=None, batch_key="batchNo", family_key="familyId"
    ):
        """Yield all NGS library names from sheet.

        When ``min_batch`` is given then only the donors for which the ``extra_infos[batch_key]`` is greater than
        ``min_batch`` will be used.

        This function can be overloaded, for example to only consider the indexes.
        """
        family_max_batch = self._build_family_max_batch(sheet, batch_key, family_key)

        # Process all libraries and filter by family batch ID.
        for donor in sheet.bio_entities.values():
            if min_batch is not None:
                batch = self._batch_of(donor, family_max_batch, batch_key, family_key)
                if batch < min_batch:
                    logger.debug(
                        "Skipping donor %s because %s = %d < min_batch = %d",
                        donor.name,
                        batch_key,
                        batch,
                        min_batch,
                    )
                    continue
            for bio_sample in donor.bio_samples.values():
                for test_sample in bio_sample.test_samples.values():
                    for library in test_sample.ngs_libraries.values():
                        yield library.name

    def build_base_dir_glob_pattern(
        self, library_name: str
    ) -> typing.Tuple[str, str]:  # pragma: nocover
        """Build base dir and glob pattern to append."""
        raise NotImplementedError("Abstract method called!")

    def build_jobs(self, library_names):
        """Build file transfer jobs."""

        # Get path to iRODS directory
        lz_uuid, lz_irods_path = self.get_sodar_info()

        transfer_jobs = []
        for library_name in library_names:
            base_dir, glob_pattern = self.build_base_dir_glob_pattern(library_name)
            glob_pattern = os.path.join(base_dir, glob_pattern)
            logger.debug("Glob pattern for library %s is %s", library_name, glob_pattern)
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
                        library_name=library_name, date=self.args.remote_dir_date
                    ),
                )
                if not os.path.exists(real_result):  # pragma: nocover
                    raise MissingFileException("Missing file %s" % real_result)
                if (
                    not os.path.exists(real_result + ".md5") and not self.fix_md5_files
                ):  # pragma: nocover
                    raise MissingFileException("Missing file %s" % (real_result + ".md5"))
                for ext in ("", ".md5"):
                    try:
                        size = os.path.getsize(real_result + ext)
                    except OSError:  # pragma: nocover
                        size = 0
                    transfer_jobs.append(
                        TransferJob(
                            path_src=real_result + ext,
                            path_dest=os.path.join(remote_dir, rel_result + ext),
                            bytes=size,
                        )
                    )
        return lz_uuid, tuple(sorted(transfer_jobs))

    def get_sodar_info(self):
        """Method evaluates user input to extract or create iRODS path. Use cases:

        1. User provides iRODS path. Same as before, use it.
        2. User provides Landing Zone UUID. Same as before, fetch path and use it.
        3. User provides Project UUID:
           i. If there are LZ associated with project, select the latest active and use it.
          ii. If there are no LZ associated with project, create a new one and use it.
        4. Data provided by user is neither an iRODS path nor a valid UUID. Report error and throw exception.

        :return: Returns landing zone UUID and path to iRODS directory.
        """
        # Initialise variables
        lz_irods_path = None
        lz_uuid = None
        not_project_uuid = False
        create_lz_bool = self.args.yes
        in_destination = self.args.destination

        # iRODS path provided by user
        # Not possible to retrieve lz uuid from path. Returns None for lz_uuid.
        if "/" in in_destination:
            lz_irods_path = in_destination

        # Project UUID provided by user
        elif is_uuid(in_destination):

            if create_lz_bool:
                # Assume that provided UUID is associated with a Project and user wants a new LZ.
                # Behavior: search for available LZ; if none,create new LZ.
                try:
                    lz_uuid, lz_irods_path = self.get_latest_landing_zone(
                        project_uuid=in_destination
                    )
                    if not lz_irods_path:
                        logger.info(
                            "No active Landing Zone available for project %s, "
                            "a new one will be created..." % lz_uuid
                        )
                        lz_uuid, lz_irods_path = self.create_landing_zone(
                            project_uuid=in_destination
                        )
                except requests.exceptions.HTTPError as e:
                    exception_str = str(e)
                    logger.error(
                        "Unable to create Landing Zone using UUID %s. HTTP error %s "
                        % (in_destination, exception_str)
                    )
                    raise

            else:
                # Assume that provided UUID is associated with a Project.
                # Behaviour: get iRODS path from latest active Landing Zone.
                try:
                    lz_uuid, lz_irods_path = self.get_latest_landing_zone(
                        project_uuid=in_destination
                    )
                except requests.exceptions.HTTPError as e:
                    not_project_uuid = True
                    exception_str = str(e)
                    logger.debug(
                        "Provided UUID may not be associated with a Project. HTTP error %s"
                        % exception_str
                    )

                # Assume that provided UUID is associated with a LZ
                # Behaviour: get iRODS path from it.
                if not_project_uuid:
                    try:
                        lz_uuid = in_destination
                        lz_irods_path = self.get_landing_zone_by_uuid(lz_uuid=lz_uuid)
                    except requests.exceptions.HTTPError as e:
                        exception_str = str(e)
                        logger.debug(
                            "Provided UUID may not be associated with a Landing Zone. HTTP error %s"
                            % exception_str
                        )

                # Request input from user.
                # Behaviour: depends on user reply to questions.
                if not not_project_uuid:
                    # Active lz available
                    # Ask user if should use latest available or create new one.
                    if lz_irods_path:
                        logger.info("Found active Landing Zone: %s" % lz_irods_path)
                        if (
                            not input("Can the process use this path? [yN] ")
                            .lower()
                            .startswith("y")
                        ):
                            logger.info(
                                "...an alternative is to create another Landing Zone using the UUID %s"
                                % in_destination
                            )
                            if (
                                input("Can the process create a new landing zone? [yN] ")
                                .lower()
                                .startswith("y")
                            ):
                                lz_uuid, lz_irods_path = self.create_landing_zone(
                                    project_uuid=in_destination
                                )
                            else:
                                msg = "Not possible to continue the process without a landing zone path. Breaking..."
                                logger.info(msg)
                                raise UserCanceledException(msg)

                    # No active lz available
                    # As user if should create new new.
                    else:
                        logger.info("No active Landing Zone available for UUID %s" % in_destination)
                        if (
                            input("Can the process create a new landing zone? [yN] ")
                            .lower()
                            .startswith("y")
                        ):
                            lz_uuid, lz_irods_path = self.create_landing_zone(
                                project_uuid=in_destination
                            )
                        else:
                            msg = "Not possible to continue the process without a landing zone path. Breaking..."
                            logger.info(msg)
                            raise UserCanceledException(msg)

        # Not able to process - raise exception.
        # UUID provided is not associated with project nor lz.
        if lz_irods_path is None:
            msg = (
                "Data provided by user is neither an iRODS path nor a valid UUID. "
                "Please review input: " + in_destination
            )
            logger.error(msg)
            raise ParameterException(msg)

        # Log
        logger.info("Target iRODS path: %s" % lz_irods_path)

        # Return
        return lz_uuid, lz_irods_path

    def move_landing_zone(self, lz_uuid):
        """
        Method calls SODAR API to validate and move transferred files.

        :param lz_uuid: Landing zone UUID.
        :type lz_uuid: str
        """
        from ..sodar.api import landing_zones

        logger.info(
            "Transferred files move to Landing Zone %s will be validated and moved in SODAR..."
            % lz_uuid
        )
        _ = landing_zones.move(
            sodar_url=self.args.sodar_url,
            sodar_api_token=self.args.sodar_api_token,
            landing_zone_uuid=lz_uuid,
        )
        logger.info("done.")

    def get_landing_zone_by_uuid(self, lz_uuid):
        """
        :param lz_uuid: Landing zone UUID.
        :type lz_uuid: str

        :return: Returns iRODS path.
        """
        from ..sodar.api import landing_zones

        lz = landing_zones.get(
            sodar_url=self.args.sodar_url,
            sodar_api_token=self.args.sodar_api_token,
            landing_zone_uuid=lz_uuid,
        )
        return lz.irods_path

    def create_landing_zone(self, project_uuid):
        """
        :param project_uuid: Project UUID.
        :type project_uuid: str

        :return: Returns landing zone UUID and iRODS path to newly created landing zone.
        """
        logger.info("Creating new Landing Zone...")
        from ..sodar.api import landing_zones

        lz = landing_zones.create(
            sodar_url=self.args.sodar_url,
            sodar_api_token=self.args.sodar_api_token,
            project_uuid=project_uuid,
        )
        logger.info("done!")
        return lz.sodar_uuid, lz.irods_path

    def get_latest_landing_zone(self, project_uuid):
        """
        :param project_uuid: Project UUID.
        :type project_uuid: str

        :return: Returns landing zone UUID and iRODS path in latest active landing zone available.
        If none available, it returns None for both.
        """
        from ..sodar.api import landing_zones

        # Initialise variables
        lz_irods_path = None
        lz_uuid = None

        # List existing lzs
        existing_lzs = sorted(
            landing_zones.list(
                sodar_url=self.args.sodar_url,
                sodar_api_token=self.args.sodar_api_token,
                project_uuid=project_uuid,
            ),
            key=lambda x: x.date_modified,
            reverse=True,
        )

        # Get the latest active lz
        existing_lzs = list(filter(lambda x: x.status == "ACTIVE", existing_lzs))
        if existing_lzs:
            lz = existing_lzs[-1]
            lz_irods_path = lz.irods_path
            lz_uuid = lz.sodar_uuid

        # Return
        return lz_uuid, lz_irods_path

    def _execute_md5_files_fix(
        self, transfer_jobs: typing.Tuple[TransferJob, ...]
    ) -> typing.Tuple[TransferJob, ...]:
        """Create missing MD5 files."""
        ok_jobs = []
        todo_jobs = []
        for job in transfer_jobs:
            if not os.path.exists(job.path_src):
                todo_jobs.append(job)
            else:
                ok_jobs.append(job)

        total_bytes = sum([os.path.getsize(j.path_src[: -len(".md5")]) for j in todo_jobs])
        logger.info(
            "Computing MD5 sums for %s files of %s with up to %d processes",
            len(todo_jobs),
            sizeof_fmt(total_bytes),
            self.args.num_parallel_transfers,
        )
        logger.info("Missing MD5 files:\n%s", "\n".join(map(lambda j: j.path_src, todo_jobs)))
        counter = Value(c_ulonglong, 0)
        with tqdm.tqdm(total=total_bytes, unit="B", unit_scale=True) as t:
            if self.args.num_parallel_transfers == 0:  # pragma: nocover
                for job in todo_jobs:
                    compute_md5sum(job, counter, t)
            else:
                pool = ThreadPool(processes=self.args.num_parallel_transfers)
                for job in todo_jobs:
                    pool.apply_async(compute_md5sum, args=(job, counter, t))
                pool.close()
                pool.join()

        # Finally, determine file sizes after done.
        done_jobs = [
            TransferJob(
                path_src=j.path_src,
                path_dest=j.path_dest,
                bytes=os.path.getsize(j.path_src),
                command=j.command,
            )
            for j in todo_jobs
        ]
        return tuple(sorted(done_jobs + ok_jobs))

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk snappy %s", self.command_name)
        logger.info("  args: %s", self.args)

        sheet = load_sheet_tsv(self.args)
        library_names = list(self.yield_ngs_library_names(sheet, min_batch=self.args.start_batch))
        logger.info("Libraries in sheet:\n%s", "\n".join(sorted(library_names)))

        lz_uuid, transfer_jobs = self.build_jobs(library_names)
        logger.debug("Transfer jobs:\n%s", "\n".join(map(lambda x: x.to_oneline(), transfer_jobs)))

        if self.fix_md5_files:
            transfer_jobs = self._execute_md5_files_fix(transfer_jobs)

        total_bytes = sum([job.bytes for job in transfer_jobs])
        logger.info(
            "Transferring %d files with a total size of %s",
            len(transfer_jobs),
            sizeof_fmt(total_bytes),
        )
        counter = Value(c_ulonglong, 0)
        with tqdm.tqdm(total=total_bytes, unit="B", unit_scale=True) as t:
            if self.args.num_parallel_transfers == 0:  # pragma: nocover
                for job in transfer_jobs:
                    irsync_transfer(job, counter, t)
            else:
                pool = ThreadPool(processes=self.args.num_parallel_transfers)
                for job in transfer_jobs:
                    pool.apply_async(irsync_transfer, args=(job, counter, t))
                pool.close()
                pool.join()

        # Validate and move transferred files
        # Behaviour: If flag is True and lz uuid is not None*,
        # it will ask SODAR to validate and move transferred files.
        # (*) It can be None if user provided path
        if lz_uuid and self.args.validate_and_move:
            self.move_landing_zone(lz_uuid=lz_uuid)
        else:
            logger.info("Transferred files will \033[1mnot\033[0m be automatically moved in SODAR.")

        logger.info("All done")
        return None


class IndexLibrariesOnlyMixin:
    """Mixin for ``SnappyItransferCommandBase`` that only considers libraries of indexes."""

    def yield_ngs_library_names(
        self, sheet, min_batch=None, batch_key="batchNo", family_key="familyId"
    ):
        family_max_batch = self._build_family_max_batch(sheet, batch_key, family_key)

        shortcut_sheet = shortcuts.GermlineCaseSheet(sheet)
        for pedigree in shortcut_sheet.cohort.pedigrees:
            donor = pedigree.index
            if min_batch is not None:
                batch = self._batch_of(donor, family_max_batch, batch_key, family_key)
                if batch < min_batch:
                    logger.debug(
                        "Skipping donor %s because %s = %d < min_batch = %d",
                        donor.name,
                        batch_key,
                        donor.extra_infos[batch_key],
                        min_batch,
                    )
                    continue
            logger.debug("Processing NGS library for donor %s", donor.name)
            yield donor.dna_ngs_library.name


@attr.s(frozen=True, auto_attribs=True)
class FileWithSize:
    """Pair of path with size."""

    #: Path to file.
    path: str
    #: File size.
    bytes: int


def compute_md5sum(job: TransferJob, counter: Value, t: tqdm.tqdm) -> None:
    """Compute MD5 sum with ``md5sum`` command."""
    dirname = os.path.dirname(job.path_src)
    filename = os.path.basename(job.path_src)[: -len(".md5")]
    path_md5 = job.path_src

    md5sum_argv = ["md5sum", filename]
    logger.debug("Computing MD5sum %s > %s", " ".join(md5sum_argv), filename + ".md5")
    try:
        with open(path_md5, "wt") as md5f:
            check_call(md5sum_argv, cwd=dirname, stdout=md5f)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing md5sum: %s", e)
        logger.info("Removing file after error: %s", path_md5)
        try:
            os.remove(path_md5)
        except OSError as e_rm:  # pragma: nocover
            logger.error("Could not remove file: %s", e_rm)
        raise e

    with counter.get_lock():
        counter.value += os.path.getsize(job.path_src[: -len(".md5")])
        try:
            t.update(counter.value)
        except TypeError:
            pass  # swallow, pyfakefs and multiprocessing don't lik each other
