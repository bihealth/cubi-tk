import os
import sys
import pandas as pd

from argparse import Namespace
from collections import defaultdict
from pathlib import PurePosixPath
from typing import TypedDict, Literal

from loguru import logger

from cubi_tk.api_models import IrodsDataObject
from cubi_tk.common import execute_checksum_files_fix
from cubi_tk.exceptions import CubiTkException, ParameterException, UserCanceledException
from cubi_tk.irods_common import TransferJob, iRODSTransfer, iRODSCommon
from cubi_tk.sodar_api import SodarApi
from cubi_tk.parsers import print_args


# API based drop-in replacement for what used to build on the `iRODSRetrieveCollection` class (to be deprecated)
class RetrieveSodarCollection(SodarApi):
    def __init__(self, argparse: Namespace, **kwargs):
        super().__init__(argparse, **kwargs)
        self.irods_hash_scheme = iRODSCommon(
            sodar_profile=argparse.config_profile,
            connection_timeout=getattr(argparse, "connection_timeout", 600),
            read_timeout=getattr(argparse, "read_timeout", 600),
            ask=True if not getattr(argparse, "yes", False) else False,
        ).irods_hash_scheme()
        self.hash_ending = "." + self.irods_hash_scheme.lower()

    def perform(self, include_hash_files=False) -> dict[str, list[IrodsDataObject]]:
        filelist = self.get_samplesheet_file_list()

        output_dict = defaultdict(list)

        for obj in filelist:
            if obj.type == "obj" and obj.name.endswith(self.hash_ending) and not include_hash_files:
                continue
            output_dict[obj.name].append(obj)

        return output_dict

    def get_assay_uuid(self):
        if self.assay_uuid:
            return self.assay_uuid

        assay, _ = self.get_assay_from_uuid()
        return assay.sodar_uuid

    def get_assay_irods_path(self):
        assay, _ = self.get_assay_from_uuid()
        return assay.irods_path


class SodarIngestBase:
    """
    Base class for iRODS transfers to Sodar.
    Includes methods for proper (study, assay &) landing zone selection/creation.
    Should always be used with argparse base parser `get_sodar_ingest_parser` or
      `get_sodar_parser(with_dest=True, with_assay_uuid=True, dest_string="destination")
    """

    command_name: str | None = None
    cubitk_section: str = "sodar"

    def __init__(self, args: Namespace):
        self.args = args
        # Check arguments & print to log
        self.check_args(self.args)
        self.sodar_api = SodarApi(args, with_dest=True, dest_string="destination")
        logger.info("Starting cubi-tk {} {}", self.cubitk_section, self.command_name)
        print_args(self.args)
        # Set / select (study, assay & ) landing zone
        self.lz_uuid, self.lz_irods_path = self._get_lz_info()
        # Init itransfer class, check that irods_environment.json exists
        self.itransfer = iRODSTransfer(
            None,
            ask=not self.sodar_api.yes,
            sodar_profile=self.args.config_profile,
            dry_run=self.args.dry_run,
            connection_timeout=getattr(self.args, "connection_timeout", 600),
            read_timeout=getattr(self.args, "read_timeout", 600),
        )
        if not self.itransfer.irods_env_path.exists():
            logger.error(
                f"Expected json config for irods ({self.itransfer.irods_env_path}) does not exist"
            )
            sys.exit(1)

    def _get_lz_info(self) -> tuple[str, str]:
        """Method evaluates user input to extract or create iRODS path. Use cases:

        1. User provide LZ path (set in SodarAPI as lz_path): fetch lz uuid
        2. User provides UUID (set in SodarAPI as project_uuid):
            i.UUID is LZ UUID: fetch path and use it.
            ii.UUID is Project UUID: get or create LZs
        3. Data provided by user is neither an iRODS path nor a valid UUID. Report error and throw exception.

        :return: lz_uuid, lz_irods_path
        """
        # lz path given, projectuuid set up, lz set up, check if valid and get lz_uuid
        if self.sodar_api.lz_path is not None:
            lz_path = self.sodar_api.lz_path
            existing_lzs = self.sodar_api.get_landingzone_list(
                sort_reverse=True, filter_for_state=["ACTIVE", "FAILED"]
            )
            if self.args.select_lz is not None:
                self._select_lz_warning()
            if existing_lzs is not None and len(existing_lzs) == 1:  # lz exists
                lz_uuid = existing_lzs[0].sodar_uuid
                assay_uuid = existing_lzs[0].assay
                if (
                    self.sodar_api.assay_uuid is not None
                    and assay_uuid != self.sodar_api.assay_uuid
                ):
                    logger.warning(
                        f"Different assay_uuid set than parsed from given lz, using the lz one: {assay_uuid} "
                    )
                self.sodar_api.assay_uuid = assay_uuid
            else:
                msg = "Unable to identify UUID of given LZ Path{0}.".format(self.sodar_api.lz_path)
                raise ParameterException(msg)
        # either projectuuid or lz uuid
        elif self.sodar_api.project_uuid is not None:
            lz = self.sodar_api.get_landingzone_retrieve(log_error=False)
            # if succees given uuid is lz, everything set up, sodarapi will set project uui and lz path
            if lz is not None:
                lz_uuid = lz.sodar_uuid
                lz_path = lz.irods_path
                if self.args.select_lz is not None:
                    self._select_lz_warning()
            # if None: projectuuid is possibly given
            # check if projectuuid is valid and start lz selection
            elif self.sodar_api.get_samplesheet_investigation_retrieve(log_error=False) is not None:
                try:
                    lz_uuid, lz_path = self._get_landing_zone(select_mode=self.args.select_lz)
                except UserCanceledException as e:
                    raise e
            # neither project nor lz uuid
            else:
                msg = "Provided UUID ({}) could neither be associated with a project nor with a Landing Zone.".format(
                    self.sodar_api.project_uuid
                )
                raise ParameterException(msg)
        # invalid input
        else:
            msg = "Data provided by user is not a valid UUID or LZ path. Please review input: {0}".format(
                self.args.destination
            )
            raise ParameterException(msg)
        # Log
        logger.info("Target iRODS path: {}", lz_path)
        return lz_uuid, lz_path

    def _create_lz(self, noninteractive_override: bool = False) -> tuple[str, str]:
        """
        Create a new landing zone (asking for user confirmation unless --yes is given) and check that is usable.
        :return: lz_uuid, lz_irods_path
        """
        if (
            noninteractive_override
            or self.sodar_api.yes
            or (input("Can the process create a new landing zone? [y/N] ").lower().startswith("y"))
        ):
            lz = self.sodar_api.post_landingzone_create(wait_until_ready=True)
            if lz:
                return lz.sodar_uuid, lz.irods_path
            else:
                raise CubiTkException("Something went wrong during Lz creation")
        else:
            msg = "Not possible to continue the process without a landing zone path. Breaking..."
            raise UserCanceledException(msg)

    def _select_existing_lz(self, existing_lzs, select_mode, sort_by) -> tuple[str, str]:
        if len(existing_lzs) == 1 and select_mode != "manual":
            lz = existing_lzs[0]
            logger.debug(f"Single active landingzone with UUID {lz.sodar_uuid} will be used")
            return lz.sodar_uuid, lz.irods_path

        if select_mode in ("newest", "last_used") or (select_mode is None and self.args.yes):
            lz = existing_lzs[-1]
            logger.info(
                f"Newest active landingzone (by {sort_by}) with UUID {lz.sodar_uuid} will be used"
            )
            return lz.sodar_uuid, lz.irods_path

        if select_mode == "oldest":
            lz = existing_lzs[0]
            logger.info(f"Oldest active landingzone with UUID {lz.sodar_uuid} will be used")
            return lz.sodar_uuid, lz.irods_path

        # select manually
        options = [
            f"{index + 1}) {os.path.basename(lz.irods_path)} ({lz.sodar_uuid})"
            for index, lz in enumerate(existing_lzs)
        ]
        input_message = (
            "####################\n"
            "Please choose target landing zone:\n"
            "0) <Create new landingzone>\n" + "\n".join(options) + "\nSelect by number: "
        )

        selection = -1
        while selection not in range(len(existing_lzs) + 1):
            user_input = input(input_message)
            if user_input.isdigit():
                selection = int(user_input)

        if selection == 0:
            logger.debug("User selected to create a new landing zone")
            return self._create_lz(noninteractive_override=True)

        lz = existing_lzs[selection - 1]
        return lz.sodar_uuid, lz.irods_path

    def _get_landing_zone(
        self,
        select_mode: None | Literal["manual", "last_used", "oldest", "newest", "create"] = None,
    ) -> tuple[str, str]:
        """
        Selection of landing zone to use for transfer. If --yes is given and select_mode not defined will use newest created zone
        or create a new one. Use select_mode for more control over LZ selection ('manual' not compatible with --yes).
        :param select_mode: str or None
        :return: lz_uuid, lz_irods_path
        """
        sort_by = "modification" if select_mode == "last_used" else "creation"
        existing_lzs = self.sodar_api.get_landingzone_list(
            filter_for_state=["ACTIVE", "FAILED"], sort_by=sort_by
        )
        if select_mode == "create" or not existing_lzs:
            logger.info(
                "No active landing zones found or selection mode is 'create', creating a new one..."
            )
            return self._create_lz(noninteractive_override=select_mode == "create")

        logger.info(
            "Found {} active landing zone{}.".format(
                len(existing_lzs), "s" if len(existing_lzs) > 1 else ""
            )
        )
        # Only ask about LZ creation if neither --select-lz nor --yes are given
        if (
            not self.args.yes
            and select_mode is None
            and (
                not input("Should the process use an existing landing zone? [y/N] ")
                .lower()
                .startswith("y")
            )
        ):
            logger.info("Creating a new landing zone...")
            return self._create_lz()

        return self._select_existing_lz(existing_lzs, select_mode, sort_by)

    @classmethod
    def run(cls, args, _parser: Namespace, _subparser: Namespace) -> int | None:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args) -> int | None:
        """Called for checking arguments, override to change behaviour."""
        # Check that all arguments provided by `get_sodar_ingest_parser / ingest_group` are present
        required_args = (
            "dry_run",
            "overwrite",
            "remote_checksums",
            "yes",
            "validate_and_move",
            "parallel_checksum_jobs",
            "recompute_checksums",
            "select_lz",
            "destination",
        )
        missing = []
        for arg in required_args:
            if not hasattr(args, arg):
                missing.append(arg)
        logger.warning(
            f"Missing the following required arguments for a (child) class of SodarIngestBase: {', '.join(missing)}\n."
            f"`setup_argparse` has not been used correctly!"
        )
        res = 1 if missing else 0
        if args.yes and args.select_lz == "manual":
            raise ValueError("The `--yes` and `--select-lz manual` options can not be combined!")
        return res

    def build_jobs(self, hash_ending) -> tuple[TransferJob]:
        """Build file transfer jobs."""
        raise NotImplementedError("Abstract method called!")

    def _no_files_found_warning(self, transfer_jobs):
        if not transfer_jobs:
            logger.error("No files for upload were found!")
            return 1
        else:
            return 0

    def execute(self) -> int | None:
        """Execute the transfer."""
        # Get iRODS hash scheme, build list of transfer
        irods_hash_scheme = self.itransfer.irods_hash_scheme()
        irods_hash_ending = "." + irods_hash_scheme.lower()
        transfer_jobs = self.build_jobs(irods_hash_ending)
        transfer_jobs = sorted(transfer_jobs, key=lambda x: x.path_local)
        # Exit early if no files were found/matched
        self._no_files_found_warning(transfer_jobs)
        # Check for md5 files and add jobs if needed
        transfer_jobs = execute_checksum_files_fix(
            transfer_jobs,
            irods_hash_scheme,
            self.args.parallel_checksum_jobs,
            self.args.recompute_checksums,
            dryrun=self.args.dry_run,
        )
        # Final go from user & transfer
        self.itransfer.jobs = transfer_jobs
        self.itransfer.put(recursive=True, overwrite=self.args.overwrite)

        # Compute server-side checksums
        if self.args.remote_checksums and not self.args.dry_run:  # pragma: no cover
            logger.info("Computing server-side checksums.")
            self.itransfer.chksum()

        # Validate and move transferred files
        # Behaviour: If flag is True and lz uuid is not None*,
        # it will ask SODAR to validate and move transferred files.
        # (*) It can be None if user provided path
        if self.lz_uuid and self.args.validate_and_move:
            logger.info(
                "Transferred files move to Landing Zone {} will be validated and moved in SODAR...",
                self.lz_uuid,
            )
            if not self.args.dry_run:
                uuid = self.sodar_api.post_landingzone_submit_move(self.lz_uuid)
                if uuid is None:
                    logger.error("Could not submit LZ for asynchronous moving")
                    return None
        else:
            logger.info("Transferred files will not be automatically moved in SODAR.")

        logger.info("All done")
        return None

    @staticmethod
    def _select_lz_warning():
        logger.warning(
            "The `--select-lz` option has no effect and will be ignored, unless used with a Sodar project UUID as destnation!"
        )


class FilePathParts(TypedDict):
    collection: str
    subcollections: str
    filename: str


class SodarPullBase:
    """
    Base class for iRODS transfers from Sodar.
    Should always be used with argparse base parser `get_sodar_pull_parser()
    """

    command_name: str | None = None
    cubitk_section: str = "sodar"

    def __init__(self, args: Namespace):
        self.args = args
        self.sodar_api_searcher = RetrieveSodarCollection(args, with_dest=False)
        # Check arguments & print to log
        self.check_args(self.args)
        logger.info("Starting cubi-tk {} {}", self.cubitk_section, self.command_name)
        print_args(self.args)

        # Init itransfer class, check that irods_environment.json exists
        self.itransfer = iRODSTransfer(
            None,
            ask=not self.sodar_api_searcher.yes,
            sodar_profile=self.args.config_profile,
            dry_run=self.args.dry_run,
            connection_timeout=getattr(self.args, "connection_timeout", 600),
            read_timeout=getattr(self.args, "read_timeout", 600),
        )
        if not self.itransfer.irods_env_path.exists():
            logger.error(
                f"Expected json config for irods ({self.itransfer.irods_env_path}) does not exist"
            )
            sys.exit(1)

    @classmethod
    def run(cls, args, _parser: Namespace, _subparser: Namespace) -> int | None:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args) -> int | None:
        """Called for checking arguments, override to change behaviour."""
        return 0

    def get_output_basepath(self) -> str:
        """Abstract method for output_path"""
        logger.debug(
            f"`cubi-tk {self.cubitk_section} {self.command_name}` does not implement it's own `get_output_basepath` function, using CWD by default."
        )
        return os.getcwd()

    def get_output_filepath(self, out_parts: FilePathParts) -> str:
        """Abstract method for output_path"""
        logger.debug(
            f"`cubi-tk {self.cubitk_section} {self.command_name}` does not implement it's own `get_output_filepath` function, using pattern from iRODs by default."
        )
        return "{collection}/{subcollections}/{filename}".format(**out_parts)

    def build_jobs(
        self, remote_files_dict: dict[str, list[IrodsDataObject]], assay_path: str
    ) -> list[TransferJob]:
        """Build list of download jobs for iRODS files."""
        # Initiate output
        output_list = []
        # Iterate over iRODS objects
        for collection, irods_objects in remote_files_dict.items():
            for irods_obj in irods_objects:
                relpath = PurePosixPath(irods_obj.path).relative_to(PurePosixPath(assay_path))
                coll, *subcolls, filename = relpath.parts
                assert coll == collection
                out_parts: FilePathParts = {
                    "collection": coll,
                    "subcollections": "/".join(subcolls),
                    "filename": filename,
                }
                job = TransferJob(
                    os.path.join(self.get_output_basepath(), self.get_output_filepath(out_parts)),
                    irods_obj.path,
                )
                output_list.append(job)

        return output_list

    def _no_files_found_warning(self, transfer_jobs) -> int:
        if not transfer_jobs:
            logger.error("No files for download were found!")
            return 1
        else:
            return 0

    def get_sample_list(self) -> set[str]:
        """Function to get samples to filter downloadable files by collection"""
        logger.debug(
            f"`cubi-tk {self.cubitk_section} {self.command_name}` does not implement it's own `get_sample_list` function, using all samples by default."
        )
        return set()

    def get_file_patterns(self) -> list[str]:
        """Function to get samples to filter downloadable files by collection"""
        logger.debug(
            f"`cubi-tk {self.cubitk_section} {self.command_name}` does not implement it's own `get_file_patterns` function, using all files by default."
        )
        return []

    def get_substring_match(self) -> bool:
        """Function to get samples to filter downloadable files by collection"""
        logger.debug(
            f"`cubi-tk {self.cubitk_section} {self.command_name}` does not implement it's own `get_substring_match` function, not using substring_match by default."
        )
        return False

    def execute(self) -> int | None:
        """Execute the transfer."""
        ret = 0
        # Get iRODS hash scheme, build list of transfer
        irods_hash_scheme = self.itransfer.irods_hash_scheme()
        irods_hash_ending = "." + irods_hash_scheme.lower()

        # Get & filter all remote files from iRODS
        # Note: subclasses should overwrite the get_... functions to modify filtering
        filtered_remote_files_dict = self.filter_irods_file_list(
            self.sodar_api_searcher.perform(),
            self.sodar_api_searcher.get_assay_irods_path(),
            self.get_file_patterns(),
            self.get_sample_list(),
            self.get_substring_match(),
        )

        transfer_jobs = self.build_jobs(filtered_remote_files_dict, irods_hash_ending)
        # Exit early if no files were found/matched
        ret = self._no_files_found_warning(transfer_jobs)
        # Optionally add checksum files to download
        if self.args.include_checksums:
            transfer_jobs += [
                TransferJob(
                    path_local=job.path_local + irods_hash_ending,
                    path_remote=job.path_remote + irods_hash_ending,
                )
                for job in transfer_jobs
            ]
        transfer_jobs = sorted(transfer_jobs, key=lambda x: x.path_local)

        # Final go from user & transfer
        self.itransfer.jobs = transfer_jobs
        self.itransfer.get(overwrite=self.args.overwrite)

        logger.info("All done")
        return ret

    @staticmethod
    def report_no_file_found(available_files):
        """Report no files found

        :param available_files: List of available files in SODAR.
        :type available_files: list
        """
        available_files = sorted(available_files)
        if len(available_files) > 50:
            limited_str = " (limited to first 50)"
            ellipsis_ = "..."
            remote_files_str = "\n".join(available_files[:50])
        else:
            limited_str = ""
            ellipsis_ = ""
            remote_files_str = "\n".join(available_files)
        logger.warning(
            f"No file was found using the selected criteria.\n"
            f"Available files{limited_str}:\n{remote_files_str}\n{ellipsis_}"
        )

    @staticmethod
    def parse_sample_tsv(tsv_path, sample_col=1, skip_rows=0, skip_comments=True) -> set[str]:
        extra_args = {"comment": "#"} if skip_comments else {}
        df = pd.read_csv(tsv_path, sep="\t", skiprows=skip_rows, **extra_args)
        try:
            samples = set(df.iloc[:, sample_col - 1])
        except IndexError:
            logger.error(
                f"Error extracting column no. {sample_col} from {tsv_path}, only {len(df.columns)} where detected."
            )
            raise

        return samples

    @staticmethod
    def filter_irods_file_list(
        remote_files_dict: dict[str, list[IrodsDataObject]],
        common_assay_path: str,
        file_patterns: list[str],
        samples: set[str],
        substring_match: bool = False,
    ) -> dict[str, list[IrodsDataObject]]:
        """Filter iRODS collection based on identifiers (sample id or library name) and file type/extension.

        :param remote_files_dict: Dictionary with iRODS collection information. Key: file name as string (e.g.,
        'P001-N1-DNA1-WES1.vcf.gz'); Value: iRODS data (``IrodsDataObject``).
        :type remote_files_dict: dict

        :param common_assay_path: Path common to all files. If provided, files in this path will be stripped.
        :type common_assay_path: str

        :param file_patterns: List of file patterns to use for file selection. Ignored if empty.
        :type file_patterns: list of strings

        :param samples: List of collection identifiers or substrings. Ignored if empty.
        :type samples: list

        :param substring_match: Fiter by extact collection matches or by substring matches.
        :type substring_match: bool

        :return: Returns dictionary: Key: sample (collection name [str]); Value: list of iRODS objects.
        """
        # Initialise variables
        filtered_dict = defaultdict(list)

        # Iterate
        for _filename, irodsobjs in remote_files_dict.items():
            for irodsobj in irodsobjs:
                # Path needs to be stripped down to collections (=remove assay part & upwards)
                try:
                    path = PurePosixPath(irodsobj.path).relative_to(
                        PurePosixPath(common_assay_path)
                    )
                except ValueError:  # wrong assay, skip
                    continue

                collection = path.parts[0]

                # Check if collection (=1st element of striped path) matches any of the samples
                if samples and not substring_match:
                    sample_match = any(s == collection for s in samples)
                elif samples:
                    sample_match = any(s in collection for s in samples)
                else:
                    sample_match = True

                if not sample_match:
                    continue

                if file_patterns:
                    file_pattern_match = any(p for p in file_patterns if path.match(p))
                else:
                    file_pattern_match = True

                if not file_pattern_match:
                    continue

                filtered_dict[collection].append(irodsobj)

        return filtered_dict
