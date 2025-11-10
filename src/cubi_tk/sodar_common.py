import os
import sys

from argparse import Namespace
from collections import defaultdict

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
    """Base class for iRODS transfers to Sodar.
    Includes methods for proper (study, assay &) landing zone selection/creation.
    Should always be used with argparse base parser `get_sodar_parser(with_dest=True, with_assay_uuid=True, dest_string="destination")
    """

    command_name: str | None = None
    cubitk_section: str = "sodar"

    def __init__(self, args: Namespace):
        self.args = args
        self.select_lz = getattr(args, "select_lz", True)
        self.sodar_api = SodarApi(args, with_dest=True, dest_string="destination")
        # Check arguments & print to log
        self.check_args(self.args)
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
            # if None: projectuuid is possibly given
            # check if projectuuid is valid and start lz selection
            elif self.sodar_api.get_samplesheet_investigation_retrieve(log_error=False) is not None:
                try:
                    lz_uuid, lz_path = self._get_landing_zone(latest=not self.select_lz)
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

    def _create_lz(self) -> tuple[str, str]:
        """
        Create a new landing zone (asking for user confirmation unless --yes is given) and check that is usable.
        :return: lz_uuid, lz_irods_path
        """
        if self.sodar_api.yes or (
            input("Can the process create a new landing zone? [yN] ").lower().startswith("y")
        ):
            lz = self.sodar_api.post_landingzone_create(wait_until_ready=True)
            if lz:
                return lz.sodar_uuid, lz.irods_path
            else:
                raise CubiTkException("Something went wrong during Lz creation")
        else:
            msg = "Not possible to continue the process without a landing zone path. Breaking..."
            raise UserCanceledException(msg)

    def _get_landing_zone(self, latest=True) -> tuple[str, str]:
        """
        Selection of landing zone to use for transfer. If --yes is given will use latest active landing zone
        or create a new one. With latest=False will ask user to select one of the available landing zones.
        :param latest: boolean
        :return: lz_uuid, lz_irods_path
        """
        # Get existing LZs from API
        existing_lzs = self.sodar_api.get_landingzone_list(
            sort_reverse=True, filter_for_state=["ACTIVE", "FAILED"]
        )
        if not existing_lzs:
            # No active landing zones available
            logger.info("No active Landing Zone available.")
            return self._create_lz()
        logger.info(
            "Found {} active landing zone{}.".format(
                len(existing_lzs), "s" if len(existing_lzs) > 1 else ""
            )
        )
        if (
            not self.sodar_api.yes
            and not latest
            and (
                not input("Should the process use an existing landing zone? [yN] ")
                .lower()
                .startswith("y")
            )
        ):
            return self._create_lz()
        if len(existing_lzs) == 1:
            lz = existing_lzs[0]
            logger.debug(f"Single active landingzone with UUID {lz.sodar_uuid} will be used")
        elif len(existing_lzs) > 1:
            if latest or self.sodar_api.yes:
                # Get latest active landing zone
                lz = existing_lzs[-1]
                logger.info(f"Latest active landingzone with UUID {lz.sodar_uuid} will be used")
            else:
                # Ask User which landing zone to use
                user_input = ""
                input_valid = False
                input_message = "####################\nPlease choose target landing zone:\n"
                for index, lz in enumerate(existing_lzs):
                    input_message += (
                        f"{index + 1}) {os.path.basename(lz.irods_path)} ({lz.sodar_uuid})\n"
                    )
                input_message += "Select by number: "
                while not input_valid:
                    user_input = input(input_message)
                    if user_input.isdigit():
                        user_input = int(user_input)
                        if 0 < user_input <= len(existing_lzs):
                            input_valid = True
                lz = existing_lzs[user_input - 1]
        return lz.sodar_uuid, lz.irods_path

    @classmethod
    def run(cls, args, _parser: Namespace, _subparser: Namespace) -> int | None:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args) -> int | None:
        """Called for checking arguments, override to change behaviour."""
        res = 0
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
        )
        # Final go from user & transfer
        self.itransfer.jobs = transfer_jobs
        self.itransfer.put(recursive=True, overwrite=self.args.overwrite)

        # Compute server-side checksums
        if self.args.remote_checksums:  # pragma: no cover
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
            uuid = self.sodar_api.post_landingzone_submit_move(self.lz_uuid)
            if uuid is None:
                logger.error("Could not submit LZ for asynchronous moving")
                return None
        else:
            logger.info("Transferred files will not be automatically moved in SODAR.")

        logger.info("All done")
        return None
