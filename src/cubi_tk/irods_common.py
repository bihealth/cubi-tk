from collections import defaultdict
import getpass
import json
import os.path
from pathlib import Path
import re
from typing import Iterable, Literal, Union
import warnings

import attrs
from irods.collection import iRODSCollection
from irods.column import Like
from irods.data_object import iRODSDataObject
from irods.keywords import FORCE_FLAG_KW
from irods.models import Collection as CollectionModel
from irods.models import DataObject as DataObjectModel
from irods.session import iRODSSession
from loguru import logger
from tqdm import tqdm

from irods.client_init import write_pam_irodsA_file
from cubi_tk.exceptions import UserCanceledException


#: Default hash scheme. Although iRODS provides alternatives, the whole of `snappy` pipeline uses MD5.
HASH_SCHEMES = {
    "MD5": {"regex": re.compile(r"[0-9a-fA-F]{32}")},
    "SHA256": {"regex": re.compile(r"[0-9a-fA-F]{64}")},
}
DEFAULT_HASH_SCHEME = "MD5"


@attrs.frozen(auto_attribs=True)
class TransferJob:
    """
    Encodes a transfer job between the local file system
    and a remote iRODS collection.
    """

    #: Source path.
    path_local: str

    #: Destination path.
    path_remote: str

    #: Number of bytes to transfer (optional).
    bytes: str = attrs.field()

    @bytes.default
    def _get_file_size(self):
        try:
            return Path(self.path_local).stat().st_size
        except FileNotFoundError:
            return -1


class iRODSCommon:
    """
    Implementation of common iRODS utility functions.

    :param ask: Confirm with user before certain actions.
    :type ask: bool, optional
    :param irods_env_path: Path to irods_environment.json
    :type irods_env_path: pathlib.Path, optional
    """

    def __init__(
        self, ask: bool = False, irods_env_path: Path = None, sodar_profile: str = "global"
    ):
        # Path to iRODS environment file
        if irods_env_path is None:
            irods_env_name = (
                "irods_environment.json"
                if sodar_profile == "global"
                else "irods_environment_" + sodar_profile + ".json"
            )
            self.irods_env_path = Path.home().joinpath(".irods", irods_env_name)
        else:
            self.irods_env_path = Path(irods_env_path)
        logger.debug(f"using irods_file: {self.irods_env_path}")
        self.irodsA_file_found = False
        self.ask = ask
        self.hash_scheme = DEFAULT_HASH_SCHEME

    @staticmethod
    def get_irods_error(e: Exception):
        """Return logger friendly iRODS exception."""
        es = str(e)
        return es if es and es != "None" else e.__class__.__name__

    def _init_irods(self) -> iRODSSession:
        """Connect to iRODS. Login if needed."""

        count_tries = 1
        while True:
            try:
                session = iRODSSession(irods_env_file=self.irods_env_path)
                session.connection_timeout = 600
                self._check_and_gen_irods_files()
                return session
            except Exception as e:  # pragma: no cover
                logger.error(f"iRODS connection failed: {self.get_irods_error(e)}")
                self._check_and_gen_irods_files(overwrite=True)
                count_tries += 1
                if count_tries > 3:
                    raise e

    def _check_and_gen_irods_files(self, overwrite=False):
        """check if irodsA exists and generate it"""
        if self.irodsA_file_found is True and not overwrite:
            return
        try:
            # check if irodsfile exists
            irodsA_path = self.irods_env_path.parent.joinpath(".irodsA")
            ##check path of last authorized irods_environment.json
            last_profile_path = self.irods_env_path.parent.joinpath("last_profile.json")
            if irodsA_path.exists():
                self.irodsA_file_found = True
                if last_profile_path.exists():
                    with open(last_profile_path) as last_profile_file:
                        last_used_env = json.load(last_profile_file)["last_used_env"]
                        overwrite = (
                            last_used_env != str(self.irods_env_path)
                        )  # overwrite irodsA file if last authenticated profile is different to current
                elif not self.irods_env_path.name == "irods_environment.json":
                    overwrite = True  # overwrite if other profile than global is used and no last_profile exists

            write_irods_file = not self.irodsA_file_found or overwrite
            if self.ask and write_irods_file:
                write_pam_irodsA_file(
                    getpass.getpass("Enter current PAM password -> "),
                    overwrite=overwrite,
                    irods_env_file=self.irods_env_path,
                )
                self.irodsA_file_found = True
                with open(last_profile_path, mode="w") as last_profile_file:
                    json.dump({"last_used_env": str(self.irods_env_path)}, last_profile_file)
            elif not self.ask and write_irods_file:
                logger.error(
                    "Password for irods conenction needs to be entered, please switch to interactive mode"
                )

            # read hashscheme vom irods env file
            with open(self.irods_env_path) as irods_env_data:
                irods_env_json = json.load(irods_env_data)
                self.hash_scheme = irods_env_json["irods_default_hash_scheme"]
                if self.hash_scheme not in HASH_SCHEMES:
                    logger.error("Hashscheme currently not supported")
                logger.debug(f"Hashscheme to use: {self.hash_scheme}")
        except FileNotFoundError as e:
            logger.error("Please check the irods_env_path")
            logger.error(e)

    def irods_hash_scheme(self):
        self._init_irods()
        return self.hash_scheme

    @property
    def session(self):
        return self._init_irods()


class iRODSTransfer(iRODSCommon):
    """
    Transfer files to iRODS.

    :param jobs: Iterable of TransferJob objects
    :type jobs: Union[list,tuple,dict,set]
    """

    def __init__(self, jobs: Iterable[TransferJob] | None, dry_run: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.dry_run = dry_run
        self.__jobs = jobs
        if jobs is not None:
            self.__total_bytes = sum([job.bytes for job in self.__jobs])
            self.__destinations = [job.path_remote for job in self.__jobs]
        else:
            self.__total_bytes = None
            self.__destinations = None

    @property
    def jobs(self):
        return self.__jobs

    @jobs.setter
    def jobs(self, jobs: Iterable[TransferJob]):
        self.__jobs = jobs
        self.__total_bytes = sum([job.bytes for job in self.__jobs])
        self.__destinations = [job.path_remote for job in self.__jobs]

    @property
    def size(self):
        return self.__total_bytes

    @property
    def destinations(self):
        return self.__destinations

    def _create_collections(self, job: TransferJob):
        collection = str(Path(job.path_remote).parent)
        with self.session as session:
            session.collections.create(collection)

    def put(
        self,
        recursive: bool = False,
        no_list: bool = False,
        overwrite: Literal["sync", "never", "always", "ask"] = "sync",
    ):  # noqa: C901
        # Log all actions before doing them
        if self.dry_run or not no_list:
            logger.info("The following actions would be performed:")
            for _, job in enumerate(self.__jobs):
                logger.info(f" - Upload file {job.path_local} to {job.path_remote}")
        if self.dry_run:
            return None
        if self.ask and not input("Is this OK? [y/N] ").lower().startswith("y"):  # pragma: no cover
            logger.info("Aborting at your request.")
            raise UserCanceledException

        if not self.ask and overwrite == "ask":
            logger.warning(
                "Both `overwrite: 'ask'` and `ask: False` given. Falling back to `overwrite: 'sync'`"
            )

        # Double tqdm for currently transferred file info
        with (
            tqdm(
                total=self.__total_bytes,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                position=1,
            ) as t,
            tqdm(total=0, position=0, bar_format="{desc}", leave=False) as file_log,
        ):
            kw_incl_overwrite = {FORCE_FLAG_KW: None}
            kw_excl_overwrite = {}
            for n, job in enumerate(self.__jobs):
                file_log.set_description_str(
                    f"File [{n + 1}/{len(self.__jobs)}]: {Path(job.path_local).name}"
                )
                try:
                    with self.session as session:
                        if recursive:
                            self._create_collections(job)

                        remote_exists = session.data_objects.exists(job.path_remote)
                        logger.debug(f"Remote file {job.path_remote} exists: {remote_exists}")
                        # never / file not present yet
                        if overwrite == "never" or not remote_exists:
                            kw_options = kw_excl_overwrite
                        elif overwrite == "always":
                            kw_options = kw_incl_overwrite
                        # ask: user decides for every file, with --yes default back to sync
                        elif self.ask and overwrite == "ask":
                            print("\n")
                            if (
                                input(
                                    "This file is already present, should it be overwritten? [y/N] "
                                )
                                .lower()
                                .startswith("y")
                            ):  # pragma: no cover
                                kw_options = kw_incl_overwrite
                                logger.info(f"Overwriting: {job.path_local}")
                            else:
                                kw_options = kw_excl_overwrite
                                logger.info(f"NOT overwriting: {job.path_local}")
                        # sync (or --yes and 'ask'): Check if file size is identical, if yes skip upload
                        else:
                            obj = session.data_objects.get(job.path_remote)
                            if obj.size != job.bytes:
                                kw_options = kw_incl_overwrite
                            else:
                                kw_options = kw_excl_overwrite

                        # kw_options will be {} if no overwrite should be done
                        if remote_exists and not kw_options:
                            t.update(job.bytes)
                            continue
                        session.data_objects.put(job.path_local, job.path_remote, **kw_options)
                        t.update(job.bytes)
                except Exception as e:  # pragma: no cover
                    logger.error(f"Problem during transfer of {job.path_local}")
                    logger.error(self.get_irods_error(e))
            t.clear()
            logger.info("File transfer complete.")

    def chksum(self):
        """Compute remote checksums for all jobs."""
        common_prefix = os.path.commonpath(self.__destinations)
        checkjobs = tuple(
            job
            for job in self.__jobs
            if not job.path_remote.endswith("." + self.hash_scheme.lower())
        )
        logger.info(f"Triggering remote checksum computation for {len(checkjobs)} files.")
        for n, job in enumerate(checkjobs):
            logger.info(
                f"[{n + 1}/{len(checkjobs)}]: {Path(job.path_remote).relative_to(common_prefix)}"
            )

            try:
                with self.session as session:
                    data_object = session.data_objects.get(job.path_remote)
                    if not data_object.checksum:
                        data_object.chksum()
            except Exception as e:  # pragma: no cover
                logger.error("Problem during iRODS checksumming.")
                logger.error(self.get_irods_error(e))

    def get(self, force_overwrite: bool = False):
        """Download files from SODAR."""
        with self.session as session:
            self.__jobs = [
                attrs.evolve(job, bytes=session.data_objects.get(job.path_remote).size)
                for job in self.__jobs
            ]
        self.__total_bytes = sum([job.bytes for job in self.__jobs])

        kw_options = {}
        if force_overwrite:
            kw_options = {FORCE_FLAG_KW: None}  # Keyword has no value, just needs to be present
        # Double tqdm for currently transferred file info
        with (
            tqdm(
                total=self.__total_bytes,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                position=1,
            ) as t,
            tqdm(total=0, position=0, bar_format="{desc}", leave=False) as file_log,
        ):
            for n, job in enumerate(self.__jobs):
                file_log.set_description_str(
                    f"File [{n + 1}/{len(self.__jobs)}]: {Path(job.path_local).name}"
                )
                if os.path.exists(job.path_local) and not force_overwrite:  # pragma: no cover
                    logger.info(
                        f"{Path(job.path_local).name} already exists. Skipping, use force_overwrite to re-download."
                    )
                    continue
                try:
                    Path(job.path_local).parent.mkdir(parents=True, exist_ok=True)
                    with self.session as session:
                        session.data_objects.get(job.path_remote, job.path_local, **kw_options)
                    t.update(job.bytes)
                except FileNotFoundError:  # pragma: no cover
                    raise
                except Exception as e:  # pragma: no cover
                    logger.error(f"Problem during transfer of {job.path_remote}")
                    logger.error(self.get_irods_error(e))
            t.clear()


class iRODSRetrieveCollection(iRODSCommon):
    """Class retrieves iRODS Collection associated with Assay"""

    def __init__(self, **kwargs):
        """Constructor.

        :param ask: Confirm with user before certain actions.
        :type ask: bool, optional

        :param irods_env_path: Path to irods_environment.json
        :type irods_env_path: pathlib.Path, optional
        """
        super().__init__(**kwargs)
        warnings.warn(
            "iRODSRetrieveCollection will be deprecated. Please use SodarAPI.get_samplesheet_file_list instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    def retrieve_irods_data_objects(self, irods_path: str) -> dict[str, list[iRODSDataObject]]:
        """Retrieve data objects from iRODS.

        :param irods_path: iRODS path.

        :return: Returns dictionary representation of iRODS collection information. Key: File name in iRODS (str);
        Value: list of iRODSDataObject (native python-irodsclient object).
        """

        # Connect to iRODS
        with self.session as session:
            try:
                root_coll = session.collections.get(irods_path)

                # Get files and run checks
                logger.info("Querying for data objects")

                if root_coll is not None:
                    irods_data_objs = self._irods_query(session, root_coll)
                    irods_obj_dict = self.parse_irods_collection(irods_data_objs)
                    return irods_obj_dict

            except Exception as e:  # pragma: no cover
                logger.error("Failed to retrieve iRODS path: {}", self.get_irods_error(e))
                raise

        return {}

    def _irods_query(
        self,
        session: iRODSSession,
        root_coll: iRODSCollection,
    ) -> dict[str, Union[dict[str, iRODSDataObject], list[iRODSDataObject]]]:
        """Get data objects recursively under the given iRODS path."""

        ignore_schemes = [k.lower() for k in HASH_SCHEMES if k != self.hash_scheme.upper()]

        query = session.query(DataObjectModel, CollectionModel).filter(
            Like(CollectionModel.name, f"{root_coll.path}%")
        )

        data_objs = {"files": [], "checksums": {}}
        for res in query:
            # If the 'res' dict is not split into Colllection&Object the resulting iRODSDataObject is not fully functional,
            # likely because a name/path/... attribute is overwritten somewhere
            magic_icat_id_separator = 500
            coll_res = {k: v for k, v in res.items() if k.icat_id >= magic_icat_id_separator}
            obj_res = {k: v for k, v in res.items() if k.icat_id < magic_icat_id_separator}
            coll = iRODSCollection(root_coll.manager, coll_res)
            obj = iRODSDataObject(session.data_objects, parent=coll, results=[obj_res])

            if obj.path.endswith("." + self.hash_scheme.lower()):
                data_objs["checksums"][obj.path] = obj
            elif obj.path.split(".")[-1] not in ignore_schemes:
                data_objs["files"].append(obj)

        return data_objs

    @staticmethod
    def parse_irods_collection(irods_data_objs) -> dict[str, list[iRODSDataObject]]:
        """Parse iRODS collection

        :param irods_data_objs: iRODS collection.
        :type irods_data_objs: dict

        :return: Returns dictionary representation of iRODS collection information. Key: File name in iRODS (str);
        Value: list of iRODSDataObject (native python-irodsclient object).
        """
        # Initialise variables
        output_dict = defaultdict(list)

        for obj in irods_data_objs["files"]:
            output_dict[obj.name].append(obj)

        return output_dict
