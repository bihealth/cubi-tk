import getpass
import os.path
from pathlib import Path
from typing import Iterable

import attrs
from irods.exception import (
    CAT_INVALID_AUTHENTICATION,
    CAT_INVALID_USER,
    CAT_PASSWORD_EXPIRED,
    PAM_AUTH_PASSWORD_FAILED,
)
from irods.password_obfuscation import encode
from irods.session import NonAnonymousLoginWithoutPassword, iRODSSession
import logzero
from logzero import logger
from tqdm import tqdm

# no-frills logger
formatter = logzero.LogFormatter(fmt="%(message)s")
output_logger = logzero.setup_logger(formatter=formatter)


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

    def __init__(self, ask: bool = False, irods_env_path: Path = None):
        # Path to iRODS environment file
        if irods_env_path is None:
            self.irods_env_path = Path.home().joinpath(".irods", "irods_environment.json")
        else:
            self.irods_env_path = irods_env_path
        self.ask = ask

    @staticmethod
    def get_irods_error(e: Exception):
        """Return logger friendly iRODS exception."""
        es = str(e)
        return es if es and es != "None" else e.__class__.__name__

    def _init_irods(self) -> iRODSSession:
        """Connect to iRODS. Login if needed."""
        while True:
            try:
                session = iRODSSession(irods_env_file=self.irods_env_path)
                session.connection_timeout = 600
                session.server_version
                return session
            except NonAnonymousLoginWithoutPassword as e:  # pragma: no cover
                logger.info(self.get_irods_error(e))
                self._irods_login()
            except (
                CAT_INVALID_AUTHENTICATION,
                CAT_INVALID_USER,
                CAT_PASSWORD_EXPIRED,
            ):  # pragma: no cover
                logger.warning("Problem with your session token.")
                self.irods_env_path.parent.joinpath(".irodsA").unlink()
                self._irods_login()
            except Exception as e:  # pragma: no cover
                logger.error(f"iRODS connection failed: {self.get_irods_error(e)}")
                raise

    def _irods_login(self):
        """Ask user to log into iRODS."""
        # No valid .irodsA file. Query user for password.
        attempts = 0
        while attempts < 3:
            try:
                session = iRODSSession(
                    irods_env_file=self.irods_env_path,
                    password=getpass.getpass(prompt="Please enter SODAR password:"),
                )
                token = session.pam_pw_negotiated
                session.cleanup()
                break
            except PAM_AUTH_PASSWORD_FAILED as e:  # pragma: no cover
                if attempts < 2:
                    logger.warning("Wrong password. Please try again.")
                    attempts += 1
                    continue
                else:
                    logger.error("iRODS connection failed.")
                    raise e
            except Exception as e:  # pragma: no cover
                logger.error(f"iRODS connection failed: {self.get_irods_error(e)}")
                raise RuntimeError

        if self.ask and input(
            "Save iRODS session for passwordless operation? [y/N] "
        ).lower().startswith("y"):
            self._save_irods_token(token)  # pragma: no cover
        elif not self.ask:
            self._save_irods_token(token)

    def _save_irods_token(self, token: str):
        """Retrieve PAM temp auth token 'obfuscate' it and save to disk."""
        irods_auth_path = self.irods_env_path.parent.joinpath(".irodsA")
        irods_auth_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(token, list) and token:
            irods_auth_path.write_text(encode(token[0]))
            irods_auth_path.chmod(0o600)
        else:
            logger.warning("No token found to be saved.")

    @property
    def session(self):
        return self._init_irods()


class iRODSTransfer(iRODSCommon):
    """
    Transfer files to iRODS.

    :param jobs: Iterable of TransferJob objects
    :type jobs: Union[list,tuple,dict,set]
    """

    def __init__(self, jobs: Iterable[TransferJob], **kwargs):
        super().__init__(**kwargs)
        self.__jobs = jobs
        self.__total_bytes = sum([job.bytes for job in self.__jobs])
        self.__destinations = [job.path_remote for job in self.__jobs]

    @property
    def jobs(self):
        return self.__jobs

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

    def put(self, recursive: bool = False, sync: bool = False):
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
                try:
                    with self.session as session:
                        if recursive:
                            self._create_collections(job)
                        if sync and session.data_objects.exists(job.path_remote):
                            t.update(job.bytes)
                            continue
                        session.data_objects.put(job.path_local, job.path_remote)
                        t.update(job.bytes)
                except Exception as e:  # pragma: no cover
                    logger.error(f"Problem during transfer of {job.path_local}")
                    logger.error(self.get_irods_error(e))
            t.clear()

    def chksum(self):
        """Compute remote md5 checksums for all jobs."""
        common_prefix = os.path.commonpath(self.__destinations)
        checkjobs = tuple(job for job in self.__jobs if not job.path_remote.endswith(".md5"))
        logger.info(f"Triggering remote checksum computation for {len(checkjobs)} files.")
        for n, job in enumerate(checkjobs):
            output_logger.info(
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

    def get(self):
        """Download files from SODAR."""
        with self.session as session:
            self.__jobs = [
                attrs.evolve(job, bytes=session.data_objects.get(job.path_remote).size)
                for job in self.__jobs
            ]
        self.__total_bytes = sum([job.bytes for job in self.__jobs])
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
                try:
                    with self.session as session:
                        session.data_objects.get(job.path_remote, job.path_local)
                    t.update(job.bytes)
                except FileNotFoundError:  # pragma: no cover
                    raise
                except Exception as e:  # pragma: no cover
                    logger.error(f"Problem during transfer of {job.path_remote}")
                    logger.error(self.get_irods_error(e))
            t.clear()
