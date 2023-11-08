from contextlib import contextmanager
import getpass
import os.path
from pathlib import Path
import sys
from typing import Iterable

import attr
from irods.exception import CAT_INVALID_AUTHENTICATION, PAM_AUTH_PASSWORD_FAILED
from irods.password_obfuscation import encode
from irods.session import NonAnonymousLoginWithoutPassword, iRODSSession
import logzero
from logzero import logger
from tqdm import tqdm

# no-frills logger
formatter = logzero.LogFormatter(fmt="%(message)s")
output_logger = logzero.setup_logger(formatter=formatter)

NUM_PARALLEL_SESSIONS = 4


@attr.s(frozen=True, auto_attribs=True)
class TransferJob:
    """Encodes a transfer job from the local file system to the remote iRODS collection."""

    #: Source path.
    path_src: str

    #: Destination path.
    path_dest: str

    #: Number of bytes to transfer.
    bytes: int


class iRODSCommon:
    """
    Implementation of common iRODS utility functions.

    Attributes:
    ask -- Confirm with user before certain actions.
    irods_env_path -- Path to irods_environment.json
    """

    def __init__(self, ask: bool = False, irods_env_path: Path = None):
        # Path to iRODS environment file
        if irods_env_path is None:
            self.irods_env_path = Path.home().joinpath(".irods", "irods_environment.json")
        else:
            self.irods_env_path = irods_env_path
        self.ask = ask
        self._check_auth()

    @staticmethod
    def get_irods_error(e: Exception):
        """Return logger friendly iRODS exception."""
        es = str(e)
        return es if es and es != "None" else e.__class__.__name__

    def _init_irods(self) -> iRODSSession:
        """Connect to iRODS."""
        try:
            session = iRODSSession(irods_env_file=self.irods_env_path)
            session.connection_timeout = 300
            return session
        except Exception as e:  # pragma: no cover
            logger.error(f"iRODS connection failed: {self.get_irods_error(e)}")
            raise

    def _check_auth(self):
        """Check auth status and login if needed."""
        try:
            self._init_irods().server_version
            return 0
        except NonAnonymousLoginWithoutPassword as e:  # pragma: no cover
            logger.info(self.get_irods_error(e))
            pass
        except CAT_INVALID_AUTHENTICATION:  # pragma: no cover
            logger.warning("Problem with your session token.")
            self.irods_env_path.parent.joinpath(".irodsA").unlink()
            pass

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
            except PAM_AUTH_PASSWORD_FAILED:  # pragma: no cover
                if attempts < 2:
                    logger.warning("Wrong password. Please try again.")
                    attempts += 1
                    continue
                else:
                    logger.error("iRODS connection failed.")
                    sys.exit(1)
            except Exception as e:  # pragma: no cover
                logger.error(f"iRODS connection failed: {self.get_irods_error(e)}")
                sys.exit(1)

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

    @contextmanager
    def _get_irods_sessions(self, count=NUM_PARALLEL_SESSIONS):
        if count < 1:
            count = 1
        irods_sessions = [self._init_irods() for _ in range(count)]
        try:
            yield irods_sessions
        finally:
            for irods in irods_sessions:
                irods.cleanup()


class iRODSTransfer(iRODSCommon):
    """
    Transfer files to iRODS.

    Attributes:
    jobs -- iterable of TransferJob objects
    """

    def __init__(self, jobs: Iterable[TransferJob], **kwargs):
        super().__init__(**kwargs)
        with self._get_irods_sessions(1) as s:
            self.session = s[0]  # TODO: use more sessions
        self.__jobs = jobs
        self.__total_bytes = sum([job.bytes for job in self.__jobs])
        self.__destinations = [job.path_dest for job in self.__jobs]

    @property
    def jobs(self):
        return self.__jobs

    @property
    def size(self):
        return self.__total_bytes

    @property
    def destinations(self):
        return self.__destinations

    def put(self):
        # Double tqdm for currently transferred file info
        # TODO: add more parenthesis after python 3.10
        with tqdm(
            total=self.__total_bytes,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            position=1,
        ) as t, tqdm(total=0, position=0, bar_format="{desc}", leave=False) as file_log:
            for job in self.__jobs:
                file_log.set_description_str(f"Current file: {job.path_src}")
                try:
                    self.session.data_objects.put(job.path_src, job.path_dest)
                    t.update(job.bytes)
                except Exception as e:  # pragma: no cover
                    logger.error(f"Problem during transfer of {job.path_src}")
                    logger.error(self.get_irods_error(e))
                    sys.exit(1)
                finally:
                    self.session.cleanup()
            t.clear()

    def chksum(self):
        """Compute remote md5 checksums for all jobs."""
        common_prefix = os.path.commonpath(self.__destinations)
        for job in self.__jobs:
            if not job.path_src.endswith(".md5"):
                output_logger.info(Path(job.path_dest).relative_to(common_prefix))
                try:
                    data_object = self.session.data_objects.get(job.path_dest)
                    if not data_object.checksum:
                        data_object.chksum()
                except Exception as e:  # pragma: no cover
                    logger.error("Problem during iRODS checksumming.")
                    logger.error(self.get_irods_error(e))
                finally:
                    self.session.cleanup()
