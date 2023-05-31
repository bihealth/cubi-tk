import getpass
import os.path
from pathlib import Path
import sys
import tempfile
from typing import Tuple

import attr
from irods.exception import (
    CAT_INVALID_AUTHENTICATION,
    PAM_AUTH_PASSWORD_FAILED,
    DataObjectDoesNotExist,
)
from irods.password_obfuscation import encode
from irods.session import iRODSSession
import logzero
from logzero import logger
from tqdm import tqdm

# no-frills logger
formatter = logzero.LogFormatter(fmt="%(message)s")
output_logger = logzero.setup_logger(formatter=formatter)


@attr.s(frozen=True, auto_attribs=True)
class TransferJob:
    """Encodes a transfer job from the local file system to the remote iRODS collection."""

    #: Source path.
    path_src: str

    #: Destination path.
    path_dest: str

    #: Number of bytes to transfer.
    bytes: int

    #: MD5 hashsum of file.
    md5: str


def get_irods_error(e: Exception):
    """Return logger friendly iRODS exception."""
    es = str(e)
    return es if es and es != "None" else e.__class__.__name__


def init_irods(irods_env_path: Path, ask: bool = False) -> iRODSSession:
    """Connect to iRODS."""
    irods_auth_path = irods_env_path.parent.joinpath(".irodsA")
    if irods_auth_path.exists():
        try:
            session = iRODSSession(irods_env_file=irods_env_path)
            session.server_version  # check for outdated .irodsA file
            return session
        except Exception as e:  # pragma: no cover
            logger.error(f"iRODS connection failed: {get_irods_error(e)}")
            pass
        finally:
            session.cleanup()

    # No valid .irodsA file. Query user for password.
    logger.info("No valid iRODS authentication file found.")
    attempts = 0
    while attempts < 3:
        try:
            session = iRODSSession(
                irods_env_file=irods_env_path,
                password=getpass.getpass(prompt="Please enter SODAR password:"),
            )
            session.server_version  # check for exceptions
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
            logger.error(f"iRODS connection failed: {get_irods_error(e)}")
            sys.exit(1)
        finally:
            session.cleanup()

    if ask and input("Save iRODS session for passwordless operation? [y/N] ").lower().startswith(
        "y"
    ):
        save_irods_token(session)  # pragma: no cover
    elif not ask:
        save_irods_token(session)

    return session


def save_irods_token(session: iRODSSession, irods_env_path=None):
    """Retrieve PAM temp auth token 'obfuscate' it and save to disk."""
    if not irods_env_path:
        irods_auth_path = Path.home().joinpath(".irods", ".irodsA")
    else:
        irods_auth_path = Path(irods_env_path).parent.joinpath(".irodsA")

    irods_auth_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        token = session.pam_pw_negotiated
    except CAT_INVALID_AUTHENTICATION:  # pragma: no cover
        raise

    if isinstance(token, list) and token:
        irods_auth_path.write_text(encode(token[0]))
        irods_auth_path.chmod(0o600)


class iRODSTransfer:
    """
    Transfer files to iRODS.

    Attributes:
    session -- initialised iRODSSession
    jobs -- a tuple of TransferJob objects
    """

    def __init__(self, session: iRODSSession, jobs: Tuple[TransferJob, ...]):
        self.session = session
        self.jobs = jobs
        self.total_bytes = sum([job.bytes for job in self.jobs])
        self.destinations = [job.path_dest for job in self.jobs]

    def put(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # Double tqdm for currently transferred file info
            # TODO: add more parenthesis after python 3.10
            with tqdm(
                total=self.total_bytes,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                position=1,
            ) as t, tqdm(total=0, position=0, bar_format="{desc}", leave=False) as file_log:
                for job in self.jobs:
                    file_log.set_description_str(f"Current file: {job.path_src}")
                    job_name = Path(job.path_src).name

                    # check if remote file exists
                    try:
                        obj = self.session.data_objects.get(job.path_dest)
                        if obj.checksum == job.md5:
                            logger.debug(
                                f"File {job_name} already exists in iRODS with matching checksum. Skipping upload."
                            )
                            t.total -= job.bytes
                            t.refresh()
                            continue
                        elif not obj.checksum and obj.size == job.bytes:
                            logger.debug(
                                f"File {job_name} already exists in iRODS with matching file size. Skipping upload."
                            )
                            t.total -= job.bytes
                            t.refresh()
                            continue
                    except DataObjectDoesNotExist:  # pragma: no cover
                        pass
                    finally:
                        self.session.cleanup()

                    # create temporary md5 files
                    hashpath = Path(temp_dir).joinpath(job_name + ".md5")
                    with hashpath.open("w", encoding="utf-8") as tmp:
                        tmp.write(f"{job.md5}  {job_name}")

                    try:
                        self.session.data_objects.put(job.path_src, job.path_dest)
                        self.session.data_objects.put(
                            hashpath,
                            job.path_dest + ".md5",
                        )
                        t.update(job.bytes)
                    except Exception as e:  # pragma: no cover
                        logger.error(f"Problem during transfer of {job.path_src}")
                        logger.error(get_irods_error(e))
                        sys.exit(1)
                    finally:
                        self.session.cleanup()
                t.clear()

    def chksum(self):
        common_prefix = os.path.commonpath(self.destinations)
        for job in self.jobs:
            if not job.path_src.endswith(".md5"):
                output_logger.info(Path(job.path_dest).relative_to(common_prefix))
                try:
                    data_object = self.session.data_objects.get(job.path_dest)
                    data_object.chksum()
                except Exception as e:  # pragma: no cover
                    logger.error("Problem during iRODS checksumming.")
                    logger.error(get_irods_error(e))
                finally:
                    self.session.cleanup()
