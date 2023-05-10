import getpass
import os.path
import sys
from typing import Tuple

import attr
from irods.session import iRODSSession
from logzero import logger
from tqdm import tqdm

# TODO: move this class to common.py?
# from .snappy.itransfer_common import TransferJob


@attr.s(frozen=True, auto_attribs=True)
class TransferJob:
    """Encodes a transfer job from the local file system to the remote iRODS collection."""

    #: Source path.
    path_src: str

    #: Destination path.
    path_dest: str

    #: Number of bytes to transfer.
    bytes: int


def get_irods_error(e: Exception):
    """Return logger friendly iRODS exception."""
    es = str(e)
    return es if es and es != "None" else e.__class__.__name__


def init_irods(irods_env_path: os.PathLike) -> iRODSSession:
    """Connect to iRODS."""
    irods_auth_path = irods_env_path.parent.joinpath(".irodsA")
    if irods_auth_path.exists():
        try:
            session = iRODSSession(irods_env_file=irods_env_path)
            session.server_version  # check for outdated .irodsA file
        except Exception as e:
            logger.error(f"iRODS connection failed: {get_irods_error(e)}")
            logger.error("Are you logged in? try 'iinit'")
            sys.exit(1)
        finally:
            session.cleanup()
    else:
        # Query user for password.
        logger.info("iRODS authentication file not found.")
        password = getpass.getpass(prompt="Please enter SODAR password:")
        try:
            session = iRODSSession(irods_env_file=irods_env_path, password=password)
            session.server_version  # check for exceptions
        except Exception as e:
            logger.error(f"iRODS connection failed: {get_irods_error(e)}")
            sys.exit(1)
        finally:
            session.cleanup()

    return session


class iRODSTransfer:
    """Transfers files to and from iRODS."""

    def __init__(self, session: iRODSSession, jobs: Tuple[TransferJob, ...]):
        self.session = session
        self.jobs = jobs
        self.total_bytes = sum([job.bytes for job in self.jobs])
        self.destinations = [job.path_dest for job in self.jobs]

    def put(self):
        # TODO: add more parenthesis after python 3.10
        with tqdm(
            total=self.total_bytes, unit="B", unit_scale=True, unit_divisor=1024, position=1
        ) as t, tqdm(total=0, position=0, bar_format="{desc}", leave=False) as file_log:
            for job in self.jobs:
                try:
                    file_log.set_description_str(f"Current file: {job.path_src}")
                    self.session.data_objects.put(job.path_src, job.path_dest)
                    t.update(job.bytes)
                except Exception as e:
                    logger.error(f"Problem during transfer of {job.path_src}")
                    logger.error(get_irods_error(e))
                    sys.exit(1)
                finally:
                    self.session.cleanup()
            t.clear()

    def get(self):
        pass

    def chksum(self):
        common_prefix = os.path.commonprefix(self.destinations)
        for job in self.jobs:
            if not job.path_src.endswith(".md5"):
                print(os.path.relpath(job.path_dest, common_prefix))
                try:
                    data_object = self.session.data_objects.get(job.path_dest)
                    data_object.chksum()
                except Exception as e:
                    logger.error("iRODS checksum error.")
                    logger.error(get_irods_error(e))
                finally:
                    self.session.cleanup()
