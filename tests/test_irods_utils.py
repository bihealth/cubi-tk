from pathlib import Path
import shutil
from unittest.mock import patch

from irods.session import iRODSSession
import pytest

from cubi_tk.irods_utils import TransferJob, iRODSTransfer


@pytest.fixture
def fake_filesystem(fs):
    yield fs


@pytest.fixture
def jobs():
    return (
        TransferJob(
            path_src="myfile.csv",
            path_dest="dest_dir/myfile.csv",
            bytes=123,
            md5="ed3b3cbb18fd148bc925944ff0861ce6",
        ),
        TransferJob(
            path_src="folder/file.csv",
            path_dest="dest_dir/folder/file.csv",
            bytes=1024,
            md5="a6e9e3c859b803adb0f1d5f08a51d0f6",
        ),
    )


@pytest.fixture
def itransfer(jobs):
    session = iRODSSession(
        irods_host="localhost",
        irods_port=1247,
        irods_user_name="pytest",
        irods_zone_name="pytest",
    )

    return iRODSTransfer(session, jobs)


def test_irods_transfer_init(jobs, itransfer):
    assert itransfer.total_bytes == sum([job.bytes for job in jobs])
    assert itransfer.destinations == [job.path_dest for job in jobs]


def test_irods_transfer_put(fs, itransfer, jobs):
    for job in jobs:
        fs.create_file(job.path_src)
        fs.create_dir(Path(job.path_dest).parent)

    with patch.object(itransfer.session.data_objects, "put", wraps=shutil.copy):
        itransfer.put()

    for job in jobs:
        assert Path(job.path_dest).exists()
        assert Path(job.path_dest + ".md5").exists()
        with Path(job.path_dest + ".md5").open() as file:
            assert file.readline() == f"{job.md5}  {Path(job.path_dest).name}"


def test_irods_transfer_chksum(itransfer):
    with patch.object(itransfer.session.data_objects, "get") as mock:
        itransfer.chksum()

        assert mock.called
        assert mock.called_with(itransfer.destinations)
