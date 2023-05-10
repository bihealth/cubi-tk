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
        TransferJob(path_src="myfile.csv", path_dest="dest_dir/myfile.csv", bytes=123),
        TransferJob(
            path_src="folder/file.csv",
            path_dest="dest_dir/folder/file.csv",
            bytes=1024,
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


def test_irods_transfer_put(fs, itransfer):
    fs.create_file("myfile.csv")
    fs.create_dir("folder")
    fs.create_file("folder/file.csv")
    fs.create_dir("dest_dir/folder")

    with patch.object(itransfer.session.data_objects, "put", wraps=shutil.copy):
        itransfer.put()
    assert Path("dest_dir/myfile.csv").exists()
    assert Path("dest_dir/folder/file.csv").exists()


def test_irods_transfer_chksum(itransfer):
    with patch.object(itransfer.session.data_objects, "get") as mock:
        itransfer.chksum()

        assert mock.called
        assert mock.called_with(itransfer.destinations)
