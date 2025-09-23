from pathlib import Path
from unittest.mock import ANY, MagicMock, call, patch

import irods.exception
import pytest

from cubi_tk.irods_common import (
    TransferJob,
    iRODSCommon,
    iRODSRetrieveCollection,
    iRODSTransfer,
)


def test_transfer_job_bytes(fs):
    fs.create_file("test_file", st_size=123)
    assert TransferJob("test_file", "remote/path").bytes == 123
    assert TransferJob("no_file.no", "remote/path").bytes == -1


@patch("cubi_tk.irods_common.iRODSSession")
def test_common_init(mocksession):
    assert iRODSCommon().irods_env_path is not None
    icommon = iRODSCommon(irods_env_path="a/b/c.json")
    assert icommon.irods_env_path == Path("a/b/c.json")
    assert type(iRODSCommon().ask) is bool
    assert iRODSCommon().session is mocksession.return_value


@patch("cubi_tk.irods_common.iRODSSession")
def test_get_irods_error(mocksession):
    e = irods.exception.NetworkException()
    assert iRODSCommon().get_irods_error(e) == "NetworkException"
    e = irods.exception.NetworkException("Connection reset")
    assert iRODSCommon().get_irods_error(e) == "Connection reset"


@patch("cubi_tk.irods_common.iRODSSession")
def test_init_irods(mocksession, fs):
    fs.create_file(".irods/irods_environment.json")
    fs.create_file(".irods/.irodsA")

    iRODSCommon()._init_irods()
    mocksession.assert_called()


@patch("getpass.getpass")
@patch("cubi_tk.irods_common.iRODSSession")
def check_and_gen_irodsA(mocksession, mockpass, fs):
    fs.create_file(".irods/irods_environment.json")
    password = "1234"
    icommon = iRODSCommon()
    mockpass.return_value = password

    icommon._check_and_gen_irods_files()
    mockpass.assert_called()
    mocksession.assert_any_call(irods_env_file=ANY, password=password)
    assert icommon.irods_env_path.parent.joinpath(".irodsA").exists()


# Test iRODSTransfer #########
@pytest.fixture
def jobs():
    return (
        TransferJob(path_local="myfile.csv", path_remote="dest_dir/myfile.csv", bytes=123),
        TransferJob(
            path_local="folder/file.csv", path_remote="dest_dir/folder/file.csv", bytes=1024
        ),
    )


def test_irods_transfer_init(jobs):
    with patch("cubi_tk.irods_common.iRODSSession"):
        itransfer = iRODSTransfer(jobs=jobs, irods_env_path="a/b/c", ask=True)
        assert itransfer.irods_env_path == Path("a/b/c")
        assert itransfer.ask is True
        assert itransfer.jobs == jobs
        assert itransfer.size == sum([job.bytes for job in jobs])
        assert itransfer.destinations == [job.path_remote for job in jobs]


@patch("cubi_tk.irods_common.iRODSTransfer._init_irods")
@patch("cubi_tk.irods_common.iRODSTransfer._create_collections")
def test_irods_transfer_put(mock_createcolls, mocksession, jobs):
    mockput = MagicMock()
    mockexists = MagicMock(return_value=False)
    mockobj = MagicMock()
    mockobj.put = mockput
    mockobj.exists = mockexists
    mockobj.get.return_value = MagicMock(size=123)

    # fit for context management
    mocksession.return_value.__enter__.return_value.data_objects = mockobj
    itransfer = iRODSTransfer(jobs)

    # expected calls
    calls_no_ov = [call(j.path_local, j.path_remote) for j in jobs]
    calls_w_ov = [call(j.path_local, j.path_remote, forceFlag=None) for j in jobs]
    calls_sync = [calls_w_ov[1]]

    # put, no options, no remote files
    itransfer.put()
    mockput.assert_has_calls(calls_no_ov)

    # recursive
    itransfer.put(recursive=True)
    calls = [call(j) for j in jobs]
    mock_createcolls.assert_has_calls(calls)

    # overwrite behaviour with existing files
    mockexists.return_value = True
    # overwrite: sync (w/ exiting files)
    mockput.reset_mock()
    itransfer.put(overwrite="sync")
    mockput.assert_has_calls(calls_sync)
    # overwrite: always
    mockput.reset_mock()
    itransfer.put(overwrite="always")
    mockput.assert_has_calls(calls_w_ov)
    # overwrite: never
    mockput.reset_mock()
    itransfer.put(overwrite="never")
    mockput.assert_not_called()
    # overwrite: ask
    mockput.reset_mock()
    itransfer.ask = True
    with patch("builtins.input", side_effect=["y", "y", "n"]):
        itransfer.put(overwrite="ask")
        mockput.assert_has_calls([calls_w_ov[0]])


@patch("cubi_tk.irods_common.iRODSTransfer._init_irods")
def test_create_collections(mocksession, jobs):
    mockcreate = MagicMock()
    mockcoll = MagicMock()
    mockcoll.create = mockcreate
    mocksession.return_value.__enter__.return_value.collections = mockcoll
    itransfer = iRODSTransfer(jobs)

    itransfer._create_collections(itransfer.jobs[1])
    coll_path = str(Path(itransfer.jobs[1].path_remote).parent)
    mockcreate.assert_called_with(coll_path)


@patch("cubi_tk.irods_common.iRODSTransfer._init_irods")
def test_irods_transfer_chksum(mocksession, jobs):
    mockget = MagicMock()
    mockobj = MagicMock()
    mockobj.get = mockget
    mocksession.return_value.__enter__.return_value.data_objects = mockobj

    mock_data_object = MagicMock()
    mock_data_object.checksum = None
    mock_data_object.chksum = MagicMock()
    mockget.return_value = mock_data_object

    itransfer = iRODSTransfer(jobs)
    itransfer.chksum()

    assert mock_data_object.chksum.call_count == len(itransfer.destinations)
    for path in itransfer.destinations:
        mockget.assert_any_call(path)


@patch("cubi_tk.irods_common.iRODSTransfer._init_irods")
def test_irods_transfer_get(mocksession, jobs):
    mockget = MagicMock()
    mockobj = MagicMock()
    mockobj.get = mockget
    mocksession.return_value.__enter__.return_value.data_objects = mockobj
    itransfer = iRODSTransfer(jobs)

    mockget.return_value.size = 111
    itransfer.get()

    for job in jobs:
        # size check
        mockget.assert_any_call(job.path_remote)
        # download
        mockget.assert_any_call(job.path_remote, job.path_local)
    assert itransfer.size == 222


# Test iRODSRetrieveCollection #########


# This tests `retrieve_irods_data_objects` and by extension `parse_irods_collection`
# A test for _irods_query would require mocking `session.query` results in a
# way that allows creation of iRODSDataObject instances from those results
@patch("cubi_tk.irods_common.iRODSCommon._init_irods")
@patch("cubi_tk.irods_common.iRODSRetrieveCollection._irods_query")
def test_irods_retrieve_data_objects(mockquery, mocksession):
    # Possible alternative to MagicMocks here:
    # create a fake iRODSDataObject class with a path attribute
    mockobj1 = MagicMock()
    mockobj1.path = "/root/coll1/file1.vcf.gz"
    mockobj1.name = "file1.vcf.gz"
    mockobj2 = MagicMock()
    mockobj2.path = "/root/coll2/file2.vcf.gz"
    mockobj2.name = "file2.vcf.gz"
    mockobj3 = MagicMock()
    mockobj3.path = "/root/coll1/subcol/file1.vcf.gz"
    mockobj3.name = "file1.vcf.gz"

    mockcksum = MagicMock()

    mockquery.return_value = {
        "files": [mockobj1, mockobj2, mockobj3],
        "checksums": {
            "/root/coll1/file1.vcf.gz": mockcksum,
            "/root/coll2/file2.vcf.gz": mockcksum,
            "/root/coll1/subcol/file1.vcf.gz": mockcksum,
        },
    }

    mocksession.collections.get.return_value = "path"

    data_objs = iRODSRetrieveCollection().retrieve_irods_data_objects("/fake/path")

    expected_data_objs = {"file1.vcf.gz": [mockobj1, mockobj3], "file2.vcf.gz": [mockobj2]}

    assert data_objs == expected_data_objs
