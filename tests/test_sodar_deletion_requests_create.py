"""Tests for ``cubi_tk.sodar.deletion_requests_create``."""

from unittest.mock import MagicMock, patch

from argparse import Namespace
import pytest

import cubi_tk.sodar.deletion_requests_create
from cubi_tk.__main__ import main, setup_argparse
from cubi_tk.sodar.deletion_requests_create import SodarDeletionRequestsCommand

def test_run_sodar_deletion_requests_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "deletion-requests", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err

def test_run_sodar_deletion_requests_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "deletion-requests"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err

@pytest.fixture
def del_req_args():
    return Namespace(
        verbose=False,
        config=None,
        sodar_api_token="****",
        sodar_server_url=None,
        base_path=None,
        project_uuid="1234",
        assay_uuid="992dc872-0033-4c3b-817b-74b324327e7d",
        description='Test deletion request',
        collections=[],
        irods_paths=['basecol1/file.txt', '/irods/project-assay/basecol1/subcol/file1.txt'],
    )

@pytest.fixture
def fake_irods_objs():
    #Sodar API ONLY returns file paths, not collection paths!
    return [
        #MagicMock(path='/irods/project-assay/basecol1'),
        MagicMock(path='/irods/project-assay/basecol1/file.txt'),
        #MagicMock(path='/irods/project-assay/basecol1/subcol'),
        MagicMock(path='/irods/project-assay/basecol1/subcol/file1.txt'),
        MagicMock(path='/irods/project-assay/basecol1/subcol/file2.txt'),
        #MagicMock(path='/irods/project-assay/basecol1/subcol2'),
        MagicMock(path='/irods/project-assay/basecol1/subcol2/file1.txt'),
        MagicMock(path='/irods/project-assay/basecol1/subcol2/file1.idx'),
        #MagicMock(path='/irods/project-assay/basecol2'),
        #MagicMock(path='/irods/project-assay/basecol2/subcol'),
        MagicMock(path='/irods/project-assay/basecol2/subcol/file1.txt'),
        MagicMock(path='/irods/project-assay/basecol2/subcol/file2.txt'),
        #MagicMock(path='/irods/project-assay/basecol2/subcol2'),
        MagicMock(path='/irods/project-assay/basecol2/subcol2/file1.txt'),
        MagicMock(path='/irods/project-assay/basecol2/subcol2/file1.idx'),
    ]

def test_sodar_deletion_requests_gather_deletion_request_paths(fake_irods_objs, del_req_args):
    """Test the gather_deletion_request_paths method of SodarDeletionRequestsCommand."""

    def get_actual(irods_path_args):
        del_req_args.irods_paths = irods_path_args
        SDR_instance = SodarDeletionRequestsCommand(del_req_args)
        return SDR_instance.gather_deletion_request_paths(fake_irods_objs, '/irods/project-assay')

    # Test single paths: relative, absolute, folder
    assert ['/irods/project-assay/basecol1/file.txt'] == get_actual(['basecol1/file.txt'])
    assert ['/irods/project-assay/basecol1/file.txt'] == get_actual(['/irods/project-assay/basecol1/file.txt'])
    assert ['/irods/project-assay/basecol1/subcol'] == get_actual(['basecol1/subcol'])

    # '*' expansion
    assert ['/irods/project-assay/basecol1/subcol/file1.txt', '/irods/project-assay/basecol1/subcol/file2.txt'] == get_actual(['basecol1/subcol/*'])
    assert [
               '/irods/project-assay/basecol1/subcol/file1.txt',
               '/irods/project-assay/basecol1/subcol/file2.txt',
               '/irods/project-assay/basecol1/subcol2/file1.txt'
           ] == get_actual(['basecol1/*/*.txt'])
    assert ['/irods/project-assay/basecol1/subcol2/file1.idx',
            '/irods/project-assay/basecol2/subcol2/file1.idx'] == get_actual(['*/*/*.idx'])
    # Only expand direct (folder) matches, not recursively the files also
    assert ['/irods/project-assay/basecol1/subcol', '/irods/project-assay/basecol2/subcol'] == get_actual(['*/subcol'])

    # Multiple paths
    assert ['/irods/project-assay/basecol1/file.txt', '/irods/project-assay/basecol1/subcol/file1.txt'] == get_actual(['basecol1/file.txt', 'basecol1/subcol/file1.txt'])
    # with '*' expansion
    assert [
               '/irods/project-assay/basecol1/file.txt',
               '/irods/project-assay/basecol1/subcol2/file1.txt',
               '/irods/project-assay/basecol2/subcol2/file1.txt'
           ] == get_actual(['*/subcol2/*.txt', '*/*.txt'])

    # Test collection whitelist
    del_req_args.collections = ['basecol1', 'basecol2']
    assert ['/irods/project-assay/basecol1/subcol', '/irods/project-assay/basecol2/subcol'] == get_actual(['*/subcol'])
    del_req_args.collections = ['basecol1']
    assert ['/irods/project-assay/basecol1/subcol'] == get_actual(['*/subcol'])


@patch('cubi_tk.sodar.deletion_requests_create.SodarApi')
def test_sodar_deletion_requests_smoke_test(mockapi, fake_irods_objs):
    mockapi_obj = MagicMock()
    mockapi_obj.get_assay_from_uuid = MagicMock(return_value=(MagicMock(irods_path="992dc872-0033-4c3b-817b-74b324327e7d"), 'study'))
    mockapi_obj.get_samplesheet_file_list = MagicMock(return_value=fake_irods_objs)
    mockapi_obj.post_samplesheet_request_create = MagicMock(return_value=0)
    mockapi.return_value = mockapi_obj

    argv = [
        "sodar",
        "deletion-requests",
        "--sodar-server-url",
        "sodar_server_url",
        "--sodar-api-token",
        "token",
        "-c", "basecol1", "dummycol2",
        '--',
        "project-uuid",
        "*/subcol",
    ]

    assert 0 == main(argv)

