"""Tests for ``cubi_tk.sodar.ingest``."""

from argparse import ArgumentParser
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, call, patch

from cubi_tk.parsers import get_sodar_parser
import pytest

from cubi_tk.__main__ import main, setup_argparse
from cubi_tk.sodar.ingest_collection import SodarIngestCollection


def test_run_sodar_ingest_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest-collection", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_sodar_ingest_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest-collection"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


@pytest.fixture
def ingest(fs):
    fs.create_dir(Path.home().joinpath(".irods"))
    fs.create_file(Path.home().joinpath(".irods", "irods_environment.json"))

    argv = [
        "--recursive",
        "--sodar-server-url",
        "sodar_server_url",
        "--sodar-api-token",
        "token",
        "testdir",
        "target",
    ]

    sodar_parser = get_sodar_parser(with_dest= True, dest_string="destination", dest_help_string="UUID from Landing Zone or Project - where files will be moved to.")
    parser = ArgumentParser(parents=[sodar_parser])
    SodarIngestCollection.setup_argparse(parser)
    args = parser.parse_args(argv)

    obj = SodarIngestCollection(args)
    obj.lz_irods_path = "/irodsZone"
    obj.target_coll = "targetCollection"
    return obj


def test_sodar_ingest_build_file_list(fs, caplog):
    class DummyArgs(object):
        pass

    fs.create_symlink("/not_existing", "/broken_link")
    fs.create_symlink("/loop_src", "/loop_src2")
    fs.create_symlink("/loop_src2", "/loop_src")

    args = DummyArgs()
    args.sources = ["broken_link", "not_here", "loop_src", "testdir", "testdir", "file5", "file6"]
    args.recursive = True
    args.exclude = ["file4", "file5"]
    dummy = MagicMock()
    args_mock = PropertyMock(return_value=args)
    type(dummy).args = args_mock

    fs.create_dir("/testdir/subdir")
    fs.create_file("/testdir/file1")
    fs.create_file("/testdir/file1.md5")
    fs.create_file("/testdir/subdir/file2")
    fs.create_file("/file3")
    fs.create_file("/testdir/file4")
    fs.create_file("/file5")
    fs.create_file("/file6")
    fs.create_symlink("/testdir/file3", "/file3")

    paths = SodarIngestCollection.build_file_list(dummy)

    # Sources
    assert "File not found: broken_link" in caplog.messages
    assert "File not found: not_here" in caplog.messages
    assert "Symlink loop: loop_src" in caplog.messages

    # Files
    assert {"spath": Path("/testdir/file1"), "ipath": Path("file1")} in paths
    assert {
        "spath": Path("/testdir/file1.md5"),
        "ipath": Path("file1.md5"),
    } not in paths
    assert {
        "spath": Path("/testdir/subdir/file2"),
        "ipath": Path("subdir/file2"),
    } in paths
    assert {"spath": Path("/testdir/file3"), "ipath": Path("file3")} in paths

    # Re-run without recursive search
    args.recursive = False
    paths = SodarIngestCollection.build_file_list(dummy)
    assert {"spath": Path("/testdir/file1"), "ipath": Path("file1")} in paths
    assert {
        "spath": Path("/testdir/file1.md5"),
        "ipath": Path("file1.md5"),
    } not in paths
    assert {
        "spath": Path("/testdir/subdir/file2"),
        "ipath": Path("subdir/file2"),
    } not in paths
    assert {"spath": Path("/testdir/file3"), "ipath": Path("file3")} in paths
    assert {"spath": Path("/testdir/file4"), "ipath": Path("file4")} not in paths
    assert {"spath": Path("file5"), "ipath": Path("file5")} not in paths
    assert {"spath": Path("file6"), "ipath": Path("file6")} in paths


@patch("cubi_tk.sodar.ingest_collection.TransferJob")
def test_sodar_ingest_build_jobs(mockjob, ingest, fs):
    paths = [
        {"spath": Path("myfile.csv"), "ipath": Path("dest_dir/myfile.csv")},
        {"spath": Path("folder/file.csv"), "ipath": Path("dest_dir/folder/file.csv")},
    ]
    for path in paths:
        fs.create_file(path["spath"])
    fs.create_file("myfile.csv.md5")

    ingest.build_jobs(paths)

    for p in paths:
        mockjob.assert_any_call(
            path_local=str(p["spath"]),
            path_remote=f"{ingest.target_coll}/{str(p['ipath'])}",
        )
        mockjob.assert_any_call(
            path_local=str(p["spath"]) + ".md5",
            path_remote=f"{ingest.target_coll}/{str(p['ipath']) + '.md5'}",
        )


@patch("cubi_tk.sodar.ingest_collection.TransferJob")
@patch("cubi_tk.sodar.ingest_collection.iRODSTransfer")
@patch("cubi_tk.sodar.ingest_collection.iRODSCommon.session")
@patch("cubi_tk.sodar_api.requests.get")
def test_sodar_ingest_smoketest(mockapi, mocksession, mocktransfer, mockjob, fs):
    class DummyColl(object):
        pass

    fs.create_dir("/source/subdir")
    fs.create_dir("/target/coll/")
    fs.create_file("/source/file1")
    fs.create_file("/source/subdir/file2")
    lz_uuid = "f46b4fc3-0927-449d-b725-9ffed231507b"
    argv = [
        "sodar",
        "ingest-collection",
        "--sodar-server-url",
        "sodar_server_url",
        "--sodar-api-token",
        "token",
        "--collection",
        "coll",
        "--yes",
        "--recursive",
        lz_uuid,
        "source",

    ]
    # to make it sortable
    mockjob.return_value.path_local = 1

    # Test env file missing
    with pytest.raises(SystemExit):
        main(argv)

    fs.create_dir(Path.home().joinpath(".irods"))
    fs.create_file(Path.home().joinpath(".irods", "irods_environment.json"))

    # Test args no api token
    with pytest.raises(SystemExit):
        argv2 = argv.copy()
        argv2.remove("--sodar-api-token")
        argv2.remove("token")
        main(argv2)

    # Test cancel no invalid LZ
    api_return = {
        "assay": "",
        "config_data": "",
        "configuration": "",
        "date_modified": "",
        "description": "",
        "irods_path": "target",
        "project": "",
        "sodar_uuid": "",
        "status": "DELETED",
        "status_locked" : "",
        "status_info": "",
        "title": "",
        "user": "",
    }
    mockapi.return_value.status_code = 200
    mockapi.return_value.json = MagicMock(return_value=api_return)

    with pytest.raises(SystemExit):
        main(argv)
        mockapi.assert_called_with(
            sodar_url="sodar_server_url", sodar_api_token="token", landingzone_uuid=lz_uuid
        )

    # Test cancel if no files to transfer
    api_return["status"] = "ACTIVE"
    with pytest.raises(SystemExit):
        argv2 = argv.copy()
        argv2[-1] = "empty"
        main(argv2)

    # Test user input for subcollection
    dcoll = DummyColl()
    dcoll.subcollections = []
    mocki = MagicMock()  # returned by the session context manager
    mocksession.__enter__.return_value = mocki
    mocki.collections.get.return_value = dcoll
    mocktransfer.return_value.size = 1234
    argv2 = argv.copy()
    argv2.remove("--collection")
    argv2.remove("coll")

    with patch("builtins.input", side_effect=["a", "100", "1"]) as mockinput:
        # Test for no subcollections
        main(argv2)
        mockinput.assert_not_called()

        # Test for 1 subcollection
        dcoll.subcollections = [
            DummyColl(),
        ]
        dcoll.subcollections[0].name = "coll"
        main(argv2)
        assert mockinput.call_count == 3
        mockjob.assert_called()

    # Test upload logic
    mockjob.reset_mock()
    main(argv)
    assert call.collections.get("target") in mocki.mock_calls
    mockjob.assert_any_call(path_local="/source/file1", path_remote="target/coll/file1")
    mockjob.assert_any_call(path_local="/source/file1.md5", path_remote="target/coll/file1.md5")
    mockjob.assert_any_call(
        path_local="/source/subdir/file2", path_remote="target/coll/subdir/file2"
    )
    mockjob.assert_any_call(
        path_local="/source/subdir/file2.md5", path_remote="target/coll/subdir/file2.md5"
    )
