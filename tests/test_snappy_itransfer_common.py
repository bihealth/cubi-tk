import argparse
import datetime
from unittest.mock import patch

from cubi_tk.common import execute_checksum_files_fix
from cubi_tk.parsers import get_snappy_itransfer_parser, get_sodar_parser
from cubi_tk.snappy.itransfer_common import SnappyItransferCommandBase
from cubi_tk.irods_common import TransferJob


@patch("cubi_tk.snappy.itransfer_common.SnappyItransferCommandBase.build_base_dir_glob_pattern")
@patch("cubi_tk.snappy.itransfer_common.SnappyItransferCommandBase.get_lz_info")
def test_snappy_itransfer_common_build_jobs(mock_lz_info, mock_glob_pattern, fs):
    mock_lz_info.return_value = "466ab946-ce6a-4c78-9981-19b79e7bbe86", "/irods/dest"
    mock_glob_pattern.return_value = "basedir", "**/*.txt"

    # Setup some fake files & expected output
    expected = []
    today = datetime.date.today().strftime("%Y-%m-%d")
    fs.create_dir("basedir")
    for i in range(2):
        for f_end in ("", ".md5"):
            fs.create_file(f"/basedir/subfolder/file{i}.txt{f_end}")
            expected.append(
                TransferJob(
                    path_local=f"/basedir/subfolder/file{i}.txt{f_end}",
                    path_remote=f"/irods/dest/dummy_name/dummy_step/{today}/subfolder/file{i}.txt{f_end}",
                )
            )
    expected = tuple(sorted(expected, key=lambda x: x.path_local))

    parser = argparse.ArgumentParser(parents=[
        get_snappy_itransfer_parser(),
        get_sodar_parser(with_dest = True, dest_string="destination", dest_help_string="Landing zone path or UUID from Landing Zone or Project")])
    args = parser.parse_args(["--sodar-server-url","https://sodar.bihealth.org/", "--sodar-api-token", "XXXX", "466ab946-ce6a-4c78-9981-19b79e7bbe86"])
    SIC = SnappyItransferCommandBase(args)
    SIC.step_name = "dummy_step"

    assert ("466ab946-ce6a-4c78-9981-19b79e7bbe86", expected) == SIC.build_jobs(["dummy_name"], ".md5")


# Need to patch multiprocessing & subprocess functions
@patch("cubi_tk.common.Value")
@patch("cubi_tk.common.check_call")
def test_snappy_itransfer_common__execute_md5_files_fix(mock_check_call, mack_value, fs):
    mock_check_call.return_value = "dummy-md5-sum dummy/file/name"

    parser = argparse.ArgumentParser(parents=[
        get_snappy_itransfer_parser(),
        get_sodar_parser(with_dest = True, dest_string="destination", dest_help_string="Landing zone path or UUID from Landing Zone or Project")])
    args = parser.parse_args(["--sodar-server-url","https://sodar.bihealth.org/",  "--sodar-api-token", "XXXX",  "466ab946-ce6a-4c78-9981-19b79e7bbe86"])

    SIC = SnappyItransferCommandBase(args)
    SIC.step_name = "dummy_step"

    expected = []
    today = datetime.date.today().strftime("%Y-%m-%d")
    fs.create_dir("basedir")
    for i in range(2):
        for f_end in ("", ".md5"):
            if f_end != ".md5":
                fs.create_file(f"/basedir/subfolder/file{i}.txt{f_end}", contents="1234567890")
            expected.append(
                TransferJob(
                    path_local=f"/basedir/subfolder/file{i}.txt{f_end}",
                    path_remote=f"/irods/dest/dummy_name/dummy_step/{today}/subfolder/file{i}.txt{f_end}",
                )
            )
    expected = tuple(sorted(expected, key=lambda x: x.path_local))

    execute_checksum_files_fix(expected, "MD5", parallel_jobs=0)
    assert mock_check_call.call_count == 2
