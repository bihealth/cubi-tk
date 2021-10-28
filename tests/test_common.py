"""Tests for common code."""

import subprocess

from pyfakefs import fake_filesystem

from cubi_tk import common


def test_is_uuid():
    assert not common.is_uuid(1)
    assert not common.is_uuid(None)
    assert not common.is_uuid("x")
    assert common.is_uuid("123e4567-e89b-12d3-a456-426655440000")


def test_compute_md5_checksum(mocker):
    file_path = "/fake_fs/hello_world.txt"
    fs = fake_filesystem.FakeFilesystem()
    fake_open = fake_filesystem.FakeFileOpen(fs)
    fs.create_file(file_path, contents="Hello World!\n", create_missing_dirs=True)
    mocker.patch("cubi_tk.common.open", fake_open)
    assert common.compute_md5_checksum(file_path) == "8ddd8be4b179a529afa5f2ffae4b9858"


def test_execute_shell_commands():
    echo = ["echo", "Hello World!"]
    tr = ["tr", "[A-Z]", "[a-z]"]
    false = ["false"]
    grep = ["grep", "world"]

    assert common.execute_shell_commands([echo]) == "Hello World!\n"
    assert common.execute_shell_commands([echo, tr]) == "hello world!\n"
    assert common.execute_shell_commands([echo, grep], check=False) == ""

    raise_error = False
    try:
        common.execute_shell_commands([false])
    except subprocess.CalledProcessError:
        raise_error = True
    assert raise_error

    raise_error = False
    try:
        common.execute_shell_commands([echo, grep])
    except subprocess.CalledProcessError:
        raise_error = True
    assert raise_error
