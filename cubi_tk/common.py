"""Common code."""
import contextlib
import difflib
import fcntl
import glob
import os
import pathlib
import shutil
import struct
import sys
import tempfile
import termios
import typing
import warnings
from subprocess import check_output, CalledProcessError
from uuid import UUID

import attr
import icdiff
import toml
from logzero import logger
from termcolor import colored

from .exceptions import IrodsIcommandsUnavailableException, IrodsIcommandsUnavailableWarning

#: Paths to search the global configuration in.
GLOBAL_CONFIG_PATHS = ("~/.cubitkrc.toml",)


def mask_password(value: str) -> str:
    return repr(value[:4] + (len(value) - 4) * "*")


@attr.s(frozen=True, auto_attribs=True)
class CommonConfig:
    """Common configuration for all commands."""

    #: Verbose mode activated
    verbose: bool

    #: API token to use for SODAR.
    sodar_api_token: str = attr.ib(repr=mask_password)  # type: ignore

    #: Base URL to SODAR server.
    sodar_server_url: typing.Optional[str]

    @staticmethod
    def create(args, toml_config=None):
        toml_config = toml_config or {}
        return CommonConfig(
            verbose=args.verbose,
            sodar_api_token=(
                args.sodar_api_token or toml_config.get("global", {})["sodar_api_token"]
            ),
            sodar_server_url=(
                args.sodar_server_url or toml_config.get("global", {})["sodar_server_url"]
            ),
        )


def find_base_path(base_path):
    base_path = pathlib.Path(base_path)
    while base_path != base_path.root:
        if (base_path / ".snappy_pipeline").exists():
            return str(base_path)
        base_path = base_path.parent
    return base_path


def run_nocmd(_, parser, subparser=None):  # pragma: no cover
    """No command given, print help and ``exit(1)``."""
    if subparser:
        subparser.print_help()
        subparser.exit(1)
    else:
        parser.print_help()
        parser.exit(1)


def yield_files_recursively(path, print_=False, file=sys.stderr):
    """Recursively yield below path to ``file`` in sorted order, print optionally"""
    while len(path) > 1 and path[-1] == "/":  # trim trailing slashes
        path = path[:-1]  # pragma: no cover
    paths = glob.glob(os.path.join(path, "**"))
    for p in sorted(paths):
        p = p[len(path) + 1 :]
        if print_:
            print(p, file=file)  # pragma: no cover
        yield p


def is_uuid(x):
    """Return True if ``x`` is a string and looks like a UUID."""
    try:
        return str(UUID(x)) == x
    except:  # noqa: E722
        return False


def check_irods_icommands(warn_only=True):  # disabled when testing  # pragma: nocover
    executables = ("iinit", "iput", "iget", "irsync")
    missing = []
    for prog in executables:
        try:
            check_output(["which", prog])
        except CalledProcessError:
            missing.append(prog)

    if missing:
        msg = "Could not find irods-icommands executables: %s", ", ".join(missing)
        if warn_only:
            warnings.warn(msg, IrodsIcommandsUnavailableWarning)
        else:
            raise IrodsIcommandsUnavailableException(msg)


def sizeof_fmt(num, suffix="B"):  # pragma: nocover
    """Source: https://stackoverflow.com/a/1094933/84349"""
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


def get_terminal_columns():
    """Return number of columns."""

    def ioctl_gwinz(fd):
        try:
            cr = struct.unpack("hh", fcntl.ioctl(fd, termios.TIOCGWINSZ, "1234"))
        except Exception:  # noqa
            return None
        return cr

    cr = ioctl_gwinz(0) or ioctl_gwinz(1) or ioctl_gwinz(2)
    if cr and cr[1] > 0:
        return cr[1]
    else:
        return 80


def overwrite_helper(
    out_path: typing.Union[str, pathlib.Path],
    contents: str,
    *,
    do_write: bool,
    show_diff: bool,
    show_diff_side_by_side: bool = False,
    answer_yes: bool = False,
    out_file: typing.IO = sys.stderr,
) -> None:
    out_path_obj = pathlib.Path(out_path)
    with tempfile.NamedTemporaryFile(mode="w+t") as sheet_file:
        lines = []

        # Write sheet to temporary file.
        sheet_file.write(contents)
        sheet_file.flush()
        sheet_file.seek(0)
        new_lines = sheet_file.read().splitlines(keepends=False)

        # Compare sheet with output if exists and --show-diff given.
        if show_diff:
            lines = _overwrite_helper_show_diff(
                lines, new_lines, out_file, out_path, out_path_obj, show_diff_side_by_side
            )

        # Actually copy the file contents.
        if (not show_diff or lines) and do_write:
            logger.info("About to write file contents to %s", out_path)
            sheet_file.seek(0)
            if out_path == "-":
                shutil.copyfileobj(sheet_file, sys.stdout)
            else:
                if show_diff:
                    logger.info("See above for the diff that will be applied.")
                if answer_yes or input("Is this OK? [yN] ").lower().startswith("y"):
                    with out_path_obj.open("wt") as output_file:
                        shutil.copyfileobj(sheet_file, output_file)


def _overwrite_helper_show_diff(
    lines, new_lines, out_file, out_path, out_path_obj, show_diff_side_by_side
):
    if out_path != "-" and out_path_obj.exists():
        with out_path_obj.open("rt") as inputf:
            old_lines = inputf.read().splitlines(keepends=False)
    else:
        old_lines = []
    if not show_diff_side_by_side:
        lines = list(
            difflib.unified_diff(old_lines, new_lines, fromfile=str(out_path), tofile=str(out_path))
        )
        for line in lines:
            if line.startswith(("+++", "---")):
                print(colored(line, color="white", attrs=("bold",)), end="", file=out_file)
            elif line.startswith("@@"):
                print(colored(line, color="cyan", attrs=("bold",)), end="", file=out_file)
            elif line.startswith("+"):
                print(colored(line, color="green", attrs=("bold",)), file=out_file)
            elif line.startswith("-"):
                print(colored(line, color="red", attrs=("bold",)), file=out_file)
            else:
                print(line, file=out_file)
    else:
        cd = icdiff.ConsoleDiff(cols=get_terminal_columns(), line_numbers=True)
        lines = list(
            cd.make_table(
                old_lines,
                new_lines,
                fromdesc=str(out_path),
                todesc=str(out_path),
                context=True,
                numlines=3,
            )
        )
        for line in lines:
            line = "%s\n" % line
            if hasattr(out_file, "buffer"):
                out_file.buffer.write(line.encode("utf-8"))  # type: ignore
            else:
                out_file.write(line)
    out_file.flush()
    if not lines:
        logger.info("File %s not changed, no diff...", out_path)
    return lines


@contextlib.contextmanager
def working_directory(path):
    """Changes working directory and returns to previous on exit."""
    prev_cwd = pathlib.Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


class UnionFind:
    """Union-Find (disjoint set) data structure allowing to address by vertex name"""

    def __init__(self, vertex_names):
        #: Node name to id mapping
        self._name_to_id = {v: i for i, v in enumerate(vertex_names)}
        #: Pointer to the containing sets
        self._id = list(range(len(vertex_names)))
        #: Size of the set (_sz[_id[v]] is the size of the set that contains v)
        self._sz = [1] * len(vertex_names)

    def find(self, v):
        assert isinstance(v, int)
        j = v

        while j != self._id[j]:
            self._id[j] = self._id[self._id[j]]
            j = self._id[j]

        return j

    def find_by_name(self, v_name):
        return self.find(self._name_to_id[v_name])

    def union_by_name(self, v_name, w_name):
        self.union(self.find_by_name(v_name), self.find_by_name(w_name))

    def union(self, v, w):
        assert isinstance(v, int)
        assert isinstance(w, int)
        i = self.find(v)
        j = self.find(w)

        if i == j:
            return

        if self._sz[i] < self._sz[j]:
            self._id[i] = j
            self._sz[j] += self._sz[i]

        else:
            self._id[j] = i

        self._sz[i] += self._sz[j]


def load_toml_config(config):
    # Load configuration from TOML cubitkrc file, if any.
    if config.config:
        config_paths = (config.config,)
    else:
        config_paths = GLOBAL_CONFIG_PATHS
    for config_path in config_paths:
        config_path = os.path.expanduser(os.path.expandvars(config_path))
        if os.path.exists(config_path):
            with open(config_path, "rt") as tomlf:
                return toml.load(tomlf)
    logger.info("Could not find any of the global configuration files %s.", config_paths)
    return None
