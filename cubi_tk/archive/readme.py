"""``cubi-tk archive prepare``: Prepare a project for archival"""

import argparse
import errno
import os
import re
import shutil
import sys
import tempfile
import time
import typing

import attr
from cookiecutter.main import cookiecutter
from logzero import logger

from . import common
from ..common import execute_shell_commands
from ..isa_tpl import IsaTabTemplate

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

TEMPLATE = IsaTabTemplate(
    name="archive",
    path=_TEMPLATE_DIR,
    description="Prepare project for archival",
    configuration=common.load_variables(template_dir=_TEMPLATE_DIR),
)

DU = re.compile("^ *([0-9]+)[ \t]+[^ \t]+.*$")
DATE = re.compile("^(20[0-9][0-9]-[01][0-9]-[0-3][0-9])[_-].+$")

MAIL = (
    "(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*"
    '|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]'
    '|\\\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")'
    "@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?"
    "|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}"
    "(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:"
    "(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]"
    "|\\\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)"
    "\\])"
)

PATTERNS = {
    "project_name": re.compile("^ *- *Project name: *.+$"),
    "date": re.compile("^ *- *Start date: *[0-9]{4}-[0-9]{2}-[0-9]{2}.*$"),
    "status": re.compile("^ *- *Current status: *(Active|Inactive|Finished|Archived) *$"),
    "PI": re.compile("^ *- P.I.: \\[([A-z '-]+)\\]\\(mailto:(" + MAIL + ")\\) *$"),
    "client": re.compile("^ *- *Client contact: \\[([A-z '-]+)\\]\\(mailto:(" + MAIL + ")\\) *$"),
    "archiver": re.compile("^ *- *CUBI contact: \\[([A-z '-]+)\\]\\(mailto:(" + MAIL + ")\\) *$"),
    "CUBI": re.compile("^ *- *CUBI project leader: ([A-z '-]+) *$"),
}

COMMANDS = {
    "size": ["du", "--bytes", "--max-depth=0"],
    "inodes": ["du", "--inodes", "--max-depth=0"],
    "size_follow": ["du", "--dereference", "--bytes", "--max-depth=0"],
    "inodes_follow": ["du", "--dereference", "--inodes", "--max-depth=0"],
}


@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for prepare."""

    filename: str
    skip_collect: bool
    is_valid: bool
    no_input: bool


class ArchiveReadmeCommand(common.ArchiveCommandBase):
    """Implementation of archive readme command."""

    command_name = "readme"

    def __init__(self, config: Config):
        super().__init__(config)
        self.project_dir = None
        self.readme_file = None

        self.start = time.time()
        self.inode = 0

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        super().setup_argparse(parser)

        parser.add_argument(
            "--skip-collect",
            "-s",
            action="store_true",
            help="Skip the collection of file size & inodes",
        )
        parser.add_argument(
            "--is-valid", "-t", action="store_true", help="Test validity of existing README file"
        )
        # Enable pytest
        parser.add_argument("--no-input", action="store_true", help=argparse.SUPPRESS)
        add_readme_parameters(parser)

        parser.add_argument("filename", help="README.md path & filename")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0

        if not self.config.is_valid and os.path.exists(self.config.filename):
            logger.error("Readme file {} already exists".format(self.config.filename))
            res = 1
        if self.config.is_valid and not os.path.exists(self.config.filename):
            logger.error("Missing readme file {}, can't test validity".format(self.config.filename))
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the upload to sodar."""
        res = self.check_args(self.config)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk archive readme")
        logger.info("  args: %s", self.config)

        # Remove all symlinks to absolute paths
        self.project_dir = os.path.realpath(self.config.project)
        self.readme_file = os.path.realpath(self.config.filename)

        # Check existing README file validity if requested
        if self.config.is_valid:
            res = not is_readme_valid(self.readme_file, verbose=True)
            if res == 0:
                logger.info("README file is valid: {}".format(self.readme_file))
            return res

        logger.info("Preparing README.md")
        extra_context = self._create_extra_context(self.project_dir)

        self.create_readme(self.readme_file, extra_context=extra_context)

        if not is_readme_valid(self.readme_file, verbose=True):
            res = 1
        return res

    def create_readme(self, readme_file, extra_context=None):
        try:
            tmp = tempfile.mkdtemp()

            # Create the readme file in temp directory
            cookiecutter(
                template=TEMPLATE.path,
                extra_context=extra_context,
                output_dir=tmp,
                no_input=self.config.no_input,
            )

            # Copy it back to destination, including contents of former incomplete README.md
            os.makedirs(os.path.dirname(readme_file), mode=488, exist_ok=True)
            shutil.copyfile(
                os.path.join(tmp, extra_context["project_name"], "README.md"), readme_file
            )
        finally:
            try:
                shutil.rmtree(tmp)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise

    def _extra_context_from_config(self):
        extra_context = {}
        if self.config:
            for name in TEMPLATE.configuration:
                var_name = "var_%s" % name
                if getattr(self.config, var_name, None) is not None:
                    extra_context[name] = getattr(self.config, var_name)
                    continue
                if isinstance(self.config, dict) and var_name in self.config:
                    extra_context[name] = self.config[var_name]
        return extra_context

    def _create_extra_context(self, project_dir):
        extra_context = self._extra_context_from_config()

        if self.config.skip_collect:
            for context_name, _ in COMMANDS.items():
                extra_context[context_name] = "NA"
            extra_context["snakemake_nb"] = "NA"
        else:
            logger.info("Collecting size & inodes numbers")
            for context_name, cmd in COMMANDS.items():
                if context_name not in extra_context.keys():
                    cmd.append(project_dir)
                    extra_context[context_name] = DU.match(
                        execute_shell_commands([cmd], check=False, verbose=False)
                    ).group(1)

            if "snakemake_nb" not in extra_context.keys():
                extra_context["snakemake_nb"] = ArchiveReadmeCommand._get_snakemake_nb(project_dir)

        if "archiver_name" not in extra_context.keys():
            extra_context["archiver_name"] = ArchiveReadmeCommand._get_archiver_name()

        if "archiver_email" not in extra_context.keys():
            extra_context["archiver_email"] = (
                "{}@bih-charite.de".format(extra_context["archiver_name"]).lower().replace(" ", ".")
            )
        if "CUBI_name" not in extra_context.keys():
            extra_context["CUBI_name"] = extra_context["archiver_name"]

        if "PI_name" in extra_context.keys() and "PI_email" not in extra_context.keys():
            extra_context["PI_email"] = (
                "{}@charite.de".format(extra_context["PI_name"]).lower().replace(" ", ".")
            )
        if "client_name" in extra_context.keys() and "client_email" not in extra_context.keys():
            extra_context["client_email"] = (
                "{}@charite.de".format(extra_context["client_name"]).lower().replace(" ", ".")
            )

        if "SODAR_UUID" in extra_context.keys() and "SODAR_URL" not in extra_context.keys():
            if getattr(self.config, "sodar_server_url", None) is not None:
                extra_context["SODAR_URL"] = "{}/projects/{}".format(
                    self.config.sodar_server_url, extra_context["SODAR_UUID"]
                )
            elif "sodar_server_url" in self.config:
                extra_context["SODAR_URL"] = "{}/projects/{}".format(
                    self.config["sodar_server_url"], extra_context["SODAR_UUID"]
                )

        if "directory" not in extra_context.keys():
            extra_context["directory"] = project_dir
        if "project_name" not in extra_context.keys():
            extra_context["project_name"] = os.path.basename(project_dir)
        if "start_date" not in extra_context.keys() and DATE.match(extra_context["project_name"]):
            extra_context["start_date"] = DATE.match(extra_context["project_name"]).group(1)
        if "current_status" not in extra_context.keys():
            extra_context["current_status"] = "Finished"

        return extra_context

    @staticmethod
    def _get_snakemake_nb(project_dir):
        cmds = [
            [
                "find",
                project_dir,
                "-type",
                "d",
                "-name",
                ".snakemake",
                "-exec",
                "du",
                "--inodes",
                "--max-depth=0",
                "{}",
                ";",
            ],
            ["cut", "-f", "1"],
            ["paste", "-sd+"],
            ["bc"],
        ]
        return execute_shell_commands(cmds, check=False, verbose=False)

    @staticmethod
    def _get_archiver_name():
        cmds = [
            ["pinky", "-l", os.getenv("USER")],
            ["grep", "In real life:"],
            ["sed", "-e", "s/.*In real life: *//"],
        ]
        output = execute_shell_commands(cmds, check=False, verbose=False)
        return output.rstrip()


def add_readme_parameters(parser):
    for name in TEMPLATE.configuration:
        key = name.replace("_", "-")
        parser.add_argument(
            "--var-%s" % key, help="template variable %s" % repr(name), default=None
        )


def is_readme_valid(filename=None, verbose=False):
    if filename is None:
        f = sys.stdin
    else:
        if not os.path.exists(filename):
            if verbose:
                logger.error("No README file {}".format(filename))
            return False
        f = open(filename, "rt")
    matching = set()
    for line in f:
        line = line.rstrip()
        for name, pattern in PATTERNS.items():
            if pattern.match(line):
                matching.add(name)
    f.close()
    if verbose:
        for name, _ in PATTERNS.items():
            if name not in matching:
                logger.warning("Entry {} missing from README.md file".format(name))
    return set(PATTERNS.keys()).issubset(matching)


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive readme``."""
    return ArchiveReadmeCommand.setup_argparse(parser)
