"""``cubi-tk archive prepare``: Prepare a project for archival"""

import argparse
import attr
import json
import os
import re
import subprocess
import sys
import tempfile
import typing

from cookiecutter.main import cookiecutter
from logzero import logger
from pathlib import Path

from . import common
from ..isa_tpl import IsaTabTemplate
from ..isa_tpl import load_variables


_BASE_DIR = os.path.dirname(__file__)
TEMPLATE = IsaTabTemplate(
    name="archive",
    path=os.path.join(os.path.dirname(_BASE_DIR), "isa_tpl", "archive"),
    description="Prepare project for archival",
    configuration=load_variables("archive")
)

DATE = re.compile("^(20[0-9]{2}-[0-9]{2}-[0-9]{2})[_-].+")

@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for find-file."""

    pass


class ArchiveFindFileCommand(common.ArchiveCommandBase):
    """Implementation of archive find-file command."""

    command_name = "find-file"

    def __init__(self, config: Config):
        super().__init__(config)

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        super().setup_argparse(parser)

        for name in TEMPLATE.configuration:
            parser.add_argument("--var-%s" % name.replace("_", "-"), help="template variables %s" % name, default=None)


    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0
        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the upload to sodar."""
        res = self.check_args(self.config)
        if res:  # pragma: nocover
            return res

        extra_context = {}
        for name in TEMPLATE.configuration:
            if getattr(self.config, "var_%s" % name, None) is not None:
                extra_context[name] = getattr(self.config, "var_%s" % name)

        logger.info("Starting cubi-tk archive prepare")
        logger.info("  args: %s", self.config)

        if "directory" not in extra_context.keys() or "directory" == "":
            extra_context["directory"] = os.path.basename(self.config.project)
        if "start_date" not in extra_context.keys() and DATE.match(extra_context["directory"]):
            extra_context["start_date"] = DATE.match(extra_context["directory"]).group(1)

        output_dir = tempfile.mkdtemp()

        extra_context["total_size"] = int(
            self._run_commands([["du", "--max-depth=0", self.config.project]]).split("\t")[0]
        )
        extra_context["inodes_nb"] = int(
            self._run_commands([["du", "--inodes", "--max-depth=0", self.config.project]]).split("\t")[0]
        )
        extra_context["snakemake_nb"] = int(
            self._run_commands([
                ["find", self.config.project, "-type", "d", "-name", ".snakemake", "-exec", "du", "--inodes", "--max-depth=0", "{}", ";"], 
                ["cut", "-f", "1"],
                ["paste", "-sd+"],
                ["bc"]
            ])
        )

        logger.info("Start running cookiecutter")
        logger.info("  template path: %s", TEMPLATE.path)
        logger.info("  vars from CLI: %s", extra_context)

        cookiecutter(
            template=TEMPLATE.path, extra_context=extra_context, output_dir=output_dir, no_input=False
        )

        extra_context["project_name"] = os.listdir(output_dir)[0]

        with open(os.path.join(output_dir, extra_context["project_name"], "README.md"), "rt") as f:
            for line in f:
                print(line.strip())

        return 0

    def _run_commands(self, commands):
        if (len(commands) == 1):
            command = commands[0]
            return subprocess.run(command, check=True, capture_output=True, encoding="utf-8").stdout
        previous = None
        for command in commands:
            if previous:
                current = subprocess.Popen(command, stdin=previous.stdout, stdout=subprocess.PIPE, encoding="utf-8")
                previous.stdout.close()
            else:
                current = subprocess.Popen(command, stdout=subprocess.PIPE, encoding="utf-8")
            previous = current
        return current.communicate()[0]

def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive find-file``."""
    return ArchiveFindFileCommand.setup_argparse(parser)
