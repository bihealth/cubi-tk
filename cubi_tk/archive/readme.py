"""``cubi-tk archive prepare``: Prepare a project for archival"""

import errno
import os
import re
import shutil
import sys
import tempfile

from cookiecutter.main import cookiecutter
from logzero import logger

from ..common import execute_shell_commands
from ..isa_tpl import IsaTabTemplate
from ..isa_tpl import load_variables


_BASE_DIR = os.path.dirname(__file__)
TEMPLATE = IsaTabTemplate(
    name="archive",
    path=os.path.join(os.path.dirname(_BASE_DIR), "isa_tpl", "archive"),
    description="Prepare project for archival",
    configuration=load_variables("archive"),
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

MSG = "**Contents of original `README.md` file**"


def _extra_context_from_config(config=None):
    extra_context = {}
    if config:
        for name in TEMPLATE.configuration:
            var_name = "var_%s" % name
            if getattr(config, var_name, None) is not None:
                extra_context[name] = getattr(config, var_name)
                continue
            if var_name in config:
                extra_context[name] = config[var_name]
    return extra_context


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


def _get_archiver_name():
    cmds = [
        ["pinky", "-l", os.getenv("USER")],
        ["grep", "In real life:"],
        ["sed", "-e", "s/.*In real life: *//"],
    ]
    output = execute_shell_commands(cmds, check=False, verbose=False)
    return output.rstrip()


def _create_extra_context(project_dir, config=None):
    extra_context = _extra_context_from_config(config)

    logger.info("Collecting size & inodes numbers")
    for (context_name, cmd) in COMMANDS.items():
        if context_name not in extra_context.keys():
            cmd.append(project_dir)
            extra_context[context_name] = DU.match(
                execute_shell_commands([cmd], check=False, verbose=False)
            ).group(1)

    if "snakemake_nb" not in extra_context.keys():
        extra_context["snakemake_nb"] = _get_snakemake_nb(project_dir)

    if "archiver_name" not in extra_context.keys():
        extra_context["archiver_name"] = _get_archiver_name()

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
        if getattr(config, "sodar_server_url", None) is not None:
            extra_context["SODAR_URL"] = "{}/projects/{}".format(
                config.sodar_server_url, extra_context["SODAR_UUID"]
            )
        elif "sodar_server_url" in config:
            extra_context["SODAR_URL"] = "{}/projects/{}".format(
                config["sodar_server_url"], extra_context["SODAR_UUID"]
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


def _copy_readme(src, target):
    os.makedirs(os.path.realpath(os.path.dirname(target)), mode=488, exist_ok=True)
    with open(src, "rt") as f:
        lines = [x.rstrip() for x in f.readlines()]

    if os.path.exists(target):
        lines.extend(["", "", "-" * 80, "", "", MSG, "", "", "-" * 80, "", ""])
        with open(target, "rt") as f:
            lines.extend([x.rstrip() for x in f.readlines()])
        os.remove(target)

    with open(os.path.realpath(target), "wt") as f:
        f.write("\n".join(lines))


def is_readme_valid(filename=None):
    if filename is None:
        f = sys.stdin
    else:
        if not os.path.exists(filename):
            return False
        f = open(filename, "rt")
    matching = set()
    for line in f:
        line = line.rstrip()
        for (name, pattern) in PATTERNS.items():
            if pattern.match(line):
                matching.add(name)
    f.close()
    return set(PATTERNS.keys()).issubset(matching)


def create_readme(filename, project_dir, config=None, no_input=False):
    # If a valid README.md file already exists in the project, do nothing
    if os.path.exists(filename) and is_readme_valid(filename):
        logger.info("Using existing file, variables ignored : '{}'".format(filename))
        return

    # Fill defaults (emails, size, inodes, ...)
    extra_context = _create_extra_context(project_dir, config)

    try:
        tmp = tempfile.mkdtemp()

        # Create the readme file in temp directory
        cookiecutter(
            template=TEMPLATE.path, extra_context=extra_context, output_dir=tmp, no_input=no_input
        )

        # Copy it back to destination, including contents of former incomplete README.md
        _copy_readme(os.path.join(tmp, extra_context["project_name"], "README.md"), filename)
    finally:
        try:
            shutil.rmtree(tmp)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise


def add_readme_parameters(parser):
    for name in TEMPLATE.configuration:
        key = name.replace("_", "-")
        parser.add_argument(
            "--var-%s" % key, help="template variables %s" % repr(name), default=None
        )
