"""``cubi-tk isa-tpl``: create ISA-tab directories using `Cookiecutter`_.

You can use this command to quickly bootstrap an ISA-tab investigation.  The functionality is built on `Cookiecutter`_.

To create a directory with ISA-tab files, run:

.. code-block:: bash

  $ cubi-tk isa-tpl <template name> <output directory>

This will prompt a number of questions interactively on the command line to collect information about the files that are going to be created.
The requested information will depend on the chosen ISA-tab template.
It is also possible to pass this information non-interactively together with other command line arguments (see ``cubi-tk isa-tpl <template name> --help``).

The completed information will then be used to create a directory with ISA-tab files.
It will be necessary to edit and extend the automatically generated files, e.g. to add additional rows to the assays.

Available Templates
-------------------

These have been moved to a separate repository: see `cubi-isa-templates`_.

Adding Templates
----------------

See `cubi-isa-templates`_.

More Information
----------------

Also see ``cubi-tk isa-tpl`` CLI documentation and ``cubi-tk isa-tab --help`` for more information.

.. _Cookiecutter: https://cookiecutter.readthedocs.io/
.. _cubi-isa-templates: https://github.com/bihealth/cubi-isa-templates
"""

import argparse
from functools import partial
from pathlib import Path
import shutil
import warnings

import altamisa
from cookiecutter.main import cookiecutter
from cubi_isa_templates import TEMPLATES
from loguru import logger
from toolz import curry

from cubi_tk.parsers import get_basic_parser

from ..common import run_nocmd, yield_files_recursively


@curry
def run_cookiecutter(tpl, args, _parser=None, _subparser=None, no_input=False):
    """Run cookiecutter, ``tpl`` will be bound with ``toolz.curry``."""
    extra_context = {}
    for name in tpl.configuration:  # pragma: nocover
        if getattr(args, "var_{}".format(name), None) is not None:
            extra_context[name] = getattr(args, "var_{}".format(name))

    if args.verbose:
        logger.info(tpl.configuration)
        logger.info(args)

    output_dir = Path(args.output_dir).resolve()
    output_base = output_dir.parent
    extra_context["__output_dir"] = Path(output_dir).name

    logger.info("Start running cookiecutter")
    logger.info("  template path: {}", tpl.path)
    logger.info("  vars from CLI: {}", extra_context)
    cookiecutter(
        template=tpl.path, extra_context=extra_context, output_dir=output_base, no_input=no_input
    )
    listing = [args.output_dir] + [
        "- %s" % path for path in yield_files_recursively(args.output_dir)
    ]
    logger.info("Resulting structure is:\n{}", "\n".join(listing))
    return 0


def validate_output_directory(parser, output_directory_path):
    """Validate output directory

    :param parser: Argument parser.
    :type parser: argparse.ArgumentParser

    :param output_directory_path: Path to output directory being checked.
    :type output_directory_path: str

    :return: Returns inputted path if valid path and directory doesn't exists already.
    """
    output_directory_parent_path = Path(output_directory_path).resolve().parent
    if Path(output_directory_path).resolve().exists():
        parser.error(
            f"Refusing to overwrite! Output directory already exists: {output_directory_path}"
        )
    if not Path(output_directory_parent_path).exists():
        parser.error(f"Path to output directory does not exist: {output_directory_parent_path}")
    return output_directory_path


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    basic_parser = get_basic_parser()
    subparsers = parser.add_subparsers(dest="tpl")

    # Create a sub parser for each template.
    for tpl in TEMPLATES.values():
        parser = subparsers.add_parser(
            tpl.name,
            parents=[basic_parser,],
            help="Create ISA-tab directory using %s" % tpl.description,
            description=(
                "When specifying the --var-* argument, you can use JSON syntax.  Failing to parse JSON "
                "will keep the string value."
            ),
        )
        parser.add_argument(
            "--hidden-cmd",
            dest="isa_tpl_cmd",
            default=partial(run_cookiecutter, tpl),
            help=argparse.SUPPRESS,
        )
        parser.add_argument(
            "output_dir",
            type=lambda x, parser = parser: validate_output_directory(parser, x),
            help="Path to output directory",
        )

        for name in tpl.configuration:
            key = name.replace("_", "-")
            parser.add_argument(
                "--var-%s" % key, help="template variables %s" % repr(name), default=None
            )


def run(args, parser, subparser):  # pragma: nocover
    """Main entry point for isa-tpl command."""
    if not args.tpl:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        status = args.isa_tpl_cmd(args, parser, subparser)

        # output validation
        if not status:
            logger.info("Running AltamISA validator:")
            i_files = Path(args.output_dir).rglob("i_*")
            warnings.filterwarnings(
                "error", category=altamisa.exceptions.CriticalIsaValidationWarning
            )
            for i in i_files:
                try:
                    with i.open() as i_file:
                        altamisa.apps.isatab_validate.main(
                            input_investigation_file=i_file,
                            show_duplicate_warnings=False,
                        )
                except (
                    altamisa.exceptions.ParseIsatabException,
                    altamisa.exceptions.CriticalIsaValidationWarning,
                ):
                    shutil.rmtree(args.output_dir)
                    raise
            return 0
        else:
            return status
