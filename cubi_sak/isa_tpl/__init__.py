"""``cubi-sak isa-tpl``: create ISA-tab directories using `Cookiecutter`_.

You can use this command to quickly bootstrap an ISA-tab investigation.  The functionality is built on `Cookiecutter`_.

Available Templates
-------------------

The `Cookiecutter`_ directories are located in this module's directory.  Currently available templates are:

- ``isatab-germline``
- ``isatab-generic``

Adding Templates
----------------

Adding templates consists of the following steps:

1. Add a new template directory below ``cubi_sak/isa_tpl``.
2. Register it appending a ``IsaTabTemplate`` object to ``_TEMPLATES`` in ``cubi_sak.isa_tpl``.
3. Add it to the list above in the docstring.

The easiest way to start out is to copy an existing cookiecutter template and registration.

More Information
----------------

Also see ``cubi-sak isa-tpl`` CLI documentation and ``cubi-sak isa-tab --help`` for more information.

.. _Cookiecutter: https://cookiecutter.readthedocs.io/
"""

import argparse
import json
import os
import typing

import attr
from cookiecutter.main import cookiecutter
from logzero import logger
from toolz import curry

from ..common import run_nocmd, yield_files_recursively


@attr.s(frozen=True, auto_attribs=True)
class IsaTabTemplate:
    """Information regarding an ISA-tab template."""

    #: Name of the ISA-tab template.
    name: str

    #: Path to template directory.
    path: str

    #: Configuration loaded from ``cookiecutter.json``.
    configuration: typing.Dict[str, typing.Any]

    #: Optional description string.
    description: typing.Optional[str] = None


#: Base directory to this file.
_BASE_DIR = os.path.dirname(__file__)


def load_variables(template_name):
    """Load variables given the template name."""
    config_path = os.path.join(_BASE_DIR, template_name, "cookiecutter.json")
    with open(config_path, "rt") as inputf:
        return json.load(inputf)


#: Known ISA-tab templates (internal, mapping generated below).
_TEMPLATES = (
    IsaTabTemplate(
        name="germline",
        path=os.path.join(_BASE_DIR, "isatab-generic"),
        description="germline DNA sequencing ISA-tab template",
        configuration=load_variables("isatab-generic"),
    ),
    IsaTabTemplate(
        name="generic",
        path=os.path.join(_BASE_DIR, "isatab-generic"),
        description="generic DNA sequencing ISA-tab template",
        configuration=load_variables("isatab-generic"),
    ),
)

#: Known ISA-tab templates.
TEMPLATES = {tpl.name: tpl for tpl in _TEMPLATES}


@curry
def run_cookiecutter(tpl, args, _parser, _subparser, no_input=False):
    """Run cookiecutter, ``tpl`` will be bound with ``toolz.curry``."""
    extra_context = {}
    for name in tpl.configuration:
        if getattr(args, "var_%s", None) is not None:
            extra_context[name] = getattr(args, "var_%s" % name)

    output_dir = os.path.realpath(args.output_dir)
    output_base = os.path.dirname(args.output_dir)
    if os.path.exists(output_dir):  # pragma: no cover
        logger.error("Output directory %s already exists. Refusing to overwrite!", output_dir)
        return 1
    if not os.path.exists(output_base):  # pragma: no cover
        logger.error("Output path to output directory does not exist: %s", output_base)
        return 1
    extra_context["i_dir_name"] = os.path.basename(output_dir)

    logger.info("Start running cookiecutter")
    logger.info("  template path: %s", tpl.path)
    logger.info("  vars from CLI: %s", extra_context)
    cookiecutter(
        template=tpl.path, extra_context=extra_context, output_dir=output_base, no_input=no_input
    )
    listing = [args.output_dir] + [
        "- %s" % path for path in yield_files_recursively(args.output_dir)
    ]
    logger.info("Resulting structure is:\n%s", "\n".join(listing))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    subparsers = parser.add_subparsers(dest="tpl")

    # Create a sub parser for each template.
    for tpl in TEMPLATES.values():
        parser = subparsers.add_parser(
            tpl.name,
            help="Create ISA-tab directory using %s" % tpl.description,
            description=(
                "When specifying the --var-* argument, you can use JSON syntax.  Failing to parse JSON "
                "will keep the string value."
            ),
        )
        parser.add_argument(
            "--hidden-cmd",
            dest="isa_tpl_cmd",
            default=run_cookiecutter(tpl),
            help=argparse.SUPPRESS,
        )
        parser.add_argument("output_dir", help="Path to output directory")

        for name in tpl.configuration:
            key = name.replace("_", "-")
            parser.add_argument(
                "--var-%s" % key, help="template variables %s" % repr(name), default=None
            )


def run(args, parser, subparser):
    """Main entry point for isa-tpl command."""
    if not args.tpl:
        return run_nocmd(args, parser, subparser)
    else:
        return args.isa_tpl_cmd(args, parser, subparser)
