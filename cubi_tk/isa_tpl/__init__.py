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

The `Cookiecutter`_ directories are located in this module's directory.  Currently available templates are:

- ``isatab-generic``
- ``isatab-germline``
- ``isatab-microarray``
- ``isatab-ms_meta_biocrates``
- ``isatab-single_cell_rnaseq``
- ``isatab-bulk_rnaseq``
- ``isatab-tumor_normal_dna``
- ``isatab-tumor_normal_triplets``
- ``isatab-stem_cell_core_bulk``
- ``isatab-stem_cell_core_sc``

Adding Templates
----------------

Adding templates consists of the following steps:

1. Add a new template directory below ``cubi_tk/isa_tpl``.
2. Register it appending a ``IsaTabTemplate`` object to ``_TEMPLATES`` in ``cubi_tk.isa_tpl``.
3. Add it to the list above in the docstring.

The easiest way to start out is to copy an existing cookiecutter template and registration.

More Information
----------------

Also see ``cubi-tk isa-tpl`` CLI documentation and ``cubi-tk isa-tab --help`` for more information.

.. _Cookiecutter: https://cookiecutter.readthedocs.io/
"""

import argparse
from functools import partial
import json
from pathlib import Path
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


def load_variables(template_name, extra=None):
    """Load variables given the template name."""
    extra = extra or {}
    config_path = os.path.join(_BASE_DIR, template_name, "cookiecutter.json")
    with open(config_path, "rt") as inputf:
        result = json.load(inputf)
    result.update(extra)
    return result


#: Known ISA-tab templates (internal, mapping generated below).
_TEMPLATES = (
    IsaTabTemplate(
        name="single_cell_rnaseq",
        path=os.path.join(_BASE_DIR, "isatab-single_cell_rnaseq"),
        description="single cell RNA sequencing ISA-tab template",
        configuration=load_variables("isatab-single_cell_rnaseq"),
    ),
    IsaTabTemplate(
        name="bulk_rnaseq",
        path=os.path.join(_BASE_DIR, "isatab-bulk_rnaseq"),
        description="bulk RNA sequencing ISA-tab template",
        configuration=load_variables("isatab-generic"),
    ),
    IsaTabTemplate(
        name="tumor_normal_dna",
        path=os.path.join(_BASE_DIR, "isatab-tumor_normal_dna"),
        description="Tumor-Normal DNA sequencing ISA-tab template",
        configuration=load_variables("isatab-tumor_normal_dna", {"is_triplet": False}),
    ),
    IsaTabTemplate(
        name="tumor_normal_triplets",
        path=os.path.join(_BASE_DIR, "isatab-tumor_normal_triplets"),
        description="Tumor-Normal DNA+RNA sequencing ISA-tab template",
        configuration=load_variables("isatab-tumor_normal_triplets", {"is_triplet": True}),
    ),
    IsaTabTemplate(
        name="germline",
        path=os.path.join(_BASE_DIR, "isatab-germline"),
        description="germline DNA sequencing ISA-tab template",
        configuration=load_variables("isatab-germline"),
    ),
    IsaTabTemplate(
        name="generic",
        path=os.path.join(_BASE_DIR, "isatab-generic"),
        description="generic RNA sequencing ISA-tab template",
        configuration=load_variables("isatab-generic"),
    ),
    IsaTabTemplate(
        name="microarray",
        path=os.path.join(_BASE_DIR, "isatab-microarray"),
        description="microarray ISA-tab template",
        configuration=load_variables("isatab-microarray"),
    ),
    IsaTabTemplate(
        name="ms_meta_biocrates",
        path=os.path.join(_BASE_DIR, "isatab-ms_meta_biocrates"),
        description="MS Metabolomics Biocrates kit ISA-tab template",
        configuration=load_variables("isatab-ms_meta_biocrates"),
    ),
    IsaTabTemplate(
        name="stem_cell_core_bulk",
        path=os.path.join(_BASE_DIR, "isatab-stem_cell_core_bulk"),
        description="Bulk RNA sequencing ISA-tab template from hiPSC for stem cell core projects",
        configuration=load_variables("isatab-stem_cell_core_bulk"),
    ),
    IsaTabTemplate(
        name="stem_cell_core_sc",
        path=os.path.join(_BASE_DIR, "isatab-stem_cell_core_sc"),
        description="Single cell RNA sequencing ISA-tab template from hiPSC for stem cell core projects",
        configuration=load_variables("isatab-stem_cell_core_sc"),
    ),
)

#: Known ISA-tab templates.
TEMPLATES = {tpl.name: tpl for tpl in _TEMPLATES}


@curry
def run_cookiecutter(tpl, args, _parser=None, _subparser=None, no_input=False):
    """Run cookiecutter, ``tpl`` will be bound with ``toolz.curry``."""
    extra_context = {}
    for name in tpl.configuration:  # pragma: nocover
        if getattr(args, "var_%s" % name, None) is not None:
            extra_context[name] = getattr(args, "var_%s" % name)

    logger.info(tpl.configuration)
    logger.info(args)

    output_dir = os.path.realpath(args.output_dir)
    output_base = os.path.dirname(args.output_dir)
    extra_context["i_dir_name"] = os.path.basename(output_dir)

    # FIXME: better solution? (added because args.var_is_triplet is None)
    if "is_triplet" in tpl.configuration:
        extra_context["is_triplet"] = tpl.configuration["is_triplet"]

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
            default=partial(run_cookiecutter, tpl),
            help=argparse.SUPPRESS,
        )
        parser.add_argument(
            "output_dir",
            type=lambda x: validate_output_directory(parser, x),
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
        return args.isa_tpl_cmd(args, parser, subparser)
