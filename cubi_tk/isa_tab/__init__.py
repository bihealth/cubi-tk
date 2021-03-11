"""``cubi-tk isa-tab``: ISA-tab tooling.

Sub Commands
------------

``validate``
    Validate ISA-tab files for correctness and perform sanity checks.

``resolve-hpo``
    Resolve lists of HPO terms to TSV suitable for copy-and-paste into ISA-tab.

``add-ped``
    Given a germline DNA sequencing ISA-tab file and a PED file, add new lines to the ISA-tab
    file and update existing ones, e.g., for newly added parents.

``annotate``
    Add annotation to an ISA-tab file, given a tsv file.


Annotate
--------

``cubi-tk isa-tab annotate`` updates material and files nodes in ISA-tab studies and assays with annotations provided as
tab-separated text file.

In the annotation file header, target node types need to be indicated in ISA-tab style (i.e. "Source Name", etc.) while
annotations are just named normally. Annotations for materials are automatically recorded as Characteristics, while
annotations for files are recorded as Comments. Different node types can be annotated using only one annotation file, as
demonstrated in the example below.

By default, if Characteristics or Comments with the same name already exist for a node type, only empty values are
updated. Overwriting existing values requires confirmation (`--force-update`).

Annotations are only applied to only one study and assay, since material names are not necessarily unique between the
same material types of different studies or different assays (and thus, annotations couldn't be assigned unambiguously).
By default the first study and assay listed in the investigation file are considered for annotation. A specific study
and assay may be selected by file name (not path!) via `--target-study` or `--target-assay`, resp.

Example execution:
``cubi-tk isa-tab annotate investigation.tsv annotation.tsv --target-study s_study.tsv --target-assay a_assay.tsv``

.. list-table:: Annotation example tsv file
   :header-rows: 1

   * - Source Name
     - Age
     - Sex
     - Sample Name
     - Volume
   * - alpha
     - 18
     - FEMALE
     - alpha-N1
     - 1000
   * - beta
     - 27
     - MALE
     - beta-N1
     - 1000
   * - gamma
     - 69
     - FEMALE
     - gamma-N1
     - 800


More Information
----------------

Also see ``cubi-tk isa-tab`` CLI documentation and ``cubi-tk isa-tab --help`` for more
information.
"""

import argparse

from ..common import run_nocmd
from .add_ped import setup_argparse as setup_argparse_add_ped
from .resolve_hpo import setup_argparse as setup_argparse_resolve_hpo
from .validate import setup_argparse as setup_argparse_validate
from .annotate import setup_argparse as setup_argparse_annotate


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    subparsers = parser.add_subparsers(dest="isa_tab_cmd")

    setup_argparse_add_ped(
        subparsers.add_parser("add-ped", help="Add records from PED file to ISA-tab")
    )
    setup_argparse_resolve_hpo(
        subparsers.add_parser("resolve-hpo", help="Resolve HPO term lists to ISA-tab fragments")
    )
    setup_argparse_annotate(
        subparsers.add_parser("annotate", help="Add annotation from CSV file to ISA-tab")
    )
    setup_argparse_validate(subparsers.add_parser("validate", help="Validate ISA-tab"))


def run(args, parser, subparser):
    """Main entry point for isa-tpl command."""
    if not args.isa_tab_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.isa_tab_cmd(args, parser, subparser)
