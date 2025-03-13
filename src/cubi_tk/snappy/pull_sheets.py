"""``cubi-tk snappy pull-sheet``: pull BiomedSheet files from SODAR.

More Information
----------------

- Also see ``cubi-tk snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-tk snappy pull-sheet --help`` for more information.
- `SNAPPY Pipeline GitLab Project <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/snappy>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.
"""

import argparse
import typing
from uuid import UUID


from cubi_tk.isa_support import InvestigationTraversal, isa_dict_to_isa_data
from cubi_tk.parsers import check_args_global_parser, print_args
from cubi_tk.snappy.parse_sample_sheet import SampleSheetBuilderCancer, SampleSheetBuilderGermline
from loguru import logger
from sodar_cli import api

from cubi_tk.sodar_api import get_assay_from_uuid

from ..common import overwrite_helper

from .common import find_snappy_root_dir
from .models import load_datasets


def strip(x):
    if hasattr(x, "strip"):
        return x.strip()
    else:
        return x


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy pull-sheet``."""
    parser.add_argument("--hidden-cmd", dest="snappy_cmd", default=run, help=argparse.SUPPRESS)

    parser.add_argument(
        "--yes", default=False, action="store_true", help="Assume all answers are yes."
    )

    parser.add_argument(
        "--dry-run",
        "-n",
        default=False,
        action="store_true",
        help="Perform a dry run, i.e., don't change anything only display change, implies '--show-diff'.",
    )
    parser.add_argument(
        "--no-show-diff",
        "-D",
        dest="show_diff",
        default=True,
        action="store_false",
        help="Don't show change when creating/updating sample sheets.",
    )
    parser.add_argument(
        "--show-diff-side-by-side",
        default=False,
        action="store_true",
        help="Show diff side by side instead of unified.",
    )

    parser.add_argument(
        "--library-types",
        nargs= "*",
        choices=( "WES", "WGS", "Panel_seq"),
        default=[],
        help="Library type(s) to use passed like '--library-types WES Panel_seq', default is to use all."
    )

    parser.add_argument(
        "--first-batch",
        default=0,
        type=int,
        help="First batch to be included in local sample sheet. Defaults: 0.",
    )
    parser.add_argument(
        "--last-batch",
        type=int,
        default=None,
        help="Last batch to be included in local sample sheet. Not used by default.",
    )

    parser.add_argument(
        "--tsv-shortcut",
        default="germline",
        choices=("cancer", "generic", "germline"),
        help="The shortcut TSV schema to use; default: 'germline'.",
    )


def check_args(args) -> int:
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    any_error = False
    any_error, args =  check_args_global_parser(args)

    return int(any_error)

def build_sheet(
    args,
    project_uuid: typing.Union[str, UUID],
) -> str:
    """Build sheet TSV file."""

    # Obtain ISA-tab from SODAR REST API.
    isa_dict = api.samplesheet.export(
        sodar_url=args.sodar_server_url,
        sodar_api_token=args.sodar_api_token,
        project_uuid=project_uuid,
    )
    assay_filename = None
    if(args.assay_uuid): #samplesheet.export doesnt pull assayuuids, get assauuuid via samplesheet.retrive
        assay = get_assay_from_uuid(
            args.sodar_server_url,
            args.sodar_api_token,
            project_uuid,
            args.assay_uuid
        )
        assay_filename = assay.file_name
    isa = isa_dict_to_isa_data(isa_dict, assay_filename)
    if args.tsv_shortcut == "germline":
        builder = SampleSheetBuilderGermline()
        builder.set_germline_specific_values(args.library_types, project_uuid, args.first_batch, args.last_batch)
    else:
        builder = SampleSheetBuilderCancer()
    iwalker = InvestigationTraversal(isa.investigation, isa.studies, isa.assays)
    iwalker.run(builder)

    # Generate the resulting sample sheet.
    result = builder.generateSheet()
    return "\n".join(result)


def run(
    args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
) -> typing.Optional[int]:
    """Run ``cubi-tk snappy pull-sheet``."""
    res: typing.Optional[int] = check_args(args)
    if res:  # pragma: nocover
        return res

    logger.info("Starting to pull sheet...")
    print_args(args)

    logger.debug("Load config...")
    args.base_path = find_snappy_root_dir(args.base_path)

    config_path = args.base_path / ".snappy_pipeline"
    datasets = load_datasets(config_path / "config.yaml")
    logger.info("Pulling for {} datasets", len(datasets))
    if(len(datasets) >1 and args.assay_uuid is not None):
        logger.warning("Assay_uuid defined but multiple projects present, this programm will only work properly for the project with the given UUID")
    for dataset in datasets.values():
        if dataset.sodar_uuid:
            overwrite_helper(
                config_path / dataset.sheet_file,
                build_sheet(
                    args, dataset.sodar_uuid,
                ),
                do_write=not args.dry_run,
                show_diff=True,
                show_diff_side_by_side=args.show_diff_side_by_side,
                answer_yes=args.yes,
            )

    return None
