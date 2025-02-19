"""``cubi-tk snappy pull-sheet``: pull BiomedSheet files from SODAR.

More Information
----------------

- Also see ``cubi-tk snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-tk snappy pull-sheet --help`` for more information.
- `SNAPPY Pipeline GitLab Project <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/snappy>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.
"""

import argparse
import os
import pathlib
import typing
from uuid import UUID


import attr
from cubi_tk.isa_support import InvestigationTraversal, isa_dict_to_isa_data
from cubi_tk.snappy.parse_sample_sheet import SampleSheetBuilderCancer, SampleSheetBuilderGermline
from logzero import logger
from sodar_cli import api

from ..common import CommonConfig, load_toml_config, overwrite_helper

from .common import find_snappy_root_dir
from .models import load_datasets



@attr.s(frozen=True, auto_attribs=True)
class PullSheetsConfig:
    """Configuration for the ``cubi-tk snappy pull-sheets`` command."""

    #: Global configuration.
    global_config: CommonConfig

    base_path: typing.Optional[pathlib.Path]
    yes: bool
    dry_run: bool
    show_diff: bool
    show_diff_side_by_side: bool
    library_types: typing.Tuple[str]
    first_batch: int
    last_batch: typing.Union[int, type(None)]
    tsv_shortcut: str
    assay_uuid:str

    @staticmethod
    def create(args, global_config, toml_config=None):
        _ = toml_config or {}
        return PullSheetsConfig(
            global_config=global_config,
            base_path=pathlib.Path(args.base_path),
            yes=args.yes,
            dry_run=args.dry_run,
            show_diff=args.show_diff,
            show_diff_side_by_side=args.show_diff_side_by_side,
            library_types=tuple(args.library_types),
            first_batch=args.first_batch,
            last_batch=args.last_batch,
            tsv_shortcut=args.tsv_shortcut,
            assay_uuid=args.assay_uuid
        )



def strip(x):
    if hasattr(x, "strip"):
        return x.strip()
    else:
        return x


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy pull-sheet``."""
    parser.add_argument("--hidden-cmd", dest="snappy_cmd", default=run, help=argparse.SUPPRESS)

    parser.add_argument(
        "--base-path",
        default=os.getcwd(),
        required=False,
        help=(
            "Base path of project (contains '.snappy_pipeline/' etc.), spiders up from current "
            "work directory and falls back to current working directory by default."
        ),
    )

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
        "--library-types", help="Library type(s) to use, comma-separated, default is to use all."
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

    parser.add_argument(
        "--assay-uuid",
        default=None,
        required=False,
        type=str,
        help="Assay UUID for assay if multiple assays are present",
    )


def check_args(args) -> int:
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    any_error = False

    # Postprocess arguments.
    if args.library_types:
        args.library_types = args.library_types.split(",")  # pragma: nocover
    else:
        args.library_types = []

    return int(any_error)

def build_sheet(
    config: PullSheetsConfig,
    project_uuid: typing.Union[str, UUID],
    first_batch: typing.Optional[int] = None,
    last_batch: typing.Optional[int] = None,
    tsv_shortcut: str = "germline",
    assay_uuid=None
) -> str:
    """Build sheet TSV file."""

    # Obtain ISA-tab from SODAR REST API.
    isa_dict = api.samplesheet.export(
        sodar_url=config.global_config.sodar_server_url,
        sodar_api_token=config.global_config.sodar_api_token,
        project_uuid=project_uuid,
    )
    assay_filename = None
    if(assay_uuid): #samplesheet.export doesnt pull assayuuids, get assauuuid via samplesheet.retrive
        investigation = api.samplesheet.retrieve(
            sodar_url=config.global_config.sodar_server_url,
            sodar_api_token=config.global_config.sodar_api_token,
            project_uuid=project_uuid,
        )
        assay = None
        for study in investigation.studies.values():
            for remote_assay_uuid in study.assays.keys():
                if assay_uuid== remote_assay_uuid:
                    assay = study.assays[remote_assay_uuid]
                    assay_filename = assay.file_name
                    break
    isa = isa_dict_to_isa_data(isa_dict, assay_filename)
    if tsv_shortcut == "germline":
        builder = SampleSheetBuilderGermline() 
        builder.set_germline_specific_values(config, project_uuid, first_batch, last_batch)
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
    logger.info("  Args: %s", args)

    logger.debug("Load config...")
    toml_config = load_toml_config(args)
    global_config = CommonConfig.create(args, toml_config)
    args.base_path = find_snappy_root_dir(args.base_path)
    config = PullSheetsConfig.create(args, global_config, toml_config)

    config_path = config.base_path / ".snappy_pipeline"
    datasets = load_datasets(config_path / "config.yaml")
    logger.info("Pulling for %d datasets", len(datasets))
    for dataset in datasets.values():
        if dataset.sodar_uuid:
            overwrite_helper(
                config_path / dataset.sheet_file,
                build_sheet(
                    config, dataset.sodar_uuid, args.first_batch, args.last_batch, args.tsv_shortcut, args.assay_uuid
                ),
                do_write=not args.dry_run,
                show_diff=True,
                show_diff_side_by_side=args.show_diff_side_by_side,
                answer_yes=args.yes,
            )

    return None
