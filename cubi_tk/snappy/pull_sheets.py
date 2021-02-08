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
from uuid import UUID
import typing

import attr
from logzero import logger

from ..common import CommonConfig, overwrite_helper, load_toml_config
from ..isa_support import InvestigationTraversal, IsaNodeVisitor, isa_dict_to_isa_data, first_value
from ..sodar import api
from .models import load_datasets
from .common import find_snappy_root_dir

#: Template for the to-be-generated file.
HEADER_TPL = (
    "[Metadata]",
    "schema\tgermline_variants",
    "schema_version\tv1",
    "",
    "[Custom Fields]",
    "key\tannotatedEntity\tdocs\ttype\tminimum\tmaximum\tunit\tchoices\tpattern",
    "batchNo\tbioEntity\tBatch No.\tinteger\t.\t.\t.\t.\t.",
    "familyId\tbioEntity\tFamily\tstring\t.\t.\t.\t.\t.",
    "projectUuid\tbioEntity\tProject UUID\tstring\t.\t.\t.\t.\t.",
    "libraryKit\tngsLibrary\tEnrichment kit\tstring\t.\t.\t.\t.\t.",
    "",
    "[Data]",
    (
        "familyId\tpatientName\tfatherName\tmotherName\tsex\tisAffected\tlibraryType\tfolderName"
        "\tbatchNo\thpoTerms\tprojectUuid\tseqPlatform\tlibraryKit"
    ),
)

#: Mapping from ISA-tab sex to sample sheet sex.
MAPPING_SEX = {"female": "F", "male": "M", "unknown": "U", None: "."}

#: Mapping from disease status to sample sheet status.
MAPPING_STATUS = {"affected": "Y", "carrier": "Y", "unaffected": "N", "unknown": ".", None: "."}


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
        )


@attr.s(frozen=True, auto_attribs=True)
class Source:
    family: typing.Optional[str]
    source_name: str
    batch_no: int
    father: str
    mother: str
    sex: str
    affected: str
    sample_name: str


@attr.s(frozen=True, auto_attribs=True)
class Sample:
    source: Source
    library_name: str
    library_type: str
    folder_name: str
    seq_platform: str
    library_kit: str


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


def check_args(args) -> int:
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    any_error = False

    # Postprocess arguments.
    if args.library_types:
        args.library_types = args.library_types.split(",")  # pragma: nocover
    else:
        args.library_types = []

    return int(any_error)


class SampleSheetBuilder(IsaNodeVisitor):
    def __init__(self):
        #: Source by sample name.
        self.sources = {}
        #: Sample by sample name.
        self.samples = {}

    def on_visit_material(self, material, node_path, study=None, assay=None):
        super().on_visit_material(material, node_path, study, assay)
        material_path = [x for x in node_path if hasattr(x, "type")]
        source = material_path[0]
        if material.type == "Sample Name" and assay is None:
            sample = material
            characteristics = {c.name: c for c in source.characteristics}
            comments = {c.name: c for c in source.comments}
            self.sources[material.name] = Source(
                family=characteristics["Family"].value[0],
                source_name=source.name,
                batch_no=characteristics.get("Batch", comments.get("Batch")).value[0],
                father=characteristics["Father"].value[0],
                mother=characteristics["Mother"].value[0],
                sex=characteristics["Sex"].value[0],
                affected=characteristics["Disease status"].value[0],
                sample_name=sample.name,
            )
        elif material.type == "Library Name":
            library = material
            sample = material_path[0]
            if library.name.split("-")[-1].startswith("WGS"):
                library_type = "WGS"
            elif library.name.split("-")[-1].startswith("WES"):
                library_type = "WES"
            elif library.name.split("-")[-1].startswith("Panel_seq"):
                library_type = "Panel_seq"
            else:
                raise Exception("Cannot infer library type from %s" % library.name)

            self.samples[sample.name] = Sample(
                source=self.sources[sample.name],
                library_name=library.name,
                library_type=library_type,
                folder_name=first_value("Folder name", node_path),
                seq_platform=first_value("Platform", node_path),
                library_kit=first_value("Library Kit", node_path),
            )

    def on_visit_process(self, process, node_path, study=None, assay=None):
        super().on_visit_node(process, study, assay)
        material_path = [x for x in node_path if hasattr(x, "type")]
        sample = material_path[0]
        if process.protocol_ref.startswith("Nucleic acid sequencing"):
            self.samples[sample.name] = attr.evolve(
                self.samples[sample.name], seq_platform=first_value("Platform", node_path)
            )


def build_sheet(config: PullSheetsConfig, project_uuid: typing.Union[str, UUID]) -> str:
    """Build sheet TSV file."""

    result = []

    # Obtain ISA-tab from SODAR REST API.
    isa_dict = api.samplesheets.get(
        sodar_url=config.global_config.sodar_server_url,
        sodar_api_token=config.global_config.sodar_api_token,
        project_uuid=project_uuid,
    )
    isa = isa_dict_to_isa_data(isa_dict)

    builder = SampleSheetBuilder()
    iwalker = InvestigationTraversal(isa.investigation, isa.studies, isa.assays)
    iwalker.run(builder)

    # Generate the resulting sample sheet.
    result.append("\n".join(HEADER_TPL))
    for sample_name, source in builder.sources.items():
        sample = builder.samples.get(sample_name, None)
        if not config.library_types or not sample or sample.library_type in config.library_types:
            row = [
                source.family or "FAM",
                source.source_name or ".",
                source.father or "0",
                source.mother or "0",
                MAPPING_SEX[source.sex.lower()],
                MAPPING_STATUS[source.affected.lower()],
                sample.library_type or "." if sample else ".",
                sample.folder_name or "." if sample else ".",
                "0" if source.batch_no is None else source.batch_no,
                ".",
                str(project_uuid),
                sample.seq_platform or "." if sample else ".",
                sample.library_kit or "." if sample else ".",
            ]
            result.append("\t".join([c.strip() for c in row]))
    result.append("")

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
                build_sheet(config, dataset.sodar_uuid),
                do_write=not args.dry_run,
                show_diff=True,
                show_diff_side_by_side=args.show_diff_side_by_side,
                answer_yes=args.yes,
            )

    return None
