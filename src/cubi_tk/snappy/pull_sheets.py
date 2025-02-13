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
from biomedsheets import io_tsv
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
from logzero import logger
from sodar_cli import api

from ..common import CommonConfig, load_toml_config, overwrite_helper
from ..isa_support import (
    InvestigationTraversal,
    IsaNodeVisitor,
    first_value,
    isa_dict_to_isa_data,
)
from .common import find_snappy_root_dir
from .models import load_datasets
from .parse_sample_sheet import ParseSampleSheet



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
    assay_txt:str

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
            assay_txt=args.assay_txt
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
        "--assay-txt",
        default=None,
        required=False,
        type=str,
        help="Assay Name e.g. 'a_project_exome_sequencing.txt' if multiple assays are present",
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

####Germline specific Classes, Templates and Constants

#: Template for the to-be-generated file.
HEADER_TPL_GERMLINE = (
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
MAPPING_SEX_GERMLNE = {"female": "F", "male": "M", "unknown": "U", None: "."}

#: Mapping from disease status to sample sheet status.
MAPPING_STATUS_GERMLINE = {"affected": "Y", "carrier": "Y", "unaffected": "N", "unknown": ".", None: "."}

@attr.s(frozen=True, auto_attribs=True)
class SourceGermline:
    family: typing.Optional[str]
    source_name: str
    batch_no: int
    father: str
    mother: str
    sex: str
    affected: str
    sample_name: str


@attr.s(frozen=True, auto_attribs=True)
class SampleGermline:
    source: SourceGermline
    library_name: str
    library_type: str
    folder_name: str
    seq_platform: str
    library_kit: str

class SampleSheetBuilderGermline(IsaNodeVisitor):
    def __init__(self):
        #: Source by sample name.
        self.sources = {}
        #: Sample by sample name.
        self.samples = {}
        #: The previous process.
        self.prev_process = None

        self.config = None
        self.project_uuid = ""
        self.first_batch = 0
        self.last_batch = 0

    def set_germline_specific_values(self, config, project_uuid, first_batch, last_batch):
        self.config = config
        self.project_uuid = project_uuid
        self.first_batch = first_batch
        self.last_batch = last_batch

    def on_visit_material(self, material, node_path, study=None, assay=None):
        super().on_visit_material(material, node_path, study, assay)
        material_path = [x for x in node_path if hasattr(x, "type")]
        source = material_path[0]
        if material.type == "Sample Name" and assay is None:
            sample = material
            characteristics = {c.name: c for c in source.characteristics}
            comments = {c.name: c for c in source.comments}
            batch = characteristics.get("Batch", comments.get("Batch"))
            family = characteristics.get("Family", comments.get("Family"))
            father = characteristics.get("Father", comments.get("Father"))
            mother = characteristics.get("Mother", comments.get("Mother"))
            sex = characteristics.get("Sex", comments.get("Sex"))
            affected = characteristics.get("Disease status", comments.get("Disease status"))
            self.sources[material.name] = SourceGermline(
                family=family.value[0] if family else None,
                source_name=source.name,
                batch_no=batch.value[0] if batch else None,
                father=father.value[0] if father else None,
                mother=mother.value[0] if mother else None,
                sex=sex.value[0] if sex else None,
                affected=affected.value[0] if affected else None,
                sample_name=sample.name,
            )
        elif material.type == "Library Name" or (
            material.type == "Extract Name"
            and self.prev_process.protocol_ref.startswith("Library construction")
        ):
            library = material
            sample = material_path[0]
            if library.name.split("-")[-1].startswith("WGS"):
                library_type = "WGS"
            elif library.name.split("-")[-1].startswith("WES"):
                library_type = "WES"
            elif library.name.split("-")[-1].startswith("Panel_seq"):
                library_type = "Panel_seq"
            elif library.name.split("-")[-1].startswith("mRNA_seq"):
                library_type = "mRNA_seq"
            elif library.name.split("-")[-1].startswith("RNA_seq"):
                library_type = "RNA_seq"
            else:
                raise Exception("Cannot infer library type from %s" % library.name)

            folder_name = first_value("Folder name", node_path)
            if not folder_name:
                folder_name = library.name
            self.samples[sample.name] = SampleGermline(
                source=self.sources[sample.name],
                library_name=library.name,
                library_type=library_type,
                folder_name=folder_name,
                seq_platform=first_value("Platform", node_path),
                library_kit=first_value("Library Kit", node_path),
            )

    def on_visit_process(self, process, node_path, study=None, assay=None):
        super().on_visit_node(process, study, assay)
        self.prev_process = process
        material_path = [x for x in node_path if hasattr(x, "type")]
        sample = material_path[0]
        if process.protocol_ref.startswith("Nucleic acid sequencing"):
            self.samples[sample.name] = attr.evolve(
                self.samples[sample.name], seq_platform=first_value("Platform", node_path)
            )

    def generateSheet(self):
        result = []
        for sample_name, source in self.sources.items():
            sample = self.samples.get(sample_name, None)
            if not self.config.library_types or not sample or sample.library_type in self.config.library_types:
                row = [
                    source.family or "FAM",
                    source.source_name or ".",
                    source.father or "0",
                    source.mother or "0",
                    MAPPING_SEX_GERMLNE[source.sex.lower()],
                    MAPPING_STATUS_GERMLINE[source.affected.lower()],
                    sample.library_type or "." if sample else ".",
                    sample.folder_name or "." if sample else ".",
                    "0" if source.batch_no is None else source.batch_no,
                    ".",
                    str(self.project_uuid),
                    sample.seq_platform or "." if sample else ".",
                    sample.library_kit or "." if sample else ".",
                ]
                result.append("\t".join([c.strip() for c in row]))
        
        load_tsv = getattr(io_tsv, "read_%s_tsv_sheet" % "germline")
    
        sheet = load_tsv(list(HEADER_TPL_GERMLINE) + result, naming_scheme=NAMING_ONLY_SECONDARY_ID)
        parser = ParseSampleSheet()
        samples_in_batch = list(parser.yield_sample_names(sheet, self.first_batch, self.last_batch))
        result = (
            list(HEADER_TPL_GERMLINE)
            + [line if line.split("\t")[1] in samples_in_batch else "#" + line for line in result]
            + [""]
        )

        return result

####Cancer specific Classes, Templates and Constants

HEADER_TPL_CANCER= (
    "[Metadata]",
    "schema\tcancer_matched",
    "schema_version\tv1",
    "",
    "[Custom Fields]",
    "key\tannotatedEntity\tdocs\ttype\tminimum\tmaximum\tunit\tchoices\tpattern",
    "extractionType\ttestSample\textraction type\tstring\t.\t.\t.\t.\t.",
    "libraryKit\tngsLibrary\texome enrichment kit\tstring\t.\t.\t.\t.\t.",
    "",
    "[Data]",
    (
        "patientName\tsampleName\textractionType\tlibraryType\tfolderName\tisTumor\tlibraryKit"
    ),
)

@attr.s(frozen=True, auto_attribs=True)
class SourceCancer:
    source_name: str
    sample_name: str
    is_tumor: str


@attr.s(frozen=True, auto_attribs=True)
class SampleCancer:
    source: SourceCancer
    extraction_type: str
    sample_name_biomed :str
    library_name: str
    library_type: str
    folder_name: str
    library_kit: str

class SampleSheetBuilderCancer(IsaNodeVisitor):
    def __init__(self):
        #: Source by sample name.
        self.sources = {}
        #: Sample by sample name.
        self.samples = {}
        #: The previous process.
        self.prev_process = None

    def on_visit_material(self, material, node_path, study=None, assay=None):
        super().on_visit_material(material, node_path, study, assay)
        material_path = [x for x in node_path if hasattr(x, "type")]
        source = material_path[0]
        if material.type == "Sample Name" and assay is None:
            sample = material
            characteristics_material = {c.name: c for c in material.characteristics}
            comments = {c.name: c for c in source.comments}
            tumor =characteristics_material.get("Is tumor", comments.get("Is tumor"))
            self.sources[material.name] = SourceCancer(
                source_name=source.name,
                sample_name=sample.name,
                is_tumor=tumor.value[0] if tumor else None
            )
        elif material.type == "Library Name" or (
            material.type == "Extract Name"
            and self.prev_process.protocol_ref.startswith("Library construction")
        ):
            library = material
            sample = material_path[0]
            splitted_lib = library.name.split("-")

            #get libtype
            lib_type_string = splitted_lib[-1]
            if lib_type_string.startswith("WGS"):
                library_type = "WGS"
            elif lib_type_string.startswith("WES"):
                library_type = "WES"
            elif lib_type_string.startswith("Panel_seq"):
                library_type = "Panel_seq"
            elif lib_type_string.startswith("mRNA_seq"):
                library_type = "mRNA_seq"
            elif lib_type_string.startswith("RNA_seq"):
                library_type = "RNA_seq"
            else:
                raise Exception("Cannot infer library type from %s" % library.name)
            
            #get extractiontype
            extr_type_string = splitted_lib[-2]
            if extr_type_string.startswith("DNA"):
                extraction_type="DNA"
            elif extr_type_string.startswith("RNA"):
                extraction_type="RNA"
            else:
                raise Exception("Cannot infer exctraction type from %s" % library.name)
            
            #get sample name for biomedsheet
            samp_name_string = splitted_lib[-3]
            if samp_name_string.startswith("T"):
                sample_name_biomed = "T"
            elif samp_name_string.startswith("N"):
                sample_name_biomed = "N"
            else:
                raise Exception("Cannot biomed sample name from %s" % library.name)
            
            folder_name = first_value("Folder name", node_path)
            if not folder_name:
                folder_name = library.name

            self.samples[sample.name] = SampleCancer(
                source=self.sources[sample.name],
                sample_name_biomed = sample_name_biomed,
                extraction_type= extraction_type,
                library_name=library.name,
                folder_name=folder_name,
                library_type=library_type,
                library_kit=first_value("Library Kit", node_path),
            )

    def on_visit_process(self, process, node_path, study=None, assay=None):
        super().on_visit_node(process, study, assay)
        self.prev_process = process
        material_path = [x for x in node_path if hasattr(x, "type")]
        sample = material_path[0]
        if process.protocol_ref.startswith("Nucleic acid sequencing"):
            self.samples[sample.name] = attr.evolve(
                self.samples[sample.name]
            )

    def generateSheet(self):
        result = []
        #for sample_name, source in self.sources.items():
            #sample = self.samples.get(sample_name, None)
            #if sample:
        for sample_name, sample in self.samples.items():
            source = self.sources.get(sample_name, None)    
            row = [
                source.source_name or ".",
                sample.sample_name_biomed or ".",
                sample.extraction_type or "." if sample else ".",
                sample.library_type or "." if sample else ".",
                sample.folder_name or "." if sample else ".",
                source.is_tumor,
                sample.library_kit or "." if sample else ".",
            ]
            result.append("\t".join([c.strip() for c in row]))
        result = (
            list(HEADER_TPL_CANCER)
            + [line for line in result]
            + [""]
        )

        return result


def build_sheet(
    config: PullSheetsConfig,
    project_uuid: typing.Union[str, UUID],
    first_batch: typing.Optional[int] = None,
    last_batch: typing.Optional[int] = None,
    tsv_shortcut: str = "germline",
    assay_txt=None
) -> str:
    """Build sheet TSV file."""

    # Obtain ISA-tab from SODAR REST API.
    isa_dict = api.samplesheet.export(
        sodar_url=config.global_config.sodar_server_url,
        sodar_api_token=config.global_config.sodar_api_token,
        project_uuid=project_uuid,
    )
    isa = isa_dict_to_isa_data(isa_dict, assay_txt)
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
                    config, dataset.sodar_uuid, args.first_batch, args.last_batch, args.tsv_shortcut, args.assay_txt
                ),
                do_write=not args.dry_run,
                show_diff=True,
                show_diff_side_by_side=args.show_diff_side_by_side,
                answer_yes=args.yes,
            )

    return None
