"""``cubi-tk snappy varfish-upload``: transfer variant_calling results into iRODS landing zone."""

import argparse
import glob
import os
import pathlib
import typing

from biomedsheets import shortcuts
from logzero import logger
from varfish_cli.__main__ import main as varfish_cli_main

from .common import load_sheet_tsv
from ..common import find_base_path
from .models import DataSet, load_datasets


#: Default pipeline steps to use.
DEFAULT_STEPS = (
    "ngs_mapping",
    "targeted_seq_cnv_export",
    "variant_export",
    "variant_export_external",
    "wgs_cnv_export",
    "wgs_cnv_export_external",
    "wgs_sv_export",
    "wgs_sv_export_external",
)

#: The extensions that we are looking for.
EXTENSIONS = ("bam-qc.tsv.gz", "db-infos.tsv.gz", "gts.tsv.gz", "feature-effects.tsv.gz", "ped")

#: File prefixes to process (tool combinations).
PREFIXES = (
    "bwa.delly2",
    "bwa.erds_sv2",
    "bwa.gatk_hc",
    "bwa.gcnv",
    "bwa.popdel",
    "bwa.xhmm",
    "write_pedigree",
)


def yield_ngs_library_names(sheet, min_batch=None, batch_key="batchNo", pedigree_field=None):
    """Yield DNA NGS library names for indexes.

    :param sheet: Sample sheet.
    :type sheet: biomedsheets.models.Sheet

    :param min_batch: Minimum batch number to be extracted from the sheet. All samples in batches below this values
    will be skipped.
    :type min_batch: int

    :param batch_key: Batch number key in sheet. Default: 'batchNo'.
    :type batch_key: str

    :param pedigree_field: Field that should be used to define a pedigree. If none is provided pedigree will be defined
    based on information in the sample sheets rows alone.
    """
    kwargs = {}
    if pedigree_field:
        kwargs = {"join_by_field": pedigree_field}
    shortcut_sheet = shortcuts.GermlineCaseSheet(sheet, **kwargs)
    for pedigree in shortcut_sheet.cohort.pedigrees:
        max_batch = max(donor.extra_infos.get(batch_key, 0) for donor in pedigree.donors)
        if (min_batch is None or min_batch <= max_batch) and pedigree.index.dna_ngs_library:
            yield pedigree.index.dna_ngs_library.name


class SnappyVarFishUploadCommand:
    """Implementation of snappy varfish-upload command for variant calling results."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--hidden-cmd", dest="snappy_cmd", default=run, help=argparse.SUPPRESS)

        group = parser.add_argument_group("VarFish Configuration")
        group.add_argument(
            "--varfish-config",
            default=os.environ.get("VARFISH_CONFIG_PATH", None),
            help="Path to configuration file.",
        )
        group.add_argument(
            "--varfish-server-url",
            default=os.environ.get("VARFISH_SERVER_URL", None),
            help="SODAR server URL key to use, defaults to env VARFISH_SERVER_URL.",
        )
        group.add_argument(
            "--varfish-api-token",
            default=os.environ.get("VARFISH_API_TOKEN", None),
            help="SODAR API token to use, defaults to env VARFISH_API_TOKEN.",
        )
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
            "--steps",
            default=[],
            action="append",
            help=(
                "Pipeline steps to consider for the export. Defaults to include all of the "
                "following; specify this with +name/-name to add/remove and either give multiple "
                "arguments or use a comma-separated list. {%s}" % ", ".join(sorted(DEFAULT_STEPS))
            ),
        )
        parser.add_argument(
            "--external-data",
            default=False,
            action="store_true",
            help=(
                "Flag to indicate that data was externally generated. Search for files will not filter based "
                "on common internally tool combinations, example: 'bwa.delly2' or 'bwa.gatk_hc'."
            ),
        )
        parser.add_argument(
            "--min-batch",
            default=None,
            required=False,
            type=int,
            help="Smallest batch to transfer, keep empty to transfer all.",
        )
        parser.add_argument(
            "--yes",
            "-y",
            dest="answer_yes",
            default=False,
            action="store_true",
            required=False,
            help="Assume yes to all answers",
        )
        parser.add_argument(
            "--samples",
            help=(
                "Limits the submission to the listed sample names. Don't include the full library name just the "
                "sample name (e.g., 'P001' instead of 'P001-N1-DNA1-WES1'). Separate the sample with comma for "
                "multiple samples, example: 'P001,P002,P003'."
            ),
            default="",
        )
        parser.add_argument(
            "project", nargs="+", help="The UUID(s) of the SODAR project to submit."
        )

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0

        args.base_path = pathlib.Path(find_base_path(args.base_path))

        steps = set(DEFAULT_STEPS)
        for s in args.steps:
            for x in s.split(","):
                x = x.strip()
                if x.startswith("+"):
                    steps.add(x[1:])
                elif x.startswith("-"):
                    steps.discard(x[1:])
                else:
                    logger.warning("Does not start with +/-: %s", x)
        args.steps = tuple(sorted(steps))

        if not os.path.exists(args.base_path):  # pragma: nocover
            logger.error("Base path %s does not exist", args.base_path)
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the upload(s)."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk varfish-upload")
        logger.info("  args: %s", self.args)

        datasets = load_datasets(self.args.base_path / ".snappy_pipeline/config.yaml")
        logger.debug("projects = %s", self.args.project)
        considered_uuid_list = []
        for dataset in datasets.values():
            logger.debug("Considering %s", dataset.sodar_uuid)
            considered_uuid_list.append(str(dataset.sodar_uuid))
            if dataset.sodar_uuid in self.args.project:
                self._process_dataset(dataset)

        # Test that the UUID was found
        if len(set(considered_uuid_list) & set(self.args.project)) > 0:
            logger.info("All done")
        else:
            input_uuid_str = ",".join(self.args.project)
            considered_uuid_str = ", ".join(considered_uuid_list)
            logger.warning(
                f"None of the considered UUIDs corresponded to the input.\n"
                f"- Requested UUID: {input_uuid_str}\n"
                f"- Considered UUID list: {considered_uuid_str}"
            )
        return None

    def _process_dataset(self, ds: DataSet):
        if ds.sodar_uuid is None:
            raise TypeError("ds.sodar_uuid must not be null")
        sodar_uuid: str = ds.sodar_uuid
        pedigree_field: str = ds.pedigree_field
        name = "%s (%s)" % (ds.sodar_title, sodar_uuid) if ds.sodar_title else sodar_uuid
        logger.info("Processing Dataset %s", name)
        logger.info("  loading from %s", self.args.base_path / ".snappy_pipeline" / ds.sheet_file)
        sheet_path = self.args.base_path / ".snappy_pipeline" / ds.sheet_file
        sheet = load_sheet_tsv(path_tsv=sheet_path)
        ngs_library_names_list = yield_ngs_library_names(
            sheet=sheet, min_batch=self.args.min_batch, pedigree_field=pedigree_field
        )
        for library in ngs_library_names_list:
            if self.args.samples and not library.split("-")[0] in self.args.samples.split(","):
                logger.info("Skipping library %s as it is not included in --samples", library)
                continue

            # Search for files of interest.
            logger.debug(
                "\nSearching in\n    %s\nfor library\n    %s\n"
                "steps\n    %s\nand extensions\n    %s\n and prefixes\n    %s",
                self.args.base_path,
                library,
                self.args.steps,
                EXTENSIONS,
                PREFIXES,
            )
            found: typing.Dict[str, str] = {}
            for step in self.args.steps:
                for ext in EXTENSIONS:
                    work_path = self.args.base_path / step / "work"
                    pattern = f"{work_path}/*.{library}/**/*.{ext}"
                    logger.debug(f"pattern: {pattern}")
                    for file_path in glob.glob(pattern, recursive=True):
                        file_name = os.path.basename(file_path)
                        # If data externally generated, cannot filter by common `snappy` tool combinations
                        if self.args.external_data:
                            key = f"{step}: {file_name}"
                            found[key] = file_path
                        elif file_name not in found and any(
                            (
                                file_name.endswith(".ped") or file_name.startswith(p)
                                for p in PREFIXES
                            )
                        ):  # must treat .ped as special case
                            found[file_name] = file_path
            logger.info("  found %d files for %s", len(found), library)
            if self.args.verbose:
                found_s = "\n".join("%s (%s)" % (k, v) for k, v in sorted(found.items()))
            else:
                found_s = "\n".join(sorted(found))
            logger.info("    files:\n%s", found_s)
            # Perform call to varfish import.
            args = [
                "varfish-cli",
                "--verbose",
                "case",
                "create-import-info",
                sodar_uuid,
                *sorted(found.values()),
            ]
            if self.args.answer_yes:
                answer = True
            else:
                while True:
                    answer_str = input("Submit to VarFish? [yN] ").lower()
                    if answer_str.startswith("y") or answer_str.startswith("n"):
                        break
                answer = answer_str == "y"
            if answer:
                logger.info("Executing '%s'", " ".join(args))
                varfish_cli_main(args[1:])
        logger.info("  -> all done with %s", name)


def run(
    args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
) -> typing.Optional[int]:
    """Run ``cubi-tk varfish-upload``."""

    return SnappyVarFishUploadCommand(args).execute()


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy varfish-upload``."""
    return SnappyVarFishUploadCommand.setup_argparse(parser)
