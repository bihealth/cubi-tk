"""``cubi-tk snappy varfish-upload``: transfer variant_calling results into iRODS landing zone."""

import argparse
import glob
import os
import pathlib
import typing

from biomedsheets import io_tsv, shortcuts
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
from logzero import logger
from varfish_cli.__main__ import main as varfish_cli_main

from ..common import find_base_path
from .models import DataSet, load_datasets


#: Default pipeline steps to use.
DEFAULT_STEPS = (
    "ngs_mapping",
    "targeted_seq_cnv_export",
    "variant_export",
    "wgs_cnv_export",
    "wgs_sv_export",
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


def load_sheet_tsv(path_tsv, tsv_shortcut="germline"):
    """Load sample sheet."""
    load_tsv = getattr(io_tsv, "read_%s_tsv_sheet" % tsv_shortcut)
    with open(path_tsv, "rt") as f:
        return load_tsv(f, naming_scheme=NAMING_ONLY_SECONDARY_ID)


def yield_ngs_library_names(sheet, min_batch=None, batch_key="batchNo"):
    shortcut_sheet = shortcuts.GermlineCaseSheet(sheet)
    for pedigree in shortcut_sheet.cohort.pedigrees:
        max_batch = max(donor.extra_infos.get(batch_key, 0) for donor in pedigree.donors)
        if min_batch is None or min_batch <= max_batch:
            yield pedigree.index.dna_ngs_library.name


class SnappyVarFishUploadCommand:
    """Implementation of snappy itransfer command for variant calling results."""

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
            "--samples", help="The samples to limit the submission for, if any", default=""
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
        for dataset in datasets.values():
            logger.debug("Considering %s", dataset.sodar_uuid)
            if dataset.sodar_uuid in self.args.project:
                self._process_dataset(dataset)

        logger.info("All done")
        return None

    def _process_dataset(self, ds: DataSet):
        if ds.sodar_uuid is None:
            raise TypeError("ds.sodar_uuid must not be null")
        sodar_uuid: str = ds.sodar_uuid
        name = "%s (%s)" % (ds.sodar_title, sodar_uuid) if ds.sodar_title else sodar_uuid
        logger.info("Processing Dataset %s", name)
        logger.info("  loading from %s", self.args.base_path / ".snappy_pipeline" / ds.sheet_file)
        sheet = load_sheet_tsv(self.args.base_path / ".snappy_pipeline" / ds.sheet_file)
        for library in yield_ngs_library_names(sheet, self.args.min_batch):
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
                    pattern = "%s/*.%s/**/*.%s" % (
                        self.args.base_path / step / "work",
                        library,
                        ext,
                    )
                    logger.debug("pattern: %s", pattern)
                    for x in glob.glob(pattern, recursive=True):
                        b = os.path.basename(x)
                        # Currently, must treat .ped specially 8-[
                        if b not in found and any(
                            (b.endswith(".ped") or b.startswith(p) for p in PREFIXES)
                        ):
                            found[b] = x
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
