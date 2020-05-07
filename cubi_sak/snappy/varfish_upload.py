"""``cubi-sak snappy varfish-upload``: transfer variant_calling results into iRODS landing zone."""

import argparse
import glob
import os
import pathlib
import typing

from biomedsheets import io_tsv, shortcuts
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
import cattr
from logzero import logger
from varfish_cli.__main__ import main as varfish_cli_main
import yaml

from .models import DataSet
from .itransfer_common import SnappyItransferCommandBase, IndexLibrariesOnlyMixin


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


def load_config_yaml(path: pathlib.Path) -> typing.Any:
    with path.open("r") as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.error("error: %s", e)


def trans_load(ds):
    """Transmogrify DataSet when loading."""

    def f(k):
        return {"type": "sheet_type", "file": "sheet_file"}.get(k, k)

    return {f(k): v for k, v in ds.items()}


def load_datasets(path: pathlib.Path) -> typing.List[DataSet]:
    """Load data sets and filter to those with SODAR UUID."""
    logger.info("Loading data sets from %s", path)
    raw_ds = load_config_yaml(path)["data_sets"]
    transmogrified = {key: trans_load(value) for key, value in raw_ds.items()}
    data_sets = cattr.structure(transmogrified, typing.Dict[str, DataSet])
    filtered = {key: ds for key, ds in data_sets.items() if ds.sodar_uuid}
    logger.info("Loaded %d data sets, %d with SODAR UUID", len(data_sets), len(filtered))

    for key, ds in sorted(filtered.items()):
        logger.debug("  - %s%s", ds.sodar_uuid, ": %s" % ds.sodar_title if ds.sodar_title else "")

    return filtered


def load_sheet_tsv(path_tsv, tsv_shortcut="germline"):
    """Load sample sheet."""
    load_tsv = getattr(io_tsv, "read_%s_tsv_sheet" % tsv_shortcut)
    with open(path_tsv, "rt") as f:
        return load_tsv(f, naming_scheme=NAMING_ONLY_SECONDARY_ID)


def yield_ngs_library_names(sheet, min_batch=None, batch_key="batchNo"):
    shortcut_sheet = shortcuts.GermlineCaseSheet(sheet)
    for pedigree in shortcut_sheet.cohort.pedigrees:
        donor = pedigree.index
        if min_batch is not None and batch_key in donor.extra_infos:
            if min_batch > donor.extra_infos[batch_key]:
                logger.debug(
                    "Skipping donor %s because %s = %d < min_batch = %d",
                    donor.name,
                    donor.extra_infos[batch_key],
                    batch_key,
                    min_batch,
                )
                continue
        yield donor.dna_ngs_library.name


class SnappyVarFishUploadCommand:
    """Implementation of snappy itransfer command for variant calling results."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--hidden-cmd", dest="snappy_cmd", default=run, help=argparse.SUPPRESS)

        group_sodar = parser.add_argument_group("SODAR-related")

        parser.add_argument(
            "--base-path",
            default=None,
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
            "project", nargs="+", help="The UUID(s) of the SODAR project to submit."
        )

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0

        base_path = pathlib.Path(os.getcwd())
        while base_path != base_path.root:
            if (base_path / ".snappy_pipeline").exists():
                args.base_path = str(base_path)
                break
            base_path = base_path.parent
        args.base_path = base_path

        if not args.base_path:
            args.base_path = os.getcwd()

        steps = set(DEFAULT_STEPS)
        for s in args.steps:
            for x in s.split(","):
                x = x.strip()
                if x.startswith("+"):
                    steps.add(x[1:])
                elif x.startswith("-"):
                    steps.discard(x[1:])
                else:
                    logger.warn("Does not start with +/-: %s", x)
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

        logger.info("Starting cubi-sak varfish-upload")
        logger.info("  args: %s", self.args)

        datasets = load_datasets(self.args.base_path / ".snappy_pipeline/config.yaml")
        for dataset in datasets.values():
            if dataset.sodar_uuid in self.args.project:
                self._process_dataset(dataset)

        logger.info("All done")

    def _process_dataset(self, ds: DataSet):
        name = "%s (%s)" % (ds.sodar_title, ds.sodar_uuid) if ds.sodar_title else sodar_uuid
        logger.info("Processing Dataset %s", name)
        logger.info("  loading from %s", self.args.base_path / ".snappy_pipeline" / ds.sheet_file)
        sheet = load_sheet_tsv(self.args.base_path / ".snappy_pipeline" / ds.sheet_file)
        for library in yield_ngs_library_names(sheet, self.args.min_batch):
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
            found = {}
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
            logger.debug("    files:\n%s", "\n".join(sorted(found)))
            # Perform call to varfish import.
            args = [
                "varfish-cli",
                "--verbose",
                "case",
                "create-import-info",
                ds.sodar_uuid,
                *sorted(found.values()),
            ]
            if self.args.answer_yes:
                answer = True
            else:
                while True:
                    answer = input("Submit to VarFish? [yN] ").lower()
                    if answer.startswith("y") or answer.startswith("n"):
                        break
                answer = answer == "y"
            if answer:
                logger.info("Executing '%s'", " ".join(args))
                varfish_cli_main(args[1:])
        logger.info("  -> all done with %s", name)


def run(
    args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
) -> typing.Optional[int]:
    """Run ``cubi-sak varfish-upload``."""

    return SnappyVarFishUploadCommand(args).execute()


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-sak snappy varfish-upload``."""
    return SnappyVarFishUploadCommand.setup_argparse(parser)
