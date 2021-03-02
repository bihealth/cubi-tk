"""``cubi-tk sodar pull-raw-data``: download raw data from iRODS via SODAR."""

import argparse
import os
import shlex
import typing
from pathlib import Path
from subprocess import SubprocessError, check_call

import attr
from logzero import logger

from . import api
from ..common import load_toml_config
from ..isa_support import InvestigationTraversal, IsaNodeVisitor, isa_dict_to_isa_data, first_value


@attr.s(frozen=True, auto_attribs=True)
class Config:
    """Configuration for the download sheet command."""

    config: str
    verbose: bool
    sodar_server_url: str
    sodar_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    overwrite: bool
    min_batch: int
    dry_run: bool
    irsync_threads: int
    yes: bool
    project_uuid: str
    assay: str
    output_dir: str


@attr.s(frozen=True, auto_attribs=True)
class LibraryInfo:
    library_name: str
    folder_name: str
    batch_no: typing.Optional[int]


class LibraryInfoCollector(IsaNodeVisitor):
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
            self.sources[material.name] = {"source_name": source.name, "sample_name": sample.name}
            batch = characteristics.get("Batch", comments.get("Batch"))
            self.sources[material.name]["batch_no"] = batch.value[0] if batch else None
            family = characteristics.get("Family", comments.get("Family"))
            self.sources[material.name]["family"] = family.value[0] if family else None
        elif material.type == "Library Name":
            library = material
            sample = material_path[0]
            folder = first_value("Folder name", node_path)
            if not folder:
                folder = library.name
            self.samples[sample.name] = {
                "source": self.sources[sample.name],
                "library_name": library.name,
                "folder_name": folder,
            }


class PullRawDataCommand:
    """Implementation of the ``pull-raw-data`` command."""

    def __init__(self, config: Config):
        #: Command line arguments.
        self.config = config

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="sodar_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        group_sodar = parser.add_argument_group("SODAR-related")
        group_sodar.add_argument(
            "--sodar-url",
            default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
            help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
        )
        group_sodar.add_argument(
            "--sodar-api-token",
            default=os.environ.get("SODAR_API_TOKEN", None),
            help="Authentication token when talking to SODAR.  Defaults to SODAR_API_TOKEN environment variable.",
        )

        parser.add_argument(
            "--overwrite", default=False, action="store_true", help="Allow overwriting of files"
        )
        parser.add_argument("--min-batch", default=0, type=int, help="Minimal batch number to pull")

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
        parser.add_argument("--irsync-threads", help="Parameter -N to pass to irsync")

        parser.add_argument(
            "--assay", dest="assay", default=None, help="UUID of assay to download data for."
        )

        parser.add_argument("project_uuid", help="UUID of project to download data for.")
        parser.add_argument("output_dir", help="Path to output directory to write the raw data to.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        args = vars(args)
        args.pop("cmd", None)
        args.pop("sodar_cmd", None)
        while args["output_dir"].endswith("/"):
            args["output_dir"] = args["output_dir"][:-1]
        return cls(Config(**args)).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the download."""
        toml_config = load_toml_config(self.config)
        if not self.config.sodar_url:
            self.config = attr.evolve(
                self.config, sodar_url=toml_config.get("global", {}).get("sodar_server_url")
            )
        if not self.config.sodar_api_token:
            self.config = attr.evolve(
                self.config, sodar_api_token=toml_config.get("global", {}).get("sodar_api_token")
            )

        logger.info("Starting cubi-tk sodar pull-raw-data")
        logger.info("  config: %s", self.config)

        out_path = Path(self.config.output_dir)
        if not out_path.exists():
            out_path.mkdir(parents=True)

        investigation = api.investigations.get(
            sodar_url=self.config.sodar_url,
            sodar_api_token=self.config.sodar_api_token,
            project_uuid=self.config.project_uuid,
        )
        assay = None
        for study in investigation.studies.values():
            for assay_uuid in study.assays.keys():
                if (self.config.assay is None) and (assay is None):
                    assay = study.assays[assay_uuid]
                if (self.config.assay is not None) and (self.config.assay == assay_uuid):
                    assay = study.assays[assay_uuid]
                    logger.info("Using irods path of assay %s: %s", assay_uuid, assay.irods_path)
                    break

        library_to_folder = self._get_library_to_folder(assay)
        commands = self._build_commands(assay, library_to_folder)
        if not commands:
            logger.info("No samples to transfer with --min-batch=%d", self.config.min_batch)
            return 0
        return self._executed_commands(commands)

    def _build_commands(self, assay, library_to_folder):
        commands = []
        for k, v in library_to_folder.items():
            commands.append(["irsync", "-r"])
            if self.config.irsync_threads:
                commands[-1] += ["-N", str(self.config.irsync_threads)]
            commands[-1] += [
                "i:%s/%s" % (assay.irods_path, k),
                "%s/%s" % (self.config.output_dir, v),
            ]
        return commands

    def _executed_commands(self, commands):
        cmds_txt = "\n".join(["- %s" % " ".join(map(shlex.quote, cmd)) for cmd in commands])
        logger.info("Pull data using the following commands?\n\n%s\n", cmds_txt)
        if self.config.yes:
            answer = True
        else:
            while True:
                answer_str = input("Execute commands? [yN] ").lower()
                if answer_str.startswith("y") or answer_str.startswith("n"):
                    break
            answer = answer_str == "y"
        if not answer:
            logger.info("Answered 'no': NOT pulling files")
        else:
            for cmd in commands:
                try:
                    cmd_str = " ".join(map(shlex.quote, cmd))
                    logger.info("Executing %s", cmd_str)
                    print(cmd)
                    print(cmd_str)
                    check_call(cmd)
                except SubprocessError as e:  # pragma: nocover
                    logger.error("Problem executing irsync: %s", e)
                    return 1
        return 0

    def _get_library_to_folder(self, assay):
        isa_dict = api.samplesheets.get(
            sodar_url=self.config.sodar_url,
            sodar_api_token=self.config.sodar_api_token,
            project_uuid=self.config.project_uuid,
        )
        isa = isa_dict_to_isa_data(isa_dict)

        assays = {}
        if assay:
            if self.config.assay is None:
                logger.info("Using irods path of first assay: %s", assay.irods_path)
            assays = {k: v for (k, v) in isa.assays.items() if k == assay.file_name}
        else:  # no assay found
            logger.info("Found no assay")
            return 1

        collector = LibraryInfoCollector()
        iwalker = InvestigationTraversal(isa.investigation, isa.studies, assays)
        iwalker.run(collector)
        return {
            sample["library_name"]: sample["folder_name"]
            for sample in collector.samples.values()
            if (
                not sample["source"].get("batch_no")
                or int(sample["source"]["batch_no"]) >= self.config.min_batch
            )
            and (not sample["source"]["family"] or not sample["source"]["family"].startswith("#"))
        }


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar download-sheet``."""
    return PullRawDataCommand.setup_argparse(parser)
