"""``cubi-tk dkfz ingest-fastq``: transfer raw FASTQs into iRODS landing zone."""

import argparse
import attr
import os
import re
import typing

from pathlib import Path
from logzero import logger

from . import common
from .assay_to_frame import AssayToFrame


@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for the pull-raw-data."""

    investigation_template: str
    study_title: str
    isatab_dir: str


class DkfzPrepareIsatabCommand(common.DkfzCommandBase):
    """Implementation of dkfz prepare-isatab command for raw data."""

    command_name = "prepare-isatab"
    step_name = "raw_data"

    material_attributes_pattern = re.compile(
        "^(Characteristics|Factor Value|Comment)\\[([^\\]]+)\\]$"
    )

    def __init__(self, config: Config):
        super().__init__(config)

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        super().setup_argparse(parser)

        parser.add_argument(
            "--investigation-template",
            dest="investigation_template",
            default=os.path.dirname(__file__)
            + "/../isa_tpl/isatab-dkfz/i_Investigation_template.txt",
            help="Configuration file for the Dkfz id mapper",
        )

        parser.add_argument("--study-title", default="cancer_study", help="Short study title")
        parser.add_argument("--isatab-dir", help="Directory to store isatab files")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        args = vars(args)
        args.pop("cmd", None)
        args.pop("dkfz_cmd", None)
        print("args = {}".format(args))
        print(Config(**args))
        return cls(Config(**args)).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0
        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the creation of isatab files."""
        res = self.check_args(self.config)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk org-raw check")
        logger.info("  args: %s", self.config)

        assays = self.get_assays()

        dfs = {}
        attributes = {}
        for assay_type, assay in assays.items():
            assay_to_frame = AssayToFrame(assay)
            dfs[assay_type] = assay_to_frame.get_data_frame()
            attributes[assay_type] = assay_to_frame.attributes

        Path(self.config.isatab_dir).mkdir(mode=0o750, parents=True, exist_ok=True)
        filename = "{}/i_Investigation.txt".format(self.config.isatab_dir)
        with open(filename, "wt") as f:
            for kw, line in self._prepare_investigation(attributes).items():
                print(line, file=f)
        logger.info("Investigation file written to {}".format(filename))

        filename = "{}/s_{}.txt".format(self.config.isatab_dir, self.config.study_title)
        self._prepare_study(dfs).to_csv(filename, sep="\t", index=False)
        logger.info("Study file written to {}".format(filename))

        for assay_type, df in dfs.items():
            filename = "{}/a_{}_{}.txt".format(
                self.config.isatab_dir, self.config.study_title, assay_type
            )
            self._prepare_assay(df).to_csv(filename, sep="\t", index=False)
            logger.info("Assay {} file written to {}".format(assay_type, filename))

        filename = "{}/mapping_table.txt".format(self.config.isatab_dir)
        df = self.mapper.df
        df.to_csv(filename, sep="\t", index=False)
        logger.info("Mapping table written to {}".format(filename))

        return 0

    def _prepare_investigation(self, attributes):
        logger.info("Loaded investigation template {}".format(self.config.investigation_template))
        with open(self.config.investigation_template, "rt") as f:
            template = f.readlines()
        template = [x.strip() for x in template]

        protocols = {}
        protocol_refs = []
        for assay_type, theAttributes in attributes.items():
            for assay_protocol, parameters in theAttributes.items():
                if "parameters" not in parameters.keys():
                    continue
                if assay_protocol in protocols.keys():
                    continue
                protocol_refs.insert(0, assay_protocol)
                theParameters = [x[0] for x in parameters["parameters"]]
                theParameters.reverse()
                protocols[assay_protocol] = theParameters

        study_protocols = {
            "Study Protocol Name": protocol_refs,
            "Study Protocol Type": protocol_refs,
            "Study Protocol Type Term Accession Number": [""] * len(protocol_refs),
            "Study Protocol Type Term Source REF": [""] * len(protocol_refs),
            "Study Protocol Description": [""] * len(protocol_refs),
            "Study Protocol URI": [""] * len(protocol_refs),
            "Study Protocol Version": [""] * len(protocol_refs),
            "Study Protocol Parameters Name": [
                ";".join(protocols[protocol_ref]) for protocol_ref in protocol_refs
            ],
            "Study Protocol Parameters Name Term Accession Number": [
                "".join([";"] * (len(protocols[protocol_ref]) - 1))
                for protocol_ref in protocol_refs
            ],
            "Study Protocol Parameters Name Term Source REF": [
                "".join([";"] * (len(protocols[protocol_ref]) - 1))
                for protocol_ref in protocol_refs
            ],
            "Study Protocol Components Name": [""] * len(protocol_refs),
            "Study Protocol Components Type": [""] * len(protocol_refs),
            "Study Protocol Components Type Term Accession Number": [""] * len(protocol_refs),
            "Study Protocol Components Type Term Source REF": [""] * len(protocol_refs),
        }

        parameters = DkfzPrepareIsatabCommand._replace_lines_in_investigation(
            self.parser.schema["Investigation"]["Parameters"]
        )

        measured = {}
        for k, v in self.parser.schema["Investigation"]["Assays"].items():
            if k in attributes.keys():
                measured[k] = v
        assays = DkfzPrepareIsatabCommand._replace_lines_in_investigation(measured)

        title = self.config.study_title
        if "Study Identifier" in parameters.keys():
            parameters["Study Identifier"][0] = parameters["Study Identifier"][0].format(
                title=title
            )
        if "Study File Name" in parameters.keys():
            parameters["Study File Name"][0] = parameters["Study File Name"][0].format(title=title)
        if "Study Assay File Name" in assays.keys():
            assays["Study Assay File Name"] = [
                x.format(title=title) for x in assays["Study Assay File Name"]
            ]

        investigation = {}
        for line in template:
            try:
                id = line[: line.index("\t")]
            except ValueError:
                id = line
            if id in parameters.keys():
                line = id + "\t" + "\t".join(parameters[id])
            if id in study_protocols.keys():
                line = id + "\t" + "\t".join(study_protocols[id])
            if id in assays.keys():
                line = id + "\t" + "\t".join(assays[id])
            investigation[id] = line

        return investigation

    def _prepare_study(self, dfs):
        samples = None
        for assay_type, df in dfs.items():
            columns = list(df.columns)
            i = columns.index("Sample Name")
            i += 1
            while DkfzPrepareIsatabCommand.material_attributes_pattern.match(columns[i]):
                i += 1
            if samples is None:
                samples = df.iloc[:, :i].drop_duplicates()
            else:
                samples = samples.append(df.iloc[:, :i].drop_duplicates(), ignore_index=True)
        samples = samples.drop_duplicates().fillna("")
        return samples.sort_values(by="Sample Name")

    def _prepare_assay(self, df):
        columns = list(df.columns)
        i = columns.index("Sample Name")
        j = i + 1
        while DkfzPrepareIsatabCommand.material_attributes_pattern.match(columns[j]):
            j += 1
        k = list(df.columns).index("Raw Data File")
        assay = df.iloc[:, list([i]) + list(range(j, k))]
        assay = assay.drop_duplicates().fillna("")
        return assay.sort_values(by="Library Name")

    @staticmethod
    def _replace_lines_in_investigation(parameter_list, return_names=False):
        parameters = {}
        parameter_names = []
        for name, values in parameter_list.items():
            parameter_names.append(name)
            for param_value in values:
                if not param_value["Entry"] in parameters.keys():
                    parameters[param_value["Entry"]] = {}
                parameters[param_value["Entry"]][name] = param_value["Value"]
        if return_names:
            return parameter_names
        replacement_lines = {}
        for parameter, values in parameters.items():
            line = []
            for name in parameter_names:
                if name in values.keys():
                    line.append(str(values[name]))
            replacement_lines[parameter] = line
        return replacement_lines


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk dkfz prepare-isatab``."""
    return DkfzPrepareIsatabCommand.setup_argparse(parser)
