"""``cubi-tk sea-snap write-sample-info``: write sample info file.

More Information
----------------

- Also see ``cubi-tk sea-snap`` :ref:`cli_main <CLI documentation>` and ``cubi-tk sea-snap write-sample-info --help`` for more information.
- `Sea-snap Pipeline GitLab Project <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/sea-snap>`__.
- `Altamisa Documentation <https://altamisa.readthedocs.io/en/latest/index.html#>`__.
"""

import argparse
import difflib
import os
import shutil
import tempfile
import re
import sys
import typing
from collections import namedtuple
from pathlib import Path
from glob import glob
import yaml
from uuid import UUID

import icdiff
from logzero import logger
from termcolor import colored
import pandas as pd
from altamisa.isatab import AssayReader
import requests

#: The URL template to use.
from ..common import get_terminal_columns

URL_TPL = "%(sodar_url)s/samplesheets/api/remote/get/%(project_uuid)s/%(api_key)s?isa=1"


def strip(x):
    if hasattr(x, "strip"):
        return x.strip()
    else:
        return x


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sea-snap write-sample-info``."""
    parser.add_argument("--hidden-cmd", dest="sea_snap_cmd", default=run, help=argparse.SUPPRESS)

    parser.add_argument(
        "--allow-overwrite",
        default=False,
        action="store_true",
        help="Allow to overwrite output file, default is not to allow overwriting output file.",
    )

    parser.add_argument(
        "--dry-run",
        default=False,
        action="store_true",
        help="Perform a dry run, i.e., don't change anything only display change, implies '--show-diff'.",
    )
    parser.add_argument(
        "--show-diff",
        default=False,
        action="store_true",
        help="Show change when creating/updating sample sheets.",
    )
    parser.add_argument(
        "--show-diff-side-by-side",
        default=False,
        action="store_true",
        help="Show diff side by side instead of unified.",
    )

    parser.add_argument(
        "--from-file",
        default=None,
        type=argparse.FileType("rt"),
        help="Path to yaml file to convert to tsv or tsv to yaml. Not used, if not specified.",
    )

    parser.add_argument(
        "--isa-assay",
        default=None,
        type=argparse.FileType("rt"),
        help="Path to ISA assay file. Not used, if not specified.",
    )

    group_isa = parser.add_argument_group("pull ISA files")
    group_isa.add_argument(
        "--project_uuid",
        default=False,
        help="If set pull ISA files from SODAR. UUID of project to pull from.",
    )

    group_isa.add_argument(
        "--output_folder", default="ISA_files/", help="Output folder path to store ISA files."
    )

    group_isa.add_argument(
        "--overwrite-isa",
        default=False,
        action="store_true",
        help="Allow to overwrite output file, default is not to allow overwriting output file.",
    )

    group_sodar = parser.add_argument_group("SODAR-related")
    group_sodar.add_argument(
        "--sodar-url",
        default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
        help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
    )
    group_sodar.add_argument(
        "--sodar-auth-token",
        default=os.environ.get("SODAR_AUTH_TOKEN", None),
        help="Authentication token when talking to SODAR.  Defaults to SODAR_AUTH_TOKEN environment variable.",
    )

    parser.add_argument(
        "in_path_pattern",
        help="Path pattern to use for extracting input file information. See "
        "https://cubi-gitlab.bihealth.org/CUBI/Pipelines/sea-snap/blob/master/"
        "documentation/prepare_input.md#fastq-files-folder-structure.",
    )

    parser.add_argument(
        "output_file",
        nargs="?",
        default="sample_info.yaml",
        type=lambda f: sys.stdout if f == "-" else argparse.FileType("at+")(f),
        help="Filename ending with '.yaml' or '.tsv'. default: sample_info.yaml.",
    )


def check_args(args) -> int:
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    any_error = False

    # if set for pulling ISA files
    if args.project_uuid:
        # Check presence of SODAR URL and auth token.
        if not args.sodar_auth_token:  # pragma: nocover
            logger.error(
                "SODAR authentication token is empty.  Either specify --sodar-auth-token, or set "
                "SODAR_AUTH_TOKEN environment variable"
            )
            any_error = True
        if not args.sodar_url:  # pragma: nocover
            logger.error("SODAR URL is empty. Either specify --sodar-url, or set SODAR_URL.")
            any_error = True

        # Check output file presence vs. overwrite allowed.
        if (
            hasattr(args.output_folder, "name") and Path(args.output_folder).exists()
        ):  # pragma: nocover
            if not args.overwrite_isa:
                logger.error(
                    "The output folder %s already exists but --allow-overwrite not given.",
                    args.output_folder,
                )
                any_error = True
            else:
                logger.warning(
                    "Output folder %s exists but --allow-overwrite given.", args.output_folder
                )

        # Check UUID syntax.
        try:
            val: typing.Optional[str] = str(UUID(args.project_uuid))
        except ValueError:  # pragma: nocover
            val = None
        finally:
            if args.project_uuid != val:  # pragma: nocover
                logger.error("Project UUID %s is not a valid UUID", args.project_uuid)
                any_error = True

    # Check options --isa-assay vs. --from_file
    if args.from_file:
        logger.warning("Option --from_file is set, in-path-pattern will be ignored.")
        if args.isa_assay:
            logger.error("Both --isa-assay and --from_file are set, choose one.")
            any_error = True

    # Check output file presence vs. overwrite allowed.
    if (
        hasattr(args.output_file, "name")
        and args.output_file.name != "<stdout>"
        and Path(args.output_file.name).stat().st_size != 0
    ):  # pragma: nocover
        if not args.allow_overwrite:
            logger.error(
                "The output path %s already exists but --allow-overwrite not given.",
                args.output_file.name,
            )
            any_error = True
        else:
            logger.warning("Output path %s exists but --allow-overwrite given.", args.output_file)

    return int(any_error)


class Bunch:
    def __init__(self, **kw):
        setattr(self, "__dict__", kw)


class SampleInfoTool:
    """ Tool to generate a sample info file before running sea-snap mapping """

    allowed_read_extensions = [".fastq", ".fastq.gz", ".fq", ".fq.gz"]

    def __init__(self, args):

        self.in_path_pattern = args.in_path_pattern

        self.wildcard_constraints = self._prepare_in_path_pattern()

        self.args = args

        self.sample_info = {}

    # ---------------------------------------------------- helper methods ----------------------------------------------------#

    def _prepare_in_path_pattern(self):
        """ read and remove wildcard constraints from in_path_pattern """
        wildcards = re.findall("{([^{}]+)}", self.in_path_pattern)
        wildcard_constraints = {}
        for wildcard in wildcards:
            comp = wildcard.split(",")
            if len(comp) > 1:
                wildcard_constraints[comp[0]] = comp[1]
                self.in_path_pattern = self.in_path_pattern.replace(wildcard, comp[0])
        return wildcard_constraints

    def _wildc_replace(self, matchobj):
        """ method used with re.sub to generate match pattern from path pattern """
        wildc_name = matchobj.group(1)
        if wildc_name in self.wildcard_constraints:
            return "({})".format(self.wildcard_constraints[wildc_name].replace("//", "/"))
        elif wildc_name == "extension":
            return "([^}/]+)"
        else:
            return "([^}./]+)"

    def _get_wildcard_values_from_read_input(self, unix_style=True):
        """ go through files in input path and get values matching the wildcards """
        glob_pattern = re.sub("{[^}./]+}", "*", self.in_path_pattern)
        wildcards = re.findall("{([^}./]+)}", self.in_path_pattern)
        match_pattern = re.sub(
            "\\\\{([^}./]+)\\\\}", self._wildc_replace, re.escape(self.in_path_pattern)
        )
        input_files = glob(glob_pattern + ("*" if glob_pattern[-1] != "*" else ""), recursive=True)
        if unix_style:
            match_pattern = re.sub(r"\\\*\\\*", "[^{}]*", match_pattern)
            match_pattern = re.sub(r"(?<!\[\^{}\]\*)\\\*", "[^{}./]*", match_pattern)

        logger.info("\ninput files:\n%s", "\n".join(input_files))
        logger.info("\nmatch pattern:\n%s", match_pattern)

        wildcard_values = {w: [] for w in wildcards}
        wildcard_values["read_extension"] = []
        for inp in input_files:
            self._get_wildcard_values_from_file_path(wildcard_values, inp, wildcards, match_pattern)

        return wildcard_values

    def _get_wildcard_values_from_file_path(
        self, wildcard_values, filename, wildcards, match_pattern
    ):
        """ get values matching wildcards from given file path """
        matches = re.match(match_pattern, filename).groups()
        assert len(matches) == len(wildcards)

        seen = set()
        for index, wildc in enumerate(wildcards):
            if wildc not in seen:
                wildcard_values[wildc].append(matches[index])
                seen.add(wildc)

        wildcard_values["read_extension"].append(
            filename.replace(re.match(match_pattern, filename).group(0), "")
        )

        return wildcard_values

    def _get_wildcard_combinations(self, wildcard_values):
        """ go through wildcard values and get combinations """

        combinations = []
        WildcardComb = namedtuple("WildcardComb", wildcard_values)
        wildcard_comb_num = len(wildcard_values["sample"])
        assert all([len(val) == wildcard_comb_num for val in wildcard_values.values()])
        for index in range(wildcard_comb_num):
            combinations.append(
                WildcardComb(**{key: wildcard_values[key][index] for key in wildcard_values})
            )
        return combinations

    def _convert_str_entries_to_lists(self, key="paired_end_extensions"):
        """ for importing lists from table entries """
        for smpl_info in self.sample_info.values():
            smpl_info[key] = [
                s.replace("'", "").replace('"', "")
                for s in re.findall(r"[^\[\]\s,]+", smpl_info[key])  # noqa: W605
            ]

    def _add_info_fields(self, add_dict):
        """ add fields from add_dict to self.sample_info if they are not already present """
        for sample, fields in add_dict.items():
            if sample in self.sample_info:
                s_info = self.sample_info[sample]
                for f_key, f_val in fields.items():
                    if f_key not in s_info:
                        s_info[f_key] = f_val

    # ---------------------------------------------------- access methods ----------------------------------------------------#

    def update_sample_info(self, library_default="unstranded", add=False):
        """
        fill mandatory info about sample by searching the input path specified in the config file.
        attention: stranded is initially set to library_default for all samples!
        This information has to be edited manually in table or yaml, if libraries were prepared differently.
        :param library_default:  options: ["unstranded", "forward", "reverse"]
        """
        wildcard_values = self._get_wildcard_values_from_read_input()
        wildcard_combs = [
            comb
            for comb in self._get_wildcard_combinations(wildcard_values)
            if comb.read_extension in self.allowed_read_extensions
        ]
        logger.info(
            "\nextracted combinations:\n%s",
            "\n".join("\t".join(i) for i in [wildcard_combs[0]._fields] + wildcard_combs),
        )

        sample_info = {}
        for comb in wildcard_combs:
            if comb.sample not in sample_info:
                sample_info[comb.sample] = {
                    "stranded": library_default,
                    "read_extension": comb.read_extension,
                }
                sample_info[comb.sample]["paired_end_extensions"] = [getattr(comb, "mate", "")]
            elif hasattr(comb, "mate"):
                paired_end_ext = getattr(comb, "mate", "")
                paired_end_ext_lst = sample_info[comb.sample]["paired_end_extensions"]
                if paired_end_ext_lst == [""]:
                    raise ValueError(
                        "Error compiling sample information: sample {} has names with and without paired end extensions".format(
                            comb.sample
                        )
                    )
                if paired_end_ext not in paired_end_ext_lst:
                    paired_end_ext_lst.append(paired_end_ext)
                    paired_end_ext_lst.sort()
        if add:
            # add missing fields
            self._add_info_fields(sample_info)
        else:
            # overwrite
            self.sample_info = sample_info

    def write_table(self, filename, sep="\t"):
        """
        write sample info to table
        """
        tab = pd.DataFrame(self.sample_info).transpose()
        tab.to_csv(filename, sep=sep)

    def read_table(self, filename, sep="\t"):
        """
        read sample info from table
        """
        tab = pd.read_csv(filename, sep=sep, index_col=0).transpose()
        self.sample_info = tab.to_dict()
        self._convert_str_entries_to_lists("paired_end_extensions")

    def write_yaml(self, filename):
        """
        write sample info to yaml
        """
        if not isinstance(filename, str):
            yaml.dump({"sample_info": self.sample_info}, filename, default_flow_style=False)
        else:
            with open(filename, "w") as f:
                yaml.dump({"sample_info": self.sample_info}, f, default_flow_style=False)

    def read_yaml(self, filename):
        """
        read sample info from yaml
        """
        with (open(filename, "r") if isinstance(filename, str) else filename) as f:
            try:
                self.sample_info = yaml.safe_load(f)["sample_info"]
            except yaml.YAMLError as exc:
                print(exc)

    def parse_isatab(self):
        """
        parse sample info from ISA-tab table
        """
        logger.info("Parsing ISA-tab...")
        logger.info("Read assay file: %s", self.args.isa_assay.name)

        # read assay
        assay = AssayReader.from_stream("S1", "A1", self.args.isa_assay).read()

        # extract relevant fields
        dummy = Bunch(type="", protocol_ref="")
        sample_info = {}
        arc_map = {a.tail: a.head for a in assay.arcs}
        for m in assay.materials.values():
            if m.type == "Sample Name":
                sample_name = m.name
                if sample_name not in sample_info:
                    sample_info[sample_name] = {}
                key = m.unique_name
                # breakpoint()
                while key in arc_map:
                    key = arc_map[key]
                    if re.match(
                        "Library construction [a-z]*RNA[-_]?[Ss]eq",
                        assay.processes.get(key, dummy).protocol_ref,
                    ):
                        self._parse_isatab_library_construction(
                            assay, key, sample_info, sample_name
                        )

                    elif re.match(
                        "Nucleic acid sequencing [a-z]*RNA[-_]?[Ss]eq",
                        assay.processes.get(key, dummy).protocol_ref,
                    ):
                        self._parse_isatab_sequencing(assay, key, sample_info, sample_name)

        logger.info("Samples in ISA assay:\n%s", ", ".join(sample_info))
        logger.debug(sample_info)

        self.sample_info = sample_info

    def _parse_isatab_sequencing(self, assay, key, sample_info, sample_name):
        for p in assay.processes[key].parameter_values:
            if p.name == "Instrument model":
                sample_info[sample_name]["instrument"] = ",".join(p.value)
            if p.name == "Sequencing kit":
                sample_info[sample_name]["sequencing_kit"] = ",".join(p.value)
            elif p.name == "Platform" and "instrument" not in sample_info[sample_name]:
                sample_info[sample_name]["instrument"] = ",".join(p.value)
            elif p.name == "Target read length":
                sample_info[sample_name]["read_length"] = p.value

    def _parse_isatab_library_construction(self, assay, key, sample_info, sample_name):
        for p in assay.processes[key].parameter_values:
            if p.name == "Library layout":
                sample_info[sample_name]["paired"] = p.value == "PAIRED"
            elif p.name == "Library strand-specificity":
                sample_info[sample_name]["stranded"] = p.value.lower()
            elif p.name == "Library name mRNA" and p.value[0]:
                if "lib_types" not in sample_info[sample_name]:
                    sample_info[sample_name]["lib_types"] = []
                sample_info[sample_name]["lib_types"].append((",".join(p.value), "Gene Expression"))
            elif p.name == "Library name sample tag" and p.value[0]:
                if "lib_types" not in sample_info[sample_name]:
                    sample_info[sample_name]["lib_types"] = []
                sample_info[sample_name]["lib_types"].append(
                    (",".join(p.value), "Antibody Capture")
                )


def pull_isa(args) -> typing.Optional[int]:
    """Pull ISA files"""

    # Query investigation JSON from API.
    url = URL_TPL % {
        "sodar_url": args.sodar_url,
        "project_uuid": args.project_uuid,
        "api_key": args.sodar_auth_token,
    }
    logger.info("Fetching %s", url)
    r = requests.get(url)
    r.raise_for_status()
    all_data = r.json()

    isa_dir = Path(args.output_folder)

    path = isa_dir / all_data["investigation"]["path"]
    path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Writing ISA files to %s", str(path.parent))

    with open(str(path), "w") as f:
        print(all_data["investigation"]["tsv"], file=f)

    for study in all_data["studies"]:
        with open(str(path.with_name(study)), "w") as f:
            print(all_data["studies"][study]["tsv"], file=f)

    for assay in all_data["assays"]:
        with open(str(path.with_name(assay)), "w") as f:
            print(all_data["assays"][assay]["tsv"], file=f)

    logger.debug("Done pulling ISA files.")


def write_sample_info(args, sample_info_file) -> typing.Optional[int]:
    """Write sample info to ``sample_info_file``."""

    sit = SampleInfoTool(args)

    if args.from_file:
        if args.from_file.name.split(".")[-1] == "tsv":
            sit.read_table(args.from_file)
        elif args.from_file.name.split(".")[-1] == "yaml":
            sit.read_yaml(args.from_file)
    else:
        add = False
        if args.project_uuid:
            assay_files = glob(os.path.join(args.output_folder, "a_*"))
            if not assay_files:
                logger.error("Project uuid set, but no assay files were downloaded!")
                return 1
            elif len(assay_files) > 1:
                logger.info(
                    "Several assay files were pulled, choose one.\n%s",
                    "\n".join(f"[{i}] {assay_files[i]}" for i in range(len(assay_files))),
                )
                args.isa_assay = open(assay_files[int(input("choice: "))], "rt")
            else:
                logger.info("Using pulled assay file %s.", assay_files[0])
                args.isa_assay = open(assay_files[0], "rt")
        if args.isa_assay:
            sit.parse_isatab()
            add = True
        sit.update_sample_info(add=add)

    if args.output_file != sys.stdout and args.output_file.name.split(".")[-1] == "tsv":
        sit.write_table(sample_info_file)
    else:
        sit.write_yaml(sample_info_file)

    logger.debug("Done writing temporary file.")
    return None


def run(
    args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
) -> typing.Optional[int]:
    """Run ``cubi-tk sea-snap write-sample-info``."""
    res: typing.Optional[int] = check_args(args)
    if res:  # pragma: nocover
        return res

    if args.project_uuid:
        logger.info("Starting to pull ISA files...")
        logger.info("  Args: %s", args)

        pull_isa(args)

    logger.info("Starting to write sample info...")
    logger.info("  Args: %s", args)

    with tempfile.NamedTemporaryFile(mode="w+t") as sample_info_file:
        # Write sample info to temporary file.
        res = write_sample_info(args, sample_info_file)
        if res:  # pragma: nocover
            return res

            # Compare sample info with output if exists and --show-diff given.
        if args.show_diff:
            if os.path.exists(args.output_file.name):
                with open(args.output_file.name, "rt") as inputf:
                    old_lines = inputf.read().splitlines(keepends=False)
            else:
                old_lines = []
            sample_info_file.seek(0)
            new_lines = sample_info_file.read().splitlines(keepends=False)

            is_diff = False
            if not args.show_diff_side_by_side:
                lines = difflib.unified_diff(
                    old_lines,
                    new_lines,
                    fromfile=args.output_file.name,
                    tofile=args.output_file.name,
                )
                for line in lines:
                    is_diff = True
                    line = line[:-1]
                    if line.startswith(("+++", "---")):
                        print(colored(line, color="white", attrs=("bold",)), file=sys.stdout)
                    elif line.startswith("@@"):
                        print(colored(line, color="cyan", attrs=("bold",)), file=sys.stdout)
                    elif line.startswith("+"):
                        print(colored(line, color="green", attrs=("bold",)), file=sys.stdout)
                    elif line.startswith("-"):
                        print(colored(line, color="red", attrs=("bold",)), file=sys.stdout)
                    else:
                        print(line, file=sys.stdout)
            else:
                cd = icdiff.ConsoleDiff(cols=get_terminal_columns(), line_numbers=True)
                lines = cd.make_table(
                    old_lines,
                    new_lines,
                    fromdesc=args.output_file.name,
                    todesc=args.output_file.name,
                    context=True,
                    numlines=3,
                )
                heading = next(lines)

                def show_line(line):
                    if hasattr(sys.stdout, "buffer"):
                        sys.stdout.buffer.write(line.encode("utf-8"))
                    else:
                        sys.stdout.write(line)

                for line in lines:
                    if not is_diff:
                        show_line("%s\n" % heading)
                    is_diff = True
                    show_line("%s\n" % line)

            sys.stdout.flush()
            if not is_diff:
                logger.info("File %s not changed, no diff...", args.output_file.name)

        # Write to output file if not --dry-run is given
        if hasattr(args.output_file, "name") and args.dry_run:
            logger.warning("Not changing %s as we are in --dry-run mode", args.output_file.name)
        else:
            if hasattr(args.output_file, "name") and args.output_file.name != "<stdout>":
                action = (
                    "Overwriting" if Path(args.output_file.name).stat().st_size != 0 else "Creating"
                )
                logger.info("%s %s", action, args.output_file.name)
            if args.output_file != "<stdout>":
                sample_info_file.seek(0)
            if hasattr(args.output_file, "name") and args.output_file.name != "<stdout>":
                args.output_file.seek(0)
                args.output_file.truncate()
            shutil.copyfileobj(sample_info_file, args.output_file)

        logger.warning(
            "in_path_pattern: %s --> Use the same in your mapping_config.yaml!",
            args.in_path_pattern,
        )

    return None
