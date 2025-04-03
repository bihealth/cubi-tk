"""``cubi-tk sodar update-samplesheet``: update ISA-tab with new values and/or entries."""

import argparse
from collections import defaultdict
from io import StringIO
import re
from typing import Iterable, Optional

from loguru import logger
import pandas as pd
from ruamel.yaml import YAML

from cubi_tk.parsers import print_args

from ..exceptions import ParameterException
from ..parse_ped import parse_ped
from ..sodar_api import SodarApi

REQUIRED_COLUMNS = ["Source Name", "Sample Name", "Extract Name"]
REQUIRED_IF_EXISTING_COLUMNS = ["Library Name"]
ISA_NON_SETTABLE = ["Term Source REF", "Term Accession Number", "Protocol REF"]

# Type definition:
IsaColumnDetails = dict[str, list[tuple[str, str]]]
# dict of short or long column names to list of tuples of (original column name, table name)
# long names include Column type, i.e. "Characteristics[organism]"
# short names are without the type, i.e. "organism"
# original column names are the names as they are returned by pandas.read_csv, this
#   may include numerical suffixes for duplicate column names (e.g. "Extract Name.1")


sheet_default_config_yaml = """
ped_defaults: &ped
    family_id: Family-ID
    name: Sample-ID
    father_name: Paternal-ID
    mother_name: Maternal-ID
    sex: Sex
    disease: Phenotype

Modellvorhaben: &MV
    sample_fields:
        - Family-ID
        - Analysis-ID
        - Paternal-ID
        - Maternal-ID
        - Sex
        - Phenotype
        - Individual-ID
        - Probe-ID
        - Barcode
        - Barcode-Name
    field_column_mapping:
        Family-ID: Family
        Analysis-ID: Source Name
        Paternal-ID: Father
        Maternal-ID: Mother
        Sex: Sex
        Phenotype: Disease status
        Individual-ID: Individual-ID
        Probe-ID: Probe-ID
        Barcode: Barcode sequence
        Barcode-Name: Barcode name
    dynamic_columns:
        Sample Name: "{Analysis-ID}-N1"
        Extract Name: "{Analysis-ID}-N1-DNA1"
        Library Name: "{Analysis-ID}-N1-DNA1-WGS1"
    ped_to_sampledata:
        <<: *ped
        name: Analysis-ID

#alias
MV:
    <<: *MV

MV-barcodes:
    <<: *MV
    sample_fields:
        - Individual-ID
        - Probe-ID
        - Analysis-ID
        - Barcode
        - Barcode-Name

germline-sheet:
    sample_fields:
        - Family-ID
        - Sample-ID
        - Paternal-ID
        - Maternal-ID
        - Sex
        - Phenotype
    field_column_mapping:
        Family-ID: Family
        Sample-ID: Source Name
        Paternal-ID: Father
        Maternal-ID: Mother
        Sex: Sex
        Phenotype: "Disease status"
    dynamic_columns:
        Sample Name: "{Sample-ID}-N1"
        Extract Name: "{Sample-ID}-N1-DNA1"
        Library Name: "{Sample-ID}-N1-DNA1-WGS1"
    ped_to_sampledata:
        <<: *ped
"""

# ruamel round-trip loader uses ordered dicts
yaml = YAML(typ="rt")
sheet_default_config = yaml.load(sheet_default_config_yaml)


def orig_col_name(col_name: str) -> str:
    """Return the original column name without any suffixes."""
    # Suffixes are added to duplicate ISAtab column names by pandas.read_csv
    return re.sub(r"\.[0-9]+$", "", col_name)


class UpdateSamplesheetCommand:
    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments for ``update-samplesheet`` command."""

        parser.add_argument(
            "--hidden-cmd", dest="sodar_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        sample_group = parser.add_argument_group(
            "Sample Definitions",
        )

        sample_group.add_argument(
            "-s",
            "--sample-data",
            nargs="+",
            action="append",
            help="Sample specific (meta)data to be added to the samplesheet. Each argument describes one sample. "
            "The number of arguments needs to match the fields defined via `--defaults` or `--sample-fields`"
            "Can be combined with `--ped` to give additional columns or additional samples. "
            "After joining both sets all fields must have a defined value.",
        )

        sample_group.add_argument(
            "-d",
            "--defaults",
            choices=sheet_default_config.keys(),
            default="germline-sheet",
            help="Choose a predefined default for field definitions (used by `--sample-data`) and `--ped-name-mapping` (used by `--ped`). "
            "Defaults to 'germline-sheet'. "
            "Field definitions are as follows:\n"
            "\n".join(
                [
                    f"{default} ({len(settings['sample_fields'])} fields, ped-Name: {settings['ped_to_sampledata']['name']}):\n"
                    f"{', '.join(settings['sample_fields'])}"
                    for default, settings in sheet_default_config.items()
                    if default != "ped_defaults"
                ]
            ),
        )
        sample_group.add_argument(
            "--sample-fields",
            nargs="+",
            help="Manual definition of fields for `--sample-data`, overrides values set by `--sheet-type`. "
            "The field names need to match column names in the samplesheet, overrides `--defaults`."
            "If values are given as 'FieldName=ISAColumn', the Fields can be matched to ped columns, while also "
            "being matched to a differently named column in the ISA samplesheet.",
        )
        sample_group.add_argument(
            "-p",
            "--ped",
            help="Ped file with sample data to be added to the samplesheet, the default ped columns are mapped to these "
            "fields: Family-ID, Sample-ID(*), Paternal-ID, Maternal-ID, Sex, PhenoType. "
            "This mapping can be changed via `--ped-field-mapping` or the `--defaults` option.\n"
            "Can be extended by `--sample-data` to give additional columns or additional samples. "
            "After joining both sets all fields must have a defined value.",
        )
        sample_group.add_argument(
            "--ped-field-mapping",
            nargs=2,
            action="append",
            metavar=("PED_COLUMN", "SAMPLE_FIELD"),
            help="Manually define how ped columns are mapped to sample data fields or ISA columns. "
            "The following ped names columns should be used: "
            ", ".join(sheet_default_config["ped_defaults"].keys()) + "\n"
            "Overwrites value set for 'name' by `--defaults`. "
            "SAMPLE_FIELD can also be a column name of the ISA samplesheet.",
        )

        sample_group.add_argument(
            "--dynamic-column",
            nargs=2,
            action="append",
            metavar=("COLUMN", "FORMAT_STR"),
            help="Dynamically fill columns in the ISA sheet based on other columns."
            "Use this option if some columns with sample-sepcific Data can be derived from other columns."
            "FORMAT_STR can contain other columns as placeholders, i.e.: '{Source Name}-N1' for a new Sample Name."
            "Note: only columns from the ped/sampledata can be used as placeholders.",
        )

        parser.add_argument(
            "-a",
            "--metadata-all",
            nargs=2,
            action="append",
            metavar=("COLUMN", "VALUE"),
            help="Set metadata value for all samples added to the samplesheet. Specify column name and value. "
            "Can be used multiple times.",
        )

        # FIXME: provide option to read ISA &/ tsv file (ignoring all other sample data & fields)
        # group_samples.add_argument(
        #     "--tsv", help="Tabular file with sample data"
        # )

        parser.add_argument(
            "--overwrite",
            default=False,
            action="store_true",
            help="Allow overwriting of existing values for samples already defined in the samplesheet",
        )

        parser.add_argument(
            "--no-autofill",
            action="store_true",
            help="Do not automatically fill values for non-specified metadata columns that have a single unique value "
            "in the existing samplesheet. Note: ontology terms & references will never be autofilled.",
        )

        parser.add_argument(
            "--snappy-compatible",
            action="store_true",
            help="Transform IDs so they are compatible with snappy processing "
            "(replaces '-' with '_' in required ISA fields).",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def execute(self) -> Optional[int]:
        """Execute the command."""

        if self.args.overwrite:
            logger.warning(
                "Existing values in the ISA samplesheet may get overwritten, there will be no checks or further warnings."
            )

        # Get samplehseet from SODAR API
        sodar_api = SodarApi(self.args, with_dest=True)
        print_args(self.args)
        isa_data = sodar_api.get_samplesheet_export()
        investigation = isa_data["investigation"]["tsv"]
        study_key = list(isa_data["studies"].keys())[0]
        study = pd.read_csv(StringIO(isa_data["studies"][study_key]["tsv"]), sep="\t", dtype=str)
        assay_key = list(isa_data["assays"].keys())[0]
        assay = pd.read_csv(StringIO(isa_data["assays"][assay_key]["tsv"]), sep="\t", dtype=str)
        isa_names = self.gather_ISA_column_names(study, assay)

        # Check that given sample-data field names can be used
        sample_fields_mapping = self.parse_sampledata_args(isa_names)

        # Collect ped & sample data, check that they can be combined
        samples = self.collect_sample_data(isa_names, sample_fields_mapping, self.args.snappy_compatible)

        # add metadata values to samples
        if self.args.metadata_all:
            samples = samples.assign(**dict(self.args.metadata_all))

        # Match sample data to ISA columns and get new study & assay dataframes
        study_new, assay_new = self.match_sample_data_to_isa(
            samples, isa_names, sample_fields_mapping
        )
        req_cols = set(REQUIRED_COLUMNS) | (
            set(REQUIRED_IF_EXISTING_COLUMNS) & set(isa_names.keys())
        )
        colset = set(study.columns.tolist() + assay.columns.tolist())
        if not req_cols.issubset(colset):
            missing_cols = req_cols - colset
            raise ValueError(f"Missing required columns in sample data: {', '.join(missing_cols)}")

        # Update ISA tables with new data
        study_final = self.update_isa_table(
            study, study_new, self.args.overwrite, self.args.no_autofill
        )
        assay_final = self.update_isa_table(
            assay, assay_new, self.args.overwrite, self.args.no_autofill
        )

        # Write new samplesheet to tsv strings, then upload via API
        study_tsv = study_final.to_csv(
            sep="\t", index=False, header=list(map(orig_col_name, study_final.columns))
        )
        assay_tsv = assay_final.to_csv(
            sep="\t", index=False, header=list(map(orig_col_name, assay_final.columns))
        )

        files_dict = {
            "file_investigation": (isa_data["investigation"]["path"], investigation),
            "file_study": (study_key, study_tsv),
            "file_assay": (assay_key, assay_tsv),
        }
        ret = sodar_api.post_samplesheet_import(
            files_dict
        )
        return ret

    def parse_sampledata_args(self, isa_names: IsaColumnDetails) -> dict[str, str]:
        """Build a dict to collect and map the names for ped or sampledata [-s] fields to ISA column names."""

        # Some samples are defined, either through ped or sample data
        if not self.args.ped and not self.args.sample_data:
            raise ValueError(
                "No sample data provided. Please provide either a ped file (`--ped`) or sample data (`-s`)."
            )

        # Base field name mapping (also used by ped)
        sample_field_mapping = sheet_default_config[self.args.defaults]["field_column_mapping"]

        # Sample data, if given, needs to match the defined fields (from default or sample_fields)
        if not self.args.sample_data:
            return sample_field_mapping

        if self.args.sample_fields:
            sample_field_mapping.update(
                {
                    k: (v or k)
                    for (k, _, v) in (s.partition("=") for s in self.args.sample_fields)
                }
            )
            n_fields = len(self.args.sample_fields)
        else:
            n_fields = len(sheet_default_config[self.args.defaults]["sample_fields"])

        incorrect_n = [sample for sample in self.args.sample_data if len(sample) != n_fields]
        if incorrect_n:
            msg = (
                f"The number of entries for some samples does not match the number of fields defined ({n_fields}):\n"
                "\n".join(
                    [
                        f"{len(sample)} instead of {n_fields} values: {', '.join(map(str, sample))}"
                        for sample in incorrect_n
                    ]
                )
            )
            raise ValueError(msg)

        unknown_fields = [
            tup for tup in sample_field_mapping.items() if tup[1] not in isa_names.keys()
        ]
        if unknown_fields:
            msg = (
                "Some columns for sample field mapping are not defined in the ISA samplesheet: "
                + ", ".join([f"{col} (from {field})" for field, col in unknown_fields])
            )
            raise NameError(msg)

        return sample_field_mapping

    def gather_ISA_column_names(self, study: pd.DataFrame, assay: pd.DataFrame) -> IsaColumnDetails:
        isa_regex = re.compile(r"(Characteristics|Parameter Value|Comment)\[(.*?)]")
        study_cols = study.columns.tolist()
        assay_cols = assay.columns.tolist()

        isa_short_names = [orig_col_name(isa_regex.sub(r"\2", x)) for x in (study_cols + assay_cols)]
        isa_long_names = list(map(orig_col_name, study_cols + assay_cols))
        isa_names_unique = study_cols + assay_cols
        isa_table = ["study"] * len(study_cols) + ["assay"] * len(assay_cols)

        out = defaultdict(list)
        for short, long, uniq, table in zip(
            isa_short_names, isa_long_names, isa_names_unique, isa_table, strict=True
        ):
            out[short].append((uniq, table))
            out[long].append((uniq, table))

        for col in ISA_NON_SETTABLE:
            if col in out:
                del out[col]

        # do NOT retain defaultdict class
        return dict(out)

    def get_dynamic_columns(
        self,
        existing_names: Iterable[str],
        isa_names: Iterable[str],
    ) -> dict[str, str]:
        dynamic_cols = {}
        re_format_names = re.compile(r"\{(.*?)}")
        if self.args.dynamic_column:
            for col, format_str in self.args.dynamic_column:
                missing_deps = (
                    set(re_format_names.findall(format_str))
                    - set(existing_names)
                    - set(dynamic_cols)
                )
                if missing_deps:
                    raise ValueError(
                        f"Dynamic column '{col}' depends on non-existing columns: {', '.join(missing_deps)}"
                    )
                if col not in isa_names:
                    raise ValueError(f"Column '{col}' is not defined in the ISA samplesheet.")
                dynamic_cols[col] = format_str
        elif sheet_default_config[self.args.defaults]["dynamic_columns"]:
            dynamic_cols = sheet_default_config[self.args.defaults]["dynamic_columns"]
            # Hardcoded dep check for defaults: see if 'Library Name' is actually defined
            if "Library Name" in dynamic_cols and "Library Name" not in isa_names:
                logger.warning(
                    'Skipping "Library Name" dynamic column, as it is not used in the ISA samplesheet.'
                )
                del dynamic_cols["Library Name"]

        # #Sample Name needs to be set before other assay columns, so ensure it goes first
        # if 'Sample Name' in dynamic_cols:
        #     dynamic_cols.move_to_end('Sample Name', last=False)
        return dynamic_cols

    def collect_sample_data(
        self,
        isa_names: IsaColumnDetails,
        sample_field_mapping: dict[str, str],
        snappy_compatible: bool = False,
    ) -> pd.DataFrame:

        ped_mapping = self.get_ped_mapping(isa_names, sample_field_mapping)
        ped_data = self.get_ped_data(ped_mapping)
        sample_data = self.get_sample_data()
        samples = self.get_samples(ped_data, ped_mapping, sample_data)

        # Convert to snappy compatible names:
        # replace '-' with '_' in all ID columns
        if snappy_compatible:
            for col in samples.columns:
                if col.endswith("ID"):
                    samples[col] = samples[col].str.replace("-", "_")

        dynamic_cols = self.get_dynamic_columns(samples.columns, isa_names)
        for col_name, format_str in dynamic_cols.items():
            if col_name in samples.columns:
                logger.warning(f'Ignoring dynamic column defintion for "{col_name}", as it is already defined.')
                continue
            # MAYBE: allow dynamic columns to change based on fill order?
            # i.e. first Extract name = -DNA1, then -DNA1-WXS1
            samples[col_name] = samples.apply(lambda row, format_str= format_str: format_str.format(**row), axis=1)

        return samples

    def get_ped_mapping(self, isa_names, sample_field_mapping):
        ped_mapping = sheet_default_config[self.args.defaults]["ped_to_sampledata"]
        if self.args.ped_field_mapping:
            allowed_sample_col_values = sample_field_mapping.keys() | isa_names.keys()
            for ped_col, sample_col in self.args.ped_field_mapping:
                if ped_col not in ped_mapping:
                    logger.warning(f"Ped column '{ped_col}' is unknown and will be ignored.")
                    continue
                if sample_col not in allowed_sample_col_values:
                    raise ParameterException(
                        f"'{sample_col}' is neither a known sample field nor a known column of the ISA samplesheet."
                    )

                ped_mapping[ped_col] = sample_col
        return ped_mapping

    def get_ped_data(self, ped_mapping):
        if self.args.ped:
            with open(self.args.ped, "rt") as inputf:
                ped_dicts = []
                for donor in parse_ped(inputf):
                    donor_dict = {}
                    for attr_name, field in ped_mapping.items():
                        donor_dict[field] =  getattr(donor, attr_name)
                    ped_dicts.append( donor_dict)
                ped_data = pd.DataFrame(ped_dicts)
        else:
            ped_data = pd.DataFrame()
        return ped_data

    def get_sample_data(self):
        if self.args.sample_data:
            if self.args.sample_fields:
                fields = self.args.sample_fields
            else:
                fields = sheet_default_config[self.args.defaults]["sample_fields"]
            sample_data = pd.DataFrame(self.args.sample_data, columns=fields)
            # FIXME: consider consistent conversion for Sex & Phenotype values? also empty parent values?
            # ped outputs: male/female, unaffected/affected, 0
        else:
            sample_data = pd.DataFrame()
        return sample_data

    def get_samples(self, ped_data, ped_mapping, sample_data):
        if self.args.ped and self.args.sample_data:
            # Check for matching fields between ped and sample data, but keep original order
            matching_fields = [col for col in ped_data.columns if col in sample_data.columns]
            combined_fields = ped_data.columns.tolist()
            combined_fields += [col for col in sample_data.columns if col not in combined_fields]
            if not matching_fields:
                raise ParameterException("No matching fields found between ped and sample data.")

            # Combine the two sample sets, reorder based on ped & then sample data
            samples = pd.merge_ordered(sample_data, ped_data, on=matching_fields)[combined_fields]
            # Check that all values for all samples exist
            if samples.isnull().values.any():
                missing_data = samples.loc[samples.isnull().any(axis=1)]
                raise ParameterException(
                    "Combination of ped and sample data has missing values:\n"
                    + missing_data.to_string(index=False, na_rep="<!!>")
                )
            # check that no different values are given for the same sample
            if samples[ped_mapping["name"]].duplicated().any():
                duplicated = samples.loc[samples[ped_mapping["name"]].duplicated(keep=False)]
                raise ParameterException(
                    "Sample with different values found in combination of ped and sample data:\n"
                    + duplicated.to_string(index=False, na_rep="")
                )
        else:
            samples = ped_data if self.args.ped else sample_data
        return samples

    def match_sample_data_to_isa(
        self,
        samples: pd.DataFrame,
        isa_names: IsaColumnDetails,
        sample_field_mapping: dict[str, str],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Take a df with sampledata and build study and assay dataframes with all corresponding ISA column names."""

        # build new assay and study dataframes, with content from samples but names from isa_names
        new_study_data = pd.DataFrame()
        new_assay_data = pd.DataFrame()

        failed_cols = []

        for col_name in samples.columns:
            if col_name in sample_field_mapping:
                matched_cols = isa_names[sample_field_mapping[col_name]]
            elif col_name in isa_names:
                matched_cols = isa_names[col_name]
            else:
                failed_cols.append(col_name)
                continue

            for matched_isa_col, isa_table in matched_cols:
                if matched_isa_col == "Sample Name":
                    new_study_data[matched_isa_col] = samples[col_name]
                    new_assay_data[matched_isa_col] = samples[col_name]
                elif isa_table == "study":
                    new_study_data[matched_isa_col] = samples[col_name]
                else:
                    new_assay_data[matched_isa_col] = samples[col_name]

        if failed_cols:
            raise ValueError(
                f"Failed to match these column names/sample fields to ISA column: {', '.join(failed_cols)}"
            )

        return new_study_data, new_assay_data

    def update_isa_table(
        self,
        isa_table: pd.DataFrame,
        update_table: pd.DataFrame,
        overwrite: bool = False,
        no_autofill: bool = False,
    ):
        if not all(update_table.columns.isin(isa_table.columns)):
            raise ValueError(
                "New ISA table has columns that are not present in the existing ISA table."
            )

        # reorder update_table columns to match isa_table
        update_table = update_table[
            [col for col in isa_table.columns if col in update_table.columns]
        ]

        # check for matches with existing data based on required cols (which should be the material cols)
        # separate new data into updates (all mat_cols match) and additions
        mat_cols = [
            col
            for col in isa_table.columns
            if orig_col_name(col) in REQUIRED_COLUMNS + REQUIRED_IF_EXISTING_COLUMNS
        ]
        common_rows_update = (
            update_table[mat_cols].isin(isa_table[mat_cols].to_dict(orient="list")).all(axis=1)
        )
        common_rows_isa = (
            isa_table[mat_cols].isin(update_table[mat_cols].to_dict(orient="list")).all(axis=1)
        )
        updates = update_table.loc[common_rows_update].copy()
        additions = update_table.loc[~common_rows_update].copy()

        if overwrite:
            # update all isa_table columns with new_data
            isa_table.loc[common_rows_isa, updates.columns] = updates.values
        else:
            # Update only values those columns that are empty/falsy
            # Give a warning if any existing values are different
            for col in updates.columns:
                isa_col = isa_table[col].loc[common_rows_isa]
                equal_values = isa_col.reset_index(drop=True) == updates[col].reset_index(drop=True)
                empty_values = isa_col.isnull().reset_index(drop=True) | isa_col.eq("").reset_index(
                    drop=True
                )
                clash_rows = ~equal_values & ~empty_values

                if equal_values.all():
                    continue
                elif clash_rows.any():
                    clash = isa_table.loc[isa_col.index[clash_rows], mat_cols + [col]]
                    clash["<New values:>" + col] = updates[col].loc[clash_rows].values
                    logger.warning(
                        f"Given values for ISA column '{col}' have different existing values, "
                        "these will not be updated. Use `--overwrite` to force update.\n"
                        + clash.to_string(index=False, na_rep="")
                    )

                isa_table.loc[isa_col.index[empty_values], col] = (
                    updates[col].loc[empty_values].values
                )

        # Check which cols should be autofilled ("Protocol REF" needs to be, for ISA to work)
        autofill_cols = {
            col: isa_table[col].unique()[0]
            for col in isa_table.columns
            if orig_col_name(col) == "Protocol REF"
        }
        if not no_autofill:
            # Do not autofill ontology terms or references (an autofilled ontology reference without values
            # would not pass altamisa validation)
            autofill_cols.update(
                {
                    col: isa_table[col].unique()[0]
                    for col in isa_table.columns
                    if isa_table[col].nunique() == 1 and orig_col_name(col) not in ISA_NON_SETTABLE
                }
            )

        additions = additions.assign(**autofill_cols)

        isa_table = pd.concat([isa_table, additions], ignore_index=True).fillna("")

        return isa_table


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar update-samplesheet``."""
    return UpdateSamplesheetCommand.setup_argparse(parser)
