"""``cubi-tk sodar update-samplesheet``: update ISA-tab with new values and/or entries."""

import argparse
from collections import OrderedDict, defaultdict
from io import StringIO
import re
import typing

from logzero import logger
import pandas as pd
from ruamel.yaml import YAML

from ..parse_ped import parse_ped
from ..sodar_api import SodarAPI

REQUIRED_COLUMNS = ["Source Name", "Sample Name", "Extract Name"]
REQUIRED_IF_EXISTING_COLUMNS = ["Library Name"]
ISA_NON_SETTABLE = ["Term Source REF", "Term Accession Number", "Protocol REF"]


sheet_default_config_yaml = """
ped_defaults: &ped
    family_id: Family-ID
    name: Sample-ID
    father_name: Paternal-ID
    mother_name: Maternal-ID
    sex: Sex
    disease: Phenotype

Modellvorhaben: &MV
    sample_field_mapping:
        - Family-ID
        - Individual-ID
        - Paternal-ID
        - Maternal-ID
        - Sex
        - Phenotype
        - Probe-ID
        - Analysis-ID
        - Barcode
        - Barcode-Name
    field_column_mapping:
        Family-ID: Family
        Individual-ID: Source Name
        Paternal-ID: Father
        Maternal-ID: Mother
        Sex: Sex
        Phenotype: "Disease status"
        Probe-ID: Sample Name
        Analysis-ID: Extract Name
        Barcode: "Barcode sequence"
        Barcode-Name: "Barcode name"
    dynamic_columns: {}
    ped_to_sampledata:
        <<: *ped
        name: Individual-ID
        # name: Analysis-ID

MV-ped:
    <<: *MV
    sample_field_mapping:
        - Individual-ID
        - Probe-ID
        - Analysis-ID
        - Barcode
        - Barcode-Name

germline-sheet:
    sample_field_mapping:
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
        Sample Name: "{Source Name}-N1"
        Extract Name: "{Sample Name}-DNA1"
        Library Name: "{Sample Name}-DNA1-{Library Strategy}1"
    ped_to_sampledata:
        <<: *ped
"""

# ruamel round-trip loader uses ordered dicts
yaml = YAML(typ="rt")
sheet_default_config = yaml.load(sheet_default_config_yaml)


def orig_col_name(col_name: str) -> str:
    """Return the original column name without any suffixes."""
    return re.sub(r"\.[0-9]+$", "", col_name)


class UpdateSamplesheetCommand:
    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments for ``update-samplehseet`` command."""

        SodarAPI.setup_argparse(parser)

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
                    f"{default} ({len(settings['sample_field_mapping'])} fields, ped-Name: {settings['ped_to_sampledata']['name']}):\n"
                    f"{', '.join(settings['sample_field_mapping'])}"
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
            help="Dyanmically fill columns in the ISA sheet based on other columns."
            "Use this option if some columns with sample-sepcific Data can be derived from other columns."
            "FORMAT_STR can contain other columns as placeholders, i.e.: '{Source Name}-N1' for a new Sample Name.",
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
            "in the existing samplesheet.",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the command."""

        if self.args.overwrite:
            logger.warning(
                "Existing values in the ISA samplesheet may get overwritten, there will be no checks or further warnings."
            )

        # Get samplehseet from SODAR API
        sodar_api = SodarAPI(
            sodar_url=self.args.sodar_url,
            sodar_api_token=self.args.sodar_api_token,
            project_uuid=self.args.project_uuid,
        )
        isa_data = sodar_api.get_ISA_samplesheet()
        investigation = isa_data["investigation"][1]
        study = pd.read_csv(StringIO(isa_data["study"][1]), sep="\t")
        assay = pd.read_csv(StringIO(isa_data["assay"][1]), sep="\t")
        isa_names = self.gather_ISA_column_names(study, assay)

        # Check that given sample-data field names can be used
        sample_fields_mapping = self.parse_sampledata_args(isa_names)

        # Collect ped & sample data, check that they can be combined
        samples = self.collect_sample_data(isa_names, sample_fields_mapping)

        # add metadata values to samples
        for col, value in dict(self.args.metadata_global).items():
            samples[col] = value

        req_cols = REQUIRED_COLUMNS[:] + [
            col for col in REQUIRED_IF_EXISTING_COLUMNS if col in isa_names.keys()
        ]
        if not set(req_cols).issubset(samples.columns):
            missing_cols = set(req_cols) - set(samples.columns)
            raise ValueError(f"Missing required columns in sample data: {', '.join(missing_cols)}")

        # Match sample data to ISA columns and get new study & assay dataframes
        study_new, assay_new = self.match_sample_data_to_isa(
            samples, isa_names, sample_fields_mapping
        )

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
        ret = sodar_api.upload_ISA_samplesheet(
            (isa_data["investigation"][0], investigation),
            (isa_data["study"][0], study_tsv),
            (isa_data["assay"][0], assay_tsv),
        )
        return ret

    def parse_sampledata_args(self, isa_names) -> OrderedDict[str, str]:
        """Build a dict of Sample-data filed name mappings and check consistency of provided data."""

        # Some samples are defined, either through ped or sample data
        if not self.args.ped and not self.args.sample_data:
            raise ValueError(
                "No sample data provided. Please provide either a ped file (`--ped`) or sample data (`-s`)."
            )

        # Base field name mapping (also used by ped)
        sample_field_mapping = sheet_default_config[self.args.defaults]["field_column_mapping"]

        # Sample data, if given, need to matche the defined fields (from default or sample_fields)
        if self.args.sample_data:
            if self.args.sample_fields:
                sample_field_mapping.update(
                    OrderedDict(
                        [
                            (field.split("=")[0], field.split("=")[1 if "=" in field else 0])
                            for field in self.args.sample_fields
                        ]
                    )
                )
                n_fields = len(self.args.sample_fields)
            else:
                n_fields = len(sheet_default_config[self.args.defaults]["sample_field_mapping"])

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
                tup for tup in sample_field_mapping.items() if tup[1] not in isa_names
            ]
            if unknown_fields:
                msg = (
                    "Some columns for sample field mapping are not defined in the ISA samplesheet: "
                    ", ".join([f"{col} (from {field})" for field, col in unknown_fields])
                )
                raise NameError(msg)

        return sample_field_mapping

    def gather_ISA_column_names(
        self, study: pd.DataFrame, assay: pd.DataFrame
    ) -> dict[str, list[tuple[str, str]]]:
        isa_regex = re.compile(r"(Characteristics|Parameter Value|Comment)\[(.*?)]")
        study_cols = study.columns.tolist()
        assay_cols = assay.columns.tolist()

        isa_short_names = list(
            map(lambda x: orig_col_name(isa_regex.sub(r"\2", x)), study_cols + assay_cols)
        )
        isa_long_names = list(map(orig_col_name, study_cols + assay_cols))
        isa_names_unique = study_cols + assay_cols
        isa_table = ["study"] * len(study_cols) + ["assay"] * len(assay_cols)

        out = defaultdict(list)
        for short, long, uniq, table in zip(
            isa_short_names, isa_long_names, isa_names_unique, isa_table
        ):
            out[short].append((uniq, table))
            out[long].append((uniq, table))

        for col in ISA_NON_SETTABLE:
            if col in out:
                del out[col]

        return out

    def get_dynamic_columns(
        self, isa_names: dict[str, list[tuple[str, str]]]
    ) -> OrderedDict[str, str]:
        dynamic_cols = OrderedDict()
        if self.args.dynamic_column:
            for col, format_str in self.args.dynamic_column:
                if col not in isa_names:
                    raise ValueError(f"Column '{col}' is not defined in the ISA samplesheet.")
                dynamic_cols[col] = format_str
        elif sheet_default_config[self.args.defaults]["dynamic_columns"]:
            dynamic_cols = sheet_default_config[self.args.defaults]["dynamic_columns"]

        # #Sample Name needs to be set before other assay columns, so ensure it goes first
        # if 'Sample Name' in dynamic_cols:
        #     dynamic_cols.move_to_end('Sample Name', last=False)
        return dynamic_cols

    def collect_sample_data(
        self, isa_names: dict[str, list[tuple[str, str]]], sample_field_mapping: dict[str, str]
    ) -> pd.DataFrame:
        ped_mapping = sheet_default_config[self.args.defaults]["ped_to_sampledata"]
        if self.args.ped_field_mapping:
            allowed_sample_col_values = [*sample_field_mapping] + [*isa_names]
            for ped_col, sample_col in self.args.ped_field_mapping:
                if ped_col not in ped_mapping:
                    logger.warning(f"Ped column '{ped_col}' is unknown and will be ignored.")
                    continue
                if sample_col not in allowed_sample_col_values:
                    raise ValueError(
                        f"'{sample_col}' is neither a known sample field nor a known column of the ISA samplesheet."
                    )

                ped_mapping[ped_col] = sample_col

        if self.args.ped:
            with open(self.args.ped, "rt") as inputf:
                ped_dicts = map(
                    lambda donor: OrderedDict(
                        [field, getattr(donor, attr_name)]
                        for attr_name, field in ped_mapping.items()
                    ),
                    parse_ped(inputf),
                )
                ped_data = pd.DataFrame(ped_dicts)

        if self.args.sample_data:
            if self.args.sample_fields:
                fields = self.args.sample_fields
            else:
                fields = sheet_default_config[self.args.defaults]["sample_field_mapping"]
            sample_data = pd.DataFrame(self.args.sample_data, columns=fields)
            # FIXME: consistent conversion for Sex & Phenotype values? also empty parent values?
            # ped outputs: male/female, unaffected/affected, 0

        if self.args.ped and self.args.sample_data:
            # Check for matching fields between ped and sample data
            matching_fields = set(ped_data.columns).intersection(sample_data.columns)
            if not matching_fields:
                raise ValueError("No matching fields found between ped and sample data.")

            # Combine the two sample sets
            samples = pd.merge(ped_data, sample_data, on=list(matching_fields), how="outer")

            if samples.isnull().values.any():
                # FIXME: add more info to error message, also different Error type?
                raise ValueError("Combination of ped and sample data has missing values.")

        else:
            samples = ped_data if self.args.ped else sample_data

        return samples

    def match_sample_data_to_isa(
        self,
        samples: pd.DataFrame,
        isa_names: dict[str, list[tuple[str, str]]],
        sample_field_mapping: OrderedDict[str, str],
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

        dynamic_cols = self.get_dynamic_columns(isa_names)
        for col_name, format_str in dynamic_cols.items():
            matched_isa_col, isa_table = isa_names[col_name]

            # FIXME: catch Error if format_str contains non-existing columns?
            if matched_isa_col == "Sample Name":
                new_study_data[matched_isa_col] = new_study_data.apply(
                    lambda row: format_str.format(**row), axis=1
                )
                new_assay_data[matched_isa_col] = new_study_data[matched_isa_col]
            elif isa_table == "study":
                new_study_data[matched_isa_col] = new_study_data.apply(
                    lambda row: format_str.format(**row), axis=1
                )
            else:
                new_assay_data[matched_isa_col] = new_study_data.apply(
                    lambda row: format_str.format(**row), axis=1
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
                        f"Given values for ISA column '{col}' have different existing values, these will not be updated."
                        " Use `--overwrite` to force update.\n"
                        + clash.to_string(index=False, na_rep="")
                    )

                isa_table.loc[isa_col.index[empty_values], col] = (
                    updates[col].loc[empty_values].values
                )

        # Check which cols should be autofilled (some need to be for ISA to work)
        autofill_cols = {
            col: isa_table[col].unique()
            for col in isa_table.columns
            if orig_col_name(col) in ISA_NON_SETTABLE
        }

        if not no_autofill:
            autofill_cols.update(
                {
                    col: isa_table[col].unique()
                    for col in isa_table.columns
                    if isa_table[col].nunique() == 1
                }
            )

        for col, value in autofill_cols.items():
            additions[col] = [value] * additions.shape[0]

        isa_table = pd.concat([isa_table, additions], ignore_index=True).fillna("")

        return isa_table


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar update-samplesheet``."""
    return UpdateSamplesheetCommand.setup_argparse(parser)
