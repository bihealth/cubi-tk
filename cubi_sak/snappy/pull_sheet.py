"""``cubi-sak snappy pull-sheet``: pull BiomedSheet files from SODAR.

More Information
----------------

- Also see ``cubi-sak snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-sak snappy pull-sheet --help`` for more information.
- `SNAPPY Pipeline GitLab Project <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/snappy>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.
"""

import argparse
import os
from uuid import UUID
import re
import typing

from logzero import logger
import requests

from .. import exceptions


#: The URL template to use.
from ..common import overwrite_helper

URL_TPL = "%(sodar_url)s/samplesheets/api/remote/get/%(project_uuid)s/%(api_key)s"

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


def strip(x):
    if hasattr(x, "strip"):
        return x.strip()
    else:
        return x


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-sak snappy pull-sheet``."""
    parser.add_argument("--hidden-cmd", dest="snappy_cmd", default=run, help=argparse.SUPPRESS)

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
        "--allow-overwrite",
        default=False,
        action="store_true",
        help="Allow to overwrite output file, default is not to allow overwriting output file.",
    )

    parser.add_argument(
        "--dry-run",
        "-n",
        default=False,
        action="store_true",
        help="Perform a dry run, i.e., don't change anything only display change, implies '--show-diff'.",
    )
    parser.add_argument(
        "--show-diff",
        "-D",
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
        "--library-types", help="Library type(s) to use, comma-separated, default is to use all."
    )

    parser.add_argument("project_uuid", help="UUID of project to pull the sample sheet for.")
    parser.add_argument(
        "output_tsv",
        default="-",
        nargs="?",
        help="Path to output TSV file, default is '-' for stdout.",
    )


def check_args(args) -> int:
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    any_error = False

    # Postprocess arguments.
    if args.library_types:
        args.library_types = args.library_types.split(",")  # pragma: nocover
    else:
        args.library_types = []

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
    if args.output_tsv != "-" and os.path.exists(args.output_tsv):  # pragma: nocover
        if not args.allow_overwrite and not args.dry_run:
            logger.error(
                "The output path %s already exists but --allow-overwrite not given.",
                args.output_tsv,
            )
            any_error = True
        else:
            logger.warn("Output path %s exists but --allow-overwrite given.", args.output_tsv)

    # Check UUID syntax.
    try:
        val: typing.Optional[str] = str(UUID(args.project_uuid))
    except ValueError:  # pragma: nocover
        val = None
    finally:
        if args.project_uuid != val:  # pragma: nocover
            logger.error("Project UUID %s is not a valid UUID", args.project_uuid)
            any_error = True

    return int(any_error)


def build_sheet(args) -> str:
    """Build sheet TSV file."""

    result = []

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
    if len(all_data["studies"]) > 1:  # pragma: nocover
        raise exceptions.UnsupportedIsaTabFeatureException("More than one study found!")

    # Parse out study data.
    study = list(all_data["studies"].values())[0]
    study_infos = study["study"]
    study_top = study_infos["top_header"]
    n_source = study_top[0]["colspan"]
    n_extraction = study_top[1]["colspan"]
    # n_sample = study_top[2]["colspan"]
    cols_source = study_infos["field_header"][:n_source]
    cols_extraction = study_infos["field_header"][n_source : n_source + n_extraction]
    cols_sample = study_infos["field_header"][n_source + n_extraction :]
    names_source = [x["value"] for x in cols_source]
    names_extraction = [x["value"] for x in cols_extraction]
    names_sample = [x["value"] for x in cols_sample]
    table = study_infos["table_data"]

    # Build study info map.
    study_map = {}
    for row in table:
        # Assign fields to table.
        dict_source = dict(zip(names_source, [strip(x["value"]) for x in row[:n_source]]))
        dict_extraction = dict(
            zip(names_extraction, [x["value"] for x in row[n_source : n_source + n_extraction]])
        )
        dict_sample = dict(
            zip(names_sample, [strip(x["value"]) for x in row[n_source + n_extraction :]])
        )
        # Extend study_map.
        study_map[dict_source["Name"]] = {
            "Source": dict_source,
            "Extraction": dict_extraction,
            "Sample": dict_sample,
        }

    # Parse out the assay data.
    #
    # NB: We're not completely cleanly decomposing the information and, e.g., overwrite
    # the "Extract name" keys here...
    if len(study["assays"]) > 1:  # pragma: nocover
        raise exceptions.UnsupportedIsaTabFeatureException("More than one assay found!")
    assay = list(study["assays"].values())[0]
    top_columns = [(x["value"], x["colspan"]) for x in assay["top_header"]]
    columns = []
    offset = 0
    for type_, colspan in top_columns:
        columns.append(
            {
                "type": type_,
                "columns": [x["value"] for x in assay["field_header"][offset : offset + colspan]],
            }
        )
        offset += colspan
    assay_map: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
    for row in assay["table_data"]:
        offset = 0
        name = row[0]["value"].strip()
        for column in columns:
            colspan = len(column["columns"])
            values = {
                "type": column["type"],
                **dict(
                    zip(column["columns"], [x["value"] for x in row[offset : offset + colspan]])
                ),
            }
            type_ = column["type"]
            if type_ == "Process":
                type_ = values["Protocol"]
            assay_map.setdefault(name, {})[type_] = values
            offset += colspan

    # Generate the resulting sample sheet.
    result.append("\n".join(HEADER_TPL) % vars(args))
    for source, info in study_map.items():
        if source not in assay_map:  # pragma: nocover
            logger.info("source %s does not have an assay.", source)
            dict_lib = {"Name": "-.1", "Folder Name": ".", "Batch": "."}  # HAAACKY
            proc_lib = {}
        else:
            for outer_key in ("Extract Name", "Library Name"):
                if outer_key in assay_map[source]:
                    dict_lib = assay_map[source][outer_key]
                    for key in assay_map[source]:
                        if key.startswith("Library construction"):
                            proc_lib = assay_map[source][key]
                            break
                    else:  # pragma: nocover
                        proc_lib = {}
        dict_source = info["Source"]
        # FIXME: remove hack
        # HACK: ignore if looks like artifact
        if "Folder Name" in dict_lib:
            library_type = dict_lib["Name"].split("-")[-1][:-1]  # hack to get library type
            folder = dict_lib["Folder Name"]
        else:  # pragma: nocover
            library_type = "."
            folder = "."
        # TODO: find better way of accessing sequencing process
        seq_platform = "Illumina"
        for the_dict in assay_map.get(source, {}).values():
            if the_dict.get("Platform") == "PACBIO_SMRT":  # pragma: nocover
                seq_platform = "PacBio"
        library_kit = proc_lib.get("Library Kit") or "."
        if (
            args.library_types and library_type != "." and library_type not in args.library_types
        ):  # pragma: nocover
            logger.info(
                "Skipping %s not in library types %s", dict_source["Name"], args.library_types
            )
            continue
        # ENDOF HACK
        haystack = dict_source.get("Batch", "1")
        m = re.search(r"(\d+)", haystack)
        if not m:  # pragma: nocover
            raise exceptions.InvalidIsaTabException("Could not find batch number in %s" % haystack)
        batch = m.group(1)
        row = [
            dict_source["Family"],
            dict_source["Name"],
            dict_source["Father"],
            dict_source["Mother"],
            MAPPING_SEX[dict_source["Sex"].lower()],
            MAPPING_STATUS[dict_source["Disease Status"].lower()],
            library_type,
            folder,
            batch,
            ".",
            args.project_uuid,
            seq_platform,
            library_kit,
        ]
        result.append("\t".join(row))
    result.append("")

    return "\n".join(result)


def run(
    args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
) -> typing.Optional[int]:
    """Run ``cubi-sak snappy pull-sheet``."""
    res: typing.Optional[int] = check_args(args)
    if res:  # pragma: nocover
        return res

    logger.info("Starting to pull sheet...")
    logger.info("  Args: %s", args)

    overwrite_helper(
        args.output_tsv,
        build_sheet(args),
        do_write=not args.dry_run,
        show_diff=args.show_diff,
        show_diff_side_by_side=args.show_diff_side_by_side,
    )

    return None
