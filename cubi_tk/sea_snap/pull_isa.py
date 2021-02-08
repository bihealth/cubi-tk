"""``cubi-tk sea-snap pull-isa``: pull ISA files from SODAR.

More Information
----------------

- Also see ``cubi-tk sea-snap`` :ref:`cli_main <CLI documentation>` and ``cubi-tk sea-snap pull-isa --help`` for more information.
- `Sea-snap Pipeline GitLab Project <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/sea-snap>`__.
- `ISA Documentation <https://isa-specs.readthedocs.io/en/latest/index.html>`__.
"""

import argparse
import os
from uuid import UUID
import typing
from pathlib import Path

from logzero import logger
import requests

URL_TPL = "%(sodar_url)s/samplesheets/api/remote/get/%(project_uuid)s/%(api_key)s?isa=1"


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sea-snap pull-isa``."""
    parser.add_argument("--hidden-cmd", dest="sea_snap_cmd", default=run, help=argparse.SUPPRESS)

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
        "--allow-overwrite",
        default=False,
        action="store_true",
        help="Allow to overwrite output file, default is not to allow overwriting output file.",
    )

    parser.add_argument(
        "--output_folder", default="ISA_files/", help="Output folder path for ISA files."
    )

    parser.add_argument("project_uuid", help="UUID of project to pull the sample sheet for.")


def check_args(args) -> int:
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""
    any_error = False

    # Check presence of SODAR URL and auth token.
    if not args.sodar_api_token:  # pragma: nocover
        logger.error(
            "SODAR authentication token is empty.  Either specify --sodar-api-token, or set "
            "SODAR_API_TOKEN environment variable"
        )
        any_error = True
    if not args.sodar_url:  # pragma: nocover
        logger.error("SODAR URL is empty. Either specify --sodar-url, or set SODAR_URL.")
        any_error = True

    # Check output file presence vs. overwrite allowed.
    if hasattr(args.output_folder, "name") and Path(args.output_folder).exists():  # pragma: nocover
        if not args.allow_overwrite:
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

    return int(any_error)


def pull_isa(args) -> typing.Optional[int]:
    """Pull ISA files"""

    # Query investigation JSON from API.
    url = URL_TPL % {
        "sodar_url": args.sodar_url,
        "project_uuid": args.project_uuid,
        "api_key": args.sodar_api_token,
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


def run(
    args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
) -> typing.Optional[int]:
    """Run ``cubi-tk sea-snap pull-isa``."""
    res: typing.Optional[int] = check_args(args)
    if res:  # pragma: nocover
        return res

    logger.info("Starting to pull files...")
    logger.info("  Args: %s", args)

    pull_isa(args)

    return None
