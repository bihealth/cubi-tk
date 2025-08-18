
import argparse
import datetime
import os

from loguru import logger

from cubi_tk.sodar_api import GLOBAL_CONFIG_PATH


def print_args(args: argparse.Namespace):
    token = getattr(args, "sodar_api_token","")
    args.sodar_api_token = "****"
    logger.info("Args: {}", args)
    args.sodar_api_token = token


basic_config_parser = argparse.ArgumentParser(description="The basic config parser", add_help=False)
basic_group = basic_config_parser.add_argument_group("Logging Configuration")
basic_group.add_argument("--verbose", action="store_true", default=False, help="Increase verbosity.")


def get_basic_parser():
        return basic_config_parser


def get_sodar_parser(with_dest = False, dest_string = "project_uuid", dest_help_string ="SODAR project UUID", with_assay_uuid = False):
    sodar_config_parser = argparse.ArgumentParser(description="The basic config parser", add_help=False)
    sodar_group = sodar_config_parser.add_argument_group("Basic Sodar Configuration")
    sodar_group.add_argument(
        "--config",
        default=GLOBAL_CONFIG_PATH,
        help="Path to configuration file.",
    )
    sodar_group.add_argument(
        "--config-profile",
        default="global",
        help="Sodar profile from configuration file and irods_environment_{config-profile}.json that should be used. If not specified global and irods_environment.json will be used",
    )
    sodar_group.add_argument(
        "--sodar-server-url",
        help="SODAR server URL key to use.",
    )
    sodar_group.add_argument(
        "--sodar-api-token",
        help="SODAR API token to use.",
    )
    if with_dest:
       sodar_config_parser.add_argument(
            dest_string,
            help=dest_help_string,
        )
    if with_assay_uuid:
        sodar_group.add_argument(
            "--assay-uuid",
            default=None,
            type=str,
            help="UUID from Assay to check. Used to specify target while dealing with multi-assay projects.",
        )
    return sodar_config_parser

sodar_specific_parser = argparse.ArgumentParser(description="The specific config parser", add_help=False)
sodar_group_spec = sodar_specific_parser.add_argument_group("Specific Sodar Configuration")
sodar_group_spec.add_argument(
    "--tsv-shortcut",
    default="germline",
    choices=("germline", "generic", "cancer"),
    help="The shortcut TSV schema to use.",
)

sodar_group_spec.add_argument(
    "--first-batch", default=0, type=int, help="First batch to be transferred. Defaults: 0."
)
sodar_group_spec.add_argument(
    "--last-batch", type=int, required=False, help="Last batch to be transferred."
)


snappy_cmd_basic_parser = argparse.ArgumentParser(description="The basic parser for snappy commands",add_help=False)
snappy_basic_group = snappy_cmd_basic_parser.add_argument_group("Snappy Configuration")
snappy_basic_group.add_argument(
    "--base-path",
    default=os.getcwd(),
    required=False,
    help=(
        "Base path of project (contains '.snappy_pipeline/' etc.), spiders up from current "
        "work directory and falls back to current working directory by default."
    ),
)

def get_snappy_cmd_basic_parser():
    return snappy_cmd_basic_parser

snappy_itransfer_parser = argparse.ArgumentParser(description="The basic parser for snappy itransfer commands", parents=[sodar_specific_parser], add_help=False)
snappy_itransfer_group = snappy_itransfer_parser.add_argument_group("Configuration for snappy itransfer commands")
snappy_itransfer_group.add_argument(
    "--overwrite-remote",
    action="store_true",
    help="Overwrite remote files if they exist, otherwise re-upload will be skipped.",
)
snappy_itransfer_group.add_argument(
    "--remote-dir-date",
    default=datetime.date.today().strftime("%Y-%m-%d"),
    help="Date to use in remote directory, defaults to YYYY-MM-DD of today.",
)
snappy_itransfer_group.add_argument(
    "--remote-dir-pattern",
    default="{library_name}/{step}/{date}",
    help="Pattern to use for constructing remote pattern",
)
snappy_itransfer_group.add_argument(
    "--yes",
    default=False,
    action="store_true",
    help="Assume all answers are yes, e.g., will create or use "
    "existing available landing zones without asking.",
)
snappy_itransfer_group.add_argument(
    "--select-lz",
    default=False,
    action="store_true",
    help="Do not use latest landingzone but ask from the avalibale ones. Note: `--yes` overrides this option.",
)
snappy_itransfer_group.add_argument(
    "--validate-and-move",
    default=False,
    action="store_true",
    help="After files are transferred to SODAR, it will proceed with validation and move.",
)
def get_snappy_itransfer_parser():
    return snappy_itransfer_parser


snappy_pull_data_parser = argparse.ArgumentParser(description="The basic parser for snappy pull data commands", parents=[sodar_specific_parser], add_help=False)
snappy_pull_data_group = snappy_pull_data_parser.add_argument_group("Configuration for snappy pull data commands")
snappy_pull_data_group.add_argument(
    "--overwrite", default=False, action="store_true", help="Allow overwriting of files"
)
snappy_pull_data_group.add_argument(
    "--samples", help="Optional list of samples to pull")
snappy_pull_data_group.add_argument(
    "--output-directory",
    default=None,
    required=True,
    help="Output directory, where downloaded files will be stored.",
)
snappy_pull_data_group.add_argument(
    "--yes", default=False, action="store_true", help="Assume all answers are yes."
)
snappy_pull_data_group.add_argument("project_uuid", help="UUID of project to download data for.")

def get_snappy_pull_data_parser():
    return snappy_pull_data_parser
