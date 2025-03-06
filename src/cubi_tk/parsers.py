
import argparse
import datetime
import os

basic_config_parser = argparse.ArgumentParser(description="The basic config parser", add_help=False)
basic_group = basic_config_parser.add_argument_group("Logging Configuration")
basic_group.add_argument("--verbose", action="store_true", default=False, help="Increase verbosity.")


def get_basic_parser():
        return basic_config_parser


sodar_config_parser = argparse.ArgumentParser(description="The basic config parser", add_help=False)
sodar_group = sodar_config_parser.add_argument_group("Basic Sodar Configuration")
sodar_group.add_argument(
    "--config",
    default=os.environ.get("SODAR_CONFIG_PATH", None),
    help="Path to configuration file.",
)
sodar_group.add_argument(
    "--sodar-server-url",
    default=os.environ.get("SODAR_SERVER_URL", None),
    help="SODAR server URL key to use, defaults to env SODAR_SERVER_URL.",
)
sodar_group.add_argument(
    "--sodar-api-token",
    default=os.environ.get("SODAR_API_TOKEN", None),
    help="SODAR API token to use, defaults to env SODAR_API_TOKEN.",
)
def get_sodar_parser():
    return sodar_config_parser

sodar_specific_parser = argparse.ArgumentParser(description="The specifig config parser", add_help=False)
sodar_group_spec = sodar_specific_parser.add_argument_group("Specific Sodar Configuration")
sodar_group_spec.add_argument(
    "--tsv-shortcut",
    default="germline",
    choices=("germline", "generic", "cancer"),
    help="The shortcut TSV schema to use.",
)
sodar_group_spec.add_argument(
    "--assay-uuid",
    default=None,
    help="UUID of assay to download data for."
)
sodar_group_spec.add_argument(
    "--first-batch", default=0, type=int, help="First batch to be transferred. Defaults: 0."
)
sodar_group_spec.add_argument(
    "--last-batch", type=int, required=False, help="Last batch to be transferred."
)

def get_specific_sodar_parser():
    return sodar_specific_parser

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
    "--validate-and-move",
    default=False,
    action="store_true",
    help="After files are transferred to SODAR, it will proceed with validation and move.",
)
snappy_itransfer_group.add_argument(
    "destination", help="Landing zone path or UUID from Landing Zone or Project"
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
snappy_pull_data_group.add_argument("project_uuid", help="UUID of project to download data for.")

def get_snappy_pull_data_parser():
    return snappy_pull_data_parser
