"""Common functionality for SNAPPY."""

import pathlib
import typing
import pkgutil

from biomedsheets import io_tsv
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
from logzero import logger
import yaml
import importlib
import re
from collections import defaultdict

import snappy_pipeline.workflows as snappy_workflows
from snappy_pipeline import expand_ref


class CouldNotFindPipelineRoot(Exception):
    """Raised when ``.snappy_pipeline`` could not be found."""


class CouldNotFindBioMedSheet(Exception):
    """Raised when BioMedSheet could not be found in configuration file."""

class DummyWorkflow:
    """Dummy workflow that does nothing.
    """
    def __init__(self):
        self.globals = defaultdict(str)

    def __getattr__(self, _):
        return self._catchall

    def _catchall(self, *_, **__):
        pass

def get_available_snappy_workflows():
    """Get all available snappy workflows in snappy_pipeline.

    :return: List of ModuleInfo with individual workflow modules.
    """
    return list(pkgutil.iter_modules(snappy_workflows.__path__))

def get_available_snappy_workflow_paths():
    """Get module paths for all available snappy workflows.

    :return: Dict of workflow name to workflow module path.
    """
    workflow_names = get_available_snappy_workflows()
    return {w.name: pathlib.Path(w.module_finder.path) / w.name for w in workflow_names}

def get_workflow_snakefile_object_name(snakefile_path):
    """Find the Workflow implementation object name.

    :param snakefile_path: Path to snakefile for workflow.
    :type snakefile_path: str, pathlib.Path

    :return: str Name of the implementation class or None if nothing as been found.
    """

    with snakefile_path.open() as f:
        if m := re.search("wf\s*=\s*(\w+)\(", f.read()):
            module_name = m.group(1)
            return module_name
    return None

def load_workflow_step_configuration(workflow_step_path):
    """Load snappy configuration and resolve any refs.

    :param workflow_step_path: Path to snappy config yaml.
    :type workflow_step_path: str, pathlib.Path

    :return: Tuple of config, lookup paths and configuration paths. If no config is found, a tuple of None is returned.
    """

    config_path = pathlib.Path(workflow_step_path) / "config.yaml"

    if not config_path.exists():
        return (None, None, None)

    with open(str(config_path)) as stream:
        config_data = yaml.safe_load(stream)

    config, lookup_paths, config_paths = expand_ref(str(config_path), config_data, [str(workflow_step_path), str(workflow_step_path.parent / ".snappy_pipeline")])
    return config, lookup_paths, config_paths


def get_snappy_config(snappy_root_dir):
    """Get snappy configuration.

    :param snappy_root_dir: Path to snappy root directory.
    :type snappy_root_dir: str, pathlib.Path

    :return: Returns loaded snappy configuration.
    """
    snappy_config = snappy_root_dir / ".snappy_pipeline" / "config.yaml"
    with open(snappy_config, "r") as stream:
        config = yaml.safe_load(stream)
    return config


def get_workflow_step_dependencies(workflow_step_path):
    """Find workflow dependencies for the given workflow step.
    :param workflow_step_path: Path to the workflow step.
    :type workflow_step_path: str, pathlib.Path

    :return: List of dependencies for the given workflow step.
    """
    workflow_step_path = pathlib.Path(workflow_step_path)

    config, lookup_paths, config_paths = load_workflow_step_configuration(workflow_step_path)
    if config is None:
        raise RuntimeError(f"Could not load workflow step confiuration for {workflow_step_path}")

    step_name = config["pipeline_step"]["name"]

    step_module_paths = get_available_snappy_workflow_paths()
    module_path = step_module_paths[step_name]
    module_config_path = module_path / "Snakefile"
    # get the name of the workflow step class name
    obj_name = get_workflow_snakefile_object_name(module_config_path)
    if obj_name is None:
        raise RuntimeError(f"Could not find workflow object for {step_name}")

    workflow_module = importlib.import_module("."+step_name, "snappy_pipeline.workflows")
    workflow_class = getattr(workflow_module, obj_name)


    workflow_object = workflow_class(DummyWorkflow(), config, lookup_paths, config_paths, str(workflow_step_path))
    dependencies = workflow_object.sub_workflows.keys()
    return list(dependencies)

def find_snappy_root_dir(
    start_path: typing.Union[str, pathlib.Path], more_markers: typing.Iterable[str] = ()
):
    """Find snappy pipeline root directory.

    :param start_path: Start path to search for snappy root directory.
    :type start_path: str, pathlib.Path

    :param more_markers: Additional markers to be included in the search. Method will always use '.snappy_pipeline'.
    :type more_markers: Iterable

    :return: Returns path to snappy pipeline root directory.

    :raises CouldNotFindPipelineRoot: if cannot find pipeline root.
    """
    markers = [".snappy_pipeline"] + list(more_markers)
    start_path = pathlib.Path(start_path)
    for path in [start_path] + list(start_path.parents):
        logger.debug("Trying %s", path)
        if any((path / name).exists() for name in markers):
            logger.info("Will start at %s", path)
            return path
    logger.error("Could not find SNAPPY pipeline directories below %s", start_path)
    raise CouldNotFindPipelineRoot()


def get_workflow_name(workflow_path):
    """Get the name of the workflow in the directory. This will parse the contained config.yaml.

    :param workflow_path: Path of the workflow.
    :type workflow_path: str, pathlib.Path

    :return: Optional str name of the workflow.
    """

    config, _, _ = load_workflow_step_configuration(workflow_path)
    if config is not None and "pipeline_step" in config:
        return config["pipeline_step"].get("name", None)


def get_snappy_step_directories(snappy_root_dir):
    """Get a dictionary of snappy workflow step names and their associated path.

    :param snappy_root_dir: Path to the snappy root directory, also containing .snappy_pipeline
    :type snappy_root_dir: str, pathlib.Path

    :return: Dict of workflow step name to workflow step path.
    """
    snappy_workflows_names = [
        s.name for s in get_available_snappy_workflows()
    ]
    folder_steps = {
        name: path for name, path in
        [(get_workflow_name(p), p) for p in pathlib.Path(snappy_root_dir).iterdir()]
        if name in snappy_workflows_names
    }

    return folder_steps



def load_sheet_tsv(path_tsv, tsv_shortcut="germline"):
    """Load sample sheet.

    :param path_tsv: Path to sample sheet TSV file.
    :type path_tsv: pathlib.Path

    :param tsv_shortcut: Sample sheet type. Default: 'germline'.
    :type tsv_shortcut: str

    :return: Returns Sheet model.
    """
    load_tsv = getattr(io_tsv, "read_%s_tsv_sheet" % tsv_shortcut)
    with open(path_tsv, "rt") as f:
        return load_tsv(f, naming_scheme=NAMING_ONLY_SECONDARY_ID)


def get_biomedsheet_path(start_path, uuid):
    """Get biomedsheet path, i.e., sample sheet.

    :param start_path: Start path to search for snappy root directory.
    :type start_path: str, pathlib.Path

    :param uuid: Project UUID.
    :type uuid: str

    :return: Returns path to sample sheet.
    """
    # Initialise variables
    biomedsheet_path = None

    # Find config file
    snappy_dir_parent = find_snappy_root_dir(start_path=start_path)
    snappy_config = snappy_dir_parent / ".snappy_pipeline" / "config.yaml"

    # Load config
    with open(snappy_config, "r") as stream:
        config = yaml.safe_load(stream)

    # Search config for the correct dataset
    for project in config["data_sets"]:
        dataset = config["data_sets"].get(project)
        try:
            if dataset["sodar_uuid"] == uuid:
                biomedsheet_path = snappy_dir_parent / ".snappy_pipeline" / dataset["file"]
        except KeyError:
            # Not every dataset has an associated UUID
            logger.info("Data set '{0}' has no associated UUID.".format(project))

    # Raise exception if none found
    if biomedsheet_path is None:
        tpl = "Could not find sample sheet for UUID {uuid}. Dataset configuration: {config}"
        config_str = "; ".join(["{} = {}".format(k, v) for k, v in config["data_sets"].items()])
        msg = tpl.format(uuid=uuid, config=config_str)
        raise CouldNotFindBioMedSheet(msg)

    # Return path
    return biomedsheet_path


def get_all_biomedsheet_paths(start_path):
    """Get paths to all biomedsheet files in a SNAPPY directory.

    :param start_path: Start path to search for snappy root directory.
    :type start_path: str, pathlib.Path

    :return: Returns paths to sample sheet.
    """
    result = []

    # Find config file
    snappy_dir_parent = find_snappy_root_dir(start_path=start_path)
    snappy_config = snappy_dir_parent / ".snappy_pipeline" / "config.yaml"

    # Load config
    with open(snappy_config, "r") as stream:
        config = yaml.safe_load(stream)

    # Search config for the datasets.
    for project in config["data_sets"]:
        dataset = config["data_sets"].get(project)
        result.append(snappy_dir_parent / ".snappy_pipeline" / dataset["file"])

    return result
