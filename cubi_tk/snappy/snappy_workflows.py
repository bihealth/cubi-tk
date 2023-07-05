from collections import defaultdict
import importlib
import pathlib
import pkgutil
import re
import typing
import yaml

from snappy_pipeline import expand_ref
import snappy_pipeline.workflows as snappy_workflows


class DummyWorkflow:
    """Dummy workflow that does nothing."""

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
        if m := re.search(r"wf\s*=\s*(\w+)\(", f.read()):
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

    config, lookup_paths, config_paths = expand_ref(
        str(config_path),
        config_data,
        [str(workflow_step_path), str(workflow_step_path.parent / ".snappy_pipeline")],
    )
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

    workflow_module = importlib.import_module(f".{step_name}", "snappy_pipeline.workflows")
    workflow_class = getattr(workflow_module, obj_name)

    workflow_object = workflow_class(
        DummyWorkflow(), config, lookup_paths, config_paths, str(workflow_step_path)
    )
    dependencies = workflow_object.sub_workflows.keys()
    return list(dependencies)


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
    snappy_workflows_names = [s.name for s in get_available_snappy_workflows()]
    folder_steps = {
        name: path
        for name, path in [
            (get_workflow_name(p), p) for p in pathlib.Path(snappy_root_dir).iterdir()
        ]
        if name in snappy_workflows_names
    }

    return folder_steps
