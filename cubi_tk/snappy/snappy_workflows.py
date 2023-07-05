from collections import defaultdict
import importlib
import pathlib
import re
import typing

from attrs import define
from logzero import logger
import yaml


def get_workflow_snakefile_object_name(
    snakefile_path: typing.Union[str, pathlib.Path]
) -> typing.Optional[str]:
    """Find the Workflow implementation object name.

    :param snakefile_path: Path to snakefile for workflow.

    :return: str Name of the implementation class or None if nothing as been found.
    """

    with open(str(snakefile_path)) as f:
        if m := re.search(r"wf\s*=\s*(\w+)\(", f.read()):
            module_name = m.group(1)
            return module_name
    return None


class DummyWorkflow:
    """Dummy workflow that does nothing."""

    def __init__(self):
        self.globals = defaultdict(str)

    def __getattr__(self, _):
        return self._catchall

    def _catchall(self, *_, **__):
        pass


@define
class SnappyWorkflowManager:
    _expand_ref: typing.Callable
    _snakefile_path: typing.Callable
    _step_to_module: typing.Dict[str, typing.Any]

    @classmethod
    def from_snappy(cls) -> typing.Optional["SnappyWorkflowManager"]:
        try:
            from snappy_pipeline import expand_ref
            from snappy_pipeline.apps.snappy_snake import STEP_TO_MODULE
            from snappy_pipeline.base import snakefile_path
        except ImportError:
            logger.warn(
                "snappy_pipeline is not available. snappy pipeline related functions will not work properly."
            )
            return None

        return cls(
            expand_ref=expand_ref, step_to_module=STEP_TO_MODULE, snakefile_path=snakefile_path
        )

    def _load_workflow_step_configuration(
        self, workflow_step_path: typing.Union[str, pathlib.Path]
    ) -> tuple:
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

        config, lookup_paths, config_paths = self._expand_ref(
            str(config_path),
            config_data,
            [
                str(workflow_step_path),
                str(pathlib.Path(workflow_step_path).parent / ".snappy_pipeline"),
            ],
        )
        return config, lookup_paths, config_paths

    def _get_workflow_name(
        self, workflow_path: typing.Union[str, pathlib.Path]
    ) -> typing.Optional[str]:
        """Get the name of the workflow in the directory. This will parse the contained config.yaml."""

        config, _, _ = self._load_workflow_step_configuration(workflow_path)
        if config is not None and "pipeline_step" in config:
            return config["pipeline_step"].get("name", None)

    def get_snappy_step_directories(
        self, snappy_root_dir: typing.Union[str, pathlib.Path]
    ) -> typing.Dict[str, pathlib.Path]:
        """Get a dictionary of snappy workflow step names and their associated path.

        :param snappy_root_dir: Path to the snappy root directory, also containing .snappy_pipeline

        :return: Dict of workflow step name to workflow step path.
        """
        folder_steps = {
            name: path
            for name, path in [
                (self._get_workflow_name(p), p) for p in pathlib.Path(snappy_root_dir).iterdir()
            ]
            if name in self._step_to_module
        }

        return folder_steps

    def get_workflow_step_dependencies(
        self, workflow_step_path: typing.Union[str, pathlib.Path]
    ) -> typing.List[str]:
        """Find workflow dependencies for the given workflow step.
        :param workflow_step_path: Path to the workflow step.

        :return: List of dependencies for the given workflow step.
        """
        workflow_step_path = pathlib.Path(workflow_step_path)

        config, lookup_paths, config_paths = self._load_workflow_step_configuration(
            workflow_step_path
        )
        if config is None:
            raise RuntimeError(
                f"Could not load workflow step confiuration for {workflow_step_path}"
            )

        step_name = config["pipeline_step"]["name"]

        module_snakefile = self._snakefile_path(step_name)

        # get the name of the workflow step class name
        obj_name = get_workflow_snakefile_object_name(module_snakefile)
        if obj_name is None:
            raise RuntimeError(f"Could not find workflow object for {step_name}")

        workflow_module = importlib.import_module(f".{step_name}", "snappy_pipeline.workflows")
        workflow_class = getattr(workflow_module, obj_name)
        assert workflow_class.name == step_name

        workflow_object = workflow_class(
            DummyWorkflow(), config, lookup_paths, config_paths, str(workflow_step_path)
        )
        dependencies = workflow_object.sub_workflows.keys()
        return list(dependencies)
