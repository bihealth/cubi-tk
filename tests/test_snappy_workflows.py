"""Tests for ``cubi_tk.snappy.snappy_workflows``.
"""

from cubi_tk.snappy.snappy_workflows import SnappyWorkflowManager

from .hide_modules import hide_modules


@hide_modules(["snappy_pipeline"])
def test_could_not_import_module():
    manager = SnappyWorkflowManager.from_snappy()
    assert manager is None


def test_could_import_module():
    manager = SnappyWorkflowManager.from_snappy()
    assert manager is not None
    assert callable(manager._expand_ref)
    assert len(manager._step_to_module.keys()) > 0
