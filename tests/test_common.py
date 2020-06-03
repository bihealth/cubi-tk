"""Tests for common code."""

from cubi_tk import common


def test_is_uuid():
    assert not common.is_uuid(1)
    assert not common.is_uuid(None)
    assert not common.is_uuid("x")
    assert common.is_uuid("123e4567-e89b-12d3-a456-426655440000")
