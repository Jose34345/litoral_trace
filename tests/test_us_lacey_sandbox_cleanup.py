from __future__ import annotations

import pytest

from litoral_trace.storage import ObjectDeleteResult
from litoral_trace.workers.sandbox_cleanup import (
    SandboxCleanupStorageError,
    SandboxCleanupUnsupportedStorage,
    SandboxObjectRef,
    _delete_storage_objects,
)


class _Storage:
    bucket_name = "bucket"

    def __init__(self, *, survives_delete=False, delete_marker=False):
        self.exists = True
        self.survives_delete = survives_delete
        self.delete_marker = delete_marker
        self.events = []

    def object_exists(self, *, key, version_id=None):
        self.events.append(("exists", key, version_id, self.exists))
        return self.exists

    def delete_object(self, *, key, version_id=None):
        self.events.append(("delete", key, version_id))
        if not self.survives_delete:
            self.exists = False
        return ObjectDeleteResult(
            delete_marker=self.delete_marker,
            version_id=version_id,
        )


def _manifest():
    return (
        SandboxObjectRef(
            id=1,
            bucket="bucket",
            key="sandbox/one.pdf",
            version_id=None,
        ),
    )


def test_storage_delete_is_followed_by_active_absence_check():
    storage = _Storage()

    _delete_storage_objects(storage=storage, manifest=_manifest())

    assert storage.events == [
        ("exists", "sandbox/one.pdf", None, True),
        ("delete", "sandbox/one.pdf", None),
        ("exists", "sandbox/one.pdf", None, False),
    ]


def test_storage_delete_fails_closed_when_object_survives():
    storage = _Storage(survives_delete=True)

    with pytest.raises(
        SandboxCleanupStorageError,
        match="still exists",
    ):
        _delete_storage_objects(storage=storage, manifest=_manifest())


def test_unversioned_delete_marker_is_not_accepted_as_physical_deletion():
    storage = _Storage(delete_marker=True)

    with pytest.raises(
        SandboxCleanupUnsupportedStorage,
        match="delete marker",
    ):
        _delete_storage_objects(storage=storage, manifest=_manifest())
