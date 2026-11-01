"""
Bounded in-memory run-group construction seam.

This module owns the only new import of ``softlab.shui.data``: the
codec module :mod:`softlab.shui.run_record` stays standard-library
only, while this thin seam attaches successfully encoded run-record
text to an empty :class:`~softlab.shui.data.base.DataGroup`. Nothing
here performs I/O; persistence integration is a later phase.
"""

import uuid
from typing import (
    Any,
    Dict,
)

from softlab.shui.data.base import DataGroup
from softlab.shui.run_record import (
    METADATA_KEY,
    encode_envelope,
)

__all__ = [
    'build_run_group',
]


def build_run_group(name: str, envelope: Dict[str, Any]) -> DataGroup:
    """Construct an in-memory ``DataGroup`` carrying one validated run record.

    Name-handling contract (frozen): the seam enforces an exact
    nonempty ``str`` name itself, first — it never relies on
    ``DataGroup``'s inherited ``str()`` coercion, which silently
    stringifies a non-text name instead of rejecting it. The group
    name is a ``DataGroup`` construction argument, not part of the
    record contract, so these argument failures are builtin
    ``TypeError``/``ValueError``, not ``RunRecordError`` subclasses.

    Steps, in order (failure atomicity: any failure raises before the
    group exists, so no partially populated group can escape):

    0. ``TypeError`` unless ``name`` is an exact ``str``; ``ValueError``
       for an empty name — both raised before any construction.
    1. ``encode_envelope(envelope)`` — full validation and encoding;
       any defect raises before anything is constructed or attached.
    2. ``DataGroup(name=name, id=uuid.UUID(envelope['group_id']))`` —
       an empty group; ``meta=`` is never passed, and the id is taken
       from the validated envelope, so ``str(group.id) ==
       envelope['group_id']`` holds by construction.
    3. ``group.set_meta(METADATA_KEY, text)`` — exactly one
       string-keyed encoded-text entry; ``set_meta`` is never called
       with value ``None``.

    No backend, no file handle, no save/connect/disconnect — the
    returned group is purely in-memory.

    Raises ``TypeError`` for a non-``str`` name, ``ValueError`` for an
    empty name, and the codec's ``TypeError``/``RecordValidationError``
    for an invalid envelope.
    """
    if type(name) is not str:
        raise TypeError('run_group: name must be an exact str')
    if name == '':
        raise ValueError('run_group: name must be nonempty')
    text = encode_envelope(envelope)
    group = DataGroup(name=name, id=uuid.UUID(envelope['group_id']))
    group.set_meta(METADATA_KEY, text)
    return group
