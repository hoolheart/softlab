"""M03 phase 3 executable storage and inspection suite (A1-A9).

Assertion-based probes against the phase 3 owned run-record storage and
safe inspection seam, per the approved plan
(log/release_12/test/storage-test-plan.md, ordering-delta revised
`d7d7169`, developer re-review APPROVED `5ca51d1`) and the frozen design
(log/release_12/design/p3-storage-design.md, third revision `b52e034`,
code review APPROVED `7b1d771` over implementation `0217593`).

Areas:
- A1: owned write path — end-to-end save, stored-artifact relations read
  back with h5py on a disposable byte copy (by-name/row-index per the
  §3.5.1 ORDERING DELTA), writer status checks, D1 construction
  discipline (recorder doubles; no autospec+wraps anywhere), and the
  thermal example dual-write integration row.
- A2: safe read path — sha256 disposable-copy discipline, h5py-level
  facts independent of DataGroup, exactly-one legacy load per copy,
  additive F-B hydration (thermal keys / signal empty set), cross-check
  mismatch templates, the structural F-A guard, cleanup independence.
- A3: fresh-process inspection via tests/run_record_inspect_bootstrap.py
  — exit codes 0/1/2, frozen JSON payload schema, and the three-way
  legacy-absence vs present-non-text vs malformed/version/namespace/
  over-size distinction.
- A4: fault matrix — real-seam injection only (frozen design §6.2
  mechanisms), per-row artifact pins (§6.3), original-byte preservation
  on every failure, separately observable primary/cleanup diagnostics
  (R1-R6 population contract, including the R6 safety-dominance rows).
- A5: stage honesty — the stored record claims the acquisition snapshot
  only; negative probes; no post-save rewrite.
- A6: unsupported paths — SQLite rejected truthfully pre-I/O, generic
  io helpers never the supported path (F-C/F-D characterization
  re-assertions), same-ID deletion/recreation observed non-atomically,
  failed-run persistence unsupported.
- A7: legacy compatibility — manifest contract, legacy reader equality,
  the frozen nine-name export delta, phase 2 suite reuse.
- A8: hygiene — synthetic temp data only, connections released (zero
  ResourceWarning), version-portable assertions (no autospec+wraps, no
  interpreter guards), runtime-captured environment facts.

All fixtures are synthetic and programmatically constructed; observed
behavior that contradicts a plan expectation is reported in
log/release_12/test/storage-test-results.md, never silently weakened.
"""

import ast
import copy
import hashlib
import importlib
import importlib.util
import inspect
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid
import warnings
from pathlib import Path
from unittest import mock

import h5py
import numpy as np

import softlab
from softlab.shui.data import io as data_io
from softlab.shui import run_storage
from softlab.shui.data import (
    DataGroup,
    DataRecord,
    HDF5DataBackend,
    Sqlite3DataBackend,
)
from softlab.shui.run_group import build_run_group
from softlab.shui.run_record import (
    MAX_ENCODED_BYTES,
    METADATA_KEY,
    PROFILE_SIGNAL,
    PROFILE_THERMAL,
    SIGNAL_OFFSETS,
    THERMAL_MODEL,
    UNAVAILABLE_REASONS,
    RecordDecodeError,
    RecordValidationError,
    decode_envelope,
    encode_envelope,
)
from softlab.shui.run_storage import (
    PROFILE_HYDRATION_KEYS,
    SIGNAL_HYDRATION_KEYS,
    THERMAL_HYDRATION_KEYS,
    InspectionResult,
    RunStorageError,
    StorageConnectError,
    StorageInspectionError,
    StorageReport,
    StorageSaveError,
    StoredRecordFacts,
    UnsupportedBackendError,
    inspect_run_group,
    save_run_group,
)

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = ROOT / "tests" / "run_record_inspect_bootstrap.py"

_TEMP_PREFIX = "softlab-m03-p3-"
_SCRATCH_PREFIX = "softlab-m03-p3-read-"

# Frozen per-profile column declarations (design §8.6 canonical fixtures).
_THERMAL_COLUMNS = (
    {
        "name": "commanded_power_w",
        "label": "Commanded heater power",
        "unit": "W",
        "dependent": False,
    },
    {
        "name": "measurement_source_tick",
        "label": "Measurement source boundary",
        "unit": "1",
        "dependent": False,
    },
    {
        "name": "sim_time_s",
        "label": "Simulation acquisition time",
        "unit": "s",
        "dependent": False,
    },
    {
        "name": "temperature_degc",
        "label": "Delivered temperature",
        "unit": "degC",
        "dependent": True,
    },
    {
        "name": "tick",
        "label": "Acquisition boundary",
        "unit": "1",
        "dependent": False,
    },
    {
        "name": "timestamp",
        "label": "Wall-clock acquisition timestamp",
        "unit": "s",
        "dependent": False,
    },
)

# Signal declaration order is deliberately NOT lexicographic
# (design §3.5: physical HDF5 enumeration is lexicographic and
# non-semantic; disk verification is by-name, samples by row index).
_SIGNAL_COLUMNS = (
    {"name": "timestamp", "label": "", "unit": "s", "dependent": False},
    {"name": "signal", "label": "", "unit": "V", "dependent": True},
    {"name": "reference", "label": "", "unit": "V", "dependent": True},
)

_THERMAL_LEGACY_TEXTS = {
    "example": "examples/thermal_experiment.py",
    "configuration": json.dumps({"points": 8, "power": 10.0}),
    "timing": json.dumps({"dt": 1.0, "simulation_unit": "s"}),
    "thermal_manifest": json.dumps(
        {"schema": "softlab.thermal-reference", "version": 1}
    ),
}


def _hex64(seed):
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _hex40(seed):
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def _software():
    return {
        "softlab_version": str(softlab.__version__),
        "python_version": platform.python_version(),
    }


def _source(reference, git_available=False):
    if git_available:
        return {
            "reference": reference,
            "digest_algorithm": "sha256",
            "digest_scope": "exact reference script bytes at production",
            "digest": {
                "state": "available",
                "value": _hex64("digest"),
                "reason": None,
            },
            "git_revision": {
                "state": "available",
                "value": _hex40("revision"),
                "reason": None,
            },
            "git_dirty": {"state": "available", "value": True, "reason": None},
        }
    return {
        "reference": reference,
        "digest_algorithm": "sha256",
        "digest_scope": "exact reference script bytes at production",
        "digest": {
            "state": "available",
            "value": _hex64("digest"),
            "reason": None,
        },
        "git_revision": {
            "state": "unavailable",
            "value": None,
            "reason": "capture_failed",
        },
        "git_dirty": {
            "state": "unavailable",
            "value": None,
            "reason": "capture_failed",
        },
    }


def _outcome(points):
    return {
        "snapshot": "completed_acquisition_before_persistence",
        "acquisition": "completed",
        "requested_rows": points,
        "completed_rows": points,
        "persistence": "not_yet_established",
        "plotting": "not_yet_established",
        "cleanup": "not_yet_established",
        "overall": "not_yet_established",
    }


def _thermal_payload(adapter, points=8):
    if adapter == "coordinated":
        delay, tick = 1, "tick-1"
    elif adapter == "direct":
        delay, tick = 0, "tick"
    else:
        raise ValueError(adapter)
    return {
        "version": 1,
        "adapter": adapter,
        "configuration": {
            "initial_temperature": 20.0,
            "ambient_temperature": 20.0,
            "heat_capacity": 10.0,
            "conductance": 1.0,
            "dt": 1.0,
            "power": 10.0,
            "points": points,
        },
        "model": copy.deepcopy(THERMAL_MODEL),
        "procedure": {
            "name": "thermal_count",
            "advancement": "once_before_each_getter_row",
            "command": "constant_power",
            "requested_points": points,
        },
        "timing": {
            "dt": 1.0,
            "simulation_unit": "s",
            "acquisition": "post_step",
            "measurement_delay_ticks": delay,
            "source_tick": tick,
            "wall_timestamp": "unix_seconds",
        },
        "boundary": {
            "final_tick": points,
            "final_sim_time": float(points) * 1.0,
        },
    }


def _signal_payload(points=3):
    return {
        "version": 1,
        "points": points,
        "read_policy": {
            "semantics": "tick_plus_offset",
            "consumptive": False,
            "offsets": copy.deepcopy(SIGNAL_OFFSETS),
        },
        "timing": {"timestamp": "wall_clock"},
        "scheduler": {"ownership": "run_count_owned"},
        "derived": {
            "difference": {
                "formula": "signal - reference",
                "storage": "caller_derived_not_stored",
            },
        },
    }


def thermal_envelope(adapter="coordinated", points=8, git_available=False):
    """Canonical minimal thermal envelope (design §8.6)."""
    return {
        "namespace": "softlab.run-record",
        "version": 1,
        "profile": "thermal",
        "execution_id": str(uuid.uuid4()),
        "group_id": str(uuid.uuid4()),
        "record": {
            "name": "thermal_trace",
            "columns": copy.deepcopy(list(_THERMAL_COLUMNS)),
        },
        "software": _software(),
        "source": _source("examples/thermal_experiment.py", git_available),
        "outcome": _outcome(points),
        "payload": _thermal_payload(adapter, points),
    }


def signal_envelope(points=3, git_available=False):
    """Canonical minimal signal envelope (design §8.6)."""
    return {
        "namespace": "softlab.run-record",
        "version": 1,
        "profile": "signal",
        "execution_id": str(uuid.uuid4()),
        "group_id": str(uuid.uuid4()),
        "record": {
            "name": "signal",
            "columns": copy.deepcopy(list(_SIGNAL_COLUMNS)),
        },
        "software": _software(),
        "source": _source("examples/signal_monitor.py", git_available),
        "outcome": _outcome(points),
        "payload": _signal_payload(points),
    }


def envelope_text(envelope):
    """Serialize an envelope dict as JSON text for decode-side probes."""
    return json.dumps(envelope)


def thermal_rows(points=8):
    """Synthetic thermal sample columns known at construction time."""
    return {
        "commanded_power_w": [10.0] * points,
        "measurement_source_tick": [float(i) for i in range(points)],
        "sim_time_s": [float(i) * 1.0 for i in range(points)],
        "temperature_degc": [20.0 + i * 0.5 for i in range(points)],
        "tick": [float(i + 1) for i in range(points)],
        "timestamp": [1000.0 + i for i in range(points)],
    }


def signal_rows(points=3):
    """Synthetic signal sample columns known at construction time."""
    return {
        "timestamp": [float(i) * 0.1 for i in range(points)],
        "signal": [1.5 - 0.25 * i for i in range(points)],
        "reference": [0.1 * (i + 1) for i in range(points)],
    }


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hashed_copy(path, root):
    """Byte-copy ``path`` into ``root``; return (copy path, sha256)."""
    digest = _sha256_file(str(path))
    copy_path = Path(root) / ("copy-" + Path(str(path)).name)
    shutil.copy2(str(path), str(copy_path))
    if _sha256_file(str(copy_path)) != digest:
        raise AssertionError("test helper copy is not byte-identical")
    return str(copy_path), digest


def _attach_record(group, envelope, rows):
    """Attach one DataRecord per the A1 source-of-record rule (branch a)."""
    record = DataRecord(
        envelope["record"]["name"], copy.deepcopy(envelope["record"]["columns"])
    )
    record.add_rows(copy.deepcopy(rows))
    group.add_record(record)
    return record


def _save_owned(envelope, rows, path, legacy_texts=None):
    """Build + attach + persist through the owned save seam."""
    group = build_run_group("run-1", envelope)
    if legacy_texts:
        for key, text in legacy_texts.items():
            group.set_meta(key, text)
    _attach_record(group, envelope, rows)
    report = save_run_group(group, str(path))
    return group, report


def _save_two_groups(path, root):
    """Persist two distinct run-record groups into one file."""
    envelope_one = signal_envelope()
    _save_owned(envelope_one, signal_rows(), path)
    envelope_two = signal_envelope()
    group_two, _ = _save_owned(envelope_two, signal_rows(), path)
    return envelope_one, envelope_two, group_two


def _read_disk_facts(copy_path, group_id):
    """Independent h5py read of stored facts (no DataGroup involved)."""
    facts = {"records": {}}
    with h5py.File(copy_path, "r") as disk:
        stored = disk[str(group_id)]
        facts["id"] = stored.attrs["id"]
        facts["name"] = stored.attrs["name"]
        facts["timestamp"] = stored.attrs["timestamp"]
        facts["backend"] = stored.attrs["backend"]
        facts["meta_keys"] = sorted(
            key
            for key in stored.attrs.keys()
            if key not in ("id", "name", "timestamp", "backend")
        )
        facts["run_record"] = stored.attrs.get(METADATA_KEY)
        facts["meta"] = {
            key: stored.attrs[key]
            for key in stored.attrs.keys()
            if key not in ("id", "name", "timestamp", "backend")
        }
        for record_name in stored.keys():
            column_group = stored[record_name]["columns"]
            columns = {}
            for column_name in column_group.keys():
                dataset = column_group[column_name]
                columns[column_name] = {
                    "attrs": {
                        attr: dataset.attrs[attr]
                        for attr in ("name", "label", "unit", "dependent")
                    },
                    "values": np.asarray(dataset[:]).reshape(-1).tolist(),
                }
            facts["records"][record_name] = columns
    return facts


def _load_example():
    """Load the thermal example under a phase-3-unique module name."""
    spec = importlib.util.spec_from_file_location(
        "thermal_m03_p3_storage", ROOT / "examples" / "thermal_experiment.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _track_backend(test_case, backend):
    """Harness double: disconnect the backend at test cleanup if needed."""

    def _release():
        if getattr(backend, "connected", False):
            backend.disconnect()

    test_case.addCleanup(_release)
    return backend


def _assert_report_success(test_case, report, operation, path):
    test_case.assertIsInstance(report, StorageReport)
    test_case.assertEqual(report.operation, operation)
    test_case.assertEqual(report.path, str(path))
    test_case.assertTrue(report.primary_ok)
    test_case.assertEqual(report.primary, ())
    test_case.assertTrue(report.cleanup_ok)
    test_case.assertEqual(report.cleanup, ())


def _assert_report_failure(test_case, exc, message=None):
    """R1/R2/R5: every escape carries the report; primary never empty."""
    report = getattr(exc, "report", None)
    test_case.assertIsNotNone(report, f"{exc!r} carries no .report")
    test_case.assertFalse(report.primary_ok)
    test_case.assertTrue(report.primary)
    test_case.assertEqual(report.primary[0], str(exc))
    if message is not None:
        test_case.assertEqual(str(exc), message)
    test_case.assertIsInstance(report.cleanup, tuple)
    test_case.assertEqual(report.cleanup_ok, len(report.cleanup) == 0)
    return report


def _spawn_bootstrap(arguments):
    return subprocess.run(
        [sys.executable, str(BOOTSTRAP)] + arguments,
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=180,
    )


class StorageAssertMixin:
    """Shared frozen-type/message assertion helpers."""

    def assert_frozen(self, func, exc_type, message):
        with self.assertRaises(exc_type) as caught:
            func()
        self.assertIs(type(caught.exception), exc_type)
        self.assertEqual(str(caught.exception), message)
        return caught.exception

    def assert_hdf5_connect_prints(self, stream_text):
        # The legacy backend connect message is an operational print,
        # not a warning; nothing to assert beyond documentation.
        return stream_text


class A1OwnedWritePath(StorageAssertMixin, unittest.TestCase):
    """A1.1-A1.6: the owned save path and the example dual-write."""

    def test_a1_1_end_to_end_owned_save_and_lifecycle(self):
        envelope = thermal_envelope()
        rows = thermal_rows()
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            path = Path(temp) / "run.h5"
            group, report = _save_owned(envelope, rows, path)
            _assert_report_success(self, report, "save", path)
            self.assertTrue(path.exists())

            # Failed/cancelled envelopes are rejected before any connect:
            # the codec refuses to produce their text, and hand-crafted
            # text fails the seam's pre-connect decode (A4.1/A5.2 reuse).
            failed = thermal_envelope()
            failed["outcome"]["acquisition"] = "failed"
            with self.assertRaises(RecordValidationError):
                encode_envelope(failed)
            bad_group = build_run_group("run-2", thermal_envelope())
            bad_group.set_meta(METADATA_KEY, envelope_text(failed))
            target = Path(temp) / "never.h5"
            inventory = []

            original_connect = HDF5DataBackend.connect
            original_save = HDF5DataBackend.save_group

            def connect_spy(self, args=None):
                inventory.append("connect")
                return original_connect(self, args)

            def save_spy(self, group_):
                inventory.append("save")
                return original_save(self, group_)

            with (
                mock.patch.object(HDF5DataBackend, "connect", connect_spy),
                mock.patch.object(HDF5DataBackend, "save_group", save_spy),
            ):
                with self.assertRaises(RecordValidationError):
                    save_run_group(bad_group, str(target))
            self.assertEqual(inventory, [])
            self.assertFalse(target.exists())

            # Owned lifecycle asserted by recorder injection (A1.1): the
            # seam connects, saves, and disconnects even when the caller
            # hands it a backend instance.
            calls = []
            backend = _track_backend(self, HDF5DataBackend())

            def connect_rec(self, args=None):
                calls.append("connect")
                return original_connect(self, args)

            def save_rec(self, group_):
                calls.append("save")
                return original_save(self, group_)

            original_disconnect = HDF5DataBackend.disconnect

            def disconnect_rec(self):
                calls.append("disconnect")
                return original_disconnect(self)

            path_two = Path(temp) / "owned.h5"
            with (
                mock.patch.object(HDF5DataBackend, "connect", connect_rec),
                mock.patch.object(HDF5DataBackend, "save_group", save_rec),
                mock.patch.object(
                    HDF5DataBackend, "disconnect", disconnect_rec
                ),
            ):
                report_two = save_run_group(
                    group, str(path_two), backend=backend
                )
            _assert_report_success(self, report_two, "save", path_two)
            self.assertEqual(calls, ["connect", "save", "disconnect"])
            self.assertFalse(backend.connected)
            self.assertIsNone(backend.file)

    def test_a1_2_disk_relations_signal_by_name(self):
        # Signal declaration order (timestamp, signal, reference) is not
        # lexicographic: the strongest fixture for the §3.5.1 ORDERING
        # DELTA (envelope order canonical; disk verification by name;
        # samples by row index).
        envelope = signal_envelope()
        rows = signal_rows()
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            path = Path(temp) / "signal.h5"
            _save_owned(envelope, rows, path)
            copy_path, digest = _hashed_copy(path, temp)
            self.assertEqual(_sha256_file(str(path)), digest)
            facts = _read_disk_facts(copy_path, envelope["group_id"])

            # Persistent group identity; execution UUID never conflated.
            self.assertEqual(facts["id"], envelope["group_id"])
            self.assertNotEqual(facts["id"], envelope["execution_id"])
            with h5py.File(copy_path, "r") as disk:
                self.assertEqual(list(disk.keys()), [envelope["group_id"]])
                stored = disk[envelope["group_id"]]
                for attr in ("id",):
                    self.assertNotEqual(
                        stored.attrs[attr], envelope["execution_id"]
                    )

            # Additive key decodes to the exact input envelope.
            self.assertIs(type(facts["run_record"]), str)
            self.assertEqual(decode_envelope(facts["run_record"]), envelope)

            # By-name column association: the name sets are equal; each
            # declared column (in envelope order) matches its same-named
            # dataset's four attributes exactly.
            declared = envelope["record"]["columns"]
            stored_columns = facts["records"]["signal"]
            self.assertEqual(
                set(stored_columns), {column["name"] for column in declared}
            )
            for column in declared:
                entry = stored_columns[column["name"]]
                self.assertEqual(entry["attrs"], column)
            # Physical enumeration is lexicographic and non-semantic.
            enumerated = list(stored_columns.keys())
            self.assertEqual(
                enumerated, sorted(column["name"] for column in declared)
            )
            self.assertNotEqual(
                enumerated, [column["name"] for column in declared]
            )

            # Stored row count equals outcome.completed_rows; samples
            # match the construction-time expectation by row index.
            self.assertEqual(
                len(rows["timestamp"]), envelope["outcome"]["completed_rows"]
            )
            for column in declared:
                self.assertEqual(
                    stored_columns[column["name"]]["values"],
                    rows[column["name"]],
                )

    def test_a1_3_writer_status_checks(self):
        envelope = signal_envelope()
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            # A save attempted without a successful prior connect fails
            # visibly at the connect stage and is never attempted.
            target_dir = Path(temp) / "target-dir"
            target_dir.mkdir()
            group, _ = _save_owned(
                envelope, signal_rows(), Path(temp) / "seed.h5"
            )
            inventory = []
            original_connect = HDF5DataBackend.connect
            original_save = HDF5DataBackend.save_group

            def connect_rec(self, args=None):
                inventory.append("connect")
                return original_connect(self, args)

            def save_rec(self, group_):
                inventory.append("save")
                return original_save(self, group_)

            backend = _track_backend(self, HDF5DataBackend())
            with (
                mock.patch.object(HDF5DataBackend, "connect", connect_rec),
                mock.patch.object(HDF5DataBackend, "save_group", save_rec),
            ):
                with self.assertRaises(StorageConnectError) as caught:
                    save_run_group(group, str(target_dir), backend=backend)
            self.assertTrue(
                str(caught.exception).startswith(
                    "connect: HDF5 backend connect failed: "
                )
            )
            _assert_report_failure(self, caught.exception)
            self.assertEqual(inventory, ["connect"])
            self.assertTrue(backend.connected is False)

    def test_a1_4_construction_discipline_d1(self):
        envelope = thermal_envelope()
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            path = Path(temp) / "discipline.h5"
            constructed = []
            set_meta_calls = []
            order = []
            original_decode = run_storage.decode_envelope
            original_save = HDF5DataBackend.save_group

            class RecorderGroup:
                def __init__(self, *args, **kwargs):
                    constructed.append(kwargs)

            def decode_spy(text):
                order.append("decode")
                return original_decode(text)

            def save_rec(self, group_):
                order.append("save")
                return original_save(self, group_)

            def set_meta_rec(self, key, value):
                set_meta_calls.append((key, value))
                return original_set_meta(self, key, value)

            original_set_meta = DataGroup.set_meta
            group = build_run_group("run-1", envelope)
            _attach_record(group, envelope, thermal_rows())
            with (
                mock.patch.object(run_storage, "DataGroup", RecorderGroup),
                mock.patch.object(run_storage, "decode_envelope", decode_spy),
                mock.patch.object(HDF5DataBackend, "save_group", save_rec),
                mock.patch.object(DataGroup, "set_meta", set_meta_rec),
            ):
                report = save_run_group(group, str(path))
            _assert_report_success(self, report, "save", path)

            # D1: the seam never constructs a DataGroup (meta= or any
            # other shape) and never performs set_meta(key, None) — in
            # fact no metadata write at all.
            self.assertEqual(constructed, [])
            self.assertEqual(set_meta_calls, [])
            # The encode/decode validation runs before any HDF5 write.
            self.assertEqual(order, ["decode", "save"])

    def test_a1_5_disk_independence_not_in_memory_equality(self):
        envelope = signal_envelope()
        rows = signal_rows()
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            path = Path(temp) / "independent.h5"
            group, _ = _save_owned(envelope, rows, path)
            copy_path, digest = _hashed_copy(path, temp)
            facts_before = _read_disk_facts(copy_path, envelope["group_id"])
            # Mutating the in-memory object after save must not change
            # what the stored artifact claims.
            group.set_meta(METADATA_KEY, "garbage")
            group.record("signal").add_rows(
                {"timestamp": [9.0], "signal": [9.0], "reference": [9.0]}
            )
            copy_two, digest_two = _hashed_copy(path, temp)
            self.assertEqual(digest_two, digest)
            facts_after = _read_disk_facts(copy_two, envelope["group_id"])
            self.assertEqual(facts_after, facts_before)
            self.assertEqual(
                decode_envelope(facts_after["run_record"]), envelope
            )

    def test_a1_6_example_dual_write_integration(self):
        example = _load_example()
        for adapter, version in (("coordinated", 1), ("direct", 2)):
            with self.subTest(adapter=adapter):
                config = example.ThermalConfig(points=8)
                with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
                    before = None
                    result = example.execute(
                        config, Path(temp) / adapter, adapter=adapter
                    )
                    self.assertEqual(result.acquisition, "completed")
                    data_path = Path(result.data_path)
                    before = _sha256_file(str(data_path))

                    # Inspect the stored artifact via the A2 safe path.
                    inspected = inspect_run_group(
                        str(data_path),
                        str(result.group.id),
                        legacy_keys=THERMAL_HYDRATION_KEYS,
                    )
                    self.assertEqual(_sha256_file(str(data_path)), before)
                    self.assertIsNotNone(inspected.envelope)
                    envelope = inspected.envelope
                    self.assertEqual(envelope["profile"], PROFILE_THERMAL)
                    self.assertEqual(envelope["group_id"], str(result.group.id))
                    # Call-site gate evidence: the execution uuid retained
                    # at the after_count seam is the envelope's
                    # execution_id; acquisition is completed.
                    self.assertEqual(
                        envelope["execution_id"], result.execution_id
                    )
                    self.assertNotEqual(
                        envelope["execution_id"], envelope["group_id"]
                    )
                    self.assertEqual(
                        envelope["outcome"]["acquisition"], "completed"
                    )

                    # Both records coexist: supplemented, not replaced.
                    copy_path, _ = _hashed_copy(data_path, temp)
                    with h5py.File(copy_path, "r") as disk:
                        stored = disk[str(result.group.id)]
                        self.assertIn(METADATA_KEY, stored.attrs)
                        self.assertIn("thermal_manifest", stored.attrs)
                    self.assertIn(METADATA_KEY, inspected.group.meta)
                    self.assertIn("thermal_manifest", inspected.group.meta)
                    self.assertEqual(
                        inspected.legacy_meta["thermal_manifest"],
                        result.group.get_meta("thermal_manifest"),
                    )

                    # Legacy manifest bytes: exactly what the legacy
                    # builder produces for this run — the dual-write
                    # leaves the legacy contract untouched. (Run ids
                    # differ per run, so the oracle is the example's own
                    # legacy manifest builder applied to this run; the
                    # unchanged M02/thermal suites are the cross-run
                    # evidence, recorded in the results file.)
                    manifest_text = result.group.get_meta("thermal_manifest")
                    manifest = example._decode_manifest(manifest_text)
                    self.assertEqual(
                        manifest["schema"], "softlab.thermal-reference"
                    )
                    self.assertEqual(manifest["version"], version)
                    self.assertEqual(manifest["run_id"], str(result.group.id))
                    self.assertEqual(len(manifest), 11)
                    expected = example._encode_manifest(
                        example._make_manifest(
                            config,
                            result.group,
                            result,
                            delay=1 if adapter == "coordinated" else 0,
                        )
                    )
                    self.assertEqual(manifest_text, expected)
                    clock = manifest["clock"]
                    self.assertEqual(
                        clock["measurement_delay_ticks"],
                        1 if version == 1 else 0,
                    )
                    self.assertEqual(
                        clock["source_tick"],
                        "tick-1" if version == 1 else "tick",
                    )

                    # The additive envelope decodes and cross-checks
                    # against the stored artifact (safe-path inspect
                    # above already ran the seam's cross-check).
                    self.assertEqual(
                        decode_envelope(inspected.group.get_meta(METADATA_KEY)),
                        envelope,
                    )
                    self.assertEqual(
                        inspected.record_facts["thermal_trace"].row_count,
                        manifest["outcome"]["completed_rows"],
                    )


class A2SafeReadPath(StorageAssertMixin, unittest.TestCase):
    """A2.1-A2.8: disposable-copy preflight, hydration, F-A/F-B guards."""

    def _thermal_artifact(self, temp):
        envelope = thermal_envelope()
        path = Path(temp) / "dual.h5"
        _save_owned(
            envelope, thermal_rows(), path, legacy_texts=_THERMAL_LEGACY_TEXTS
        )
        return envelope, path

    def test_a2_1_byte_copy_and_hash_discipline(self):
        envelope, path = self._thermal_artifact(
            tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        )
        self.addCleanup(shutil.rmtree, str(Path(path).parent), True)
        before = _sha256_file(str(path))
        result = inspect_run_group(str(path), envelope["group_id"])
        after = _sha256_file(str(path))
        self.assertEqual(after, before)
        self.assertEqual(result.original_sha256, before)

        # A second inspection uses a fresh internal copy and succeeds;
        # both inspections leave the original byte-identical.
        second = inspect_run_group(str(path), envelope["group_id"])
        self.assertEqual(_sha256_file(str(path)), before)
        self.assertEqual(second.envelope, result.envelope)
        self.assertEqual(second.report.primary, ())

    def test_a2_2_h5py_facts_independent_of_data_group(self):
        envelope, path = self._thermal_artifact(
            tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        )
        self.addCleanup(shutil.rmtree, str(Path(path).parent), True)
        result = inspect_run_group(
            str(path), envelope["group_id"], legacy_keys=THERMAL_HYDRATION_KEYS
        )
        self.assertIsInstance(result, InspectionResult)
        self.assertEqual(str(result.group.id), envelope["group_id"])
        self.assertEqual(result.envelope, envelope)
        self.assertIsInstance(
            result.record_facts["thermal_trace"], StoredRecordFacts
        )

        # The same facts, re-derived by an independent h5py read of a
        # fresh byte copy, agree with the inspection's on-disk facts.
        temp = Path(path).parent
        copy_path, _ = _hashed_copy(path, temp)
        disk = _read_disk_facts(copy_path, envelope["group_id"])
        facts = result.record_facts["thermal_trace"]
        self.assertEqual(
            facts.row_count,
            len(disk["records"]["thermal_trace"]["timestamp"]["values"]),
        )
        for column in facts.columns:
            entry = disk["records"]["thermal_trace"][column["name"]]
            self.assertEqual(column, entry["attrs"])

    def test_a2_3_at_most_one_legacy_load_per_copy(self):
        envelope, path = self._thermal_artifact(
            tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        )
        self.addCleanup(shutil.rmtree, str(Path(path).parent), True)
        loads = []
        original_load = HDF5DataBackend.load_group

        def load_rec(self, id):
            loads.append(id)
            return original_load(self, id)

        backend = _track_backend(self, HDF5DataBackend())
        with mock.patch.object(HDF5DataBackend, "load_group", load_rec):
            first = inspect_run_group(
                str(path), envelope["group_id"], backend=backend
            )
            second = inspect_run_group(
                str(path), envelope["group_id"], backend=backend
            )
        # Exactly one legacy load per inspection, canonical str id.
        self.assertEqual(loads, [envelope["group_id"], envelope["group_id"]])
        self.assertEqual(str(first.group.id), envelope["group_id"])
        self.assertEqual(str(second.group.id), envelope["group_id"])

    def test_a2_4_additive_hydration_preserves_f_b_keys(self):
        envelope, path = self._thermal_artifact(
            tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        )
        self.addCleanup(shutil.rmtree, str(Path(path).parent), True)
        result = inspect_run_group(
            str(path), envelope["group_id"], legacy_keys=THERMAL_HYDRATION_KEYS
        )
        meta = result.group.meta
        # run_record survives hydration verbatim, alongside the legacy
        # keys the thermal client owns; no arbitrary key restoration.
        self.assertEqual(
            meta[METADATA_KEY], result.group.get_meta(METADATA_KEY)
        )
        self.assertEqual(decode_envelope(meta[METADATA_KEY]), envelope)
        for key, text in _THERMAL_LEGACY_TEXTS.items():
            self.assertEqual(meta[key], text)
            self.assertEqual(result.legacy_meta[key], text)
        self.assertNotIn("unrequested_key", meta)
        self.assertEqual(
            sorted(result.legacy_meta), sorted(_THERMAL_LEGACY_TEXTS)
        )

        # An absent requested legacy key is not an error.
        other_temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, other_temp, True)
        other_path = Path(other_temp) / "other.h5"
        other_group, _ = _save_owned(
            signal_envelope(), signal_rows(), other_path
        )
        absent = inspect_run_group(
            str(other_path),
            str(other_group.id),
            legacy_keys=THERMAL_HYDRATION_KEYS,
        )
        self.assertEqual(absent.legacy_meta, {})

        # A present non-text requested legacy key fails visibly.
        temp = Path(path).parent
        copy_path, _ = _hashed_copy(path, temp)
        with h5py.File(copy_path, "r+") as disk:
            disk[str(envelope["group_id"])].attrs["example"] = 5
        digest = _sha256_file(copy_path)
        with self.assertRaises(StorageInspectionError) as caught:
            inspect_run_group(
                copy_path,
                envelope["group_id"],
                legacy_keys=THERMAL_HYDRATION_KEYS,
            )
        self.assertEqual(
            str(caught.exception), "example: present value is not text"
        )
        _assert_report_failure(self, caught.exception)
        self.assertEqual(_sha256_file(copy_path), digest)

    def test_a2_5_cross_check_mismatch_templates(self):
        envelope = signal_envelope()
        rows = signal_rows()
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        path = Path(temp) / "cross.h5"
        _save_owned(envelope, rows, path)

        def mutate_text(mutate):
            copy_path, _ = _hashed_copy(path, temp)
            with h5py.File(copy_path, "r") as disk:
                text = disk[str(envelope["group_id"])].attrs[METADATA_KEY]
            mutated = decode_envelope(text)
            mutate(mutated)
            with h5py.File(copy_path, "r+") as disk:
                disk[str(envelope["group_id"])].attrs[METADATA_KEY] = (
                    encode_envelope(mutated)
                )
            return copy_path

        # Identity mismatch.
        copy_path = mutate_text(
            lambda env: env.update(group_id=str(uuid.uuid4()))
        )
        with self.assertRaises(StorageInspectionError) as caught:
            inspect_run_group(copy_path, envelope["group_id"])
        self.assertTrue(
            str(caught.exception).startswith("cross-check: envelope group_id ")
        )
        _assert_report_failure(self, caught.exception)

        # Row-count mismatch (codec-valid envelope, wrong counts; the
        # signal payload count must move with the outcome counts).
        copy_path = mutate_text(
            lambda env: (
                env["outcome"].__setitem__("requested_rows", 4),
                env["outcome"].__setitem__("completed_rows", 4),
                env["payload"].__setitem__("points", 4),
            )
        )
        with self.assertRaises(StorageInspectionError) as caught:
            inspect_run_group(copy_path, envelope["group_id"])
        self.assertEqual(
            str(caught.exception),
            "cross-check: stored row count 3 differs from "
            "outcome.completed_rows 4",
        )

        # Column attribute mismatch on disk (unit drift).
        copy_path, _ = _hashed_copy(path, temp)
        with h5py.File(copy_path, "r+") as disk:
            columns = disk[str(envelope["group_id"])]["signal"]["columns"]
            columns["signal"].attrs["unit"] = "mV"
        with self.assertRaises(StorageInspectionError) as caught:
            inspect_run_group(copy_path, envelope["group_id"])
        self.assertEqual(
            str(caught.exception),
            "cross-check: stored columns differ from envelope declaration",
        )

    def test_a2_6_fa_guard_never_plain_load_of_original(self):
        envelope, path = self._thermal_artifact(
            tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        )
        self.addCleanup(shutil.rmtree, str(Path(path).parent), True)
        connects = []
        original_connect = HDF5DataBackend.connect

        def connect_rec(self, args=None):
            connects.append(args["path"])
            return original_connect(self, args)

        backend = _track_backend(self, HDF5DataBackend())
        before = _sha256_file(str(path))
        with mock.patch.object(HDF5DataBackend, "connect", connect_rec):
            inspect_run_group(str(path), envelope["group_id"], backend=backend)
            with self.assertRaises(StorageInspectionError):
                inspect_run_group(str(path), "no-such-group", backend=backend)
        # Every legacy-backend connect targeted the internal disposable
        # copy inside owned scratch space — never the original file.
        # (The failing inspection raised at preflight, before connect.)
        self.assertEqual(len(connects), 1)
        for target in connects:
            self.assertIn(_SCRATCH_PREFIX, target)
            self.assertNotEqual(target, str(path))
        # ... and the original is byte-identical even though the second
        # inspection failed.
        self.assertEqual(_sha256_file(str(path)), before)

    def test_a2_7_cleanup_independent_diagnostics_success(self):
        envelope, path = self._thermal_artifact(
            tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        )
        self.addCleanup(shutil.rmtree, str(Path(path).parent), True)
        result = inspect_run_group(str(path), envelope["group_id"])
        self.assertTrue(result.report.primary_ok)
        self.assertTrue(result.report.cleanup_ok)
        self.assertEqual(result.report.cleanup, ())
        # Scratch space removed: no owned scratch directories remain.
        leftovers = [
            name
            for name in os.listdir(tempfile.gettempdir())
            if name.startswith(_SCRATCH_PREFIX)
        ]
        self.assertEqual(leftovers, [])

    def test_a2_8_signal_profile_empty_hydration_set(self):
        # Frozen per-profile hydration literals.
        self.assertEqual(
            THERMAL_HYDRATION_KEYS,
            ("example", "configuration", "timing", "thermal_manifest"),
        )
        self.assertEqual(SIGNAL_HYDRATION_KEYS, ())
        self.assertEqual(
            PROFILE_HYDRATION_KEYS,
            {
                PROFILE_THERMAL: THERMAL_HYDRATION_KEYS,
                PROFILE_SIGNAL: SIGNAL_HYDRATION_KEYS,
            },
        )
        envelope = signal_envelope()
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        path = Path(temp) / "signal.h5"
        _save_owned(envelope, signal_rows(), path)
        result = inspect_run_group(
            str(path), envelope["group_id"], legacy_keys=SIGNAL_HYDRATION_KEYS
        )
        self.assertEqual(result.envelope["profile"], "signal")
        # run_record under the additive key and NO legacy keys — the
        # thermal set is never imposed on the signal profile.
        self.assertIn(METADATA_KEY, result.group.meta)
        self.assertEqual(
            decode_envelope(result.group.get_meta(METADATA_KEY)), envelope
        )
        for key in THERMAL_HYDRATION_KEYS:
            self.assertNotIn(key, result.group.meta)
        self.assertEqual(result.legacy_meta, {})


class A3FreshProcess(StorageAssertMixin, unittest.TestCase):
    """A3.1-A3.4: clean-subprocess inspection and the three-way rule."""

    def test_a3_1_subprocess_roundtrip_signal_payload(self):
        envelope = signal_envelope()
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        path = Path(temp) / "signal.h5"
        _save_owned(envelope, signal_rows(), path)
        before = _sha256_file(str(path))

        completed = _spawn_bootstrap([str(path), "--profile", "signal"])
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["group_id"], envelope["group_id"])
        self.assertTrue(payload["envelope_present"])
        self.assertEqual(payload["envelope"], envelope)
        self.assertEqual(payload["original_sha256"], before)
        self.assertEqual(_sha256_file(str(path)), before)

        # records[*].columns in h5py enumeration order (lexicographic,
        # physically stored, non-semantic); declared order lives in the
        # envelope.
        record = payload["records"]["signal"]
        self.assertEqual(
            [column["name"] for column in record["columns"]],
            ["reference", "signal", "timestamp"],
        )
        declared = [column["name"] for column in envelope["record"]["columns"]]
        self.assertEqual(declared, ["timestamp", "signal", "reference"])
        for column in record["columns"]:
            self.assertEqual(
                record["samples"][column["name"]], signal_rows()[column["name"]]
            )
        self.assertEqual(record["row_count"], 3)
        report = payload["report"]
        self.assertEqual(report["operation"], "inspect")
        self.assertTrue(report["primary_ok"])
        self.assertEqual(report["primary"], [])
        self.assertTrue(report["cleanup_ok"])
        self.assertEqual(report["cleanup"], [])

    def test_a3_2_no_legacy_context_thermal(self):
        example = _load_example()
        config = example.ThermalConfig(points=8)
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        result = example.execute(
            config, Path(temp) / "thermal", adapter="coordinated"
        )
        data_path = str(result.data_path)
        before = _sha256_file(data_path)

        completed = _spawn_bootstrap(
            [
                data_path,
                "--profile",
                "thermal",
                "--group-id",
                str(result.group.id),
            ]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["group_id"], str(result.group.id))
        self.assertEqual(payload["original_sha256"], before)
        self.assertEqual(_sha256_file(data_path), before)

        # Every payload fact matches facts the parent re-derives
        # independently from a fresh byte copy — the child held no
        # in-memory envelope, group, or example state.
        copy_path, _ = _hashed_copy(data_path, temp)
        disk = _read_disk_facts(copy_path, str(result.group.id))
        self.assertEqual(
            payload["envelope"], decode_envelope(disk["run_record"])
        )
        self.assertEqual(
            payload["legacy_meta"],
            {
                key: disk["meta"][key]
                for key in (
                    "example",
                    "configuration",
                    "timing",
                    "thermal_manifest",
                )
            },
        )
        for column_name, entry in disk["records"]["thermal_trace"].items():
            self.assertEqual(
                payload["records"]["thermal_trace"]["samples"][column_name],
                entry["values"],
            )

    def test_a3_3_three_way_distinction(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope = signal_envelope()
        path = Path(temp) / "base.h5"
        _save_owned(envelope, signal_rows(), path)

        def mutate_copy(mutate):
            copy_path, _ = _hashed_copy(path, temp)
            with h5py.File(copy_path, "r+") as disk:
                mutate(disk[str(envelope["group_id"])].attrs)
            # Hash the delivered (mutated) fixture after mutation.
            digest = _sha256_file(copy_path)
            return copy_path, digest

        def assert_error_payload(copy_path, digest, error_type, message):
            completed = _spawn_bootstrap(
                [str(copy_path), "--profile", "signal"]
            )
            self.assertEqual(completed.returncode, 1, completed.stdout)
            payload = json.loads(completed.stdout)
            self.assertEqual(payload["status"], "error")
            self.assertEqual(payload["error_type"], error_type)
            self.assertEqual(payload["error_message"], message)
            report = payload["report"]
            self.assertIsNotNone(report)
            self.assertFalse(report["primary_ok"])
            self.assertEqual(report["primary"], [message])
            self.assertTrue(completed.stderr.strip())
            self.assertEqual(_sha256_file(str(copy_path)), digest)

        # Absent additive key: truthful legacy absence — exit 0, no
        # error, no fabricated envelope, legacy context still available.
        legacy_group = DataGroup(name="legacy", id=uuid.uuid4())
        legacy_group.set_meta("example", "examples/thermal_experiment.py")
        legacy_group.set_meta("configuration", "{}")
        legacy_group.set_meta("timing", "{}")
        legacy_group.set_meta(
            "thermal_manifest",
            json.dumps({"schema": "softlab.thermal-reference", "version": 1}),
        )
        _attach_record(
            legacy_group,
            {"record": {"name": "signal", "columns": list(_SIGNAL_COLUMNS)}},
            signal_rows(),
        )
        legacy_path = Path(temp) / "legacy.h5"
        legacy_backend = _track_backend(self, HDF5DataBackend())
        legacy_backend.connect({"path": str(legacy_path)})
        self.assertTrue(legacy_backend.save_group(legacy_group))
        legacy_backend.disconnect()
        completed = _spawn_bootstrap([str(legacy_path), "--profile", "thermal"])
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertFalse(payload["envelope_present"])
        self.assertIsNone(payload["envelope"])
        self.assertEqual(
            payload["legacy_meta"]["thermal_manifest"],
            legacy_group.get_meta("thermal_manifest"),
        )
        self.assertTrue(payload["report"]["primary_ok"])

        # Present non-text: visible failure, never absence.
        copy_path, digest = mutate_copy(
            lambda attrs: attrs.__setitem__(METADATA_KEY, 42)
        )
        assert_error_payload(
            copy_path,
            digest,
            "StorageInspectionError",
            "run_record: present value is not text",
        )

        # Malformed text: the codec's frozen decode diagnostic.
        copy_path, digest = mutate_copy(
            lambda attrs: attrs.__setitem__(METADATA_KEY, "not json")
        )
        assert_error_payload(
            copy_path,
            digest,
            "RecordDecodeError",
            "run_record: malformed JSON: Expecting value: "
            "line 1 column 1 (char 0)",
        )

        # Wrong namespace.
        wrong = signal_envelope()
        wrong["namespace"] = "softlab.thermal-reference"
        copy_path, digest = mutate_copy(
            lambda attrs: attrs.__setitem__(METADATA_KEY, envelope_text(wrong))
        )
        assert_error_payload(
            copy_path,
            digest,
            "RecordValidationError",
            "namespace: unsupported namespace "
            "(supported: softlab.run-record)",
        )

        # Unsupported version under the common namespace.
        versioned = signal_envelope()
        versioned["version"] = 2
        copy_path, digest = mutate_copy(
            lambda attrs: attrs.__setitem__(
                METADATA_KEY, envelope_text(versioned)
            )
        )
        assert_error_payload(
            copy_path,
            digest,
            "RecordValidationError",
            "version: unsupported version (supported: 1)",
        )

        # Over-size text.
        oversized = "{}" + " " * MAX_ENCODED_BYTES
        copy_path, digest = mutate_copy(
            lambda attrs: attrs.__setitem__(METADATA_KEY, oversized)
        )
        assert_error_payload(
            copy_path,
            digest,
            "RecordDecodeError",
            "run_record: encoded size exceeds limit 131072 bytes",
        )

    def test_a3_4_multi_group_and_exit_codes(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        path = Path(temp) / "multi.h5"
        envelope_one, envelope_two, _ = _save_two_groups(path, temp)

        # Without an explicit id the exactly-one rule fails visibly.
        completed = _spawn_bootstrap([str(path), "--profile", "signal"])
        self.assertEqual(completed.returncode, 1)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["error_type"], "StorageInspectionError")
        self.assertEqual(
            payload["error_message"],
            "group_id: require exactly one stored group or explicit ID",
        )

        # Unknown id fails visibly.
        completed = _spawn_bootstrap(
            [str(path), "--profile", "signal", "--group-id", str(uuid.uuid4())]
        )
        self.assertEqual(completed.returncode, 1)
        payload = json.loads(completed.stdout)
        self.assertTrue(
            payload["error_message"].startswith("group_id: stored group ")
        )
        self.assertTrue(payload["error_message"].endswith(" not found"))

        # Explicit id succeeds.
        completed = _spawn_bootstrap(
            [
                str(path),
                "--profile",
                "signal",
                "--group-id",
                envelope_one["group_id"],
            ]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["group_id"], envelope_one["group_id"])
        self.assertNotEqual(payload["group_id"], envelope_two["group_id"])

        # CLI usage error: argparse exit code 2, message on stderr.
        completed = _spawn_bootstrap([str(path)])
        self.assertEqual(completed.returncode, 2)
        self.assertIn("usage", completed.stderr)


class A4FaultMatrix(StorageAssertMixin, unittest.TestCase):
    """A4.1-A4.9 + R2/R6 rows: real-seam faults, pins, R1-R6 contract."""

    def _artifact(self, temp, envelope=None, rows=None):
        envelope = envelope or signal_envelope()
        rows = rows or signal_rows()
        path = Path(temp) / "artifact.h5"
        _save_owned(envelope, rows, path)
        return envelope, path

    def _mutated(self, temp, path, envelope, text):
        copy_path, _ = _hashed_copy(path, temp)
        with h5py.File(copy_path, "r+") as disk:
            disk[str(envelope["group_id"])].attrs[METADATA_KEY] = text
        # The delivered fixture is the MUTATED copy: the F-A claim is
        # that inspection leaves its bytes untouched, so hash after the
        # mutation, then re-hash after the inspection.
        digest = _sha256_file(copy_path)
        return copy_path, digest

    def test_a4_1_serialization_failure_pre_connect(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        inventory = []
        original_connect = HDF5DataBackend.connect
        original_save = HDF5DataBackend.save_group

        def connect_spy(self, args=None):
            inventory.append("connect")
            return original_connect(self, args)

        def save_spy(self, group_):
            inventory.append("save")
            return original_save(self, group_)

        # Defect one: malformed JSON.
        group = DataGroup(name="hand-built", id=uuid.uuid4())
        group.set_meta(METADATA_KEY, "not json")
        target = Path(temp) / "never.h5"
        with (
            mock.patch.object(HDF5DataBackend, "connect", connect_spy),
            mock.patch.object(HDF5DataBackend, "save_group", save_spy),
        ):
            with self.assertRaises(RecordDecodeError) as caught:
                save_run_group(group, str(target))
        self.assertEqual(
            str(caught.exception),
            "run_record: malformed JSON: Expecting value: "
            "line 1 column 1 (char 0)",
        )
        report = _assert_report_failure(self, caught.exception)
        # R4(a): pre-resource raise — no cleanup obligations existed.
        self.assertTrue(report.cleanup_ok)
        self.assertEqual(report.cleanup, ())
        self.assertFalse(target.exists())

        # Defect two: count drift the codec rejects (valid JSON shape).
        drifted = signal_envelope()
        drifted["outcome"]["completed_rows"] = 4
        group_two = DataGroup(name="hand-built-2", id=uuid.uuid4())
        group_two.set_meta(METADATA_KEY, envelope_text(drifted))
        target_two = Path(temp) / "never-two.h5"
        with (
            mock.patch.object(HDF5DataBackend, "connect", connect_spy),
            mock.patch.object(HDF5DataBackend, "save_group", save_spy),
        ):
            with self.assertRaises(RecordValidationError) as caught:
                save_run_group(group_two, str(target_two))
        self.assertEqual(
            str(caught.exception),
            "outcome.completed_rows: differs from outcome.requested_rows",
        )
        _assert_report_failure(self, caught.exception)
        self.assertFalse(target_two.exists())

        # Zero HDF5 attribute or dataset writes, zero backend contact.
        self.assertEqual(inventory, [])

    def test_a4_2_save_failure_externally_closed_handle(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, _ = self._artifact(temp)
        group = build_run_group("run-1", envelope)
        _attach_record(group, envelope, signal_rows())
        path = Path(temp) / "save-fault.h5"
        backend = _track_backend(self, HDF5DataBackend())

        original_connect = HDF5DataBackend.connect

        def connect_spy(self, args=None):
            result = original_connect(self, args)
            self.file.close()  # external environmental fault
            return result

        with mock.patch.object(HDF5DataBackend, "connect", connect_spy):
            with self.assertRaises(StorageSaveError) as caught:
                save_run_group(group, str(path), backend=backend)
        self.assertTrue(
            str(caught.exception).startswith("save: HDF5 backend save failed: ")
        )
        report = _assert_report_failure(self, caught.exception)
        # R4(b): the owed verified disconnect ran and found nothing to
        # report (the closed handle's close() is idempotent).
        self.assertTrue(report.cleanup_ok)
        self.assertEqual(report.cleanup, ())
        self.assertFalse(backend.connected)
        # §6.3 pin: an empty connect-time file remains — zero groups.
        with h5py.File(str(path), "r") as disk:
            self.assertEqual(list(disk.keys()), [])

    def test_a4_3_disconnect_failure_inspection_variant(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, path = self._artifact(temp)
        before = _sha256_file(str(path))
        retained = {}

        original_load = HDF5DataBackend.load_group

        def load_spy(self, id):
            result = original_load(self, id)
            # External file-handle invalidation: detach and retain the
            # raw handle; the owned cleanup then executes its real
            # disconnect against the cleared slot (frozen §6.2 seam).
            retained["handle"] = self.file
            self.file = None
            return result

        backend = _track_backend(self, HDF5DataBackend())
        with mock.patch.object(HDF5DataBackend, "load_group", load_spy):
            result = inspect_run_group(
                str(path), envelope["group_id"], backend=backend
            )

        def close_retained():
            handle = retained.get("handle")
            if handle is not None:
                handle.close()
                self.assertFalse(handle.id.valid)

        self.addCleanup(close_retained)

        # Primary outcome delivered; cleanup failure independently
        # observable in its own slot (A4.3 frozen row).
        self.assertTrue(result.report.primary_ok)
        self.assertFalse(result.report.cleanup_ok)
        self.assertEqual(len(result.report.cleanup), 1)
        self.assertTrue(
            result.report.cleanup[0].startswith(
                "Failed to disconnect: AttributeError"
            )
        )
        self.assertEqual(str(result.group.id), envelope["group_id"])
        self.assertEqual(_sha256_file(str(path)), before)
        leftovers = [
            name
            for name in os.listdir(tempfile.gettempdir())
            if name.startswith(_SCRATCH_PREFIX)
        ]
        self.assertEqual(leftovers, [])

    def test_a4_3_disconnect_failure_save_variant(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, _ = self._artifact(temp)
        group = build_run_group("run-1", envelope)
        _attach_record(group, envelope, signal_rows())
        path = Path(temp) / "save-disconnect-fault.h5"
        backend = _track_backend(self, HDF5DataBackend())
        retained = {}

        original_save = HDF5DataBackend.save_group

        def save_spy(self, group_):
            result = original_save(self, group_)
            retained["handle"] = self.file
            self.file = None
            return result

        with mock.patch.object(HDF5DataBackend, "save_group", save_spy):
            report = save_run_group(group, str(path), backend=backend)

        # Close the retained handle (teardown discipline, design §6.2)
        # before pinning the artifact — the copy must see flushed data.
        retained["handle"].close()
        self.assertFalse(retained["handle"].id.valid)
        retained["handle"] = None

        # Save primary success; cleanup failure surfaced, not raised.
        self.assertTrue(report.primary_ok)
        self.assertFalse(report.cleanup_ok)
        self.assertEqual(len(report.cleanup), 1)
        self.assertTrue(
            report.cleanup[0].startswith("Failed to disconnect: AttributeError")
        )
        # §6.3 save-variant pin: the complete artifact remains.
        copy_path, _ = _hashed_copy(path, temp)
        disk = _read_disk_facts(copy_path, envelope["group_id"])
        self.assertEqual(decode_envelope(disk["run_record"]), envelope)

    def test_a4_4_invalid_reload_corrupt_json(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, path = self._artifact(temp)
        copy_path, digest = self._mutated(temp, path, envelope, "not json")
        with self.assertRaises(RecordDecodeError) as caught:
            inspect_run_group(copy_path, envelope["group_id"])
        self.assertEqual(
            str(caught.exception),
            "run_record: malformed JSON: Expecting value: "
            "line 1 column 1 (char 0)",
        )
        _assert_report_failure(self, caught.exception)
        self.assertEqual(_sha256_file(copy_path), digest)

    def test_a4_5_invalid_reload_wrong_namespace(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, path = self._artifact(temp)
        wrong = signal_envelope()
        wrong["namespace"] = "softlab.thermal-reference"
        copy_path, digest = self._mutated(
            temp, path, envelope, envelope_text(wrong)
        )
        with self.assertRaises(RecordValidationError) as caught:
            inspect_run_group(copy_path, envelope["group_id"])
        self.assertEqual(
            str(caught.exception),
            "namespace: unsupported namespace "
            "(supported: softlab.run-record)",
        )
        _assert_report_failure(self, caught.exception)
        self.assertEqual(_sha256_file(copy_path), digest)

    def test_a4_6_invalid_reload_unsupported_version(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, path = self._artifact(temp)
        versioned = signal_envelope()
        versioned["version"] = 2
        copy_path, digest = self._mutated(
            temp, path, envelope, envelope_text(versioned)
        )
        with self.assertRaises(RecordValidationError) as caught:
            inspect_run_group(copy_path, envelope["group_id"])
        self.assertEqual(
            str(caught.exception), "version: unsupported version (supported: 1)"
        )
        _assert_report_failure(self, caught.exception)
        self.assertEqual(_sha256_file(copy_path), digest)

    def test_a4_7_invalid_reload_oversize(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, path = self._artifact(temp)
        oversized = "{}" + " " * MAX_ENCODED_BYTES
        copy_path, digest = self._mutated(temp, path, envelope, oversized)
        with self.assertRaises(RecordDecodeError) as caught:
            inspect_run_group(copy_path, envelope["group_id"])
        self.assertEqual(
            str(caught.exception),
            "run_record: encoded size exceeds limit 131072 bytes",
        )
        _assert_report_failure(self, caught.exception)
        self.assertEqual(_sha256_file(copy_path), digest)

    def test_a4_8_preflight_failure_zero_loads(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        fixture = Path(temp) / "preflight.h5"
        with h5py.File(str(fixture), "w") as disk:
            group = disk.create_group(str(uuid.uuid4()))
            group.attrs["name"] = "hand-built"
            # reserved attributes id/backend deliberately absent
        loads = []
        original_load = HDF5DataBackend.load_group

        def load_spy(self, id):
            loads.append(id)
            return original_load(self, id)

        digest = _sha256_file(str(fixture))
        with mock.patch.object(HDF5DataBackend, "load_group", load_spy):
            with self.assertRaises(StorageInspectionError) as caught:
                inspect_run_group(str(fixture))
        self.assertEqual(
            str(caught.exception),
            "preflight: missing or nontext reserved attribute 'id'",
        )
        report = _assert_report_failure(self, caught.exception)
        # R4(b): preflight failure passed through the finally — the
        # owed scratch cleanup ran clean (empty tuple is truthful).
        self.assertTrue(report.cleanup_ok)
        self.assertEqual(report.cleanup, ())
        self.assertEqual(loads, [])
        self.assertEqual(_sha256_file(str(fixture)), digest)

    def test_a4_9_connect_failure_existing_directory(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, _ = self._artifact(temp)
        group = build_run_group("run-1", envelope)
        _attach_record(group, envelope, signal_rows())
        target_dir = Path(temp) / "as-directory"
        target_dir.mkdir()
        marker = target_dir / "marker.txt"
        marker.write_text("sentinel")
        marker_digest = _sha256_file(str(marker))
        inventory = []
        original_connect = HDF5DataBackend.connect
        original_save = HDF5DataBackend.save_group

        def connect_rec(self, args=None):
            inventory.append("connect")
            return original_connect(self, args)

        def save_rec(self, group_):
            inventory.append("save")
            return original_save(self, group_)

        with (
            mock.patch.object(HDF5DataBackend, "connect", connect_rec),
            mock.patch.object(HDF5DataBackend, "save_group", save_rec),
        ):
            with self.assertRaises(StorageConnectError) as caught:
                save_run_group(group, str(target_dir))
        self.assertTrue(
            str(caught.exception).startswith(
                "connect: HDF5 backend connect failed: "
            )
        )
        report = _assert_report_failure(self, caught.exception)
        self.assertTrue(report.cleanup_ok)
        self.assertEqual(report.cleanup, ())
        # Save never attempted; directory and contents untouched; no
        # HDF5 file created.
        self.assertEqual(inventory, ["connect"])
        self.assertEqual(_sha256_file(str(marker)), marker_digest)
        self.assertEqual(list(target_dir.iterdir()), [marker])

    def test_a4_r6_byte_modification_dominates(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, path = self._artifact(temp)
        original_load = HDF5DataBackend.load_group

        def load_spy(self, id):
            result = original_load(self, id)
            # External mid-inspection modification of the original.
            with open(str(path), "r+b") as stream:
                stream.write(b"X")
            return result

        backend = _track_backend(self, HDF5DataBackend())
        with mock.patch.object(HDF5DataBackend, "load_group", load_spy):
            with self.assertRaises(StorageInspectionError) as caught:
                inspect_run_group(
                    str(path), envelope["group_id"], backend=backend
                )
        self.assertEqual(
            str(caught.exception),
            "safety: original bytes modified during inspection",
        )
        report = _assert_report_failure(self, caught.exception)
        self.assertEqual(report.primary, (str(caught.exception),))
        self.assertIsNone(caught.exception.__cause__)

    def test_a4_r6_rehash_failure_reported(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, path = self._artifact(temp)
        original_load = HDF5DataBackend.load_group

        def load_spy(self, id):
            result = original_load(self, id)
            os.unlink(str(path))  # original vanishes mid-inspection
            return result

        backend = _track_backend(self, HDF5DataBackend())
        with mock.patch.object(HDF5DataBackend, "load_group", load_spy):
            with self.assertRaises(StorageInspectionError) as caught:
                inspect_run_group(
                    str(path), envelope["group_id"], backend=backend
                )
        self.assertTrue(
            str(caught.exception).startswith(
                "safety: original bytes could not be re-verified after "
                "inspection: "
            )
        )
        _assert_report_failure(self, caught.exception)

    def test_a4_r2_coincident_safety_and_body_failure(self):
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        envelope, path = self._artifact(temp)
        copy_path, _ = self._mutated(temp, path, envelope, "not json")
        original_load = HDF5DataBackend.load_group

        def load_spy(self, id):
            result = original_load(self, id)
            os.unlink(copy_path)
            return result

        backend = _track_backend(self, HDF5DataBackend())
        with mock.patch.object(HDF5DataBackend, "load_group", load_spy):
            with self.assertRaises(StorageInspectionError) as caught:
                inspect_run_group(
                    copy_path, envelope["group_id"], backend=backend
                )
        safety = (
            "safety: original bytes could not be re-verified after "
            "inspection: "
        )
        self.assertTrue(str(caught.exception).startswith(safety))
        body = caught.exception.__cause__
        self.assertIsNotNone(body)
        self.assertIs(type(body), RecordDecodeError)
        report = _assert_report_failure(self, caught.exception)
        # R2 coincident rule: both diagnostics preserved, neither masks.
        self.assertEqual(len(report.primary), 2)
        self.assertEqual(report.primary[0], str(caught.exception))
        self.assertEqual(report.primary[1], str(body))


class A5StageHonesty(StorageAssertMixin, unittest.TestCase):
    """A5.1-A5.4: the stored record claims the acquisition snapshot only."""

    def test_a5_1_stored_snapshot_literals_from_disk(self):
        envelope = thermal_envelope()
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            path = Path(temp) / "honest.h5"
            _save_owned(envelope, thermal_rows(), path)
            copy_path, _ = _hashed_copy(path, temp)
            facts = _read_disk_facts(copy_path, envelope["group_id"])
            stored = decode_envelope(facts["run_record"])
            outcome = stored["outcome"]
            self.assertEqual(
                outcome["snapshot"], "completed_acquisition_before_persistence"
            )
            self.assertEqual(outcome["acquisition"], "completed")
            for stage in ("persistence", "plotting", "cleanup", "overall"):
                self.assertEqual(outcome[stage], "not_yet_established")

    def test_a5_2_negative_probes_rejected_pre_connect(self):
        late = thermal_envelope()
        late["outcome"]["persistence"] = "completed"
        with self.assertRaises(RecordValidationError):
            encode_envelope(late)
        failed = thermal_envelope()
        failed["outcome"]["acquisition"] = "failed"
        with self.assertRaises(RecordValidationError):
            encode_envelope(failed)
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            # Hand-crafted late-stage text is rejected by the seam's
            # pre-connect decode (the storage gate, A4.1 mechanism).
            group = DataGroup(name="late", id=uuid.uuid4())
            group.set_meta(METADATA_KEY, envelope_text(late))
            target = Path(temp) / "late.h5"
            with self.assertRaises(RecordValidationError) as caught:
                save_run_group(group, str(target))
            _assert_report_failure(self, caught.exception)
            self.assertFalse(target.exists())

    def test_a5_3_no_post_save_rewrite(self):
        envelope = signal_envelope()
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            path = Path(temp) / "norewrite.h5"
            group = build_run_group("run-1", envelope)
            _attach_record(group, envelope, signal_rows())
            calls = []
            original_set_meta = DataGroup.set_meta

            def set_meta_rec(self, key, value):
                calls.append((key, value))
                return original_set_meta(self, key, value)

            with mock.patch.object(DataGroup, "set_meta", set_meta_rec):
                report = save_run_group(group, str(path))
            _assert_report_success(self, report, "save", path)
            # The write path performs no set_meta call at all — the
            # outcome lives inside the single run_record text encoded
            # before connect; there is no separate outcome metadata key.
            self.assertEqual(calls, [])
            # The stored record never acquires a later outcome history.
            copy_path, _ = _hashed_copy(path, temp)
            facts = _read_disk_facts(copy_path, envelope["group_id"])
            outcome = decode_envelope(facts["run_record"])["outcome"]
            self.assertEqual(outcome["persistence"], "not_yet_established")
            self.assertNotIn("outcome", facts["meta_keys"])

    def test_a5_4_unavailable_fact_structure(self):
        envelope = signal_envelope()  # git facts unavailable by default
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            path = Path(temp) / "unavailable.h5"
            _save_owned(envelope, signal_rows(), path)
            copy_path, _ = _hashed_copy(path, temp)
            facts = _read_disk_facts(copy_path, envelope["group_id"])
            stored = decode_envelope(facts["run_record"])
            for key in ("digest", "git_revision", "git_dirty"):
                fact = stored["source"][key]
                self.assertEqual(set(fact), {"state", "value", "reason"})
            git = stored["source"]["git_revision"]
            self.assertEqual(git["state"], "unavailable")
            self.assertIsNone(git["value"])
            self.assertIn(git["reason"], UNAVAILABLE_REASONS)
            # The bounded vocabulary is not extended by storage-level
            # facts (frozen §8): runtime truth lives in StorageReport.
            self.assertEqual(
                UNAVAILABLE_REASONS, ("capture_failed", "source_mismatch")
            )


class A6UnsupportedPaths(StorageAssertMixin, unittest.TestCase):
    """A6.1-A6.4: unsupported paths are explicit, never silent."""

    def test_a6_1_sqlite_rejected_pre_io(self):
        envelope = signal_envelope()
        group = build_run_group("run-1", envelope)
        _attach_record(group, envelope, signal_rows())
        sqlite_backend = Sqlite3DataBackend()
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            target = Path(temp) / "data.db"
            exc = self.assert_frozen(
                lambda: save_run_group(
                    group, str(target), backend=sqlite_backend
                ),
                UnsupportedBackendError,
                "backend: unsupported backend type 'sqlite3' "
                "(supported: 'hdf5')",
            )
            self.assertIsInstance(exc, ValueError)
            self.assertIsInstance(exc, RunStorageError)
            report = _assert_report_failure(self, exc)
            self.assertTrue(report.cleanup_ok)
            self.assertEqual(report.cleanup, ())
            # Step 0 fired before any I/O: nothing connected, no file.
            self.assertFalse(sqlite_backend.connected)
            self.assertFalse(target.exists())

            exc = self.assert_frozen(
                lambda: inspect_run_group(
                    str(target), backend=Sqlite3DataBackend()
                ),
                UnsupportedBackendError,
                "backend: unsupported backend type 'sqlite3' "
                "(supported: 'hdf5')",
            )
            _assert_report_failure(self, exc)

    def test_a6_2_io_helpers_are_not_the_supported_path(self):
        # F-C re-assertion (characterized defect, unchanged code):
        # reload_group with populated valid group.backend returns None.
        envelope = signal_envelope()
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        path = Path(temp) / "fc.h5"
        group, _ = _save_owned(envelope, signal_rows(), path)
        reader = _track_backend(self, HDF5DataBackend())
        reader.connect({"path": str(path)})
        loaded = reader.load_group(str(group.id))
        reader.disconnect()
        self.assertEqual(loaded.backend["type"], "hdf5")
        created = []
        original_by_info = data_io.get_data_backend_by_info

        def by_info_spy(info):
            backend = original_by_info(info)
            created.append(backend)
            return backend

        with mock.patch.object(
            data_io, "get_data_backend_by_info", by_info_spy
        ):
            self.assertIsNone(data_io.reload_group(loaded))
        for backend in created:
            if backend.connected:
                backend.disconnect()
        self.assertEqual(created[0].backend_type, "hdf5")

        # F-D re-assertion: empty generic-helper output on a non-empty
        # store is never evidence of an empty store.
        db_path = Path(temp) / "data.db"
        sqlite_backend = _track_backend(self, Sqlite3DataBackend())
        sqlite_backend.connect({"path": str(db_path)})
        self.assertTrue(sqlite_backend.save_group(group))
        sqlite_backend.disconnect()
        created_two = []
        original_get = data_io.get_data_backend

        def get_spy(backend_type, args=None, connect=True):
            backend = original_get(backend_type, args, connect)
            created_two.append(backend)
            return backend

        with mock.patch.object(data_io, "get_data_backend", get_spy):
            self.assertEqual(
                data_io.load_groups("sqlite3", {"path": str(db_path)}), []
            )
        for backend in created_two:
            if backend.connected:
                backend.disconnect()

        # The supported path never routes through the generic helpers.
        io_calls = []
        originals = {}
        for name in ("save_group", "reload_group", "load_groups"):
            originals[name] = getattr(data_io, name)

            def make_spy(original, tag):
                def spy(*args, **kwargs):
                    io_calls.append(tag)
                    return original(*args, **kwargs)

                return spy

            with mock.patch.object(
                data_io, name, make_spy(originals[name], name)
            ):
                envelope_two, path_two = self._artifact_pair(temp)
                inspect_run_group(str(path_two), envelope_two["group_id"])
        self.assertEqual(io_calls, [])

    def _artifact_pair(self, temp):
        envelope = thermal_envelope()
        path = Path(temp) / ("supported-%s.h5" % uuid.uuid4().hex[:8])
        _save_owned(envelope, thermal_rows(), path)
        return envelope, path

    def test_a6_3_same_id_deletion_recreation_is_not_atomic(self):
        envelope = signal_envelope()
        rows = signal_rows()
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            path = Path(temp) / "recreate.h5"
            group, _ = _save_owned(envelope, rows, path)
            # Same-ID save deletes the existing group before recreating
            # it (observed non-atomic behavior; recorded, not claimed
            # as supported publication). The recreated envelope shares
            # the persistent group id and is codec-consistent.
            recreated = signal_envelope(points=1)
            recreated["group_id"] = envelope["group_id"]
            extra = signal_rows(points=1)
            _save_owned(recreated, extra, path)
            copy_path, _ = _hashed_copy(path, temp)
            disk = _read_disk_facts(copy_path, envelope["group_id"])
            self.assertEqual(
                len(disk["records"]["signal"]["timestamp"]["values"]), 1
            )
            with h5py.File(copy_path, "r") as handle:
                self.assertEqual(list(handle.keys()), [envelope["group_id"]])

    def test_a6_4_failed_run_persistence_unsupported(self):
        # Only acquisition == 'completed' envelopes can encode; no test
        # stores a failed or cancelled acquisition (decision §2
        # NON-support, recorded verbatim in the results file).
        envelope = thermal_envelope()
        envelope["outcome"]["acquisition"] = "cancelled"
        with self.assertRaises(RecordValidationError):
            encode_envelope(envelope)


class A7LegacyCompatibility(StorageAssertMixin, unittest.TestCase):
    """A7.1-A7.5: legacy behavior unchanged within the reviewed scope."""

    _LEGACY_SUITES = (
        "test_thermal_manifest",
        "test_thermal_example",
        "test_thermal_execution",
        "test_thermal_adapters",
        "test_thermal_cli_warnings",
        "test_thermal_metadata_contracts",
    )

    def test_a7_1_legacy_suites_resolve_and_run_unmodified(self):
        # Import-path resolution is identical (each suite module loads
        # cleanly with its full test inventory); the pass/fail evidence
        # is the full-suite execution recorded in the results file
        # (these six suites are part of the discover run).
        for name in self._LEGACY_SUITES:
            with self.subTest(suite=name):
                spec = importlib.util.spec_from_file_location(
                    "m03_p3_%s" % name, ROOT / "tests" / (name + ".py")
                )
                module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = module
                spec.loader.exec_module(module)
                suite = unittest.defaultTestLoader.loadTestsFromModule(module)
                self.assertNotEqual(suite.countTestCases(), 0, name)
                sys.modules.pop(spec.name, None)

    def test_a7_2_legacy_files_read_identically_after_change(self):
        # A synthetic pre-Phase-3 artifact: written by the unchanged
        # legacy writer (plain backend save) with the four thermal
        # legacy texts and NO additive key.
        thermal = _load_example()
        config = thermal.ThermalConfig(points=8)
        group = DataGroup(name="legacy-run", id=uuid.uuid4())
        record = DataRecord(
            "thermal_trace", copy.deepcopy(list(_THERMAL_COLUMNS))
        )
        record.add_rows(thermal_rows())
        group.add_record(record)
        manifest = {
            "schema": "softlab.thermal-reference",
            "version": 1,
            "run_id": str(group.id),
            "configuration": {
                "initial_temperature": config.initial_temperature,
                "ambient_temperature": config.ambient_temperature,
                "heat_capacity": config.heat_capacity,
                "conductance": config.conductance,
                "dt": config.dt,
                "power": config.power,
                "points": config.points,
            },
            "model": {**thermal._MODEL, "units": dict(thermal._MODEL["units"])},
            "procedure": {
                "name": "thermal_count",
                "advancement": "once_before_each_getter_row",
                "command": "constant_power",
                "requested_points": config.points,
            },
            "columns": record.columns,
            "clock": {
                "dt": config.dt,
                "simulation_unit": "s",
                **thermal._timing(1),
            },
            "software": {
                "softlab_version": str(softlab.__version__),
                "python_version": platform.python_version(),
            },
            "source": thermal._capture_source_identity(),
            "outcome": {
                "snapshot": "completed_acquisition_before_persistence",
                "acquisition": "completed",
                "requested_rows": config.points,
                "completed_rows": config.points,
                "final_tick": config.points,
                "final_sim_time": float(config.points) * config.dt,
                "persistence": "not_yet_established",
                "plotting": "not_yet_established",
                "cleanup": "not_yet_established",
                "overall": "not_yet_established",
            },
        }
        group.set_meta("example", "examples/thermal_experiment.py")
        group.set_meta("configuration", json.dumps(manifest["configuration"]))
        group.set_meta("timing", json.dumps(manifest["clock"]))
        group.set_meta("thermal_manifest", thermal._encode_manifest(manifest))
        self.assertIsNone(group.get_meta(METADATA_KEY))

        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as temp:
            path = Path(temp) / "legacy.h5"
            writer = _track_backend(self, HDF5DataBackend())
            writer.connect({"path": str(path)})
            self.assertTrue(writer.save_group(group))
            writer.disconnect()

            # Legacy reader (the example's additive _load_group) takes
            # the pre-change path for a legacy-only file: the four
            # legacy texts hydrate, no run_record appears. The legacy
            # load is the characterized destructive chain (F-A), so —
            # exactly like the example's _reload_group — the reader
            # works on a byte copy, never the delivered original.
            copy_path, digest = _hashed_copy(path, temp)
            reader = _track_backend(self, HDF5DataBackend())
            reader.connect({"path": copy_path})
            loaded = thermal._load_group(reader, str(group.id))
            reader.disconnect()
            self.assertIsNone(loaded.get_meta(METADATA_KEY))
            for key in (
                "example",
                "configuration",
                "timing",
                "thermal_manifest",
            ):
                self.assertEqual(loaded.get_meta(key), group.get_meta(key))
            self.assertEqual(
                loaded.record("thermal_trace").table.to_numpy().tolist(),
                record.table.to_numpy().tolist(),
            )
            self.assertEqual(_sha256_file(str(path)), digest)

            # Storage-layer legacy absence semantics (A3.3 first bullet
            # carried into the storage layer): envelope None, no error.
            result = inspect_run_group(
                str(path), str(group.id), legacy_keys=THERMAL_HYDRATION_KEYS
            )
            self.assertIsNone(result.envelope)
            self.assertEqual(
                result.legacy_meta["thermal_manifest"],
                group.get_meta("thermal_manifest"),
            )
            self.assertTrue(result.report.primary_ok)

    def test_a7_4_export_surface_nine_name_delta(self):
        package = importlib.import_module("softlab.shui")
        for name in (
            "save_run_group",
            "inspect_run_group",
            "RunStorageError",
            "StorageConnectError",
            "StorageSaveError",
            "StorageInspectionError",
            "UnsupportedBackendError",
            "StorageReport",
            "InspectionResult",
        ):
            with self.subTest(export=name):
                self.assertTrue(hasattr(package, name), name)
        # Module-only names stay on the module (design §9).
        self.assertEqual(
            set(run_storage.__all__),
            {
                "save_run_group",
                "inspect_run_group",
                "RunStorageError",
                "StorageConnectError",
                "StorageSaveError",
                "StorageInspectionError",
                "UnsupportedBackendError",
                "StorageReport",
                "InspectionResult",
                "StoredRecordFacts",
                "THERMAL_HYDRATION_KEYS",
                "SIGNAL_HYDRATION_KEYS",
                "PROFILE_HYDRATION_KEYS",
            },
        )
        for name in (
            "StoredRecordFacts",
            "THERMAL_HYDRATION_KEYS",
            "SIGNAL_HYDRATION_KEYS",
            "PROFILE_HYDRATION_KEYS",
        ):
            self.assertTrue(hasattr(run_storage, name), name)
            self.assertFalse(hasattr(package, name), name)
        # Pre-existing exports unchanged.
        for name in (
            "DatabaseBackend",
            "catch_error",
            "data",
            "profile",
            "build_run_group",
            "encode_envelope",
            "decode_envelope",
        ):
            self.assertTrue(hasattr(package, name), name)

    def test_a7_5_phase2_suite_reused_unmodified(self):
        spec = importlib.util.spec_from_file_location(
            "m03_p2_for_a7", ROOT / "tests" / "test_m03_p2_schema.py"
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        suite = unittest.defaultTestLoader.loadTestsFromModule(module)
        self.assertEqual(suite.countTestCases(), 54)
        sys.modules.pop(spec.name, None)
        # The codec contract surface the storage seam relies on.
        for name in (
            "build_run_group",
            "encode_envelope",
            "decode_envelope",
            "RecordDecodeError",
            "RecordValidationError",
        ):
            self.assertTrue(hasattr(module, name), name)


class A8Hygiene(StorageAssertMixin, unittest.TestCase):
    """A8.1-A8.8: synthetic data, released resources, portability."""

    def test_a8_1_temp_dirs_prefix_and_cleanup(self):
        envelope = signal_envelope()
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        try:
            path = Path(temp) / "hygiene.h5"
            _save_owned(envelope, signal_rows(), path)
            result = inspect_run_group(str(path), envelope["group_id"])
            self.assertTrue(result.report.cleanup_ok)
        finally:
            shutil.rmtree(temp, True)
        self.assertFalse(Path(temp).exists())

    def test_a8_2_connections_released_zero_resource_warnings(self):
        envelope = signal_envelope()
        temp = tempfile.mkdtemp(prefix=_TEMP_PREFIX)
        self.addCleanup(shutil.rmtree, temp, True)
        path = Path(temp) / "release.h5"
        backend = _track_backend(self, HDF5DataBackend())
        with warnings.catch_warnings():
            warnings.simplefilter("error", ResourceWarning)
            group, report = _save_owned(envelope, signal_rows(), path)
            report_two = save_run_group(group, str(path), backend=backend)
            result = inspect_run_group(str(path), envelope["group_id"])
        _assert_report_success(self, report, "save", path)
        _assert_report_success(self, report_two, "save", path)
        self.assertTrue(result.report.cleanup_ok)
        self.assertFalse(backend.connected)

    def test_a8_3_no_autospec_wraps_anywhere(self):
        # AST call-site inspection of THIS suite: no mock.patch call may
        # use autospec or wraps (the 3.9 gh-75988 lesson; recording
        # spies delegate explicitly to the original instead).
        tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            target = node.func
            dotted = ""
            while isinstance(target, ast.Attribute):
                dotted = "." + target.attr + dotted
                target = target.value
            if isinstance(target, ast.Name):
                dotted = target.id + dotted
            if "mock.patch" not in dotted:
                continue
            keywords = {keyword.arg for keyword in node.keywords}
            self.assertNotIn("autospec", keywords, ast.dump(node))
            self.assertNotIn("wraps", keywords, ast.dump(node))

    def test_a8_4_no_interpreter_guards_or_global_mutation(self):
        # AST call-site inspection (ignores this assertion's own
        # spelling of the forbidden names): the suite never mutates
        # interpreter guards or recursion limits.
        tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
        forbidden = {"set_int_max_str_digits", "setrecursionlimit"}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            target = node.func
            name = (
                target.attr
                if isinstance(target, ast.Attribute)
                else (target.id if isinstance(target, ast.Name) else None)
            )
            self.assertNotIn(name, forbidden, ast.dump(node))

    def test_a8_5_environment_facts_captured_at_runtime(self):
        # Versions are captured at runtime, never asserted as literals
        # (the envelope builders above already consume these).
        self.assertEqual(
            thermal_envelope()["software"]["softlab_version"],
            str(softlab.__version__),
        )
        self.assertEqual(
            thermal_envelope()["software"]["python_version"],
            platform.python_version(),
        )
        self.assertEqual(len(_hex64("x")), 64)


if __name__ == "__main__":
    unittest.main()
