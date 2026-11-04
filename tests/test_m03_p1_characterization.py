"""M03 phase 1 executable characterization (C1-C6).

Assertion-based probes against the UNCHANGED production baseline, per the
approved plan (log/release_12/test/characterization-test-plan.md, commit
de239e0; developer review closed all findings).

Areas:
- C1: legacy thermal v1/v2 manifest readers (entry points, strict validation,
  absent-vs-invalid metadata, completed-acquisition-only outcome snapshot,
  execution UUID vs persistent group UUID distinctness).
- C2: DataGroup/metadata actual behavior (constructor R4-BASE-META-01,
  set_meta(None) mismatch, HDF5 text/lossy storage on both load paths,
  Sqlite3DataBackend metadata reconstruction).
- C3: on-disk HDF5 metadata inspection with h5py on sha256-verified
  disposable byte-copies (original bytes preserved).
- C4: backend caller inventory evidence (static + runtime) and observed
  h5py/libhdf5/sqlite3 versions.
- C5: two-client inventory (thermal client truthful fields; signal-monitor
  example characterized as written, with independent analytic oracles).
- C6: hygiene (synthetic temp data only, temp dirs cleaned, no hardware,
  no source changes).

All fixtures are synthetic; observed defects are recorded as facts, never
repaired, relaxed or broadened.  If observed behavior contradicts a plan
expectation, the observed behavior is asserted and the deviation is noted
in log/release_12/test/characterization-results.md.
"""

import dataclasses
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
import warnings
from unittest import mock

import h5py
import numpy as np

import softlab
from softlab.huo.process.bounded import CountResult
from softlab.huo.scheduler import impl_scheduler
from softlab.shui.data import (
    DataGroup,
    DataRecord,
    HDF5DataBackend,
    Sqlite3DataBackend,
)
from softlab.shui.data import io as data_io


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "thermal_experiment.py"
MONITOR = ROOT / "examples" / "signal_monitor.py"


def load_example(name, path):
    """Load an example script as a module, like the existing thermal tests."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


thermal = load_example("thermal_m03_p1_characterization", EXAMPLE)
monitor = load_example("signal_monitor_m03_p1_characterization", MONITOR)


def sha256_of(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def track_io_backends(test):
    """Record every backend data_io creates and disconnect it at cleanup.

    The production io helpers (save_group/reload_group/load_groups) never
    disconnect; this harness double preserves the C6 hygiene rule that no
    connection leaks between tests, without modifying production code.
    """
    created = []
    original = data_io.get_data_backend

    def spy(backend_type, args=None, connect=True):
        backend = original(backend_type, args, connect)
        created.append(backend)
        return backend

    patcher = mock.patch.object(data_io, "get_data_backend",
                                side_effect=spy)
    patcher.start()
    test.addCleanup(patcher.stop)

    def disconnect_all():
        for backend in created:
            if backend.connected:
                backend.disconnect()

    test.addCleanup(disconnect_all)
    return created


def save_group_file(group, path):
    """Save a synthetic group through the plain HDF5 backend."""
    backend = HDF5DataBackend()
    try:
        assert backend.connect({"path": str(path)})
        assert backend.save_group(group)
    finally:
        backend.disconnect()


class ThermalRunFixture:
    """One synthetic thermal run in a shared temporary directory."""

    def __init__(self, adapter="coordinated", prefix="softlab-m03-p1-"):
        self.temp = tempfile.TemporaryDirectory(prefix=prefix)
        self.result = thermal.execute(
            thermal.ThermalConfig(), Path(self.temp.name) / adapter,
            adapter=adapter)

    @property
    def path(self):
        return self.result.data_path

    @property
    def group_id(self):
        return str(self.result.group.id)

    def copy(self, root, name="copy"):
        target = Path(root) / (name + ".h5")
        shutil.copy2(self.path, target)
        return target

    def cleanup(self):
        self.temp.cleanup()


class ManifestReaderEntryPoints(unittest.TestCase):
    """C1.1: enumerate and probe every path that reads a stored manifest."""

    @classmethod
    def setUpClass(cls):
        cls.shared = ThermalRunFixture("coordinated")
        cls.addClassCleanup(cls.shared.cleanup)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c1-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_execute_save_reopen_verification_path(self):
        """In-process execute() writes, reopens via _reload_group and
        verifies round trip (_verify_round_trip already ran inside)."""
        result = self.shared.result
        self.assertEqual(result.persistence, "verified")
        self.assertIsNotNone(result.loaded)
        self.assertIsInstance(result.loaded, DataGroup)
        self.assertEqual(str(result.loaded.id), self.shared.group_id)
        self.assertIn("thermal_manifest", result.loaded.meta)

    def test_inspect_stored_without_and_with_group_id(self):
        copy = self.shared.copy(self.root)
        without_id, manifest_a = thermal.inspect_stored(copy)
        self.assertIsInstance(without_id, DataGroup)
        self.assertEqual(str(without_id.id), self.shared.group_id)
        self.assertIsInstance(manifest_a, dict)
        with_id, manifest_b = thermal.inspect_stored(copy, self.shared.group_id)
        self.assertEqual(str(with_id.id), self.shared.group_id)
        self.assertEqual(manifest_b, manifest_a)

    def test_inspect_stored_multi_group_requires_explicit_id(self):
        """A file with two stored groups fails without an explicit id. With
        an explicit id the thermal-shaped group loads; a group WITHOUT the
        legacy _META_KEYS fails 'Missing or nontext disk metadata' - the
        inspection helper is thermal-specific, not generic."""
        combined = self.root / "combined.h5"
        bare = DataGroup("bare")
        legacy = DataGroup("legacy")
        for key in ("example", "configuration", "timing"):
            legacy.set_meta(key, "text")
        save_group_file(bare, combined)
        backend = HDF5DataBackend()
        try:
            assert backend.connect({"path": str(combined)})
            assert backend.save_group(legacy)
        finally:
            backend.disconnect()
        with self.assertRaisesRegex(ValueError, "exactly one stored group"):
            thermal.inspect_stored(combined)
        group, manifest = thermal.inspect_stored(combined, str(legacy.id))
        self.assertEqual(str(group.id), str(legacy.id))
        self.assertIsNone(manifest)  # no legacy envelope on this file
        with self.assertRaises(thermal._HelperFailure) as caught:
            thermal.inspect_stored(combined, str(bare.id))
        self.assertIn("Missing or nontext disk metadata: example",
                      str(caught.exception.primary))

    def test_cli_inspect_file_on_disposable_copy(self):
        copy = self.shared.copy(self.root)
        completed = subprocess.run(
            [sys.executable, "-W", "error",
             str(ROOT / "tests" / "thermal_cli_bootstrap.py"), str(EXAMPLE),
             "--inspect-file", str(copy)],
            cwd=ROOT, text=True, capture_output=True, timeout=120)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["context"], "available")
        self.assertEqual(payload["run_id"], self.shared.group_id)
        self.assertEqual(len(payload["rows"]),
                         thermal.ThermalConfig().points)


class ManifestStrictValidation(unittest.TestCase):
    """C1.2: one synthetic manifest defect at a time; exact observed text."""

    @classmethod
    def setUpClass(cls):
        cls.shared = ThermalRunFixture("coordinated")
        cls.addClassCleanup(cls.shared.cleanup)

    def manifest(self):
        return thermal._decode_manifest(
            self.shared.result.group.get_meta("thermal_manifest"))

    def assert_defect(self, mutate, message):
        manifest = self.manifest()
        mutate(manifest)
        with self.assertRaisesRegex(ValueError, re.escape(message)):
            thermal._check_manifest(manifest)

    def test_wrong_type_and_missing_and_extra_top_level_keys(self):
        with self.assertRaisesRegex(ValueError, "manifest: expected object"):
            thermal._check_manifest([])
        self.assert_defect(
            lambda m: m.pop("run_id"),
            "manifest: expected exact object fields")
        self.assert_defect(
            lambda m: m.update(extra=1),
            "manifest: expected exact object fields")

    def test_schema_version_and_run_id_defects(self):
        self.assert_defect(
            lambda m: m.update(schema="other"),
            "schema: unsupported manifest schema")
        self.assert_defect(
            lambda m: m.update(version=3),
            "version: unsupported manifest version")
        self.assert_defect(
            lambda m: m.update(run_id="run-1"),
            "run_id: expected canonical UUID")
        # canonical but nonmatching form (uppercase) is also rejected
        self.assert_defect(
            lambda m: m.update(run_id=str(uuid.uuid4()).upper()),
            "run_id: expected canonical UUID")

    def test_configuration_model_procedure_column_defects(self):
        self.assert_defect(
            lambda m: m.update(configuration={**m["configuration"],
                                              "dt": "1.0"}),
            "configuration.dt: expected finite real")
        invalid = self.manifest()
        invalid["configuration"] = {**invalid["configuration"], "dt": 100.0}
        with self.assertRaisesRegex(ValueError, "Require 0 < dt"):
            thermal._check_manifest(invalid)
        self.assert_defect(
            lambda m: m.update(model={**m["model"], "equation": "wrong"}),
            "model: unsupported model semantics or units")
        self.assert_defect(
            lambda m: m.update(procedure={**m["procedure"],
                                          "requested_points":
                                          m["procedure"]["requested_points"]
                                          + 1}),
            "procedure: inconsistent declared acquisition")
        self.assert_defect(
            lambda m: m.update(columns=m["columns"][:-1]),
            "columns: expected ordered thermal schema")
        self.assert_defect(
            lambda m: m.update(columns=[{**m["columns"][0], "unit": "bad"}]
                               + m["columns"][1:]),
            "columns.0: invalid column definition")

    def test_clock_source_and_outcome_defects(self):
        self.assert_defect(
            lambda m: m.update(clock={**m["clock"],
                                      "measurement_delay_ticks": 0}),
            "clock: inconsistent acquisition timing")
        self.assert_defect(
            lambda m: m.update(source={**m["source"], "digest": "zzz"}),
            "source.digest: invalid digest or availability")
        self.assert_defect(
            lambda m: m.update(outcome={**m["outcome"],
                                        "completed_rows":
                                        m["outcome"]["completed_rows"] - 1}),
            "outcome: inconsistent completed acquisition snapshot")

    def test_decode_manifest_malformed_json_and_hygiene(self):
        with self.assertRaises(ValueError):
            thermal._decode_manifest("{not json")
        with self.assertRaisesRegex(ValueError,
                                    "manifest: duplicate object key"):
            thermal._decode_manifest('{"schema": 1, "schema": 2}')
        with self.assertRaisesRegex(ValueError,
                                    "manifest: nonfinite JSON token"):
            thermal._decode_manifest("NaN")
        with self.assertRaisesRegex(TypeError,
                                    "manifest: expected JSON text"):
            thermal._decode_manifest(None)

    def test_validate_manifest_data_identity_and_row_count(self):
        manifest = self.manifest()
        other = {**manifest, "run_id": str(uuid.uuid4())}
        with self.assertRaisesRegex(ValueError,
                                    "run_id: differs from stored group"):
            thermal._validate_manifest_data(other, self.shared.result.group)
        # a group whose stored record has one row fewer than declared
        record = DataRecord("thermal_trace", manifest["columns"])
        record.add_rows(
            self.shared.result.group.record("thermal_trace").table.head(
                thermal.ThermalConfig().points - 1))
        truncated = DataGroup("thermal_experiment",
                              id=self.shared.result.group.id)
        truncated.add_record(record)
        with self.assertRaisesRegex(
                ValueError,
                "outcome.completed_rows: differs from stored row count"):
            thermal._validate_manifest_data(manifest, truncated)


class ManifestAbsentVsInvalid(unittest.TestCase):
    """C1.3: legacy absence is accepted; invalid-present never means absent."""

    @classmethod
    def setUpClass(cls):
        cls.shared = ThermalRunFixture("coordinated")
        cls.original_digest = sha256_of(cls.shared.path)
        cls.addClassCleanup(cls.shared.cleanup)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c1b-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_absent_manifest_accepted_as_legacy_unavailable(self):
        copy = self.shared.copy(self.root)
        with h5py.File(copy, "a") as disk:
            del disk[self.shared.group_id].attrs["thermal_manifest"]
        group, manifest = thermal.inspect_stored(copy)
        self.assertIsNone(manifest)
        self.assertEqual(str(group.id), self.shared.group_id)
        completed = subprocess.run(
            [sys.executable, "-W", "error",
             str(ROOT / "tests" / "thermal_cli_bootstrap.py"), str(EXAMPLE),
             "--inspect-file", str(copy)],
            cwd=ROOT, text=True, capture_output=True, timeout=120)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout)["context"],
                         "legacy_unavailable")

    def test_present_non_text_manifest_rejected(self):
        copy = self.shared.copy(self.root)
        with h5py.File(copy, "a") as disk:
            disk[self.shared.group_id].attrs["thermal_manifest"] = 42
        with self.assertRaises(thermal._HelperFailure) as caught:
            thermal.inspect_stored(copy)
        self.assertIsInstance(caught.exception.primary, ValueError)
        self.assertIn("present value is not text",
                      str(caught.exception.primary))

    def test_present_invalid_text_manifest_rejected(self):
        copy = self.shared.copy(self.root)
        with h5py.File(copy, "a") as disk:
            disk[self.shared.group_id].attrs["thermal_manifest"] = "not json"
        with self.assertRaises(thermal._HelperFailure) as caught:
            thermal.inspect_stored(copy)
        self.assertIsInstance(caught.exception.primary, ValueError)

    def test_original_bytes_preserved_throughout(self):
        """Every probe above ran on disposable copies; the delivered
        synthetic original is byte-identical afterwards."""
        self.assertEqual(sha256_of(self.shared.path), self.original_digest)


class ManifestOutcomeSnapshot(unittest.TestCase):
    """C1.4: completed-acquisition-only outcome snapshot, verified from disk."""

    @classmethod
    def setUpClass(cls):
        cls.shared = ThermalRunFixture("coordinated")
        cls.addClassCleanup(cls.shared.cleanup)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c1c-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_outcome_block_from_disk_bytes(self):
        """Parse the manifest from on-disk bytes on a disposable copy
        (independent of any in-memory object; cross-checked again in C3)."""
        copy = self.shared.copy(self.root)
        with h5py.File(copy, "r") as disk:
            text = disk[self.shared.group_id].attrs["thermal_manifest"]
        self.assertIs(type(text), str)
        outcome = json.loads(text)["outcome"]
        config = thermal.ThermalConfig()
        self.assertEqual(outcome["snapshot"],
                         "completed_acquisition_before_persistence")
        self.assertEqual(outcome["acquisition"], "completed")
        self.assertEqual(outcome["requested_rows"], config.points)
        self.assertEqual(outcome["completed_rows"], config.points)
        self.assertEqual(outcome["final_tick"], config.points)
        self.assertEqual(outcome["final_sim_time"], config.points * config.dt)
        # later stages are explicitly not yet recorded at snapshot time
        for key in ("persistence", "plotting", "cleanup", "overall"):
            self.assertEqual(outcome[key], "not_yet_established")
            self.assertNotIn(key, ("acquisition",))

    def test_make_manifest_requires_completed_acquisition(self):
        result = thermal.RunResult(
            4, Path("thermal.h5"), Path("thermal.png"),
            thermal.StopRequest(), acquisition="failed")
        with self.assertRaisesRegex(
                ValueError,
                "manifest requires completed acquisition"):
            thermal._make_manifest(thermal.ThermalConfig(),
                                   self.shared.result.group, result)


class ExecutionVsGroupUuid(unittest.TestCase):
    """C1.5: bounded-execution UUID (CountResult.run_id) vs persistent
    DataGroup.id are distinct; RunResult retains no execution UUID."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c1d-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_execution_uuid_observed_via_after_row_and_distinct(self):
        captured = {}
        original_run_count = thermal.run_count

        def spy_run_count(process, total, row_count, boundary, **kwargs):
            user_after_row = kwargs.get("after_row")

            def spy(snapshot):
                captured["snapshot"] = snapshot
                if user_after_row is not None:
                    user_after_row(snapshot)

            kwargs["after_row"] = spy
            return original_run_count(process, total, row_count,
                                      boundary, **kwargs)

        with mock.patch.object(thermal, "run_count",
                               side_effect=spy_run_count):
            result = thermal.execute(thermal.ThermalConfig(),
                                     self.root / "uuid")
        self.assertTrue(result.overall_success)
        snapshot = captured["snapshot"]
        self.assertIs(type(snapshot), CountResult)
        execution_uuid = snapshot.run_id
        self.assertIsInstance(execution_uuid, uuid.UUID)
        # distinctness: execution UUID != persistent group UUID
        self.assertNotEqual(str(execution_uuid), str(result.group.id))
        # RunResult does not retain the execution UUID (verbatim fact)
        field_names = [f.name for f in dataclasses.fields(thermal.RunResult)]
        self.assertNotIn("run_id", field_names)
        with self.assertRaises(AttributeError):
            _ = result.run_id


class DataGroupMetaConstructor(unittest.TestCase):
    """C2.1: constructor defect R4-BASE-META-01 observed verbatim."""

    def test_meta_argument_dropped_but_caller_dict_mutated(self):
        caller = {"a": 1}
        group = DataGroup("probe", meta=caller)
        # observed: self._meta is never populated from meta=
        self.assertEqual(group.meta, {})
        self.assertIsNone(group.get_meta("a"))
        # observed: the caller's mapping is not copied; str keys already
        # text are written back unchanged (mutation is invisible here)
        self.assertEqual(caller, {"a": 1})

    def test_meta_argument_key_coercion_mutates_caller_dict(self):
        caller = {2: "x"}
        # observed: a non-str key is coerced and written back into the
        # caller's dict WHILE it is being iterated, so construction fails
        # with RuntimeError - and the caller's dict is left mutated, holding
        # BOTH the original int key and the coerced str key
        with self.assertRaisesRegex(RuntimeError,
                                    "dictionary changed size during"):
            DataGroup("probe", meta=caller)
        self.assertEqual(caller, {2: "x", "2": "x"})

    def test_meta_argument_mixed_keys_same_failure(self):
        caller = {"a": 1, 2: "x"}
        with self.assertRaisesRegex(RuntimeError,
                                    "dictionary changed size during"):
            DataGroup("probe", meta=caller)
        self.assertEqual(caller, {"a": 1, 2: "x", "2": "x"})
        # in every observed case group.meta stays empty (R4-BASE-META-01)
        group = DataGroup("probe")
        self.assertEqual(group.meta, {})


class SetMetaNoneSemantics(unittest.TestCase):
    """C2.2: set_meta(None) keeps the key with value None, contradicting
    the docstring claim "will be removed"."""

    def test_set_meta_none_on_existing_key(self):
        group = DataGroup("probe")
        group.set_meta("k", 1)
        group.set_meta("k", None)
        # observed: the key REMAINS with value None (docstring says removed)
        self.assertIn("k", group.meta)
        self.assertIsNone(group.meta["k"])
        self.assertIsNone(group.get_meta("k"))  # indistinguishable from absent
        self.assertIsNone(group.pop_meta("k"))  # pops the stored None
        self.assertNotIn("k", group.meta)

    def test_set_meta_none_on_absent_key_creates_it(self):
        group = DataGroup("probe")
        group.set_meta("new", None)
        self.assertIn("new", group.meta)
        self.assertIsNone(group.meta["new"])


class Hdf5MetaRoundTrip(unittest.TestCase):
    """C2.3: HDF5 save writes str(value); both load paths recorded verbatim."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c2-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.values = {
            "example": "thermal_probe",
            "configuration": "{}",
            "timing": "{}",
            "text": "héllo",
            "int": 7,
            "float": 2.5,
            "list": [1, 2],
            "dict": {"k": 1},
            "none": None,
        }
        self.group = DataGroup("probe")
        for key, value in self.values.items():
            self.group.set_meta(key, value)
        self.path = self.root / "probe.h5"
        save_group_file(self.group, self.path)

    def test_disk_bytes_store_str_of_every_value(self):
        copy = self.root / "disk-copy.h5"
        shutil.copy2(self.path, copy)
        digest_before = sha256_of(self.path)
        with h5py.File(copy, "r") as disk:
            attrs = disk[str(self.group.id)].attrs
            for key, value in self.values.items():
                self.assertIs(type(attrs[key]), str, key)
                self.assertEqual(attrs[key], str(value), key)
        self.assertEqual(sha256_of(self.path), digest_before)

    def test_plain_backend_load_path_returns_empty_meta(self):
        """backend.py load_group -> hdf5.py: the live attrs proxy passed as
        meta= is dropped by R4-BASE-META-01, compounded by the reserved-attr
        pops of R4-BASE-HDF5-01: observed meta is EMPTY (recorded as-is)."""
        backend = HDF5DataBackend()
        try:
            assert backend.connect({"path": str(self.path)})
            loaded = backend.load_group(str(self.group.id))
            self.assertIsInstance(loaded, DataGroup)
            self.assertEqual(loaded.meta, {})
        finally:
            backend.disconnect()

    def test_plain_load_mutates_reserved_attrs_of_open_file(self):
        """R4-BASE-HDF5-01 verbatim: pops of name/id/timestamp/backend land
        on the live attrs proxy of the file opened in 'a' mode, so the
        mutation persists to the synthetic file on close."""
        victim = self.root / "mutation-victim.h5"
        shutil.copy2(self.path, victim)
        backend = HDF5DataBackend()
        try:
            assert backend.connect({"path": str(victim)})
            backend.load_group(str(self.group.id))
            group = backend.file[str(self.group.id)]  # keep the id alive
            attrs = group.attrs
            for reserved in ("name", "id", "timestamp", "backend"):
                self.assertNotIn(reserved, attrs)
            # non-reserved meta keys remain on disk (only pops are removed)
            self.assertIn("example", attrs)
        finally:
            backend.disconnect()
        with h5py.File(victim, "r") as disk:
            reopened = dict(disk[str(self.group.id)].attrs)
        for reserved in ("name", "id", "timestamp", "backend"):
            self.assertNotIn(reserved, reopened)
        self.assertIn("example", reopened)

    def test_example_hydration_path_restores_only_legacy_keys(self):
        """_load_group/_reload_group hydrate meta from captured disk attrs,
        but ONLY the three _META_KEYS (plus the manifest): every custom
        meta key is dropped by the example load path, not even lossily."""
        copy = self.root / "hydration-copy.h5"
        shutil.copy2(self.path, copy)
        digest_before = sha256_of(self.path)
        loaded = thermal._reload_group(copy, str(self.group.id))
        self.assertEqual(loaded.meta, {
            "example": "thermal_probe",
            "configuration": "{}",
            "timing": "{}",
        })
        self.assertEqual(sha256_of(self.path), digest_before)


class SqliteMetaCapability(unittest.TestCase):
    """C2.4: which metadata SQLite actually stores vs drops (observed)."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c2b-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "probe.db"
        self.group = DataGroup("sqlite_probe")
        self.group.set_meta("sample", "s1")
        self.group.set_meta("run_id", "run-42")
        self.group.set_meta("extra", "dropped")
        backend = Sqlite3DataBackend()
        try:
            assert backend.connect({"path": str(self.db)})
            assert backend.save_group(self.group)
        finally:
            backend.disconnect()

    def test_schema_columns_and_run_id_type_quirk(self):
        raw = sqlite3.connect(str(self.db))
        try:
            columns = raw.execute("pragma table_info(datagroup)").fetchall()
        finally:
            raw.close()
        names = [row[1] for row in columns]
        self.assertEqual(names, ["id", "experiment", "sample", "run_id",
                                 "timestamp", "backend"])
        declared = {row[1]: row[2] for row in columns}
        # observed schema quirk: run_id declared INTEGER while the saved
        # value is unconstrained (a text run_id persists as text)
        self.assertEqual(declared["run_id"], "INTEGER")
        raw = sqlite3.connect(str(self.db))
        try:
            sample, run_id = raw.execute(
                "select sample, run_id from datagroup").fetchone()
        finally:
            raw.close()
        self.assertEqual(sample, "s1")
        self.assertEqual(run_id, "run-42")

    def test_plain_load_reconstructs_no_meta(self):
        """DEVIATION from plan expectation: sqlite3.py:159-162 builds a
        {'sample', 'run_id'} dict for DataGroup(..., meta=), but
        R4-BASE-META-01 drops it, so the plain load path's observed meta is
        EMPTY - asserted here as the actual behavior."""
        backend = Sqlite3DataBackend()
        try:
            assert backend.connect({"path": str(self.db)})
            loaded = backend.load_group(str(self.group.id))
        finally:
            backend.disconnect()
        self.assertIsInstance(loaded, DataGroup)
        self.assertEqual(loaded.meta, {})
        self.assertEqual(loaded.name, "sqlite_probe")
        self.assertEqual(str(loaded.id), str(self.group.id))

    def test_defaults_and_extra_metadata_dropped(self):
        plain = DataGroup("defaults_only")
        backend = Sqlite3DataBackend()
        try:
            assert backend.connect({"path": str(self.db)})
            assert backend.save_group(plain)
        finally:
            backend.disconnect()
        raw = sqlite3.connect(str(self.db))
        try:
            row = raw.execute(
                "select sample, run_id from datagroup where id=?",
                (str(plain.id),)).fetchone()
        finally:
            raw.close()
        # observed: absent meta keys save as the hard-coded defaults
        self.assertEqual(row, ("demo", 0))
        # 'extra' was never persisted anywhere: only sample/run_id columns
        raw = sqlite3.connect(str(self.db))
        try:
            names = [r[1] for r in raw.execute(
                "pragma table_info(datagroup)").fetchall()]
        finally:
            raw.close()
        self.assertNotIn("extra", names)


class Hdf5OnDiskInspection(unittest.TestCase):
    """C3: direct on-disk HDF5 metadata inspection on sha256-verified
    disposable byte-copies; original bytes preserved; read-only path is
    safe, so no BLOCKED result arises."""

    @classmethod
    def setUpClass(cls):
        cls.v1 = ThermalRunFixture("coordinated")
        cls.v2 = ThermalRunFixture("direct")
        cls.addClassCleanup(cls.v1.cleanup)
        cls.addClassCleanup(cls.v2.cleanup)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c3-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def inspect_copy(self, fixture):
        digest_before = sha256_of(fixture.path)
        copy = fixture.copy(self.root)
        with h5py.File(copy, "r") as disk:
            attrs = dict(disk[fixture.group_id].attrs)
        digest_after = sha256_of(fixture.path)
        self.assertEqual(digest_before, digest_after)
        return attrs

    def test_v1_and_v2_disk_attrs_and_manifest(self):
        for fixture, version, delay in ((self.v1, 1, 1), (self.v2, 2, 0)):
            with self.subTest(version=version):
                attrs = self.inspect_copy(fixture)
                for key in ("id", "name", "timestamp", "backend",
                            "example", "configuration", "timing",
                            "thermal_manifest"):
                    self.assertIn(key, attrs)
                    self.assertIs(type(attrs[key]), str, key)
                self.assertEqual(attrs["id"], fixture.group_id)
                self.assertEqual(attrs["name"], "thermal_experiment")
                backend_info = json.loads(attrs["backend"])
                self.assertEqual(backend_info["type"], "hdf5")
                manifest = json.loads(attrs["thermal_manifest"])
                self.assertEqual(manifest["schema"],
                                 "softlab.thermal-reference")
                self.assertEqual(manifest["version"], version)
                self.assertEqual(manifest["run_id"], fixture.group_id)
                self.assertEqual(manifest["clock"]["measurement_delay_ticks"],
                                 delay)
                self.assertEqual(
                    manifest["outcome"]["snapshot"],
                    "completed_acquisition_before_persistence")
                # legacy text metadata cross-checks against the manifest
                configuration = json.loads(attrs["configuration"])
                self.assertEqual(configuration,
                                 dataclasses.asdict(thermal.ThermalConfig()))
                self.assertEqual(json.loads(attrs["timing"]),
                                 thermal._timing(delay))

    def test_hand_built_hdf5_fixture_types_verbatim(self):
        """A hand-built h5py file (no softlab writer) keeps native attr
        types - contrast with backend-written files where all attrs are
        text (hdf5.py:81-82 writes str(value))."""
        built = self.root / "handbuilt.h5"
        with h5py.File(built, "w") as disk:
            group = disk.create_group("handbuilt")
            group.attrs["id"] = "handbuilt"
            group.attrs["note"] = "synthetic"
            group.attrs["count"] = 7
            group.attrs["ratio"] = 0.5
        digest_before = sha256_of(built)
        copy = self.root / "handbuilt-copy.h5"
        shutil.copy2(built, copy)
        with h5py.File(copy, "r") as disk:
            attrs = dict(disk["handbuilt"].attrs)
        self.assertEqual(sha256_of(built), digest_before)
        # observed verbatim: h5py returns numpy scalar types for numeric
        # attributes (not Python int/float); text stays str
        self.assertIs(type(attrs["id"]), str)
        self.assertIs(type(attrs["note"]), str)
        self.assertIs(type(attrs["count"]), np.int64)
        self.assertIs(type(attrs["ratio"]), np.float64)
        self.assertEqual(attrs["count"], 7)
        self.assertEqual(attrs["ratio"], 0.5)

    def test_observed_library_versions_recorded(self):
        self.assertTrue(h5py.__version__)
        self.assertTrue(h5py.version.hdf5_version)
        self.assertTrue(sqlite3.sqlite_version)


class BackendCallers(unittest.TestCase):
    """C4: who calls HDF5DataBackend / Sqlite3DataBackend today."""

    def test_static_caller_inventory(self):
        io_source = (ROOT / "softlab" / "shui" / "data" / "io.py").read_text()
        self.assertIn("HDF5DataBackend()", io_source)
        self.assertIn("Sqlite3DataBackend()", io_source)
        thermal_source = EXAMPLE.read_text()
        self.assertIn("HDF5DataBackend", thermal_source)
        compatibility = (ROOT / "tests" / "test_compatibility.py").read_text()
        self.assertIn("Sqlite3DataBackend", compatibility)
        self.assertIn("HDF5DataBackend", compatibility)
        # module __main__ examples exercise each backend directly
        self.assertIn("HDF5DataBackend()",
                      (ROOT / "softlab" / "shui" / "data" / "hdf5.py")
                      .read_text())
        # notebooks contain no backend references (scanned, none found)
        for notebook in (ROOT / "tests").glob("*.ipynb"):
            self.assertNotIn("DataBackend", notebook.read_text())

    def test_io_entry_points_instantiate_both_backends(self):
        track_io_backends(self)
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c4-")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        with self.assertRaises(ValueError):
            data_io.get_data_backend("")
        with self.assertRaises(NotImplementedError):
            data_io.get_data_backend("unknown")
        hdf5 = data_io.get_data_backend(
            "hdf5", {"path": str(root / "io.h5")})
        self.assertIsInstance(hdf5, HDF5DataBackend)
        hdf5.disconnect()
        sqlite = data_io.get_data_backend(
            "sqlite3", {"path": str(root / "io.db")})
        self.assertIsInstance(sqlite, Sqlite3DataBackend)
        sqlite.disconnect()
        by_info = data_io.get_data_backend_by_info(
            {"type": "hdf5", "arguments": {"path": str(root / "io2.h5")}})
        self.assertIsInstance(by_info, HDF5DataBackend)
        by_info.disconnect()

    def test_save_and_reload_group_depend_on_group_backend_info(self):
        track_io_backends(self)
        """save_group writes group.backend when a backend_type is given.
        OBSERVED DEFECT: reload_group passes group.id (a UUID) to
        load_group, which BOTH backends reject - sqlite3 raises a binding
        ProgrammingError, hdf5 raises a TypeError - each swallowed by
        catch_error, so reload_group silently returns None even with
        populated, valid group.backend info.  The manual str(id) load
        succeeds on both backends."""
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c4b-")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        group = DataGroup("io_probe")
        # default (empty) backend info: reload observes None
        self.assertFalse(data_io.reload_group(group))
        for kind, name, cls in (("sqlite3", "io.db", Sqlite3DataBackend),
                                ("hdf5", "io.h5", HDF5DataBackend)):
            with self.subTest(backend=kind):
                target = DataGroup("io_probe_" + kind)
                self.assertTrue(data_io.save_group(
                    target, kind, {"path": str(root / name)}))
                # observed: save_group populated group.backend from the
                # live backend (type + arguments verbatim)
                self.assertEqual(target.backend["type"], kind)
                self.assertEqual(target.backend["arguments"],
                                 {"path": str(root / name)})
                # observed: reload_group silently returns None on both
                backends = []
                original = data_io.get_data_backend_by_info

                def spy(info, connect=True, _original=original,
                        _backends=backends):
                    backend = _original(info, connect)
                    _backends.append(backend)
                    return backend

                with mock.patch.object(data_io, "get_data_backend_by_info",
                                       side_effect=spy):
                    self.assertIsNone(data_io.reload_group(target))
                self.assertEqual(len(backends[0].errors), 1)
                self.assertIn("Failed to load group", backends[0].errors[0])
                # the same load with str(id) succeeds (id-type is the cause)
                manual = cls()
                try:
                    assert manual.connect({"path": str(root / name)})
                    loaded = manual.load_group(str(target.id))
                finally:
                    manual.disconnect()
                self.assertIsInstance(loaded, DataGroup)
                self.assertEqual(str(loaded.id), str(target.id))

    def test_load_groups_hdf5_works_sqlite_returns_empty(self):
        track_io_backends(self)
        """OBSERVED: hdf5 load_groups lists str keys and round-trips; the
        sqlite3 list_impl returns fetchall() ROW TUPLES, so load_group
        receives a tuple id, fails silently (catch_error), and load_groups
        returns [] for a non-empty sqlite database."""
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c4e-")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        hdf5_group = DataGroup("lg_hdf5")
        sqlite_group = DataGroup("lg_sqlite3")
        self.assertTrue(data_io.save_group(
            hdf5_group, "hdf5", {"path": str(root / "lg.h5")}))
        self.assertTrue(data_io.save_group(
            sqlite_group, "sqlite3", {"path": str(root / "lg.db")}))
        # NOTE: asserted on fresh files BEFORE any plain load, because a
        # plain load destructively removes the reserved attrs (C2/R4 chain)
        groups = data_io.load_groups("hdf5", {"path": str(root / "lg.h5")})
        self.assertEqual([str(g.id) for g in groups], [str(hdf5_group.id)])
        self.assertEqual(data_io.load_groups(
            "sqlite3", {"path": str(root / "lg.db")}), [])

    def test_second_plain_load_after_destructive_first_load(self):
        """R4-BASE-HDF5-01 compounding chain, observed end to end: the first
        plain load removes the reserved attrs from the file on close; a
        second plain load of the same file then fails inside uuid.UUID(),
        whose str default is fed a UUID object - AttributeError, recorded
        verbatim, load observes None."""
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c4d-")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        group = DataGroup("chain")
        save_group_file(group, root / "chain.h5")
        first = HDF5DataBackend()
        try:
            assert first.connect({"path": str(root / "chain.h5")})
            self.assertIsInstance(first.load_group(str(group.id)), DataGroup)
        finally:
            first.disconnect()
        second = HDF5DataBackend()
        try:
            assert second.connect({"path": str(root / "chain.h5")})
            self.assertIsNone(second.load_group(str(group.id)))
            self.assertEqual(len(second.errors), 1)
            self.assertIn("'UUID' object has no attribute 'replace'",
                          second.errors[0])
        finally:
            second.disconnect()

    def test_runtime_caller_instrumentation_of_thermal_execute(self):
        """Record which backend entry points the thermal example exercises,
        with which arguments (test-harness doubles only, no source change).

        OBSERVED HARNESS FACT: explicit recording spies are used instead of
        ``mock.patch.object(..., autospec=True, wraps=original)`` because on
        Python 3.9 the autospec'd mock does NOT delegate to ``wraps``
        (CPython gh-75988, fixed in 3.11.9+/3.12/3.13, never backported to
        3.9): the patched ``connect`` returned a child MagicMock without
        running the real connect, so ``writer.connected`` stayed False and
        the example's own guard failed the run with
        ``RuntimeError('HDF5 writer connect failed: ')``.  Plain delegation
        spies record identically on every supported version; plain
        ``execute()`` (all other tests here) is unaffected on 3.9."""
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c4c-")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        calls = {"connect": [], "save_group": [], "load_group": []}

        def make_spy(name, original):
            def spy(instance, *args, **kwargs):
                calls[name].append(mock.call(instance, *args, **kwargs))
                return original(instance, *args, **kwargs)
            return spy

        spies = {name: make_spy(name, getattr(HDF5DataBackend, name))
                 for name in calls}
        patches = [mock.patch.object(HDF5DataBackend, name, spy)
                   for name, spy in spies.items()]
        for patch in patches:
            patch.start()
        try:
            result = thermal.execute(thermal.ThermalConfig(),
                                     root / "instrumented")
        finally:
            for patch in patches:
                patch.stop()
        self.assertTrue(result.overall_success)
        connects = calls["connect"]
        saves = calls["save_group"]
        loads = calls["load_group"]
        # writer connect on the output file + reader connect on the copy
        self.assertEqual(len(connects), 2)
        self.assertEqual(connects[0].args[1], {"path": str(result.data_path)})
        self.assertTrue(connects[1].args[1]["path"].endswith("thermal.h5"))
        self.assertEqual(len(saves), 1)
        self.assertIs(saves[0].args[1], result.group)
        self.assertEqual(len(loads), 1)
        self.assertEqual(loads[0].args[1], str(result.group.id))

    def test_observed_versions_are_recordable(self):
        self.assertTrue(sys.version)
        self.assertTrue(softlab.__version__)
        self.assertTrue(np.__version__)
        self.assertTrue(sqlite3.sqlite_version)


class ThermalClientInventory(unittest.TestCase):
    """C5a: field-by-field inventory of what the stored thermal record
    truthfully provides today (both adapters), validated against an
    independent Euler oracle."""

    CONFIG = thermal.ThermalConfig()

    def euler(self, delay):
        """Independent numeric oracle for the documented Euler recurrence
        (same formula as M02 ThermalPolicyComparison.euler, re-derived here
        so this suite's evidence stands alone)."""
        config = self.CONFIG
        temperature = config.initial_temperature
        expected = []
        for _ in range(config.points):
            if delay:
                expected.append(temperature)
            temperature += config.dt * (
                config.power - config.conductance *
                (temperature - config.ambient_temperature)
            ) / config.heat_capacity
            if not delay:
                expected.append(temperature)
        return expected

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c5-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def run_adapter(self, adapter):
        result = thermal.execute(self.CONFIG, self.root / adapter,
                                 adapter=adapter)
        self.assertTrue(result.overall_success)
        return result, thermal._decode_manifest(
            result.group.get_meta("thermal_manifest"))

    def test_truthful_fields_v1_coordinated(self):
        result, manifest = self.run_adapter("coordinated")
        self.assert_common_fields(manifest, result, version=1, delay=1)

    def test_truthful_fields_v2_direct(self):
        result, manifest = self.run_adapter("direct")
        self.assert_common_fields(manifest, result, version=2, delay=0)

    def assert_common_fields(self, manifest, result, version, delay):
        config = self.CONFIG
        self.assertEqual(set(manifest), {
            "schema", "version", "run_id", "configuration", "model",
            "procedure", "columns", "clock", "software", "source",
            "outcome"})
        self.assertEqual(manifest["schema"], "softlab.thermal-reference")
        self.assertEqual(manifest["version"], version)
        self.assertEqual(manifest["run_id"], str(result.group.id))
        self.assertEqual(manifest["configuration"],
                         dataclasses.asdict(config))
        self.assertEqual(manifest["model"]["name"], "lumped_thermal_body")
        self.assertEqual(manifest["model"]["method"], "explicit_euler")
        self.assertEqual(manifest["model"]["units"]["heat_capacity"], "J/K")
        self.assertEqual(manifest["model"]["units"]["power"], "W")
        self.assertEqual(manifest["procedure"], {
            "name": "thermal_count",
            "advancement": "once_before_each_getter_row",
            "command": "constant_power",
            "requested_points": config.points})
        self.assertEqual(manifest["columns"],
                         result.group.record("thermal_trace").columns)
        self.assertEqual(manifest["clock"]["dt"], config.dt)
        self.assertEqual(manifest["clock"]["simulation_unit"], "s")
        self.assertEqual(manifest["clock"]["measurement_delay_ticks"], delay)
        self.assertEqual(manifest["clock"]["source_tick"],
                         "tick-1" if delay else "tick")
        self.assertEqual(manifest["software"]["softlab_version"],
                         softlab.__version__)
        self.assertEqual(manifest["software"]["python_version"],
                         platform.python_version())
        source = manifest["source"]
        self.assertEqual(source["reference"], "examples/thermal_experiment.py")
        self.assertEqual(source["digest_algorithm"], "sha256")
        self.assertEqual(source["digest"],
                         hashlib.sha256(EXAMPLE.read_bytes()).hexdigest())
        self.assertIsNone(source["digest_unavailable_reason"])
        # git query result or recorded unavailable reason
        if source["git_revision"] is None:
            self.assertIsNone(source["git_dirty"])
            self.assertTrue(source["git_unavailable_reason"])
        else:
            self.assertIsNotNone(re.fullmatch(
                "[0-9a-f]{40}|[0-9a-f]{64}", source["git_revision"]))
            self.assertIs(type(source["git_dirty"]), bool)
            self.assertIsNone(source["git_unavailable_reason"])
        outcome = manifest["outcome"]
        self.assertEqual(outcome["snapshot"],
                         "completed_acquisition_before_persistence")
        self.assertEqual(outcome["acquisition"], "completed")
        self.assertEqual(outcome["requested_rows"], config.points)
        self.assertEqual(outcome["completed_rows"], config.points)
        self.assertEqual(outcome["final_tick"], config.points)
        self.assertEqual(outcome["final_sim_time"], config.points * config.dt)
        # not truthfully available today (recorded as unavailable):
        for key in ("persistence", "plotting", "cleanup", "overall"):
            self.assertEqual(outcome[key], "not_yet_established")
        # no environment lock or dependency lock fields exist at all
        self.assertFalse({"environment", "dependencies", "lock"}
                         & set(manifest))

    def test_stored_samples_match_independent_euler_oracle(self):
        for adapter, delay in (("coordinated", 1), ("direct", 0)):
            with self.subTest(adapter=adapter):
                result, manifest = self.run_adapter(adapter)
                table = result.group.record("thermal_trace").table
                n = self.CONFIG.points
                ticks = np.arange(1, n + 1)
                np.testing.assert_array_equal(
                    table["tick"].to_numpy(dtype=float), ticks)
                np.testing.assert_array_equal(
                    table["measurement_source_tick"].to_numpy(dtype=float),
                    ticks - delay)
                np.testing.assert_allclose(
                    table["sim_time_s"].to_numpy(dtype=float), ticks,
                    rtol=0, atol=1e-10)
                np.testing.assert_allclose(
                    table["commanded_power_w"].to_numpy(dtype=float),
                    np.full(n, self.CONFIG.power), rtol=0, atol=1e-10)
                np.testing.assert_allclose(
                    table["temperature_degc"].to_numpy(dtype=float),
                    self.euler(delay), rtol=0, atol=1e-10)


class SignalMonitorInventory(unittest.TestCase):
    """C5b: signal-monitor example characterized as written - in-memory
    only, non-consumptive reads, independent oracle [3, 3, 3]."""

    def test_deterministic_non_consumptive_reads_and_oracle(self):
        first = monitor.acquire(points=3)
        second = monitor.acquire(points=3)
        for result, record, differences, reads in (first, second):
            self.assertEqual(differences, [3, 3, 3])
            self.assertEqual(list(record.table["signal"]), [6, 7, 8])
            self.assertEqual(list(record.table["reference"]), [3, 4, 5])
            self.assertEqual(reads, [
                ("signal", 1), ("reference", 1),
                ("signal", 2), ("reference", 2),
                ("signal", 3), ("reference", 3)])
            self.assertTrue(result.loop_closed)
            self.assertTrue(result.work_drained)
        # non-consumptive determinism: a fresh run reproduces every
        # non-timestamp value (timestamps are wall-clock and differ)
        left, right = first[1].table, second[1].table
        columns = [name for name in left.columns if name != "timestamp"]
        self.assertEqual(left[columns].to_numpy(dtype=float).tolist(),
                         right[columns].to_numpy(dtype=float).tolist())

    def test_owned_persistence_wiring_uses_shared_seams(self):
        """Phase 4 (oracle freeze section 5.1) wires owned persistence:
        persist() gates on acquisition success and routes every storage
        step through the shared run_group/run_record/run_storage seams;
        the example still performs no direct backend I/O of its own."""
        source = MONITOR.read_text()
        for absent in ("import h5py", "Sqlite", "sqlite3"):
            self.assertNotIn(absent, source)
        for present in ("def acquire(", "def persist(",
                        "build_run_group", "build_envelope",
                        "build_signal_payload", "save_run_group",
                        "add_record"):
            self.assertIn(present, source)
        # the functions DEFINED by this module; other visible names are
        # imports (count, run_count, DataGroup, save_run_group, ...)
        defined = [name for name, value in vars(monitor).items()
                   if callable(value)
                   and getattr(value, "__module__", None) == monitor.__name__]
        self.assertEqual(sorted(defined),
                         ["SavedRun", "_capture_source_identity", "_git",
                          "_source_facts", "acquire", "persist"])

    def test_persist_gate_refuses_unsuccessful_acquisition(self):
        """A failed acquisition never reaches the storage seam: the gate
        raises and no artifact is created (freeze section 5.1 input 1)."""
        from softlab.huo.process import CountResult
        result, record, _, _ = monitor.acquire(points=3)
        bad = CountResult(3, result.stop, acquisition="failed")
        with tempfile.TemporaryDirectory(prefix="softlab-m03-p1-gate-") as root:
            target = Path(root) / "never.h5"
            with self.assertRaises(RuntimeError):
                monitor.persist(bad, record, target)
            self.assertFalse(target.exists())

    def test_field_origins_and_scheduler_ownership(self):
        _, record, differences, _ = monitor.acquire(points=3)
        # stored-record candidate fields come from DataRecord column defs
        self.assertEqual([column["name"] for column in record.columns],
                         ["timestamp", "signal", "reference"])
        # 'difference' is computed in memory by the caller only
        self.assertNotIn("difference", record.table.columns)
        self.assertEqual(differences, (record.table["signal"]
                                       - record.table["reference"]).tolist())
        # scheduler/loop ownership: run_count owns a fresh SerialScheduler;
        # the module-global scheduler singleton is untouched by a run
        before = impl_scheduler._scheduler
        monitor.acquire(points=2)
        self.assertIs(impl_scheduler._scheduler, before)


class Hygiene(unittest.TestCase):
    """C6: synthetic temp data only, temp dirs cleaned, no hardware,
    environment actually observed."""

    def test_temporary_directories_are_removed(self):
        temp = tempfile.TemporaryDirectory(prefix="softlab-m03-p1-c6-")
        root = Path(temp.name)
        result = thermal.execute(thermal.ThermalConfig(), root / "run")
        self.assertTrue(result.overall_success)
        # all produced artifacts live inside the temp root
        self.assertEqual(result.data_path.parent, root / "run")
        self.assertEqual(
            sorted(p.name for p in (root / "run").iterdir()),
            ["thermal.h5", "thermal.png"])
        temp.cleanup()
        self.assertFalse(root.exists())

    def test_environment_actually_observed(self):
        versions = {
            "python": sys.version,
            "softlab": softlab.__version__,
            "numpy": np.__version__,
            "pandas": __import__("pandas").__version__,
            "h5py": h5py.__version__,
            "libhdf5": h5py.version.hdf5_version,
            # observed: reading sqlite3.version emits a DeprecationWarning
            # on Python >= 3.13 (attribute deprecated there, removal 3.14);
            # on older Pythons (e.g. 3.9) the read is silent
            "sqlite3_module": None,
            "sqlite3_library": sqlite3.sqlite_version,
            "matplotlib": __import__("matplotlib").__version__,
        }
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            versions["sqlite3_module"] = sqlite3.version
        deprecations = [item for item in caught
                        if issubclass(item.category, DeprecationWarning)]
        # both observations asserted as facts, conditional on the
        # interpreter actually running (not skipped)
        if sys.version_info >= (3, 13):
            self.assertEqual(len(deprecations), 1)
            self.assertIn("version is deprecated",
                          str(deprecations[0].message))
        else:
            self.assertEqual(deprecations, [])
        for name, value in versions.items():
            self.assertTrue(value, name)


if __name__ == "__main__":
    unittest.main()
