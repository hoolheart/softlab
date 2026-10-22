"""M01-W03 approved closed manifest and actual fresh-process inspection contracts."""

import copy
import dataclasses
import hashlib
import importlib.util
import json
import os
import platform
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import h5py
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples/thermal_experiment.py"
SPEC = importlib.util.spec_from_file_location("thermal_manifest_contract", EXAMPLE)
thermal = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = thermal
SPEC.loader.exec_module(thermal)


class ThermalManifestTests(unittest.TestCase):
    """Use real records/files; mutate only disposable synthetic copies."""

    @classmethod
    def setUpClass(cls):
        cls.shared = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.shared.cleanup)
        cls.default = thermal.execute(thermal.ThermalConfig(), Path(cls.shared.name) / "default")
        if not cls.default.overall_success:
            raise AssertionError(str(cls.default.cause))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def manifest(self):
        return thermal._decode_manifest(self.default.group.get_meta("thermal_manifest"))

    def copied_file(self, name="copy"):
        path = self.root / (name + ".h5")
        shutil.copy2(self.default.data_path, path)
        return path

    def inspect_cli(self, path):
        return subprocess.run([sys.executable, "-W", "error", str(EXAMPLE),
                               "--inspect-file", str(path)], cwd=ROOT,
                              text=True, capture_output=True, timeout=60)

    def test_complete_closed_schema_and_typed_fresh_process(self):
        manifest = self.manifest()
        self.assertEqual(set(manifest), {"schema", "version", "run_id", "configuration",
            "model", "procedure", "columns", "clock", "software", "source", "outcome"})
        self.assertEqual(manifest["schema"], "softlab.thermal-reference")
        self.assertIs(type(manifest["version"]), int)
        self.assertEqual(manifest["version"], 1)
        self.assertEqual(manifest["run_id"], str(self.default.group.id))
        self.assertEqual(manifest["configuration"], dataclasses.asdict(thermal.ThermalConfig()))
        for key, value in manifest["configuration"].items():
            self.assertIs(type(value), int if key == "points" else float)
        self.assertEqual(manifest["model"]["name"], "lumped_thermal_body")
        self.assertEqual(manifest["model"]["method"], "explicit_euler")
        self.assertEqual(manifest["model"]["units"]["heat_capacity"], "J/K")
        self.assertEqual(set(manifest["model"]), {"name", "method", "equation", "validity", "units"})
        self.assertEqual(manifest["model"]["equation"],
            "T_next = T + dt * (P - G * (T - T_ambient)) / C")
        self.assertEqual(manifest["model"]["validity"],
            "educational; constant parameters; 0 < dt*G/C <= 1; no physical calibration")
        self.assertEqual(manifest["model"]["units"], {
            "initial_temperature": "degC", "ambient_temperature": "degC",
            "heat_capacity": "J/K", "conductance": "W/K", "dt": "s", "power": "W", "points": "1"})
        self.assertEqual(manifest["procedure"], {"name": "thermal_count",
            "advancement": "once_before_each_getter_row", "command": "constant_power", "requested_points": 8})
        self.assertEqual(manifest["clock"], {"dt": 1.0, "simulation_unit": "s",
            "acquisition": "post_step", "measurement_delay_ticks": 1,
            "source_tick": "tick-1", "wall_timestamp": "unix_seconds"})
        import softlab
        self.assertEqual(manifest["software"], {"softlab_version": softlab.__version__,
                                               "python_version": platform.python_version()})
        self.assertEqual(set(manifest["source"]), {"reference", "digest_algorithm", "digest_scope",
            "digest", "digest_unavailable_reason", "git_revision", "git_dirty", "git_unavailable_reason"})
        self.assertEqual(manifest["source"]["reference"], "examples/thermal_experiment.py")
        self.assertEqual(manifest["source"]["digest_algorithm"], "sha256")
        self.assertEqual(manifest["source"]["digest"], hashlib.sha256(EXAMPLE.read_bytes()).hexdigest())
        self.assertEqual(manifest["outcome"], {"snapshot": "completed_acquisition_before_persistence",
            "acquisition": "completed", "requested_rows": 8, "completed_rows": 8,
            "final_tick": 8, "final_sim_time": 8.0, "persistence": "not_yet_established",
            "plotting": "not_yet_established", "cleanup": "not_yet_established", "overall": "not_yet_established"})
        self.assertEqual(manifest["procedure"]["requested_points"], 8)
        self.assertEqual(manifest["clock"]["measurement_delay_ticks"], 1)
        self.assertIs(type(manifest["clock"]["dt"]), float)
        self.assertEqual(manifest["columns"], self.default.group.record("thermal_trace").columns)
        result = self.inspect_cli(self.default.data_path)
        self.assertEqual(result.returncode, 0, result.stderr)
        decoded = json.loads(result.stdout)  # no backend diagnostics on stdout
        self.assertEqual(decoded["context"], "available")
        self.assertEqual(decoded["manifest"], manifest)
        self.assertEqual(decoded["run_id"], manifest["run_id"])
        self.assertEqual(len(decoded["rows"]), 8)
        self.assertEqual(decoded["columns"], [c["name"] for c in manifest["columns"]])
        thermal._validate_manifest_data(decoded["manifest"], self.default.group)

    def test_strict_types_unknown_missing_and_nonfinite_values(self):
        manifest = self.manifest()
        invalid = []
        for value in (True, 8.0, "8", None):
            item = copy.deepcopy(manifest);item["configuration"]["points"] = value;invalid.append(item)
        for value in (True, float("nan"), float("inf"), -float("inf"), object(), (1, 2)):
            item = copy.deepcopy(manifest);item["configuration"]["dt"] = value;invalid.append(item)
        for change in ("extra", "missing", "nonstring", "nested"):
            item = copy.deepcopy(manifest)
            if change == "extra": item["extra"] = "unexpected"
            elif change == "missing": del item["outcome"]
            elif change == "nonstring": item[1] = "bad key"
            else: item["clock"]["extra"] = "unexpected"
            invalid.append(item)
        for index, item in enumerate(invalid):
            with self.subTest(index=index), self.assertRaises((TypeError, ValueError)):
                thermal._encode_manifest(item)
        encoded = thermal._encode_manifest(manifest)
        self.assertEqual(json.loads(encoded), manifest)
        self.assertEqual(encoded, thermal._encode_manifest(manifest))
        integer_real = copy.deepcopy(manifest)
        integer_real["configuration"]["dt"] = 1
        normalized = thermal._decode_manifest(thermal._encode_manifest(integer_real))
        self.assertIs(type(normalized["configuration"]["dt"]), float)

    def test_decode_duplicate_malformed_nontext_and_unsupported_versions(self):
        manifest = self.manifest()
        text = json.dumps(manifest, sort_keys=True)
        bad = [None, {}, "{", text[:-1] + ',"version":1}',
               text.replace('"version": 1', '"version": NaN'),
               text.replace('"version": 1', '"version": Infinity')]
        for value in bad:
            with self.subTest(value=str(value)[:40]), self.assertRaises((TypeError, ValueError)):
                thermal._decode_manifest(value)
        for key, value in (("schema", "unknown.schema"), ("version", 999)):
            item = copy.deepcopy(manifest);item[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, key):
                thermal._decode_manifest(json.dumps(item))

    def test_absent_legacy_is_distinct_from_corrupt_present_metadata(self):
        for state in ("absent", "malformed", "nontext", "version"):
            with self.subTest(state=state):
                path = self.copied_file(state)
                with h5py.File(path, "a") as disk:
                    attrs = disk[str(self.default.group.id)].attrs
                    if "thermal_manifest" in attrs: del attrs["thermal_manifest"]
                    if state == "malformed": attrs["thermal_manifest"] = "{"
                    elif state == "nontext": attrs["thermal_manifest"] = 42
                    elif state == "version":
                        item = self.manifest();item["version"] = 999
                        attrs["thermal_manifest"] = json.dumps(item)
                result = self.inspect_cli(path)
                if state == "absent":
                    self.assertEqual(result.returncode, 0, result.stderr)
                    decoded = json.loads(result.stdout)
                    self.assertEqual(decoded["context"], "legacy_unavailable")
                    self.assertIsNone(decoded["manifest"])
                    self.assertEqual(len(decoded["rows"]), 8)
                else:
                    self.assertEqual(result.returncode, 1, result.stderr)
                    self.assertTrue(result.stderr.strip())
                    self.assertNotIn('"context": "available"', result.stdout)

    def test_manifest_identity_counts_units_and_timing_tampering_rejected(self):
        for field in ("run_id", "count", "tick", "units", "clock", "configuration"):
            with self.subTest(field=field):
                item = self.manifest()
                if field == "run_id": item["run_id"] = "00000000-0000-0000-0000-000000000001"
                elif field == "count": item["outcome"]["completed_rows"] = 7
                elif field == "tick": item["outcome"]["final_tick"] = 9
                elif field == "units": item["columns"][0]["unit"] = "kW"
                elif field == "clock": item["clock"]["dt"] = 2.0
                else: item["configuration"]["power"] = 11.0
                path = self.copied_file(field)
                with h5py.File(path, "a") as disk:
                    disk[str(self.default.group.id)].attrs["thermal_manifest"] = json.dumps(item)
                with self.assertRaises((RuntimeError, ValueError)):
                    thermal.inspect_stored(path)

    def test_actual_numeric_samples_cannot_be_repaired_from_manifest(self):
        self.manifest()  # require a present valid producer envelope
        for column in ("temperature_degc", "tick", "measurement_source_tick", "sim_time_s", "commanded_power_w"):
            with self.subTest(column=column):
                path = self.copied_file(column)
                with h5py.File(path, "a") as disk:
                    values = disk[str(self.default.group.id)]["thermal_trace"]["columns"][column]
                    values[2, 0] += 0.25
                with self.assertRaises((RuntimeError, ValueError)):
                    thermal.inspect_stored(path)

    def test_changed_configuration_and_repeats_match_independent_euler_oracle(self):
        config = dataclasses.replace(thermal.ThermalConfig(), dt=0.5, power=6.0, points=5)
        result = thermal.execute(config, self.root / "changed")
        self.assertTrue(result.overall_success)
        group, manifest = thermal.inspect_stored(result.data_path)
        self.assertEqual(manifest["configuration"], dataclasses.asdict(config))
        expected, value = [], config.initial_temperature
        for _ in range(config.points):
            expected.append(value)
            value += config.dt * (config.power - config.conductance *
                    (value - config.ambient_temperature)) / config.heat_capacity
        np.testing.assert_allclose(group.record("thermal_trace").table["temperature_degc"],
                                   expected, rtol=0, atol=1e-10)
        repeat = thermal.execute(thermal.ThermalConfig(), self.root / "repeat")
        _, other = thermal.inspect_stored(repeat.data_path)
        self.assertNotEqual(other["run_id"], self.manifest()["run_id"])
        self.assertEqual(other["configuration"], self.manifest()["configuration"])
        columns = [c for c in repeat.group.record("thermal_trace").table if c != "timestamp"]
        np.testing.assert_allclose(repeat.group.record("thermal_trace").table[columns],
            self.default.group.record("thermal_trace").table[columns], rtol=0, atol=1e-10)

    def test_source_identity_scopes_real_git_to_source_and_handles_unavailability(self):
        source_root = self.root / "producer";source_root.mkdir()
        source = source_root / "thermal_experiment.py";source.write_bytes(b"# synthetic source\n")
        for args in (["init", "-q"], ["add", source.name],
                     ["-c", "user.name=Tester", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"]):
            subprocess.run(["git", *args], cwd=source_root, check=True, capture_output=True)
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source_root, text=True).strip()
        previous = Path.cwd()
        try:
            os.chdir(self.root)  # unrelated caller cwd
            with mock.patch.object(thermal, "__file__", str(source)):
                clean = thermal._capture_source_identity()
                self.assertEqual(clean["git_revision"], revision)
                self.assertIs(clean["git_dirty"], False)
                self.assertEqual(clean["digest"], hashlib.sha256(source.read_bytes()).hexdigest())
                self.assertEqual(clean["digest_scope"], "exact reference script bytes at production")
                source.write_bytes(b"# synthetic dirty source\n")
                dirty = thermal._capture_source_identity()
                self.assertEqual(dirty["git_revision"], revision)
                self.assertIs(dirty["git_dirty"], True)
                self.assertNotEqual(dirty["digest"], clean["digest"])
                with mock.patch.object(subprocess, "run", side_effect=FileNotFoundError("git unavailable")):
                    unavailable = thermal._capture_source_identity()
                    installed = thermal.execute(thermal.ThermalConfig(), self.root / "no-git")
                    self.assertTrue(installed.overall_success)
                self.assertIsNone(unavailable["git_revision"])
                self.assertIsNone(unavailable["git_dirty"])
                self.assertTrue(unavailable["git_unavailable_reason"])
                with mock.patch.object(thermal, "__file__", str(self.root / "absent.py")):
                    absent = thermal._capture_source_identity()
                self.assertIsNone(absent["digest"])
                self.assertTrue(absent["digest_unavailable_reason"])
        finally:
            os.chdir(previous)

    def test_inspection_preserves_producer_identity_without_recapture(self):
        captured = self.manifest()["source"]
        before = hashlib.sha256(self.default.data_path.read_bytes()).digest()
        with mock.patch.object(thermal, "_capture_source_identity", side_effect=AssertionError("reader recaptured")):
            for _ in range(2):
                group, manifest = thermal.inspect_stored(self.default.data_path)
                self.assertEqual(manifest["source"], captured)
                self.assertEqual(group.id, self.default.group.id)
        self.assertEqual(hashlib.sha256(self.default.data_path.read_bytes()).digest(), before)

    def test_snapshot_does_not_forecast_later_plot_or_cleanup_success(self):
        with mock.patch.object(thermal, "_plot_record", side_effect=ValueError("later plot")):
            result = thermal.execute(thermal.ThermalConfig(), self.root / "plot")
        self.assertEqual(result.acquisition, "completed")
        self.assertEqual(result.persistence, "verified")
        self.assertFalse(result.overall_success)
        _, manifest = thermal.inspect_stored(result.data_path)
        outcome = manifest["outcome"]
        self.assertEqual(outcome["snapshot"], "completed_acquisition_before_persistence")
        self.assertEqual(outcome["acquisition"], "completed")
        for key in ("persistence", "plotting", "cleanup", "overall"):
            self.assertEqual(outcome[key], "not_yet_established")

    def test_failed_encoding_precedes_writer_and_partials_skip_manifest(self):
        cause = ValueError("manifest encoding marker")
        with mock.patch.object(thermal, "_encode_manifest", side_effect=cause), \
                mock.patch.object(thermal, "HDF5DataBackend") as backend:
            result = thermal.execute(thermal.ThermalConfig(), self.root / "encoding")
        self.assertEqual(result.acquisition, "completed")
        self.assertEqual(result.persistence, "failed")
        self.assertEqual(result.plotting, "not_attempted")
        self.assertIs(result.cause, cause)
        self.assertTrue(result.work_drained and result.loop_closed)
        backend.assert_not_called()
        stop = thermal.StopRequest();stop.request()
        with mock.patch.object(thermal, "_make_manifest") as make:
            partial = thermal.execute(thermal.ThermalConfig(), self.root / "partial", stop=stop)
        self.assertEqual(partial.acquisition, "cancelled")
        self.assertEqual(partial.persistence, "not_attempted")
        self.assertIsNone(partial.group.get_meta("thermal_manifest"))
        make.assert_not_called()

    def test_removed_manifest_cannot_pass_fresh_delivery_verification(self):
        original_reload = thermal._reload_group

        def removed(path, identity):
            group = original_reload(path, identity)
            group.pop_meta("thermal_manifest")
            return group

        with mock.patch.object(thermal, "_reload_group", side_effect=removed):
            result = thermal.execute(thermal.ThermalConfig(), self.root / "missing")
        self.assertFalse(result.overall_success)
        self.assertEqual(result.persistence, "failed")
        self.assertEqual(result.plotting, "not_attempted")


if __name__ == "__main__":
    unittest.main()
