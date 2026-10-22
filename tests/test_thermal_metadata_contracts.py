"""M01-W03 preimplementation persistence characterization, not manifest APIs."""

import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

import h5py
import numpy as np

from softlab.shui.data import DataGroup, HDF5DataBackend

SPEC = importlib.util.spec_from_file_location(
    "thermal_metadata_contract", Path(__file__).resolve().parents[1]
    / "examples/thermal_experiment.py")
thermal = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = thermal
SPEC.loader.exec_module(thermal)


class ThermalMetadataContracts(unittest.TestCase):
    """Exercise synthetic disk bytes and current legacy hydration boundaries."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def save(self, group, path):
        backend = HDF5DataBackend()
        try:
            self.assertTrue(backend.connect({"path": str(path)}))
            self.assertTrue(backend.save_group(group))
        finally:
            backend.disconnect()
        self.assertFalse(backend.connected)
        self.assertIsNone(backend.file)

    def test_explicit_json_text_preserves_scalar_and_container_types(self):
        value = {"null": None, "flag": False, "count": 8, "dt": 1.0,
                 "text": "thermal", "list": [1, 2.5], "object": {"unit": "s"}}
        encoded = json.dumps(value, sort_keys=True, allow_nan=False)
        group = DataGroup("encoded")
        group.set_meta("encoded_context", encoded)
        group.set_meta("raw_object", {"flag": False})
        path = self.root / "encoded.h5"
        self.save(group, path)
        with h5py.File(path, "r") as disk:
            text = disk[str(group.id)].attrs["encoded_context"]
            self.assertIs(type(text), str)
            self.assertEqual(text, encoded)
            restored = json.loads(text)
            self.assertEqual(restored, value)
            for key in value:
                self.assertIs(type(restored[key]), type(value[key]))
            self.assertEqual(disk[str(group.id)].attrs["raw_object"], str({"flag": False}))
            with self.assertRaises(json.JSONDecodeError):
                json.loads(disk[str(group.id)].attrs["raw_object"])

    def test_raw_loader_consumes_reserved_attrs_and_constructor_loses_metadata(self):
        group = DataGroup("raw")
        group.set_meta("encoded_context", '{"count": 8}')
        source, copy = self.root / "source.h5", self.root / "copy.h5"
        self.save(group, source)
        before = hashlib.sha256(source.read_bytes()).digest()
        shutil.copy2(source, copy)
        backend = HDF5DataBackend()
        try:
            self.assertTrue(backend.connect({"path": str(copy)}))
            loaded = backend.load_group(str(group.id))
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.meta, {})
            attrs = backend.file[str(group.id)].attrs
            self.assertEqual(attrs["encoded_context"], '{"count": 8}')
            for key in ("id", "name", "timestamp", "backend"):
                self.assertNotIn(key, attrs)
        finally:
            backend.disconnect()
        self.assertEqual(hashlib.sha256(source.read_bytes()).digest(), before)
        self.assertEqual(DataGroup("constructor", meta={"value": 1}).meta, {})

    def test_legacy_text_hydration_numeric_schema_and_source_integrity(self):
        coordinator, heater, probe = thermal._build_simulation(thermal.ThermalConfig())
        scheduler = thermal._make_scheduler()
        try:
            original = thermal._acquire(thermal.ThermalConfig(), coordinator,
                                        heater, probe, scheduler)
        finally:
            scheduler.stop()
            scheduler._loop.close()
        # Explicitly construct legacy context without selecting a future schema key.
        original.meta = {key: original.get_meta(key)
                         for key in ("example", "configuration", "timing")}
        source = self.root / "legacy.h5"
        self.save(original, source)
        before = hashlib.sha256(source.read_bytes()).digest()
        for _ in range(2):
            loaded = thermal._reload_group(source, str(original.id))
            self.assertEqual(loaded.id, original.id)
            self.assertEqual(loaded.meta, original.meta)
            self.assertEqual(set(loaded.meta), {"example", "configuration", "timing"})
            self.assertEqual(json.loads(loaded.get_meta("configuration"))["points"], 8)
            actual, expected = loaded.record("thermal_trace"), original.record("thermal_trace")
            self.assertEqual(actual.columns, expected.columns)
            self.assertEqual(actual.shape, expected.shape)
            np.testing.assert_allclose(actual.table.to_numpy(dtype=float),
                                       expected.table.to_numpy(dtype=float),
                                       rtol=0, atol=1e-10)
            self.assertEqual(Path(loaded.backend["arguments"]["path"]), source)
            self.assertEqual(hashlib.sha256(source.read_bytes()).digest(), before)


if __name__ == "__main__":
    unittest.main()
