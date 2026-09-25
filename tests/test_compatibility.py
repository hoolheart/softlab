"""Integration checks for the supported Python and scientific stack."""

import tempfile
import unittest
from importlib.util import find_spec
from pathlib import Path

import numpy as np
from numpy.testing import assert_array_equal

from softlab.huo.process import SimpleProcessWrapper, count, run_process, scan
from softlab.huo.scheduler import get_scheduler
from softlab.jin.sp import Wavement
from softlab.jin.validator import ValInt, ValNumber
from softlab.shui.data import (
    DataGroup,
    DataRecord,
    HDF5DataBackend,
    Sqlite3DataBackend,
)
from softlab.tu.station import Parameter, VisaHandle, VisaParameter


class CompatibilityTests(unittest.TestCase):
    def test_quantize_with_default_and_explicit_limits(self):
        wavement = Wavement(
            np.array([0.0, 1.0, 2.0]),
            np.array([-1.2, 0.2, 2.2]),
        )

        assert_array_equal(
            wavement.quantize(1.0).values, np.array([-2.0, 0.0, 2.0])
        )
        assert_array_equal(
            wavement.quantize(1.0, min=0.0, max=2.0).values,
            np.array([0.0, 0.0, 2.0]),
        )

    def test_wrapped_sync_and_async_processes(self):
        calls = []

        async def async_task():
            calls.append("async")

        scheduler = get_scheduler()
        scheduler.start()
        try:
            self.assertTrue(
                run_process(
                    SimpleProcessWrapper(lambda: calls.append("sync")),
                    scheduler,
                    verbose=False,
                )[0]
            )
            self.assertTrue(
                run_process(
                    SimpleProcessWrapper(async_task),
                    scheduler,
                    verbose=False,
                )[0]
            )
            self.assertEqual(calls, ["sync", "async"])
        finally:
            scheduler.stop()

    def test_count_and_scan_record_values(self):
        value = Parameter("value", ValNumber(), init_value=1.0)
        reading = Parameter(
            "reading", ValNumber(), settable=False,
            before_get=lambda _: value() * 2,
        )
        group = DataGroup("workflow")
        scheduler = get_scheduler()
        scheduler.start()
        try:
            self.assertTrue(run_process(
                count("measure", group, None, reading, times=2),
                scheduler, verbose=False,
            )[0])
            self.assertTrue(run_process(
                scan("sweep", [reading], group, None, value, [2.0, 3.0]),
                scheduler, verbose=False,
            )[0])
        finally:
            scheduler.stop()

        assert_array_equal(
            group.record("measure").table["reading"].to_numpy(),
            np.array([2.0, 2.0]),
        )
        assert_array_equal(
            group.record("sweep").table["reading"].to_numpy(),
            np.array([4.0, 6.0]),
        )

    def test_data_round_trip(self):
        group = DataGroup("experiment")
        group.add_record(
            DataRecord(
                "readings",
                columns=[
                    {"name": "x", "dependent": False},
                    {"name": "y", "dependent": True},
                ],
                data=np.array([[1.0, 2.0], [3.0, 4.0]]),
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            for backend_type, extension in (
                (Sqlite3DataBackend, "db"),
                (HDF5DataBackend, "hdf5"),
            ):
                with self.subTest(backend=backend_type.__name__):
                    backend = backend_type()
                    path = Path(directory) / f"readings.{extension}"
                    self.assertTrue(backend.connect({"path": str(path)}))
                    try:
                        self.assertTrue(backend.save_group(group))
                        loaded = backend.load_group(str(group.id))
                        self.assertIsNotNone(loaded, backend.last_error)
                        self.assertEqual(loaded.id, group.id)
                        self.assertEqual(loaded.timestamp, group.timestamp)
                        assert_array_equal(
                            loaded.record("readings").table.to_numpy(),
                            group.record("readings").table.to_numpy(),
                        )
                    finally:
                        backend.disconnect()

    @unittest.skipUnless(find_spec("pyvisa_sim"), "pyvisa-sim is not installed")
    def test_visa_simulator(self):
        simulator = str(Path(__file__).parent / "visa_sim.yaml") + "@sim"
        handle = VisaHandle(
            "GPIB::1::INSTR", visalib=simulator,
            read_termination="\r", write_termination="\r",
            device_clear=False,
        )
        try:
            self.assertIn("Simulated", handle.query("*IDN?"))
            channels = VisaParameter(
                "channels", handle, get_cmd="RFCONFIG? CHAN",
                encoder=int, validator=ValInt(0), settable=False,
            )
            self.assertEqual(channels(), 9)
        finally:
            handle.close()


if __name__ == "__main__":
    unittest.main()
