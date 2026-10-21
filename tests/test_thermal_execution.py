"""M01-W02 example execution contracts using real synthetic acquisition."""

import asyncio
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import matplotlib.pyplot as plt
import numpy as np

from softlab.huo.scheduler import impl_scheduler
from softlab.tu.simulation import Coordinator


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "thermal_experiment.py"
SPEC = importlib.util.spec_from_file_location("thermal_w02_contract", EXAMPLE)
thermal = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = thermal
SPEC.loader.exec_module(thermal)
EXPECTED = [20, 21, 21.9, 22.71, 23.439, 24.0951, 24.68559, 25.217031]


class ThermalExecutionTests(unittest.TestCase):
    """Keep stop, row and actual model-boundary evidence independent."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = thermal.ThermalConfig()
        self.figures = set(plt.get_fignums())
        self.addCleanup(self.close_figures)
        self.schedulers = []
        original_make = thermal._make_scheduler

        def make():
            scheduler = original_make()
            self.schedulers.append(scheduler)
            return scheduler

        scheduler_patch = mock.patch.object(thermal, "_make_scheduler", side_effect=make)
        scheduler_patch.start()
        self.addCleanup(scheduler_patch.stop)

    def close_figures(self):
        for number in set(plt.get_fignums()) - self.figures:
            plt.close(number)

    def record(self, result):
        return result.group.record("thermal_trace")

    def assert_terminal(self, result, rows, tick, acquisition):
        self.assertIsInstance(result, thermal.RunResult)
        self.assertEqual(result.requested_rows, 8)
        self.assertEqual(result.completed_rows, rows)
        self.assertEqual(len(self.record(result)), rows)
        self.assertEqual(result.final_tick, tick)
        self.assertEqual(result.final_sim_time, float(tick))
        self.assertEqual(result.acquisition, acquisition)
        self.assertTrue(result.terminal)
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)
        self.assertEqual(result.cleanup_errors, [])
        for scheduler in self.schedulers:
            self.assertFalse(scheduler.is_running())
            self.assertTrue(scheduler._loop.is_closed())
        table = self.record(result).table
        if rows:
            np.testing.assert_allclose(table["temperature_degc"], EXPECTED[:rows],
                                       rtol=0, atol=1e-10)
            np.testing.assert_array_equal(table["tick"], range(1, rows + 1))
            self.assertTrue(np.isfinite(table.to_numpy(dtype=float)).all())

    def assert_partial(self, result, rows, tick, acquisition):
        self.assert_terminal(result, rows, tick, acquisition)
        self.assertFalse(result.overall_success)
        self.assertEqual(result.persistence, "not_attempted")
        self.assertEqual(result.plotting, "not_attempted")
        self.assertIsNone(result.loaded)
        self.assertFalse(result.data_path.exists())
        self.assertFalse(result.plot_path.exists())

    def execute_fault(self, stage, name, stop=None):
        coordinator, heater, probe = thermal._build_simulation(self.config)
        model = coordinator.station.object("body")
        original_evolve, original_read = model._evolve, probe.read
        cause = ValueError("injected " + stage)

        def evolve(inputs, states):
            if stage == "model" and coordinator.tick == 3:
                if stop is not None:
                    stop.request()
                raise cause
            return original_evolve(inputs, states)

        def read():
            if stage == "sample" and coordinator.tick == 4:
                if stop is not None:
                    stop.request()
                raise cause
            return original_read()

        with mock.patch.object(thermal, "_build_simulation",
                               return_value=(coordinator, heater, probe)), \
                mock.patch.object(model, "_evolve", side_effect=evolve), \
                mock.patch.object(probe, "read", side_effect=read):
            result = thermal.execute(self.config, self.root / name, stop=stop)
        return result, coordinator, cause

    def test_stop_request_has_separate_idempotent_request_and_ack(self):
        stop = thermal.StopRequest()
        self.assertFalse(stop.requested)
        self.assertFalse(stop.acknowledged)
        stop.request()
        stop.request()
        self.assertTrue(stop.requested)
        self.assertFalse(stop.acknowledged)

    def test_detailed_success_preserves_reopened_default_result(self):
        result = thermal.execute(self.config, self.root / "success")
        self.assert_terminal(result, 8, 8, "completed")
        self.assertTrue(result.overall_success)
        self.assertEqual(result.persistence, "verified")
        self.assertEqual(result.plotting, "completed")
        self.assertIsNone(result.cause)
        self.assertEqual(result.group.id, result.loaded.id)
        reopened = thermal._reload_group(result.data_path, str(result.group.id))
        self.assertEqual(len(reopened.record("thermal_trace")), 8)
        self.assertTrue(result.plot_path.is_file())
        self.assertGreater(float(np.ptp(plt.imread(result.plot_path))), 0)

    def test_stop_before_first_cycle_retains_empty_group(self):
        stop = thermal.StopRequest()
        stop.request()
        with mock.patch.object(Coordinator, "step", autospec=True) as step:
            result = thermal.execute(self.config, self.root / "early", stop=stop)
        self.assert_partial(result, 0, 0, "cancelled")
        self.assertTrue(stop.acknowledged)
        step.assert_not_called()

    def test_mid_stop_acknowledgment_prevents_every_later_step(self):
        stop = thermal.StopRequest()
        original_step = Coordinator.step
        boundaries, callbacks = [], []

        def step(coordinator):
            self.assertFalse(stop.acknowledged)
            original_step(coordinator)
            boundaries.append(coordinator.tick)

        def after_row(result):
            callbacks.append(result.completed_rows)
            self.assertFalse(result.terminal)
            if result.completed_rows == 3:
                stop.request()
                self.assertFalse(stop.acknowledged)

        with mock.patch.object(Coordinator, "step", autospec=True, side_effect=step), \
                mock.patch.object(Coordinator, "reset", autospec=True) as reset:
            result = thermal.execute(self.config, self.root / "mid", stop=stop,
                                     after_row=after_row)
        self.assert_partial(result, 3, 3, "cancelled")
        self.assertEqual(boundaries, [1, 2, 3])
        self.assertEqual(callbacks, [1, 2, 3])
        self.assertTrue(stop.acknowledged)
        reset.assert_not_called()

    def test_inflight_requests_finish_current_cycle_then_acknowledge(self):
        for stage in ("model", "sample"):
            with self.subTest(stage=stage):
                stop = thermal.StopRequest()
                coordinator, heater, probe = thermal._build_simulation(self.config)
                body = coordinator.station.object("body")
                original_evolve, original_read = body._evolve, probe.read
                observed = []

                def request():
                    stop.request()
                    observed.append(stop.acknowledged)

                def evolve(inputs, states):
                    if stage == "model" and coordinator.tick == 2:
                        request()
                    return original_evolve(inputs, states)

                def read():
                    if stage == "sample" and coordinator.tick == 3:
                        request()
                    return original_read()

                with mock.patch.object(thermal, "_build_simulation",
                        return_value=(coordinator, heater, probe)), \
                        mock.patch.object(body, "_evolve", side_effect=evolve), \
                        mock.patch.object(probe, "read", side_effect=read):
                    result = thermal.execute(self.config, self.root / stage,
                                             stop=stop)
                self.assert_partial(result, 3, 3, "cancelled")
                self.assertEqual(observed, [False])
                self.assertTrue(stop.acknowledged)
                self.assertEqual(coordinator.tick, 3)

    def test_request_at_final_row_is_late_and_does_not_cancel(self):
        stop = thermal.StopRequest()

        def after_row(result):
            if result.completed_rows == 8:
                stop.request()

        result = thermal.execute(self.config, self.root / "late", stop=stop,
                                 after_row=after_row)
        self.assert_terminal(result, 8, 8, "completed")
        self.assertTrue(result.overall_success)
        self.assertTrue(stop.requested)
        self.assertFalse(stop.acknowledged)

    def test_model_and_sample_failure_retain_distinct_committed_boundaries(self):
        for stage, tick in (("model", 3), ("sample", 4)):
            with self.subTest(stage=stage):
                result, coordinator, cause = self.execute_fault(stage, stage)
                self.assert_partial(result, 3, tick, "failed")
                self.assertIs(result.cause, cause)
                self.assertEqual(coordinator.tick, tick)

    def test_failure_is_not_erased_by_request_during_failed_cycle(self):
        for stage, tick in (("model", 3), ("sample", 4)):
            with self.subTest(stage=stage):
                stop = thermal.StopRequest()
                result, _, cause = self.execute_fault(stage, stage, stop)
                self.assert_partial(result, 3, tick, "failed")
                self.assertIs(result.cause, cause)
                self.assertTrue(stop.requested)
                self.assertFalse(stop.acknowledged)

    def test_callback_failure_preserves_just_completed_row(self):
        cause = ValueError("after row failure")

        def after_row(result):
            if result.completed_rows == 3:
                raise cause

        result = thermal.execute(self.config, self.root / "callback",
                                 after_row=after_row)
        self.assert_partial(result, 3, 3, "failed")
        self.assertIs(result.cause, cause)

    def test_save_and_reload_failures_do_not_reclassify_acquisition(self):
        for stage in ("save", "reload"):
            with self.subTest(stage=stage):
                target = (mock.patch.object(thermal.HDF5DataBackend, "save_group",
                                           return_value=False) if stage == "save"
                          else mock.patch.object(thermal, "_reload_group",
                                                side_effect=ValueError("reload")))
                with target, mock.patch.object(thermal, "_plot_record") as plot:
                    result = thermal.execute(self.config, self.root / stage)
                self.assert_terminal(result, 8, 8, "completed")
                self.assertEqual(result.persistence, "failed")
                self.assertEqual(result.plotting, "not_attempted")
                self.assertIsNone(result.loaded)
                self.assertIsNotNone(result.cause)
                self.assertFalse(result.overall_success)
                plot.assert_not_called()

    def test_plot_failure_retains_verified_storage(self):
        cause = ValueError("plot failed")
        with mock.patch.object(thermal, "_plot_record", side_effect=cause):
            result = thermal.execute(self.config, self.root / "plot")
        self.assert_terminal(result, 8, 8, "completed")
        self.assertEqual(result.persistence, "verified")
        self.assertEqual(result.plotting, "failed")
        self.assertIsNotNone(result.loaded)
        self.assertIs(result.cause, cause)
        self.assertFalse(result.overall_success)
        self.assertTrue(result.data_path.is_file())

    def test_explicit_reset_preserves_terminal_snapshot_and_starts_no_work(self):
        captured = []
        original_build = thermal._build_simulation

        def build(config):
            built = original_build(config)
            captured.append(built)
            return built

        stop = thermal.StopRequest()
        with mock.patch.object(thermal, "_build_simulation", side_effect=build):
            result = thermal.execute(self.config, self.root / "reset", stop=stop,
                after_row=lambda r: stop.request() if r.completed_rows == 3 else None)
        coordinator, _, _ = captured[0]
        self.assert_partial(result, 3, 3, "cancelled")
        thermal.reset_after_run(result)
        self.assertEqual(coordinator.tick, 0)
        self.assertEqual(coordinator.sim_time, 0)
        self.assertEqual(coordinator.station.object("body").get_state("temperature"),
                         self.config.initial_temperature)
        self.assert_partial(result, 3, 3, "cancelled")

    def test_reset_rejects_live_and_undrained_or_unclosed_work(self):
        checked = []

        def after_row(result):
            with self.assertRaises(RuntimeError):
                thermal.reset_after_run(result)
            checked.append(result.completed_rows)

        result = thermal.execute(self.config, self.root / "guard", after_row=after_row)
        self.assertEqual(checked, list(range(1, 9)))
        for field in ("work_drained", "loop_closed"):
            with self.subTest(field=field), mock.patch.object(result, field, False):
                with self.assertRaises(RuntimeError):
                    thermal.reset_after_run(result)

    def test_reset_factory_failure_preserves_model_and_terminal_evidence(self):
        captured = []
        original_build = thermal._build_simulation

        def build(config):
            built = original_build(config)
            captured.append(built)
            return built

        with mock.patch.object(thermal, "_build_simulation", side_effect=build):
            result = thermal.execute(self.config, self.root / "resetfail")
        coordinator, _, _ = captured[0]
        body = coordinator.station.object("body")
        before = (coordinator.tick, coordinator.sim_time, body.get_state("temperature"))
        def fail_factory():
            raise ValueError("reset failed")

        with mock.patch.dict(body._state_sources, {"temperature": fail_factory}):
            with self.assertRaisesRegex(Exception, "reset failed"):
                thermal.reset_after_run(result)
        self.assertEqual((coordinator.tick, coordinator.sim_time,
                          body.get_state("temperature")), before)
        self.assert_terminal(result, 8, 8, "completed")
        repeats = [thermal.execute(self.config, self.root / ("after-reset-failure" + str(k)))
                   for k in (1, 2)]
        for repeated in repeats:
            self.assert_terminal(repeated, 8, 8, "completed")
            self.assertTrue(repeated.overall_success)
        self.assertNotEqual(repeats[0].group.id, repeats[1].group.id)

    def test_two_fresh_repeats_after_each_interruption_or_failure(self):
        for stage in ("cancel", "model", "sample"):
            with self.subTest(stage=stage):
                if stage == "cancel":
                    stop = thermal.StopRequest()
                    interrupted = thermal.execute(self.config, self.root / stage,
                        stop=stop, after_row=lambda r: stop.request()
                        if r.completed_rows == 3 else None)
                else:
                    interrupted, _, _ = self.execute_fault(stage, stage)
                thermal.reset_after_run(interrupted)
                runs = [thermal.execute(self.config, self.root / (stage + str(k)))
                        for k in (1, 2)]
                self.assertEqual(len({str(r.group.id) for r in runs + [interrupted]}), 3)
                for result in runs:
                    self.assert_terminal(result, 8, 8, "completed")
                    self.assertTrue(result.overall_success)
                    reopened = thermal._reload_group(result.data_path, str(result.group.id))
                    self.assertEqual(len(reopened.record("thermal_trace")), 8)
                columns = [c for c in self.record(runs[0]).table if c != "timestamp"]
                np.testing.assert_allclose(self.record(runs[0]).table[columns],
                    self.record(runs[1]).table[columns], rtol=0, atol=1e-10)

    def test_stop_cleanup_obstruction_preserves_cause_and_closes_owned_loop(self):
        scheduler = thermal._make_scheduler()
        cause = ValueError("primary acquisition")
        try:
            with mock.patch.object(thermal, "_make_scheduler", return_value=scheduler), \
                    mock.patch.object(thermal, "_acquire", side_effect=cause), \
                    mock.patch.object(scheduler, "stop", side_effect=RuntimeError("stop blocked")):
                result = thermal.execute(self.config, self.root / "cleanup")
            self.assertFalse(result.overall_success)
            self.assertIs(result.cause, cause)
            self.assertTrue(any("stop blocked" in e for e in result.cleanup_errors))
            self.assertTrue(result.work_drained)
            self.assertTrue(result.loop_closed)
            self.assertTrue(scheduler._loop.is_closed())
        finally:
            scheduler.stop()
            if not scheduler._loop.is_closed():
                scheduler._loop.close()

    def test_borrowed_scheduler_loop_and_figure_unchanged(self):
        borrowed_scheduler = impl_scheduler._scheduler
        try:
            original_loop = asyncio.get_event_loop()
        except RuntimeError:
            original_loop = None
        borrowed_loop = asyncio.new_event_loop()
        borrowed_figure = plt.figure()
        try:
            asyncio.set_event_loop(borrowed_loop)
            result = thermal.execute(self.config, self.root / "borrowed")
            self.assertTrue(result.overall_success)
            self.assertIs(impl_scheduler._scheduler, borrowed_scheduler)
            self.assertIs(asyncio.get_event_loop(), borrowed_loop)
            self.assertFalse(borrowed_loop.is_closed())
            self.assertTrue(plt.fignum_exists(borrowed_figure.number))
        finally:
            asyncio.set_event_loop(original_loop)
            borrowed_loop.close()

    def test_compatibility_failure_wrapper_exposes_result_and_cause(self):
        cause = ValueError("wrapper model failure")
        with mock.patch.object(thermal, "_acquire", side_effect=cause):
            with self.assertRaises(thermal.RunFailure) as caught:
                thermal.run(self.config, self.root / "wrapper")
        self.assertIsInstance(caught.exception, RuntimeError)
        self.assertIs(caught.exception.result.cause, cause)
        self.assertIs(caught.exception.__cause__, cause)
        self.assertFalse(caught.exception.result.overall_success)

    def test_cli_stop_and_late_stop_have_truthful_exit_and_artifacts(self):
        for points, status in ((0, 1), (3, 1), (8, 0)):
            with self.subTest(points=points):
                target = self.root / ("cli" + str(points))
                completed = subprocess.run([sys.executable, str(EXAMPLE),
                    "--output-dir", str(target), "--stop-after", str(points)],
                    cwd=ROOT, text=True, capture_output=True, timeout=60)
                self.assertEqual(completed.returncode, status, completed.stderr)
                combined = completed.stdout + completed.stderr
                if status:
                    self.assertIn("cancelled", combined.lower())
                    self.assertNotIn("Complete", completed.stdout)
                    self.assertFalse((target / "thermal.h5").exists())
                    self.assertFalse((target / "thermal.png").exists())
                else:
                    self.assertIn("Complete", completed.stdout)
                    self.assertTrue((target / "thermal.h5").exists())
                    self.assertTrue((target / "thermal.png").exists())


if __name__ == "__main__":
    unittest.main()
