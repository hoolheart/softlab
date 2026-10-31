"""Legacy execution characterization for M01-W02; no new stop API promised."""

from historical_count_scheduler import _CountScheduler, _make_scheduler

import asyncio
import importlib.util
from pathlib import Path
import sys
import unittest

from softlab.huo.process import SimpleProcessWrapper, count, run_process
from softlab.huo.scheduler import Action
from softlab.huo.scheduler import impl_scheduler
from softlab.shui.data import DataGroup
from softlab.tu.station import Parameter


SPEC = importlib.util.spec_from_file_location(
    "thermal_execution_contract_fixture",
    Path(__file__).resolve().parents[1] / "examples/thermal_experiment.py",
)
thermal = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = thermal
SPEC.loader.exec_module(thermal)


class ProcessExecutionContracts(unittest.TestCase):
    """Exercise existing processes with the bounded W01 executor."""

    def setUp(self):
        self.scheduler = _make_scheduler()
        self.scheduler.start()
        self.addCleanup(self.scheduler._loop.close)
        self.addCleanup(self.scheduler.stop)

    def test_run_process_resets_completed_wrapper_before_each_run(self):
        calls = []
        process = SimpleProcessWrapper(lambda: calls.append("sample"))
        for expected in (1, 2):
            succeeded, duration = run_process(
                process, self.scheduler, verbose=False
            )
            self.assertTrue(succeeded)
            self.assertGreaterEqual(duration, 0)
            self.assertEqual(len(calls), expected)
            self.assertFalse(process.is_pending())
            self.assertFalse(process.has_more())
            self.assertFalse(process.commit(self.scheduler))
        self.assertTrue(self.scheduler.is_running())  # caller owns stopping

    def test_reset_running_wrapper_signals_abort_without_preemption(self):
        events = []

        async def body():
            events.append("before reset")
            process.reset()
            self.assertTrue(process.aborting)
            self.assertTrue(process.is_pending())
            await asyncio.sleep(0)
            events.append("after reset")

        process = SimpleProcessWrapper(body)
        self.assertTrue(process.commit(self.scheduler))
        self.assertTrue(process.join(self.scheduler))
        self.assertEqual(events, ["before reset", "after reset"])
        self.assertFalse(process.is_pending())
        self.assertFalse(process.aborting)
        self.assertTrue(process.has_more())  # abort did not mark finished
        process.reset()
        self.assertTrue(process.has_more())

    def test_failed_body_retains_more_and_exposes_executor_failure(self):
        calls = []

        def fail():
            calls.append("attempt")
            raise ValueError("characterized failure")

        process = SimpleProcessWrapper(fail)
        succeeded, _ = run_process(process, self.scheduler, verbose=False)
        self.assertFalse(succeeded)
        self.assertEqual(calls, ["attempt"])
        self.assertIn("ValueError: characterized failure", self.scheduler.last_error)
        self.assertFalse(process.is_pending())
        self.assertTrue(process.has_more())
        process.reset()
        self.assertTrue(process.has_more())

    def test_count_reset_reuses_record_and_appends_instead_of_clearing(self):
        group = DataGroup("legacy")
        events = []
        reading = Parameter(
            "reading", settable=False,
            before_get=lambda _: events.append("get") or 5.0,
        )
        process = count(
            "trace", group=group, getters=[reading], times=2,
            hook_before_get=lambda: events.append("before"),
            hook_after_get=lambda: events.append("after"),
        )
        for rows in (2, 4):
            self.assertTrue(run_process(process, self.scheduler, verbose=False)[0])
            self.assertEqual(len(group.record("trace")), rows)
        self.assertEqual(events, ["before", "get", "after"] * 4)
        self.assertEqual(list(group.record("trace").table["reading"]), [5.0] * 4)


class SchedulerExecutionContracts(unittest.TestCase):
    """Preserve borrowed loop/global state while measuring legacy limitations."""

    def run_owned_loop(self, coroutine):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coroutine)
        finally:
            loop.close()

    def test_control_point_failed_wait_raises_original_error(self):
        async def scenario():
            point = impl_scheduler._CtrlPoint(asyncio.get_running_loop().create_future())
            self.assertIsNone(point.status)
            point.add_previous("source")
            with self.assertLogs(impl_scheduler.__name__, level="WARNING"):
                point.finish(ValueError("failed point"))
            self.assertFalse(point.status)
            # Already-finished point returns False; waiting before failure raises.
            self.assertFalse(await point.wait())
            other = impl_scheduler._CtrlPoint(asyncio.get_running_loop().create_future())
            waiter = asyncio.create_task(other.wait())
            await asyncio.sleep(0)
            with self.assertLogs(impl_scheduler.__name__, level="WARNING"):
                other.finish(ValueError("in-flight failure"))
            with self.assertRaisesRegex(ValueError, "in-flight failure"):
                await waiter
            self.assertFalse(other.status)
        self.run_owned_loop(scenario())

    def test_cancelled_control_point_status_raises_cancelled_error(self):
        async def scenario():
            point = impl_scheduler._CtrlPoint(asyncio.get_running_loop().create_future())
            with self.assertLogs(impl_scheduler.__name__, level="WARNING"):
                point.cancel("characterization")
            self.assertTrue(point.is_done)
            with self.assertRaises(asyncio.CancelledError):
                _ = point.status
            with self.assertRaises(asyncio.CancelledError):
                await point.wait()
        self.run_owned_loop(scenario())

    def test_stop_cancels_but_does_not_drain_or_close_borrowed_loop(self):
        borrowed_global = impl_scheduler._scheduler

        async def scenario():
            loop = asyncio.get_running_loop()
            scheduler = impl_scheduler._SchedulerImpl()
            self.assertIs(scheduler._loop, loop)
            scheduler.start()
            entered = asyncio.Event()
            finished = []

            async def pending():
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    finished.append("drained")

            point = scheduler.acquire_point()
            self.assertTrue(scheduler.commit_action(Action("", point, func=pending)))
            await entered.wait()
            tasks = [runner._task for runner in scheduler._runners.values()]
            with self.assertLogs(impl_scheduler.__name__, level="WARNING"):
                scheduler.stop()
            self.assertFalse(scheduler.is_running())
            self.assertEqual(scheduler.snapshot()["point_count"], 0)
            self.assertEqual(scheduler.snapshot()["action_count"], 0)
            self.assertEqual(finished, [])
            self.assertFalse(tasks[0].done())
            await asyncio.gather(*tasks, return_exceptions=True)
            self.assertEqual(finished, ["drained"])
            self.assertTrue(tasks[0].cancelled())
            self.assertFalse(loop.is_closed())
            scheduler.start()
            self.assertTrue(scheduler.is_running())
            self.assertEqual(scheduler.snapshot()["point_count"], 0)
            scheduler.stop()
        self.run_owned_loop(scenario())
        self.assertIs(impl_scheduler._scheduler, borrowed_global)


if __name__ == "__main__":
    unittest.main()
