"""M02-W01 executable characterization of existing count execution.

Assertion-based probes against the UNCHANGED production baseline, per the
approved written plan (log/release_8/test/characterization-plan.md) and
detailed design (log/release_8/design/characterization-design.md).

Groups: count/scan compatibility (T02/T03), scheduler isolation (T04),
thermal policy comparison (T05), signal-monitor client (T06), failure
characterization (T07), stop/reset/cancellation (T08), failure/cleanup (T09)
and repeat/reset transitions (T10).  T01/T11/T12 are documentation and
gate activities, not executable fixture groups.

All fixtures are synthetic; the thermal reference example is loaded as a
test fixture exactly like the existing thermal tests.  No production code
is modified; observed defects are recorded as findings, not guarantees.
"""

import asyncio
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import warnings
from unittest import mock

import numpy as np

from softlab.huo.process import count, run_process, scan
from softlab.huo.process.simple import SimpleProcessWrapper
from softlab.huo.scheduler import Action
from softlab.huo.scheduler import impl_scheduler
from softlab.jin.validator import ValNumber
from softlab.shui.data import DataGroup, DataRecord
from softlab.tu.simulation import (
    Coordinator,
    MockControl,
    MockMeasurement,
    SimulatedObject,
)
from softlab.tu.station import Parameter


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "thermal_experiment.py"
SPEC = importlib.util.spec_from_file_location(
    "thermal_m02_w01_characterization", EXAMPLE)
thermal = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = thermal
SPEC.loader.exec_module(thermal)

PROBE_TIMEOUT = 30  # parent-enforced deadline for isolated scheduler probes


def run_probe(test, code, name):
    """Run an isolated scheduler probe in a child process with a deadline.

    Returns the parsed list of TRACE items.  A parent timeout is an
    observed failure, never a pass; the child is terminated and reaped.
    """
    try:
        completed = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True, text=True, timeout=PROBE_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        test.fail(
            f"{name}: child probe timed out after {PROBE_TIMEOUT}s; "
            "terminated and reaped; cleanup unverifiable")
    if completed.returncode != 0:
        test.fail(
            f"{name}: child exited {completed.returncode}: "
            f"{completed.stderr.strip()}")
    trace = []
    for line in completed.stdout.splitlines():
        if line.startswith("TRACE "):
            trace.append(eval(line[len("TRACE "):], {"__builtins__": {}}))
    test.assertTrue(trace, f"{name}: child produced no TRACE output: "
                           f"{completed.stdout!r} {completed.stderr!r}")
    return trace


PROBE_SINGLETON = r"""
import asyncio
from softlab.huo.scheduler import Action
from softlab.huo.scheduler import impl_scheduler

trace = []
scheduler = impl_scheduler.get_scheduler()
trace.append(('global-is-instance',
              isinstance(impl_scheduler._scheduler,
                         impl_scheduler._SchedulerImpl)))
trace.append(('start', scheduler.start()))
point = scheduler.acquire_point()
trace.append(('commit', scheduler.commit_action(
    Action('', point, func=lambda: trace.append('action-ran')))))
trace.append(('wait', scheduler.wait_point(point, timeout=5)))
trace.append(('point-status', scheduler._points[point].status))
scheduler.stop()
trace.append(('running-after-stop', scheduler.is_running()))
try:
    scheduler.wait_point(point)
except Exception as exc:
    trace.append(('wait-after-stop', type(exc).__name__))
trace.append(('commit-after-stop', scheduler.commit_action(
    Action('', scheduler.acquire_point(), func=lambda: None))))
borrowed = asyncio.new_event_loop()
borrowed.run_until_complete(asyncio.sleep(0))
borrowed.close()
trace.append(('borrowed-loop-usable', True))
for item in trace:
    print('TRACE %r' % (item,))
"""

PROBE_PRIVATE = r"""
import asyncio
from softlab.huo.scheduler import Action
from softlab.huo.scheduler import impl_scheduler

trace = []
scheduler = impl_scheduler._SchedulerImpl()  # direct: NOT the global singleton
trace.append(('global-is-none', impl_scheduler._scheduler is None))
trace.append(('start', scheduler.start()))
point = scheduler.acquire_point()
trace.append(('commit', scheduler.commit_action(
    Action('', point, func=lambda: trace.append('action-ran')))))
try:
    trace.append(('wait-timeout-2s', scheduler.wait_point(point, timeout=2)))
except BaseException as exc:
    # observed: pending point + timeout raises instead of returning None
    trace.append(('wait-timeout-2s-raised', type(exc).__name__))
trace.append(('action-executed', 'action-ran' in trace))
scheduler.stop()
trace.append(('running-after-stop', scheduler.is_running()))
scheduler._loop.close()
trace.append(('loop-closed', scheduler._loop.is_closed()))
for item in trace:
    print('TRACE %r' % (item,))
"""


def make_scheduler_case(test):
    """Caller-owned thermal serial scheduler; released by test cleanup."""
    scheduler = thermal._make_scheduler()
    test.assertTrue(scheduler.start())
    test.addCleanup(scheduler._loop.close)
    test.addCleanup(scheduler.stop)
    return scheduler


class CountCompatibility(unittest.TestCase):
    """T02: count via public imports and run_process, caller-owned scheduler."""

    def test_count_two_readings_hooks_record_and_shape(self):
        scheduler = make_scheduler_case(self)
        events = []
        reading = Parameter(
            "reading", settable=False,
            before_get=lambda _: events.append("get") or 7.5,
        )
        group = DataGroup("t02")
        record = DataRecord(
            "trace", [
                {"name": "timestamp", "unit": "s", "dependent": False},
                {"name": "reading", "unit": "V", "dependent": True},
            ])
        group.add_record(record)
        process = count(
            "trace", group=group, record=record, getters=[reading], times=2,
            hook_before_get=lambda: events.append("before"),
            hook_after_get=lambda: events.append("after"),
        )
        result = run_process(process, scheduler, verbose=False)
        self.assertIsInstance(result, tuple)
        succeeded, elapsed = result
        self.assertIs(succeeded, True)
        self.assertIsInstance(elapsed, float)
        self.assertGreaterEqual(elapsed, 0.0)
        self.assertEqual(events, ["before", "get", "after"] * 2)
        self.assertEqual(len(record), 2)
        self.assertEqual(
            [column["name"] for column in record.columns],
            ["timestamp", "reading"],
        )
        self.assertEqual(list(record.table["reading"]), [7.5, 7.5])
        self.assertFalse(process.is_pending())
        self.assertFalse(process.has_more())
        # scheduler remains usable before caller cleanup
        self.assertTrue(scheduler.is_running())
        point = scheduler.acquire_point()
        self.assertTrue(scheduler.commit_action(
            Action("", point, func=lambda: events.append("extra"))))
        self.assertIs(scheduler.wait_point(point), True)
        self.assertIn("extra", events)

    def test_count_invalid_inputs_and_commit_join_transitions(self):
        scheduler = make_scheduler_case(self)
        reading = Parameter("reading", settable=False,
                            before_get=lambda _: 1.0)
        with self.assertRaises(ValueError):
            count("empty", None, None)  # no getter
        with self.assertRaises(ValueError):
            count("zero", None, None, reading, times=0)
        with self.assertRaises(TypeError):
            count("float", None, None, reading, times=1.5)
        with self.assertRaises(ValueError):
            count("badarg", None, None, 123)
        with self.assertRaises(TypeError):
            run_process(object(), scheduler, verbose=False)
        # commit/join against a not-started (borrowed) scheduler
        borrowed = thermal._CountScheduler()
        try:
            process = count("idle", None, None, reading, times=1)
            self.assertFalse(process.commit(borrowed))
            self.assertFalse(process.join(borrowed))
            self.assertFalse(process.is_pending())
            self.assertTrue(process.has_more())
            # invalid point/wait transitions on the thermal scheduler
            self.assertFalse(borrowed.wait_point("no-such-point"))
            self.assertFalse(borrowed.commit_action(
                Action("chained", borrowed.acquire_point(), func=lambda: None)))
            self.assertFalse(borrowed.commit_action(
                Action("", borrowed.acquire_point(), to_thread=True,
                       func=lambda: None)))
        finally:
            borrowed.stop()
            borrowed._loop.close()


class ScanCompatibility(unittest.TestCase):
    """T03: scan setter/getter order, terminal dry-run and reset/repeat."""

    def test_scan_traces_rows_dry_run_and_repeat(self):
        scheduler = make_scheduler_case(self)
        events = []
        waits = []
        original_wait = scheduler.wait_point

        def wait_spy(point, timeout=None):
            waits.append(point)
            return original_wait(point, timeout)

        scheduler.wait_point = wait_spy
        value = Parameter("value", ValNumber(), init_value=1.0)
        reading = Parameter(
            "reading", settable=False,
            before_get=lambda _: events.append("get") or value() * 2,
        )
        group = DataGroup("t03")
        record = DataRecord(
            "sweep", [
                {"name": "timestamp", "unit": "s", "dependent": False},
                {"name": "value", "unit": "V", "dependent": False},
                {"name": "reading", "unit": "V", "dependent": True},
            ])
        group.add_record(record)
        process = scan(
            "sweep", [reading], group, record, value, [2.0, 3.0],
            hook_before_set=lambda: events.append("before_set"),
            hook_after_set=lambda: events.append("after_set"),
            hook_before_get=lambda: events.append("before_get"),
            hook_after_get=lambda: events.append("after_get"),
        )

        succeeded, _ = run_process(process, scheduler, verbose=False)
        self.assertTrue(succeeded)
        per_row = ["before_set", "after_set", "before_get", "get", "after_get"]
        self.assertEqual(events, per_row * 2)
        self.assertEqual(list(record.table["value"]), [2.0, 3.0])
        self.assertEqual(list(record.table["reading"]), [4.0, 6.0])
        # terminal dry-run: one extra dispatch, no set/get events, no row
        self.assertEqual(len(waits), 3)
        self.assertEqual(len(record), 2)
        # reset/repeat appends instead of clearing (legacy behavior)
        succeeded, _ = run_process(process, scheduler, verbose=False)
        self.assertTrue(succeeded)
        self.assertEqual(len(record), 4)
        self.assertEqual(list(record.table["value"]), [2.0, 3.0, 2.0, 3.0])
        self.assertEqual(list(record.table["reading"]), [4.0, 6.0, 4.0, 6.0])


class SchedulerIsolation(unittest.TestCase):
    """T04: singleton vs thermal private scheduler, isolated probes."""

    def test_singleton_probe_in_child_process(self):
        before = impl_scheduler._scheduler
        trace = run_probe(self, PROBE_SINGLETON, "singleton probe")
        pairs = dict(
            (item[0], item[1]) for item in trace if isinstance(item, tuple))
        self.assertTrue(pairs["global-is-instance"])
        self.assertTrue(pairs["start"])
        self.assertTrue(pairs["commit"])
        self.assertIs(pairs["wait"], True)
        self.assertTrue(pairs["point-status"])
        self.assertFalse(pairs["running-after-stop"])
        self.assertEqual(pairs["wait-after-stop"], "RuntimeError")
        self.assertFalse(pairs["commit-after-stop"])
        self.assertTrue(pairs["borrowed-loop-usable"])
        self.assertIn("action-ran", trace)
        self.assertIs(impl_scheduler._scheduler, before)  # global untouched

    def test_private_concrete_scheduler_probe_in_child_process(self):
        """Direct _SchedulerImpl: completion callback routes via the module
        global singleton; without it the point never finishes (bounded by
        the wait timeout).  Observed for R4-BASE-SCHED-01."""
        before = impl_scheduler._scheduler
        trace = run_probe(self, PROBE_PRIVATE, "private scheduler probe")
        pairs = dict(
            (item[0], item[1]) for item in trace if isinstance(item, tuple))
        self.assertTrue(pairs["global-is-none"])
        self.assertTrue(pairs["start"])
        self.assertTrue(pairs["commit"])
        self.assertTrue(pairs["action-executed"])
        # observed for R4-BASE-SCHED-01: the completion callback routes via
        # the module-global singleton, so this point never finishes; the
        # bounded wait then raises CancelledError (not the documented None)
        self.assertEqual(pairs["wait-timeout-2s-raised"], "CancelledError")
        self.assertNotIn("wait-timeout-2s", pairs)
        self.assertFalse(pairs["running-after-stop"])
        self.assertTrue(pairs["loop-closed"])
        self.assertIs(impl_scheduler._scheduler, before)

    def test_thermal_private_scheduler_owns_loop_and_leaves_borrowed_state(self):
        before = impl_scheduler._scheduler
        original_loop = None
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            try:
                original_loop = asyncio.get_event_loop()
            except RuntimeError:
                original_loop = None
        borrowed = asyncio.new_event_loop()
        asyncio.set_event_loop(borrowed)
        try:
            scheduler = thermal._make_scheduler()
            self.assertTrue(scheduler.start())
            events = []
            point = scheduler.acquire_point()
            self.assertTrue(scheduler.commit_action(
                Action("", point, func=lambda: events.append("ran"))))
            self.assertIs(scheduler.wait_point(point), True)
            self.assertEqual(events, ["ran"])
            self.assertIs(asyncio.get_event_loop(), borrowed)
            self.assertFalse(borrowed.is_closed())
            scheduler.stop()
            self.assertFalse(scheduler.is_running())
        finally:
            asyncio.set_event_loop(original_loop)
            borrowed.close()
            if not scheduler._loop.is_closed():
                scheduler._loop.close()
        self.assertTrue(scheduler._loop.is_closed())
        self.assertIs(impl_scheduler._scheduler, before)


class ThermalPolicyComparison(unittest.TestCase):
    """T05: coordinated previous-boundary vs direct current-boundary policy."""

    CONFIG = thermal.ThermalConfig()

    def euler(self, delay):
        """Independent numeric oracle for the documented Euler recurrence."""
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

    def run_instrumented(self, adapter):
        trace = []
        coordinator_cls = Coordinator
        original_step = coordinator_cls.step
        measurement_cls = MockMeasurement
        original_measurement_read = measurement_cls.read
        control_cls = MockControl
        original_write = control_cls.write
        direct_cls = thermal._DirectMeasurement
        original_direct_read = direct_cls.read
        original_observe = SimulatedObject.observe_outputs
        record_cls = DataRecord
        original_add_rows = record_cls.add_rows

        def step_spy(self_):
            trace.append("advance")
            return original_step(self_)

        def measurement_read_spy(self_):
            trace.append("read")
            return original_measurement_read(self_)

        def write_spy(self_, value, *args, **kwargs):
            trace.append("command")
            return original_write(self_, value, *args, **kwargs)

        def direct_read_spy(self_):
            trace.append("observe")
            return original_direct_read(self_)

        def observe_spy(self_):
            trace.append("pure-observe")
            return original_observe(self_)

        def add_rows_spy(self_, row):
            trace.append(("append", id(self_)))
            return original_add_rows(self_, row)

        with mock.patch.object(coordinator_cls, "step", autospec=True,
                               side_effect=step_spy), \
                mock.patch.object(measurement_cls, "read", autospec=True,
                                  side_effect=measurement_read_spy), \
                mock.patch.object(control_cls, "write", autospec=True,
                                  side_effect=write_spy), \
                mock.patch.object(direct_cls, "read", autospec=True,
                                  side_effect=direct_read_spy), \
                mock.patch.object(SimulatedObject, "observe_outputs",
                                  autospec=True,
                                  side_effect=observe_spy), \
                mock.patch.object(record_cls, "add_rows", autospec=True,
                                  side_effect=add_rows_spy):
            with tempfile.TemporaryDirectory() as tmp:
                result = thermal.execute(self.CONFIG, Path(tmp) / adapter,
                                         adapter=adapter)
        self.assertTrue(result.overall_success)
        return result, trace

    @staticmethod
    def split_trace(result, trace):
        """Separate acquisition-record appends from persistence replays.

        ``DataGroup.record()`` copies and HDF5 reload replays ``add_rows``
        on rebuilt records; only events on the acquisition record count
        toward row order.  Returns (clean_trace, reload_append_count).
        """
        acquisition = id(result.group._records["thermal_trace"])
        clean, reload_appends = [], 0
        for event in trace:
            if isinstance(event, tuple) and event[0] == "append":
                if event[1] == acquisition:
                    clean.append("append")
                else:
                    reload_appends += 1
            else:
                clean.append(event)
        return clean, reload_appends

    def test_coordinated_previous_boundary_policy(self):
        result, trace = self.run_instrumented("coordinated")
        clean, reload_appends = self.split_trace(result, trace)
        self.assertEqual(clean, ["command"]
                         + ["advance", "read", "append"] * self.CONFIG.points)
        # persistence replays add_rows on rebuilt records (observed, separate)
        self.assertEqual(reload_appends, 9)
        table = result.group.record("thermal_trace").table
        self.assertEqual(list(table["measurement_source_tick"]),
                         list(range(0, self.CONFIG.points)))
        np.testing.assert_allclose(
            table["temperature_degc"].to_numpy(dtype=float),
            self.euler(delay=1), rtol=0, atol=1e-10)
        self.assertEqual(trace.count("pure-observe"), 0)

    def test_direct_current_boundary_policy_extra_observation(self):
        result, trace = self.run_instrumented("direct")
        clean, reload_appends = self.split_trace(result, trace)
        self.assertEqual(clean, ["command"]
                         + ["advance", "observe", "pure-observe", "append"]
                         * self.CONFIG.points)
        self.assertEqual(reload_appends, 9)
        # direct adapter bypasses MockMeasurement entirely
        self.assertEqual(clean.count("read"), 0)
        table = result.group.record("thermal_trace").table
        self.assertEqual(list(table["measurement_source_tick"]),
                         list(range(1, self.CONFIG.points + 1)))
        np.testing.assert_allclose(
            table["temperature_degc"].to_numpy(dtype=float),
            self.euler(delay=0), rtol=0, atol=1e-10)
        # hypothesis verified: one extra pure observation per completed row
        self.assertEqual(trace.count("pure-observe"), self.CONFIG.points)


class _MonitorSource:
    """Consumptive synthetic source; each read consumes one queued value."""

    def __init__(self, signals, references, events,
                 fail_signal_call=None, fail_reference_call=None):
        self._signals = list(signals)
        self._references = list(references)
        self._events = events
        self.signal_reads = 0
        self.reference_reads = 0
        self.fail_signal_call = fail_signal_call
        self.fail_reference_call = fail_reference_call

    def configure(self, value):
        self._events.append(("configure", value))
        self.configured = value

    def read_signal(self):
        self.signal_reads += 1
        value = self._signals.pop(0)
        self._events.append(("signal", value))
        if self.signal_reads == self.fail_signal_call:
            raise RuntimeError("injected signal failure")
        return value

    def read_reference(self):
        self.reference_reads += 1
        value = self._references.pop(0)
        self._events.append(("reference", value))
        if self.reference_reads == self.fail_reference_call:
            raise RuntimeError("injected reference failure")
        return value


class _CountingDouble:
    """Separate counting double supplementing the monitoring client."""

    def __init__(self, source):
        self._source = source
        self.signal_calls = 0
        self.reference_calls = 0

    def configure(self, value):
        return self._source.configure(value)

    def read_signal(self):
        self.signal_calls += 1
        return self._source.read_signal()

    def read_reference(self):
        self.reference_calls += 1
        return self._source.read_reference()


def run_monitor(source, boundaries, scheduler, events):
    """Distinct signal-monitoring count client.

    Configures a fixed source once, advances an integer clock per attempt,
    reads consumptive signal then reference once each through the public
    count machinery (one single-row count per boundary), and commits a
    caller-owned derived row only after both reads succeeded.
    Returns (events, count_owned_record, derived_record, run_outcomes).
    """
    clock = {"tick": 0}
    group = DataGroup("monitor")
    pairs = DataRecord(
        "monitor_pairs", [
            {"name": "timestamp", "unit": "s", "dependent": False},
            {"name": "signal", "unit": "V", "dependent": True},
            {"name": "reference", "unit": "V", "dependent": True},
        ])
    group.add_record(pairs)
    derived = DataRecord(
        "monitor_derived", [
            {"name": "boundary", "unit": "1", "dependent": False},
            {"name": "signal", "unit": "V", "dependent": True},
            {"name": "reference", "unit": "V", "dependent": True},
            {"name": "difference", "unit": "V", "dependent": True},
        ])
    group.add_record(derived)
    source.configure(5)
    captured = {}
    outcomes = []
    for _ in range(boundaries):
        clock["tick"] += 1
        boundary = clock["tick"]

        def advance():
            events.append(("advance", boundary))

        def signal_get(_):
            value = source.read_signal()
            captured["signal"] = value
            return value

        def reference_get(_):
            value = source.read_reference()
            captured["reference"] = value
            return value

        signal_para = Parameter("signal", settable=False,
                                before_get=signal_get)
        reference_para = Parameter("reference", settable=False,
                                   before_get=reference_get)
        process = count(
            "monitor_pairs", group=group, record=pairs,
            getters=[("signal", signal_para), ("reference", reference_para)],
            times=1, hook_before_get=advance)
        succeeded, elapsed = run_process(process, scheduler, verbose=False)
        outcomes.append((succeeded, elapsed))
        if not succeeded:
            break  # caller-owned policy: no derived row without both reads
        difference = captured["signal"] - captured["reference"]
        events.append(("difference", difference))
        derived.add_rows({
            "boundary": boundary,
            "signal": captured["signal"],
            "reference": captured["reference"],
            "difference": difference,
        })
        events.append(("append", boundary, captured["signal"],
                       captured["reference"], difference))
    return events, pairs, derived, outcomes


class SignalMonitorClient(unittest.TestCase):
    """T06: distinct synthetic signal-monitoring client via public count."""

    def test_three_boundaries_paired_readings_and_differences(self):
        scheduler = make_scheduler_case(self)
        events = []
        source = _MonitorSource([10, 13, 17], [1, 2, 4], events)
        double = _CountingDouble(source)
        result = run_monitor(double, 3, scheduler, events)
        client_events, pairs, derived, outcomes = result
        self.assertEqual(client_events, [
            ("configure", 5),
            ("advance", 1), ("signal", 10), ("reference", 1),
            ("difference", 9), ("append", 1, 10, 1, 9),
            ("advance", 2), ("signal", 13), ("reference", 2),
            ("difference", 11), ("append", 2, 13, 2, 11),
            ("advance", 3), ("signal", 17), ("reference", 4),
            ("difference", 13), ("append", 3, 17, 4, 13),
        ])
        self.assertTrue(all(ok for ok, _ in outcomes))
        self.assertEqual(len(outcomes), 3)
        # exactly three reads per source; counting double agrees
        self.assertEqual(source.signal_reads, 3)
        self.assertEqual(source.reference_reads, 3)
        self.assertEqual(double.signal_calls, 3)
        self.assertEqual(double.reference_calls, 3)
        self.assertEqual(len(source._signals), 0)
        self.assertEqual(len(source._references), 0)
        # count-owned rows and caller-owned derived rows are both inspectable
        self.assertEqual(len(pairs), 3)
        self.assertEqual(len(derived), 3)
        self.assertEqual(
            [column["name"] for column in pairs.columns],
            ["timestamp", "signal", "reference"])
        self.assertEqual(list(derived.table["boundary"]), [1, 2, 3])
        self.assertEqual(list(derived.table["signal"]), [10, 13, 17])
        self.assertEqual(list(derived.table["reference"]), [1, 2, 4])
        self.assertEqual(list(derived.table["difference"]), [9, 11, 13])
        # no thermal heater feedback/policy is involved in this client
        self.assertNotIn("command", [event[0] for event in client_events])


class SignalMonitorFailures(unittest.TestCase):
    """T07: independent first-read/second-read failures; thermal faults."""

    def test_first_read_failure_at_boundary_two(self):
        scheduler = make_scheduler_case(self)
        events = []
        source = _MonitorSource([10, 13, 17], [1, 2, 4], events,
                                fail_signal_call=2)
        client_events, pairs, derived, outcomes = run_monitor(
            _CountingDouble(source), 3, scheduler, events)
        self.assertEqual(client_events, [
            ("configure", 5),
            ("advance", 1), ("signal", 10), ("reference", 1),
            ("difference", 9), ("append", 1, 10, 1, 9),
            ("advance", 2), ("signal", 13),
        ])
        self.assertIs(outcomes[0][0], True)
        self.assertIs(outcomes[1][0], False)
        self.assertEqual(len(outcomes), 2)
        self.assertIn("RuntimeError", scheduler.last_error)
        # one completed pair/derived row; boundary 2 reached; no rollback
        self.assertEqual(len(pairs), 1)
        self.assertEqual(len(derived), 1)
        self.assertEqual(list(derived.table["boundary"]), [1])
        self.assertEqual(source.signal_reads, 2)  # failed attempt consumed
        self.assertEqual(source.reference_reads, 1)
        self.assertEqual(source._signals, [17])
        self.assertEqual(source._references, [2, 4])

    def test_second_read_failure_consumes_signal_value(self):
        scheduler = make_scheduler_case(self)
        events = []
        source = _MonitorSource([10, 13, 17], [1, 2, 4], events,
                                fail_reference_call=2)
        client_events, pairs, derived, outcomes = run_monitor(
            _CountingDouble(source), 3, scheduler, events)
        self.assertEqual(client_events, [
            ("configure", 5),
            ("advance", 1), ("signal", 10), ("reference", 1),
            ("difference", 9), ("append", 1, 10, 1, 9),
            ("advance", 2), ("signal", 13), ("reference", 2),
        ])
        self.assertIs(outcomes[1][0], False)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(len(derived), 1)  # no derived row for boundary 2
        self.assertEqual(source.signal_reads, 2)  # second signal consumed
        self.assertEqual(source.reference_reads, 2)
        self.assertEqual(source._signals, [17])
        self.assertEqual(source._references, [4])


class ThermalFaultFixtures(unittest.TestCase):
    """T07/T09: thermal advance/read faults and cleanup evidence."""

    CONFIG = thermal.ThermalConfig()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def acquire_fault(self, stage, name):
        coordinator, heater, probe = thermal._build_simulation(self.CONFIG)
        model = coordinator.station.object("body")
        original_evolve, original_read = model._evolve, probe.read
        cause = ValueError("injected " + stage)

        def evolve(inputs, states):
            if stage == "model" and coordinator.tick == 3:
                raise cause
            return original_evolve(inputs, states)

        def read():
            if stage == "sample" and coordinator.tick == 4:
                raise cause
            return original_read()

        with mock.patch.object(thermal, "_build_simulation",
                               return_value=(coordinator, heater, probe)), \
                mock.patch.object(model, "_evolve", side_effect=evolve), \
                mock.patch.object(probe, "read", side_effect=read):
            result = thermal.execute(self.CONFIG, self.root / name)
        return result, cause

    def test_advance_fault_before_boundary_retains_partial_rows(self):
        result, cause = self.acquire_fault("model", "model-fault")
        self.assertEqual(result.acquisition, "failed")
        self.assertIs(result.cause, cause)
        self.assertEqual(result.completed_rows, 3)
        self.assertEqual(result.final_tick, 3)
        self.assertEqual(len(result.group.record("thermal_trace")), 3)
        self.assertEqual(result.persistence, "not_attempted")

    def test_read_fault_after_boundary_retains_partial_rows(self):
        result, cause = self.acquire_fault("sample", "sample-fault")
        self.assertEqual(result.acquisition, "failed")
        self.assertIs(result.cause, cause)
        self.assertEqual(result.completed_rows, 3)
        self.assertEqual(result.final_tick, 4)  # boundary differs from rows
        self.assertEqual(len(result.group.record("thermal_trace")), 3)

    def test_failed_run_closes_owned_resources(self):
        result, _ = self.acquire_fault("model", "model-cleanup")
        self.assertTrue(result.terminal)
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)
        self.assertEqual(result.cleanup_errors, [])
        self.assertEqual(result.stage, "acquisition")

    def test_primary_failure_with_obstructed_cleanup_records_both(self):
        scheduler = thermal._make_scheduler()
        cause = ValueError("primary acquisition failure")
        try:
            with mock.patch.object(thermal, "_make_scheduler",
                                   return_value=scheduler), \
                    mock.patch.object(thermal, "_acquire",
                                      side_effect=cause), \
                    mock.patch.object(scheduler, "stop",
                                      side_effect=RuntimeError("stop blocked")):
                result = thermal.execute(self.CONFIG, self.root / "cleanup")
            self.assertFalse(result.overall_success)
            self.assertIs(result.cause, cause)
            self.assertTrue(any("stop blocked" in error
                                for error in result.cleanup_errors))
            # other cleanup actions still attempted and evidenced
            self.assertTrue(result.work_drained)
            self.assertTrue(result.loop_closed)
            self.assertTrue(scheduler._loop.is_closed())
        finally:
            scheduler.stop()
            if not scheduler._loop.is_closed():
                scheduler._loop.close()


class StopResetCancellation(unittest.TestCase):
    """T08: thermal stop request/ack, legacy reset, private point cancel."""

    CONFIG = thermal.ThermalConfig()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_stop_before_first_cycle_acknowledges_without_steps(self):
        stop = thermal.StopRequest()
        stop.request()
        with mock.patch.object(Coordinator, "step", autospec=True) as step:
            result = thermal.execute(self.CONFIG, self.root / "early",
                                     stop=stop)
        step.assert_not_called()
        self.assertEqual(result.acquisition, "cancelled")
        self.assertEqual(result.completed_rows, 0)
        self.assertEqual(result.final_tick, 0)
        self.assertTrue(stop.requested)
        self.assertTrue(stop.acknowledged)
        self.assertFalse(result.data_path.exists())

    def test_stop_after_known_row_prevents_later_cycles(self):
        stop = thermal.StopRequest()
        boundaries = []
        original_step = Coordinator.step

        def step_spy(coordinator):
            original_step(coordinator)
            boundaries.append(coordinator.tick)

        def after_row(result):
            if result.completed_rows == 3:
                stop.request()

        with mock.patch.object(Coordinator, "step", autospec=True,
                               side_effect=step_spy):
            result = thermal.execute(self.CONFIG, self.root / "mid",
                                     stop=stop, after_row=after_row)
        self.assertEqual(boundaries, [1, 2, 3])
        self.assertEqual(result.acquisition, "cancelled")
        self.assertEqual(result.completed_rows, 3)
        self.assertEqual(result.final_tick, 3)
        self.assertTrue(stop.acknowledged)

    def test_stop_request_after_completion_is_late(self):
        stop = thermal.StopRequest()

        def after_row(result):
            if result.completed_rows == self.CONFIG.points:
                stop.request()

        result = thermal.execute(self.CONFIG, self.root / "late", stop=stop,
                                 after_row=after_row)
        self.assertTrue(result.overall_success)
        self.assertEqual(result.acquisition, "completed")
        self.assertEqual(result.completed_rows, self.CONFIG.points)
        self.assertTrue(stop.requested)
        self.assertFalse(stop.acknowledged)

    def test_reset_during_running_body_signals_abort_without_preemption(self):
        scheduler = make_scheduler_case(self)
        events = []

        async def body():
            events.append("before")
            process.reset()
            self.assertTrue(process.aborting)
            await asyncio.sleep(0)
            events.append("after")

        process = SimpleProcessWrapper(body)
        self.assertTrue(process.commit(scheduler))
        self.assertTrue(process.join(scheduler))
        self.assertEqual(events, ["before", "after"])
        self.assertFalse(process.is_pending())
        self.assertTrue(process.has_more())  # abort did not mark finished
        process.reset()
        self.assertFalse(process.aborting)
        self.assertTrue(process.has_more())

    def test_reset_rejected_while_pending(self):
        scheduler = make_scheduler_case(self)
        group = DataGroup("pending")
        reading = Parameter("reading", settable=False,
                            before_get=lambda _: 1.0)
        process = count("pending", group=group, getters=[reading], times=1)
        self.assertTrue(process.commit(scheduler))
        self.assertTrue(process.is_pending())
        with self.assertRaises(RuntimeError):
            process.data_group = DataGroup("other")
        self.assertTrue(process.join(scheduler))
        self.assertFalse(process.is_pending())

    def test_private_control_point_cancellation_seam(self):
        """Private _CtrlPoint.cancel only; not a public Scheduler.cancel API."""
        loop = asyncio.new_event_loop()
        try:
            async def scenario():
                point = impl_scheduler._CtrlPoint(
                    asyncio.get_running_loop().create_future())
                point.add_previous("source-action")
                self.assertFalse(point.is_done)
                self.assertIsNone(point.status)
                with self.assertLogs(impl_scheduler.__name__, level="WARNING"):
                    point.cancel("characterization")
                self.assertTrue(point.is_done)
                with self.assertRaises(asyncio.CancelledError):
                    _ = point.status
                with self.assertRaises(asyncio.CancelledError):
                    await point.wait()
            loop.run_until_complete(scenario())
        finally:
            loop.close()


class RepeatAndTransitions(unittest.TestCase):
    """T10: invalid transitions, terminal snapshots and fresh repeats."""

    CONFIG = thermal.ThermalConfig()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_invalid_scheduler_operations_observed_outcomes(self):
        scheduler = make_scheduler_case(self)
        point = scheduler.acquire_point()
        self.assertTrue(scheduler.commit_action(
            Action("", point, func=lambda: None)))
        self.assertIs(scheduler.wait_point(point), True)
        scheduler.stop()
        self.assertFalse(scheduler.is_running())
        # commits rejected after stop; unknown points report False
        other = scheduler.acquire_point()
        self.assertFalse(scheduler.commit_action(
            Action("", other, func=lambda: None)))
        self.assertFalse(scheduler.wait_point("no-such-point"))

    def test_two_fresh_runs_after_success_match_excluding_timestamp(self):
        first = thermal.execute(self.CONFIG, self.root / "first")
        second = thermal.execute(self.CONFIG, self.root / "second")
        self.assertTrue(first.overall_success)
        self.assertTrue(second.overall_success)
        self.assertNotEqual(first.group.id, second.group.id)
        left = first.group.record("thermal_trace").table
        right = second.group.record("thermal_trace").table
        columns = [name for name in left.columns if name != "timestamp"]
        np.testing.assert_allclose(left[columns].to_numpy(dtype=float),
                                   right[columns].to_numpy(dtype=float),
                                   rtol=0, atol=1e-10)

    def test_two_fresh_runs_after_interruption_match_excluding_timestamp(self):
        stop = thermal.StopRequest()

        def after_row(result):
            if result.completed_rows == 3:
                stop.request()

        interrupted = thermal.execute(self.CONFIG, self.root / "interrupted",
                                      stop=stop, after_row=after_row)
        self.assertEqual(interrupted.acquisition, "cancelled")
        runs = [thermal.execute(self.CONFIG, self.root / name)
                for name in ("fresh-a", "fresh-b")]
        ids = {str(interrupted.group.id)} | {str(r.group.id) for r in runs}
        self.assertEqual(len(ids), 3)
        table = runs[0].group.record("thermal_trace").table
        columns = [name for name in table.columns if name != "timestamp"]
        for run in runs[1:]:
            other = run.group.record("thermal_trace").table
            np.testing.assert_allclose(
                table[columns].to_numpy(dtype=float),
                other[columns].to_numpy(dtype=float),
                rtol=0, atol=1e-10)
        self.assertTrue(all(run.overall_success for run in runs))

    def test_count_record_append_versus_clear_on_repeat(self):
        scheduler = make_scheduler_case(self)
        group = DataGroup("append")
        reading = Parameter("reading", settable=False,
                            before_get=lambda _: 5.0)
        process = count("trace", group=group, getters=[reading], times=2)
        # DataGroup.record() returns a copy; use the process-owned record
        record = process.record
        self.assertEqual(len(group.record("trace")), 0)
        for expected_rows in (2, 4):
            succeeded, _ = run_process(process, scheduler, verbose=False)
            self.assertTrue(succeeded)
            self.assertEqual(len(record), expected_rows)
        self.assertEqual(list(record.table["reading"]), [5.0] * 4)
        process.reset()
        self.assertEqual(len(record), 4)  # reset does not clear the record


if __name__ == "__main__":
    unittest.main()
