"""Independent synthetic contract tests for M02-W02 bounded count."""

import asyncio
import contextlib
import io
import unittest
from unittest import mock

from examples.signal_monitor import acquire
from softlab.huo.process import StopRequest, count, run_count
from softlab.huo.scheduler import Action, SerialScheduler
from softlab.huo.scheduler import impl_scheduler
from softlab.shui.data import DataGroup, DataRecord
from softlab.tu.station import Parameter


class Monitor:
    """Paired, consumptive sources; a failed second read still consumes first."""

    def __init__(self, fail=None):
        self.events = [('configure', 5)]
        self.tick = 0
        self.fail = fail
        self.cause = RuntimeError('injected failure')
        self.group = DataGroup('monitor')
        self.record = DataRecord('trace', [
            {'name': 'timestamp', 'unit': 's', 'dependent': False},
            {'name': 'signal', 'unit': 'V', 'dependent': True},
            {'name': 'reference', 'unit': 'V', 'dependent': True},
        ])
        self.group.add_record(self.record)
        def advance():
            self.events.append(('advance', self.tick + 1))
            if self.fail == 'advance' and self.tick == 1:
                raise self.cause
            self.tick += 1
        def read(name, offset):
            self.events.append((name, self.tick))
            if self.fail == name and self.tick == 2:
                raise self.cause
            return self.tick + offset
        getters = [
            Parameter('signal', settable=False,
                      before_get=lambda _: read('signal', 5)),
            Parameter('reference', settable=False,
                      before_get=lambda _: read('reference', 2)),
        ]
        self.process = count('trace', group=self.group, record=self.record,
                             getters=getters, times=3, hook_before_get=advance)

    def run(self, **kwargs):
        return run_count(self.process, 3, lambda: len(self.record),
                         lambda: self.tick, **kwargs)


class SerialSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.scheduler = SerialScheduler()
        # The runner owns normal cleanup. This direct Scheduler contract fixture
        # closes its own loop; private access is not a supported client API.
        self.addCleanup(self.scheduler._loop.close)
        self.addCleanup(self.scheduler.stop)

    def test_rejections_have_no_action_side_effects(self):
        scheduler = self.scheduler
        effects = []
        point = scheduler.acquire_point()
        action = lambda p, begin='', threaded=False: Action(
            begin, p, threaded, lambda: effects.append('ran'))
        self.assertFalse(scheduler.commit_action(action(point)))
        self.assertTrue(scheduler.start())
        self.assertFalse(scheduler.commit_action(action(point, begin='chain')))
        self.assertFalse(scheduler.commit_action(action(point, threaded=True)))
        self.assertFalse(scheduler.commit_action(action('missing')))
        self.assertTrue(scheduler.commit_action(action(point)))
        self.assertFalse(scheduler.commit_action(action(point)))
        self.assertEqual(effects, [])
        self.assertIs(scheduler.wait_point('missing'), False)
        self.assertIs(scheduler.wait_point(point), True)
        self.assertIs(scheduler.wait_point(point), True)
        self.assertEqual(effects, ['ran'])
        scheduler.stop()
        self.assertFalse(scheduler.commit_action(action(scheduler.acquire_point())))
        self.assertEqual(effects, ['ran'])

    def test_action_raised_timeout_is_failure_not_deadline(self):
        self.scheduler.start()
        for timeout in (None, 1.0):
            with self.subTest(timeout=timeout):
                cause = TimeoutError('action timeout')
                def fail():
                    raise cause
                point = self.scheduler.acquire_point()
                self.assertTrue(self.scheduler.commit_action(
                    Action('', point, func=fail)))
                self.assertIs(self.scheduler.wait_point(point, timeout), False)
                self.assertIs(self.scheduler.last_exception, cause)

    def test_awaitable_failure_and_timeout_are_distinct(self):
        scheduler = self.scheduler
        scheduler.start()
        effects = []
        async def async_action():
            await asyncio.sleep(0)
            effects.append('async')
        point = scheduler.acquire_point()
        self.assertTrue(scheduler.commit_action(Action('', point, func=async_action)))
        self.assertIs(scheduler.wait_point(point), True)
        self.assertEqual(effects, ['async'])
        cause = ValueError('failure')
        def fail():
            raise cause
        point = scheduler.acquire_point()
        self.assertTrue(scheduler.commit_action(Action('', point, func=fail)))
        self.assertIs(scheduler.wait_point(point), False)
        self.assertIs(scheduler.last_exception, cause)
        async def delayed():
            await asyncio.sleep(1)
        point = scheduler.acquire_point()
        self.assertTrue(scheduler.commit_action(Action('', point, func=delayed)))
        self.assertIsNone(scheduler.wait_point(point, timeout=0.001))
        self.assertIsNone(scheduler.wait_point(point))


class BoundedCountTests(unittest.TestCase):
    def assert_clean(self, result):
        self.assertTrue(result.terminal)
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)
        self.assertEqual(result.cleanup_errors, [])

    def test_actual_signal_client_uses_paired_reads_once(self):
        result, record, differences, reads = acquire(3)
        self.assertEqual(result.acquisition, 'completed')
        self.assertEqual((result.completed_rows, result.boundary), (3, 3))
        self.assertEqual(reads, [(channel, tick) for tick in (1, 2, 3)
                                 for channel in ('signal', 'reference')])
        self.assertEqual(list(record.table['signal']), [6, 7, 8])
        self.assertEqual(list(record.table['reference']), [3, 4, 5])
        self.assertEqual(differences, [3, 3, 3])
        self.assert_clean(result)

    def test_monitor_success_trace_and_values(self):
        monitor = Monitor()
        rows = []
        result = monitor.run(after_row=lambda r: rows.append(r.completed_rows))
        self.assertEqual(result.acquisition, 'completed')
        self.assertTrue(result.overall_success)
        self.assertEqual((result.completed_rows, result.boundary), (3, 3))
        self.assertEqual(rows, [1, 2, 3])
        self.assertEqual(monitor.events, [('configure', 5)] + [
            (op, tick) for tick in (1, 2, 3)
            for op in ('advance', 'signal', 'reference')])
        self.assertEqual(list(monitor.record.table['signal']), [6, 7, 8])
        self.assertEqual(list(monitor.record.table['reference']), [3, 4, 5])
        self.assert_clean(result)

    def test_reference_previous_and_current_boundary_policies(self):
        for delay in (1, 0):
            with self.subTest(delay=delay):
                trace = [('command', 4)]
                state = {'tick': 0}
                group = DataGroup('reference')
                record = DataRecord('trace', [
                    {'name': 'timestamp', 'unit': 's', 'dependent': False},
                    {'name': 'value', 'unit': 'C', 'dependent': True},
                ])
                group.add_record(record)
                def advance():
                    state['tick'] += 1
                    trace.append(('advance', state['tick']))
                def read(_):
                    source = state['tick'] - delay
                    trace.append(('observe' if delay == 0 else 'read', source))
                    return 20 + source * 2
                getter = Parameter('value', settable=False, before_get=read)
                process = count('trace', group=group, record=record,
                                getters=[getter], times=2,
                                hook_before_get=advance)
                def committed(result):
                    trace.append(('row', result.completed_rows))
                result = run_count(process, 2, lambda: len(record),
                                   lambda: state['tick'], after_row=committed)
                self.assertEqual(result.acquisition, 'completed')
                self.assertEqual(trace, [('command', 4),
                    ('advance', 1), ('observe' if delay == 0 else 'read', 1-delay), ('row', 1),
                    ('advance', 2), ('observe' if delay == 0 else 'read', 2-delay), ('row', 2)])
                self.assertEqual(list(record.table['value']),
                                 [20 + (1-delay)*2, 20 + (2-delay)*2])
                self.assert_clean(result)

    def test_stop_before_after_row_and_late(self):
        for when, expected, status, acknowledged in [
            (0, 0, 'cancelled', True), (2, 2, 'cancelled', True),
            (3, 3, 'completed', False),
        ]:
            with self.subTest(when=when):
                monitor = Monitor()
                stop = StopRequest()
                if when == 0:
                    stop.request()
                def after(result):
                    if result.completed_rows == when:
                        stop.request()
                result = monitor.run(stop=stop, after_row=after)
                self.assertEqual(result.acquisition, status)
                self.assertTrue(stop.requested)
                self.assertEqual(stop.acknowledged, acknowledged)
                self.assertEqual((len(monitor.record), monitor.tick),
                                 (expected, expected))
                self.assertEqual(len(monitor.events), 1 + 3 * expected)
                self.assert_clean(result)

    def test_failure_preserves_cause_partial_rows_and_boundary(self):
        for failure, boundary in [('advance', 1), ('signal', 2),
                                  ('reference', 2)]:
            with self.subTest(failure=failure):
                monitor = Monitor(failure)
                result = monitor.run()
                self.assertEqual(result.acquisition, 'failed')
                self.assertIs(result.cause, monitor.cause)
                self.assertEqual((result.completed_rows, result.boundary),
                                 (1, boundary))
                self.assertEqual(len(monitor.record), 1)
                if failure == 'reference':
                    self.assertEqual(monitor.events[-2:],
                                     [('signal', 2), ('reference', 2)])
                self.assert_clean(result)

    def test_preflight_rejects_invalid_total_before_callbacks(self):
        monitor = Monitor()
        inspected = []
        for invalid in (True, -1, 1.5):
            with self.subTest(invalid=invalid):
                with self.assertRaises((TypeError, ValueError)):
                    run_count(monitor.process, invalid,
                              lambda: inspected.append('row') or 0,
                              lambda: inspected.append('boundary'))
        self.assertEqual(inspected, [])
        self.assertEqual(monitor.events, [('configure', 5)])

    def test_running_loop_rejected_before_inspection(self):
        monitor = Monitor()
        inspected = []
        async def invoke():
            with self.assertRaises(RuntimeError):
                run_count(monitor.process, 3,
                          lambda: inspected.append('row') or 0,
                          lambda: monitor.tick)
        asyncio.run(invoke())
        self.assertEqual(inspected, [])
        self.assertEqual(monitor.events, [('configure', 5)])

    def test_preflight_nonzero_row_and_inspection_exception(self):
        monitor = Monitor()
        cause = RuntimeError('inspection')
        def bad_reader():
            raise cause
        with self.assertRaises(ValueError):
            run_count(monitor.process, 3, lambda: 1, lambda: 0)
        with self.assertRaises(RuntimeError) as caught:
            run_count(monitor.process, 3, bad_reader, lambda: 0)
        self.assertIs(caught.exception, cause)
        self.assertEqual(monitor.events, [('configure', 5)])

    def test_primary_and_cleanup_failure_are_independent(self):
        monitor = Monitor('reference')
        with mock.patch.object(SerialScheduler, 'stop',
                               side_effect=RuntimeError('stop obstructed')):
            result = monitor.run()
        self.assertIs(result.cause, monitor.cause)
        self.assertEqual(result.acquisition, 'failed')
        self.assertTrue(any('stop obstructed' in e for e in result.cleanup_errors))
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)
        self.assertFalse(result.overall_success)

    def test_success_with_cleanup_failure_is_not_overall_success(self):
        monitor = Monitor()
        with mock.patch.object(SerialScheduler, 'stop',
                               side_effect=RuntimeError('stop obstructed')):
            result = monitor.run()
        self.assertEqual(result.acquisition, 'completed')
        self.assertFalse(result.overall_success)
        self.assertTrue(any('stop obstructed' in e for e in result.cleanup_errors))
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)

    def test_cleanup_interruption_preserves_primary_and_later_cleanup(self):
        monitor = Monitor()
        primary = KeyboardInterrupt('primary interruption')
        cleanup = KeyboardInterrupt('cleanup interruption')
        def fail(_):
            raise primary
        with mock.patch.object(SerialScheduler, 'stop', side_effect=cleanup):
            with self.assertRaises(KeyboardInterrupt) as caught:
                monitor.run(after_row=fail)
        self.assertIs(caught.exception, primary)
        result = primary.count_result
        self.assertIs(result.cause, primary)
        self.assertTrue(any('cleanup interruption' in error
                            for error in result.cleanup_errors))
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)

    def test_cleanup_origin_interruption_is_reraised_after_later_cleanup(self):
        for exception_type in (KeyboardInterrupt, SystemExit):
            with self.subTest(exception_type=exception_type):
                monitor = Monitor()
                interruption = exception_type('cleanup interruption')
                with mock.patch.object(SerialScheduler, 'stop',
                                       side_effect=interruption):
                    with self.assertRaises(exception_type) as caught:
                        monitor.run()
                self.assertIs(caught.exception, interruption)
                result = interruption.count_result
                self.assertEqual(result.acquisition, 'completed')
                self.assertTrue(result.terminal)
                self.assertTrue(result.work_drained)
                self.assertTrue(result.loop_closed)
                self.assertFalse(result.overall_success)
                self.assertTrue(any('cleanup interruption' in error
                                    for error in result.cleanup_errors))

    def test_interruption_reraises_same_object_after_cleanup(self):
        monitor = Monitor()
        interrupt = KeyboardInterrupt('interrupt')
        def fail(_):
            raise interrupt
        with self.assertRaises(KeyboardInterrupt) as caught:
            monitor.run(after_row=fail)
        self.assertIs(caught.exception, interrupt)
        result = interrupt.count_result
        self.assertIs(result.cause, interrupt)
        self.assertEqual((result.completed_rows, result.boundary), (1, 1))
        self.assert_clean(result)

    def test_rejected_scheduling_fails_and_cleans_up(self):
        monitor = Monitor()
        with mock.patch.object(SerialScheduler, 'commit_action', return_value=False):
            result = monitor.run()
        self.assertEqual(result.acquisition, 'failed')
        self.assertIsInstance(result.cause, RuntimeError)
        self.assertEqual((result.completed_rows, result.boundary), (0, 0))
        self.assertEqual(monitor.events, [('configure', 5)])
        self.assert_clean(result)

    def test_interruption_attachment_fallback_emits_cleanup_diagnostics(self):
        class ImmutableInterrupt(BaseException):
            def __setattr__(self, name, value):
                if name == 'count_result':
                    raise TypeError('immutable')
                super().__setattr__(name, value)
        monitor = Monitor()
        interruption = ImmutableInterrupt('interrupted')
        def fail(_):
            raise interruption
        stderr = io.StringIO()
        with mock.patch.object(SerialScheduler, 'stop',
                               side_effect=RuntimeError('stop obstructed')):
            with contextlib.redirect_stderr(stderr):
                with self.assertRaises(ImmutableInterrupt) as caught:
                    monitor.run(after_row=fail)
        self.assertIs(caught.exception, interruption)
        self.assertIn('stop obstructed', stderr.getvalue())

    def test_borrowed_loop_and_global_scheduler_remain_usable(self):
        loop = asyncio.new_event_loop()
        self.addCleanup(loop.close)
        singleton = impl_scheduler._scheduler
        for failure in (None, 'reference'):
            monitor = Monitor(failure)
            monitor.run()
            self.assertIs(impl_scheduler._scheduler, singleton)
            self.assertEqual(loop.run_until_complete(asyncio.sleep(0, result=7)), 7)

    def test_secondary_inspection_failure_does_not_mask_primary(self):
        monitor = Monitor('reference')
        def boundary():
            if monitor.tick == 2:
                raise ValueError('boundary inspection')
            return monitor.tick
        result = run_count(monitor.process, 3, lambda: len(monitor.record), boundary)
        self.assertIs(result.cause, monitor.cause)
        self.assertTrue(any('boundary inspection' in e for e in result.cleanup_errors))
        self.assertEqual(result.completed_rows, 1)
        self.assertEqual(result.boundary, 1)
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)

    def test_zero_target_completes_without_acquisition(self):
        monitor = Monitor()
        result = run_count(monitor.process, 0, lambda: len(monitor.record),
                           lambda: monitor.tick)
        self.assertEqual(result.acquisition, 'completed')
        self.assertEqual((result.completed_rows, result.boundary), (0, 0))
        self.assertEqual(monitor.events, [('configure', 5)])
        self.assert_clean(result)

    def test_after_row_failure_retains_committed_row(self):
        monitor = Monitor()
        cause = RuntimeError('after row')
        def fail(_):
            raise cause
        result = monitor.run(after_row=fail)
        self.assertEqual(result.acquisition, 'failed')
        self.assertIs(result.cause, cause)
        self.assertEqual((result.completed_rows, result.boundary), (1, 1))
        self.assert_clean(result)


if __name__ == '__main__':
    unittest.main()
