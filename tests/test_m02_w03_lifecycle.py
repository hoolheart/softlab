"""Independent lifecycle contract tests for the bounded count path."""
import asyncio
import unittest
from unittest import mock

from softlab.huo.process import run_count
from softlab.huo.scheduler import SerialScheduler
from test_m02_w02_bounded import Monitor


class LifecycleTests(unittest.TestCase):
    def test_fresh_runs_have_independent_ids_and_evidence(self):
        first, second = Monitor(), Monitor()
        a, b = first.run(), second.run()
        self.assertNotEqual(a.run_id, b.run_id)
        self.assertEqual(first.events, second.events)
        for column in ('signal', 'reference'):
            self.assertEqual(list(first.record.table[column]),
                             list(second.record.table[column]))
        self.assertEqual((a.completed_rows, a.boundary), (3, 3))
        self.assertTrue(a.boundary_observed)

    def test_claim_precedes_reader_and_preflight_failure_does_not_claim(self):
        monitor = Monitor()
        cause = ValueError('initial inspection')
        def bad():
            raise cause
        with self.assertRaises(ValueError) as caught:
            run_count(monitor.process, 3, bad, lambda: 0)
        self.assertIs(caught.exception, cause)
        result = monitor.run()
        self.assertTrue(result.overall_success)
        effects = []
        with self.assertRaises(RuntimeError):
            run_count(monitor.process, 3, lambda: effects.append('read') or 0,
                      lambda: 0)
        self.assertEqual(effects, [])

    def test_reset_failure_claims_process_without_acquisition(self):
        monitor = Monitor()
        cause = RuntimeError('reset failed')
        with mock.patch.object(monitor.process, 'reset', side_effect=cause):
            result = monitor.run()
        self.assertIs(result.cause, cause)
        self.assertEqual(monitor.events, [('configure', 5)])
        with self.assertRaises(RuntimeError):
            monitor.run()
        self.assertEqual(monitor.events, [('configure', 5)])

    def test_independent_inspection_and_observed_none(self):
        monitor = Monitor()
        calls = []
        cause = ValueError('row inspection')
        def rows():
            calls.append('row')
            if len(calls) > 1:
                raise cause
            return 0
        def boundary():
            calls.append('boundary')
            return None
        result = run_count(monitor.process, 3, rows, boundary)
        self.assertIs(result.cause, cause)
        self.assertIn('boundary', calls)
        self.assertTrue(result.boundary_observed)
        self.assertIsNone(result.boundary)

    def test_primary_and_secondary_reader_identity(self):
        monitor = Monitor('reference')
        row_error, boundary_error = ValueError('row'), LookupError('boundary')
        def rows():
            if monitor.tick == 2:
                raise row_error
            return len(monitor.record)
        def boundary():
            if monitor.tick == 2:
                raise boundary_error
            return monitor.tick
        result = run_count(monitor.process, 3, rows, boundary)
        self.assertIs(result.cause, monitor.cause)
        errors = [error for _, error in result.secondary_errors]
        self.assertIn(row_error, errors)
        self.assertIn(boundary_error, errors)
        self.assertEqual(result.completed_rows, 1)
        self.assertEqual(result.boundary, 1)

    def test_later_inspection_interruption_preserves_primary(self):
        monitor = Monitor('reference')
        interruption = KeyboardInterrupt('inspect')
        def boundary():
            if monitor.tick == 2:
                raise interruption
            return monitor.tick
        with self.assertRaises(KeyboardInterrupt) as caught:
            run_count(monitor.process, 3, lambda: len(monitor.record), boundary)
        self.assertIs(caught.exception, interruption)
        self.assertIs(interruption.count_result.cause, monitor.cause)
        self.assertTrue(interruption.count_result.loop_closed)

    def test_reset_helper_eligibility_and_failure(self):
        from softlab.huo.process import CountResult, StopRequest, reset_after_count
        result = CountResult(1, StopRequest())
        calls = []
        with self.assertRaises((ValueError, RuntimeError)):
            reset_after_count(result, lambda: calls.append('reset'))
        self.assertEqual(calls, [])
        result.terminal = result.work_drained = result.loop_closed = True
        cause = RuntimeError('model reset')
        def fail():
            calls.append('reset')
            raise cause
        with self.assertRaises(RuntimeError) as caught:
            reset_after_count(result, fail)
        self.assertIs(caught.exception, cause)
        self.assertEqual(calls, ['reset'])
        self.assertTrue(result.terminal)
        evidence = (result.run_id, result.acquisition, result.cause,
                    result.completed_rows, result.boundary)
        reset_after_count(result, lambda: calls.append('success'))
        self.assertEqual(calls, ['reset', 'success'])
        self.assertEqual((result.run_id, result.acquisition, result.cause,
                          result.completed_rows, result.boundary), evidence)

    def test_cleanup_fault_retains_structured_identity(self):
        monitor = Monitor()
        cause = RuntimeError('stop failure')
        with mock.patch.object(SerialScheduler, 'stop', side_effect=cause):
            result = monitor.run()
        self.assertEqual(result.acquisition, 'completed')
        self.assertFalse(result.overall_success)
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)
        self.assertIn(cause, [error for _, error in result.secondary_errors])


    def test_scheduler_setup_faults_preserve_identity_and_cleanup(self):
        monitor = Monitor()
        cause = RuntimeError('constructor failure')
        with mock.patch('softlab.huo.process.bounded.SerialScheduler',
                        side_effect=cause):
            result = monitor.run()
        self.assertIs(result.cause, cause)
        self.assertFalse(result.work_drained)
        self.assertFalse(result.loop_closed)
        self.assertEqual(monitor.events, [('configure', 5)])

        monitor = Monitor()
        cause = RuntimeError('start failure')
        with mock.patch.object(SerialScheduler, 'start', side_effect=cause):
            result = monitor.run()
        self.assertIs(result.cause, cause)
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)
        self.assertEqual(monitor.events, [('configure', 5)])

    def test_drain_and_close_failures_do_not_suppress_later_cleanup(self):
        scheduler = SerialScheduler()
        loop = scheduler._loop
        drain_error = RuntimeError('drain failed')
        close_error = RuntimeError('close failed')
        events = []
        real_stop = scheduler.stop
        def stop():
            events.append('stop')
            real_stop()
        def drain(_):
            events.append('drain')
            raise drain_error
        def close():
            events.append('close')
            raise close_error
        try:
            with mock.patch.object(scheduler, 'stop', side_effect=stop), \
                 mock.patch('softlab.huo.scheduler.serial.asyncio.all_tasks',
                            side_effect=drain), \
                 mock.patch.object(loop, 'close', side_effect=close):
                scheduler.close()
            self.assertEqual(events, ['stop', 'drain', 'close'])
            self.assertFalse(scheduler.work_drained)
            self.assertFalse(scheduler.loop_closed)
            errors = [error for _, error in scheduler.secondary_errors]
            self.assertIn(drain_error, errors)
            self.assertIn(close_error, errors)
        finally:
            loop.close()

    def test_first_interruption_wins_over_cleanup_interruption(self):
        monitor = Monitor()
        first = KeyboardInterrupt('acquisition interrupt')
        second = SystemExit('cleanup interrupt')
        def interrupt(_):
            raise first
        with mock.patch.object(SerialScheduler, 'stop', side_effect=second):
            with self.assertRaises(KeyboardInterrupt) as caught:
                monitor.run(after_row=interrupt)
        self.assertIs(caught.exception, first)
        result = first.count_result
        self.assertIs(result.cause, first)
        self.assertIn(second, [error for _, error in result.secondary_errors])
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)

    def test_never_observed_boundary_is_distinct_from_none(self):
        monitor = Monitor()
        cause = ValueError('boundary unavailable')
        def unavailable():
            raise cause
        result = run_count(monitor.process, 3, lambda: len(monitor.record),
                           unavailable)
        self.assertIs(result.cause, cause)
        self.assertIsNone(result.boundary)
        self.assertFalse(result.boundary_observed)
        self.assertEqual(monitor.events, [('configure', 5)])

    def test_interrupted_run_rejects_reuse_and_fresh_run_succeeds(self):
        first = Monitor()
        interruption = KeyboardInterrupt('stop')
        def interrupt(_):
            raise interruption
        with self.assertRaises(KeyboardInterrupt):
            first.run(after_row=interrupt)
        evidence = interruption.count_result
        trace = list(first.events)
        with self.assertRaises(RuntimeError):
            first.run()
        self.assertEqual(first.events, trace)
        second = Monitor()
        fresh = second.run()
        self.assertTrue(fresh.overall_success)
        self.assertNotEqual(evidence.run_id, fresh.run_id)
        self.assertEqual((evidence.completed_rows, evidence.boundary), (1, 1))

    def test_join_exception_is_primary_and_not_retried(self):
        monitor = Monitor()
        cause = RuntimeError('join failure')
        with mock.patch.object(monitor.process, 'join', side_effect=cause):
            result = monitor.run()
        self.assertIs(result.cause, cause)
        self.assertEqual(monitor.events, [('configure', 5)])
        self.assertTrue(result.work_drained)
        self.assertTrue(result.loop_closed)


    def test_constructor_failure_preserves_primary_and_close_failure(self):
        loop = asyncio.new_event_loop()
        primary = RuntimeError('initialize failed')
        secondary = RuntimeError('constructor close failed')
        try:
            with mock.patch('softlab.huo.scheduler.serial.asyncio.new_event_loop',
                            return_value=loop), \
                 mock.patch.object(SerialScheduler, '_initialize',
                                   side_effect=primary), \
                 mock.patch.object(loop, 'close', side_effect=secondary):
                with self.assertRaises(RuntimeError) as caught:
                    SerialScheduler()
            self.assertIs(caught.exception, primary)
            self.assertIs(primary.__cause__, secondary)
            self.assertFalse(loop.is_closed())
        finally:
            loop.close()

    def test_async_and_awaitable_reset_are_rejected_without_retry(self):
        from softlab.huo.process import reset_after_count
        monitor = Monitor()
        result = monitor.run()
        original = (result.run_id, result.completed_rows, result.boundary,
                    list(monitor.events))
        calls = []
        async def async_reset():
            calls.append('ran')
        with self.assertRaises(TypeError):
            reset_after_count(result, async_reset)
        self.assertEqual(calls, [])
        coroutine = async_reset()
        def returns_coroutine():
            calls.append('called')
            return coroutine
        with self.assertRaises(TypeError):
            reset_after_count(result, returns_coroutine)
        self.assertEqual(calls, ['called'])
        self.assertIsNone(coroutine.cr_frame)
        self.assertEqual((result.run_id, result.completed_rows, result.boundary,
                          monitor.events), original)


    def test_runner_constructor_dual_failure_and_interruption_precedence(self):
        class ImmutableError(Exception):
            def __setattr__(self, name, value):
                raise TypeError('immutable')
        for primary, cleanup in (
            (ImmutableError('init'), RuntimeError('close')),
            (RuntimeError('init'), KeyboardInterrupt('close')),
            (KeyboardInterrupt('init'), SystemExit('close')),
        ):
            with self.subTest(primary=type(primary), cleanup=type(cleanup)):
                monitor = Monitor()
                loop = asyncio.new_event_loop()
                try:
                    with mock.patch('softlab.huo.scheduler.serial.asyncio.new_event_loop',
                                    return_value=loop), \
                         mock.patch.object(SerialScheduler, '_initialize',
                                           side_effect=primary), \
                         mock.patch.object(loop, 'close', side_effect=cleanup):
                        if isinstance(primary, Exception) and isinstance(cleanup, Exception):
                            result = monitor.run()
                        else:
                            expected = primary if not isinstance(primary, Exception) else cleanup
                            with self.assertRaises(type(expected)) as caught:
                                monitor.run()
                            self.assertIs(caught.exception, expected)
                            result = expected.count_result
                    self.assertIs(result.cause, primary)
                    self.assertIn(cleanup, [error for _, error in result.secondary_errors])
                    self.assertFalse(result.work_drained)
                    self.assertFalse(result.loop_closed)
                    self.assertFalse(loop.is_closed())
                    self.assertEqual(monitor.events, [('configure', 5)])
                finally:
                    loop.close()

if __name__ == '__main__':
    unittest.main()
