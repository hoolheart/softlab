# M02-W02 pre-implementation red evidence

Owner: Tester | 2026-10-30 Asia/Shanghai

Command (repository root):

```sh
MPLCONFIGDIR=/private/tmp/softlab-mpl XDG_CACHE_HOME=/private/tmp/softlab-cache .venv/bin/python -W all -m unittest discover -s tests -p 'test_m02_w02_bounded.py'
```

Result: exit 1; unittest reported one loader error because the intended public
`StopRequest` import from `softlab.huo.process` does not yet exist. No test
assertions executed. This is expected pre-implementation red, not a test pass
or proof of behavioral coverage. The test suite defines synthetic monitor,
reference-policy, completion, stop, acquisition failure, callback failure and
preflight cases; further scheduler and cleanup cases are required before the
final verification. Python 3.13 was used; Python 3.9 and CI are not claimed.
An initial run without writable cache overrides emitted Matplotlib and
fontconfig cache warnings; the command above uses temporary cache locations.

## Review-finding regressions

Two independent tests were added for CR-W02-01 and CR-W02-02: action-raised
`TimeoutError` both with and without a deadline must return `False` and retain
the exception identity; a `BaseException` during scheduler stop must preserve
the original interruption while still draining and closing the loop and
recording cleanup diagnostics. The developer fixed both findings concurrently
before the new tests executed, so **no pre-fix red run was observed**. The
post-fix targeted command above passed all 21 tests. This is not retroactive red
evidence; the findings' pre-fix behavior is documented in the independent code
review, and reviewer reinspection remains required.

A further follow-up independently tests cleanup-origin `KeyboardInterrupt` and
`SystemExit`, including identity, completed acquisition, diagnostics and later
drain/close. The earlier precedence test covers simultaneous primary and cleanup
interruptions. The implementation was updated before execution; the 22-test
targeted suite passed, and no pre-fix red is claimed for this follow-up.
