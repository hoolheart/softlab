# M01-W02-001 nested cleanup review regressions

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Review subject: 6ca80ab, open CR-W02-01; implementation remains unchanged.
Test source: 9cfd570 plus 6061ebe on codex/m01-w02-controlled-execution.
Verdict: **EXPECTED RED reproduced; Developer correction pending**.
Only Designer may close CR-W02-01; Tester closes no review issue here.

Two additional methods in tests/test_thermal_execution.py cover six deterministic
subcases: figure-close, reader-disconnect and scratch-removal obstruction, each
alone and alongside an original operation exception. Real acquisition, HDF5
save/copy/read and plot helpers run; injected faults are beneath those helpers.
Figure savefig raises the exact primary sentinel, reader _load_group raises the
exact load sentinel, disconnect_impl records a cleanup marker while raw handle
fallback still closes it, and a real TemporaryDirectory wrapper obstructs its
cleanup. Synthetic outputs/scratch directories and remaining figures are removed
by test-owned cleanup after patches exit.

Assertions before the expected failure verify completed eight-row acquisition,
correct independent storage/plot disposition, unsuccessful overall result,
work-drained/loop-closed facts, actual scheduler stop/loop close, reader handles
closed and independent reader-then-scratch attempts. Plot faults retain verified
storage and borrowed figure; reader/scratch faults keep persistence failed and
plotting not attempted. Cleanup-only subcases require the structured marker in
cleanup_errors. Combined-failure subcases first require result.cause identity
with the primary operation sentinel, then require separate cleanup_errors.
No helper-message parsing or production repair is included.

## Actual checks

Environment: repository root, .venv/bin/python 3.13.15;
MPLBACKEND=Agg, MPLCONFIGDIR=/tmp/softlab-w02-mpl,
XDG_CACHE_HOME=/tmp/softlab-w02-cache. No dependency change.

- `.venv/bin/python -W error -m unittest discover -s tests -p
  test_thermal_execution.py -v` against committed 6061ebe: exit 1,
  21 methods, 7.120s, FAILED (failures=6); no errors/skips/Python warnings.
  The 19 existing feature methods pass. Three cleanup-only subcases show empty
  cleanup_errors; three combined faults show a generic helper RuntimeError
  replacing the original ValueError sentinel. This independently reproduces
  both parts of CR-W02-01.
- `.venv/bin/python -W error -m unittest discover -s tests -p
  test_huo_execution_contracts.py`: exit 0, 7 tests, 0.012s, OK; no skips.
- Initial new-regression run also exposed a Tester fixture issue: the existing
  borrowed-loop test called get_event_loop with no current loop under -W error,
  raising DeprecationWarning. Tester corrected loop retrieval to catch that
  precise no-current-loop condition without creating/replacing a borrowed loop.
  Corrected replay: 21 methods, 7.147s, six expected new subcase failures and
  no errors; the existing 19 feature methods remained green.
- `git diff --check`: exit 0. Only tests plus these process records changed.

No final full-suite/integration/guide acceptance is claimed while these reviewed
regressions remain red. The defect is structured evidence loss in existing new
RunResult reporting, not a dependency or unrelated baseline failure. Historical
library debt is unchanged. Board remains Code Review; Developer correction,
Designer issue closure and independent Tester verification are required.
