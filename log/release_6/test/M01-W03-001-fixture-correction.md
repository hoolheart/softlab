# M01-W03-001 targeted test-fixture correction

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Test correction9f956e7 on codex/m01-w03-stored-context. Developer implementation
remains intentionally uncommitted in examples/thermal_experiment.py; Tester
neither edits nor stages it. Observed local source SHA256:
14aad711c3e01c20419fb713001b3004f2817821c6ddebcc52dd7628479d4ceb.
Disposition: **two Tester fixture defects confirmed and corrected**. No product
failure/acceptance or committed implementation validation is inferred.

## Integer sample tamper

A real temporary synthetic execute/HDF5 probe showed tick int64 value3 and
measurement_source_tick int64 value2 remain unchanged after `+=0.25`; HDF5
converts the fractional assignment back to the integer dtype. Temperature float64
21.9 becomes22.15. Thus the two integer subcases never corrupted a sample, making
an expected rejection invalid. Tester changed the offset to representable `+1`
for every targeted column and added an explicit readback inequality assertion
before inspection. No rejection expectation or production validation was removed.

Actual repository-root command, with .venv/bin/python3.13.15 and environment
MPLBACKEND=Agg, MPLCONFIGDIR=/tmp/softlab-w03-mpl,
XDG_CACHE_HOME=/tmp/softlab-w03-cache:
`-W error -m unittest discover -s tests -p test_thermal_manifest.py -k
actual_numeric -v`: exit0, one method/five column subcases,0.203s,OK.
All target samples now actually change and are rejected by inspection on the
Developer's current local source. Synthetic temporary output is removed.

## Independent cleanup fixture

The existing W01 loop-drain/close fixture intentionally mocked _acquire with
record=[] to isolate primary writer-save failure and independent cleanup.
W03's approved pre-save manifest work now inspects record.columns, so this
synthetic list fails before the mocked writer is constructed; all3 subcases
miss their intended 'primary marker'. Actual before-correction command:
`-W error -m unittest discover -s tests -p test_thermal_example.py -k
loop_drain_and_close -v`: exit1,one method,0.007s,three failures showing
`'list' object has no attribute 'columns'`, not the intended save operation.

Tester isolated _make_manifest/_encode_manifest within this cleanup-only fixture,
leaving actual schema validation to the real-group manifest tests. The mock values
are plumbing placeholders, not asserted valid manifests. Added an explicit
writer.save_group.assert_called_once_with(original), retaining every original
primary/cleanup marker, cancellation, gather, disconnect and loop-close assertion.
No production fallback or validation relaxation introduced. Same targeted command
after correction: exit0,one method/three subcases,0.008s,OK.

## Scope and handoff

`git diff --check`: exit0. Staged paths were only tests/test_thermal_manifest.py
and tests/test_thermal_example.py; source commit9f956e7 pushed independently.
No Python warning or skip reported in these named checks. Tester closes only
these demonstrated fixture-authoring failures. Other W03 red/product outcomes
remain Developer work and require committed-source review/independent testing;
no full suite or source-identity defect investigation was performed here.
Board remains Development. This completed record commits/pushes immediately as
docs(log). Expected remaining worktree modification is Developer's source file;
HEAD/upstream equality does not imply a clean worktree or accepted implementation.
