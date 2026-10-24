# M01-W03-001 exact logical-boundary review regression

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Review081ce4b requires correction of open CR-W03-01 on implementationd48e925.
Regression test committedbc21224 on codex/m01-w03-stored-context.
Verdict: **EXPECTED RED reproduced; Developer correction next**.
No production edits; only Designer may close CR-W03-01.

One focused method covers tick and measurement_source_tick independently with
real copied HDF5 files and real inspect_stored. It explicitly recreates each
chosen dataset as float64, preserving its shape/data/attribute definitions, so
the earlier integer-truncation fixture defect cannot recur. Before corruption,
inspection succeeds with integer-valued float samples, preserving their allowed
representation. Then row3 receives +1e-11; actual stored readback must differ
with delta strictly positive and less than1e-10. Inspection must visibly reject
this fractional logical identity rather than round/coerce/tolerate it. A finally
assertion verifies inspecting the corrupt file preserves its exact SHA256 bytes.
All originals/copies remain synthetic under automatically removed temporary paths.

## Actual committed execution

Repository root, .venv/bin/python3.13.15; MPLBACKEND=Agg,
MPLCONFIGDIR=/tmp/softlab-w03-mpl, XDG_CACHE_HOME=/tmp/softlab-w03-cache.

- `-W error -m unittest discover -s tests -p test_thermal_manifest.py -k
  fractional_logical -v`: exit1; one method/two subcases,0.240s,
  FAILED(failures=2). Each reaches expected-rejection assertion but no error is
  raised. Actual representable sub-tolerance corruption and permitted pre-corrupt
  float representation are proven before failure; hash finally assertions pass.
- `-W error -m unittest discover -s tests -p
  test_thermal_metadata_contracts.py`: exit0; three tests,0.048s,OK.
- `git diff --check`: exit0; no whitespace diagnostics. No skips/Python warnings
  or cache-permission diagnostics in named checks. No broad repeated suite run.

This reproduces actual logical-boundary acceptance, not a missing surface,
fixture no-op or physical-value tolerance concern. Existing three metadata
characterizations stay green; no baseline repair is authorized. Tester leaves
both new regression failures OPEN until correction/actual green verification.
Designer retains review-issue ownership. Board remains Code Review/Developer
correction pending. Full independent verification, principles/candidate CI and
release acceptance remain pending. Completed report/board commits/pushes
immediately with docs(log), separate from test source.
