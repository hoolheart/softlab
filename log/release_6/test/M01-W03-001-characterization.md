# M01-W03-001 metadata characterization and baseline

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Subject tests35d68f2 on codex/m01-w03-stored-context, from integrated setup
dev6d332ce; task activationb8ee187. Production source unchanged.
Verdict: **PASS for preimplementation compatibility characterization/baseline**.
Test plan is proposed; Developer review, detailed design, feature red tests and
implementation are pending. No W03 manifest/interface or acceptance certified.

## Source/callers and three committed assertions

Read current example ThermalConfig/RunResult/acquire/execute/run, _META_KEYS,
_load_group/_reload_group/_verify_round_trip, HDF5 save/load and DataGroup
constructor/meta behavior, existing W01/W02 tests and release_6 PRD/envelope.
Existing W02 callers constrain successful DataGroup return, distinct operational
outcomes/primary/cleanup, copy-only inspection, reset and fresh repeats.

| Existing behavior | Actual assertion / consequence |
| --- | --- |
| HDF5 metadata save uses str(value) | Exact explicitly encoded JSON string survives attribute save/read; json.loads restores null/bool/int/float/string/list/object types. Raw dict attribute is Python repr and fails JSON decoding, so a dict passed directly is not a typed envelope. |
| Legacy loader mutates open attrs and loses metadata | On an owned copy, raw load pops id/name/timestamp/backend; arbitrary text attr remains on disk but returned meta is empty. Direct DataGroup(meta=...) also drops metadata. Source digest remains unchanged because destructive loading touched only a copy. Existing debt is characterized, not repaired. |
| Example hydrates only existing legacy text and preserves numeric/schema | Real synthetic _acquire creates eight rows; explicitly retain only example/configuration/timing as a legacy group, save it, call actual _reload_group twice. UUID, those text values, JSON configuration points, column definitions/shape and all numeric values match; returned path names delivered source; source hash stays identical each time. No future manifest key/interface is assumed. |

Tests are tests/test_thermal_metadata_contracts.py. All backends disconnect,
private acquisition scheduler stops/loop closes, source/copies live only under
a TemporaryDirectory; no plot, real devices or experimental data accessed by
these three characterization tests. They intentionally assert known legacy debt;
this does not authorize extending the bounded work into a backend repair.

## Actual execution/environment

Repository root. Commands use .venv/bin/python, environment MPLBACKEND=Agg,
MPLCONFIGDIR=/tmp/softlab-w03-mpl, XDG_CACHE_HOME=/tmp/softlab-w03-cache.
Python3.13.15 conda-forge/Clang19.1.7, softlab0.3.0; NumPy2.5.3,
SciPy1.18.1, pandas3.0.6, h5py3.16.0, Matplotlib3.11.2, PyVISA1.16.2,
pyvisa-sim0.7.1. No packages/configuration changed.

| Actual command | Result |
| --- | --- |
| `-W error -m unittest discover -s tests -p test_thermal_metadata_contracts.py -v` | Exit0; three tests,0.047s,OK. Committed35d68f2 replay: three tests,0.049s,OK. |
| `-W error -m unittest discover -s tests -p 'test_*.py'` | Exit0; **260 tests,16.147s,OK**, no skips; includes unchanged W01/W02/legacy behavior. |
| `-W error -m compileall -q softlab examples tests` | Exit0; no diagnostics. |
| `-W error` import/version collection via sys/importlib.metadata | Exit0; actual versions above, softlab0.3.0. |
| `git diff --check` | Exit0; no whitespace diagnostics. |

All runs use warning-as-error. No Python warning, unexpected failure, skip or
cache-permission diagnostic reported; the first fresh cache generated only
Matplotlib's informational font-cache build message. Backend connection prints
are informational. This certifies named Python/compile checks, not unconfigured
linters or every system/platform warning. Test bytes were unchanged between full
baseline execution and source commit; focused tests are additionally replayed on
the committed subject. No new test-authoring failure occurred.

## Debt, applicability and handoff

R4-BASE-META-01 constructor loss/hydration and R4-BASE-HDF5-01 destructive attrs/
copy-only workaround are explicitly reproduced and remain unfixed. Raw dict
stringification is current backend behavior, not a new W03 regression or a JSON
contract for arbitrary metadata. R4-BASE-SCHED-01 private executor and W02
status/cancellation/drain characterization remain unchanged and green.
OBS-004/005/006/DEFECT-2 remain tracked historical out-of-scope debt with no new
failure/warning in named checks. No new prerequisite justifies a library change.

Only local macOS Python3.13 executed. Exact configured Ubuntu Python3.9/3.13
candidate CI remains a later integration gate; setup CI and historical257 tests
are separate evidence. Current260 is historical257 plus three new assertions,
not a W03 feature-completion claim. No future schema/type/source-identity/fresh-
process validation is inferred from legacy compatibility tests. Proposed plan
covers AC01–12 and leaves concrete interfaces to reviewed design. Developer plan
review precedes detailed design/Architect approval and feature red tests. Board
remains Design with Developer implementability review next. Completed logs commit/
push immediately as docs(log); no release acceptance or task integration claim.
