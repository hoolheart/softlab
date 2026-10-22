# M01-W03-001 implementation and Developer self-checks

Developer: sw-tom. 2026-10-22 Asia/Shanghai.
Preimplementation gates: plan45ee0bb/Developer9a8347f, design125d6be/Architect21d2736,
feature tests4e9a3f7/b4dcad3 with expected-red9f7f6c9. Tester fixture correction
9f956e7/evidence27723b4 preceded resumed implementation.
Implementation `d48e925`; guide `1ab392a`; pushed before handoff.
State: **Developer self-checks PASS; independent code review/testing pending**.

## Scope and behavior

Only examples/thermal_experiment.py and docs/user-guide/thermal-experiment.md
changed. Closed version1 thermal_manifest JSON text attaches to completed
acquisition before any writer construction. Schema/type/finite/duplicate/key
validation rejects malformed/unsupported input without implicit conversion or
fallback serialization. Declared real fields normalize to float; counts exclude
bool. Producer source identity captures exact reference-file SHA256 byte scope
and source-relative tracked Git revision/repository dirty status, or explicit
null/reasons when unavailable. Git uses bounded argument-list subprocess calls;
unrelated caller cwd does not supply identity. No environment lock is claimed.

Optional envelope capture precedes destructive legacy copy loading; required
legacy keys remain three. Typed inspection validates actual disk-derived UUID,
ordered schema/units, counts, finite samples, boundaries/time/power and independent
delayed Euler recurrence. Present-invalid/unknown schema/version errors never
silently downgrade to absent legacy context or substitute reader defaults/source.
_copy-only _reload_group and successful run signatures remain compatible. Current
RunResult remains authoritative for post-snapshot storage/plot/cleanup; partial
acquisition skips manifest/save/plot. W02 structured helper cleanup is preserved.

New inspect_stored and mutually exclusive CLI inspection recover group/context in
a fresh interpreter, with JSON-only stdout and informational backend prints on
stderr. No sidecar/library/backend/dependency/config/version change. The guide
records closed fields/types, byte scope/uncertainty, snapshot timing, changed
configuration and legacy/malformed/version journeys. Existing tracked debt remains
open under copy-only/text-hydration/private-executor measures.

## Actual checks and corrected fixture disposition

First implementation self-check found two integer-dataset fractional tamper
subcases and three legacy acquisition-Mock cleanup subcases could not reach their
intended assertion/writer. Developer paused mutation/testing and reported them.
Tester independently corrected only fixtures in9f956e7 and documented27723b4;
Developer never edited tests or bypassed required pre-save validation. Those
Tester-owned failures are not closed by this Developer record.

After resume, corrected source-relative tracked-path resolution for the nested
examples directory and used finite absolute1e-10 tolerance for accumulated final
simulation time. No test changes by Developer. Executed final implementation
bytes subsequently committed d48e925, repository root, .venv/bin/python3.13.15,
MPLBACKEND=Agg, MPLCONFIGDIR=/tmp/softlab-w03-mpl,
XDG_CACHE_HOME=/tmp/softlab-w03-cache:

- `-W error -m unittest discover -s tests -p 'test_thermal*.py'`: **55 PASS**,
  30.255s, including new manifest12, metadata3, W01 19 and W02 21; no skips.
- `-W error -m unittest discover -s tests -p 'test_*.py'`: **272 PASS**,
  31.863s, no skips or Python warnings; includes characterization and all retained
  compatibility assertions.
- `-W error -m compileall -q softlab examples tests`: exit0, no diagnostics.
- `-W error -c 'import softlab; print(softlab.__version__)'`: exit0,0.3.0.
- Executed guide normal/changed generation and fresh-process JSON inspection in
  temporary substituted output paths, asserting decoded float dt, actual source
  revision/dirty boolean and eight/five numeric rows. Executed the verbatim guide
  legacy/malformed/version fixture heredoc with temporary source-path substitution:
  legacy inspected successfully as unavailable/null; corrupt and version999
  each exited1 with stderr/no stdout. All temporary files removed.
- Explicit dt0.1/points6 real execution confirmed accumulated-time consistency and
  full successful output. Actual tracked source identity is available despite
  nested examples directory; real-source working tree was dirty as expected.
- `git diff --check`: exit0. Scope inspection shows only example/guide plus these
  process records; no generated experiment files tracked.

Named warning-as-error checks reported no Python warnings/cache-permission issues;
backend connection prints are informational. No unconfigured lint/static-analysis,
local Python3.9/Ubuntu, hardware, calibration/authenticity or complete-reproducibility
claim. Exact configured3.9/3.13 CI remains an integration gate. Independent Designer
review and Tester verification/owned closure remain pending. Developer does not
approve own code or close reviewer/Tester findings. Board handoff is Code Review;
this evidence/board commits and pushes immediately as docs(log).

## CR-W03-01 repair and review iteration 2 handoff

Review081ce4b and Testerbc21224/evidencede48c45 require exact logical boundary
validation. Fix341c55b is pushed: tick/measurement_source_tick now use exact array
equality against integer sequences; no rounding/coercion repairs stored values.
Existing finite-sample checks and absolute1e-10 thermal/time comparisons remain.
No test, guide, library/config/dependency changes. Review issue remains OPEN for
Designer closure; Tester owns independent regression verification/failure closure.

Final fix bytes self-checked in the same Python3.13.15/Agg/writable-cache environment:
`-W error -m unittest discover -s tests -p test_thermal_manifest.py` **13 PASS**,
11.100s; full `-W error -m unittest discover -s tests -p 'test_*.py'` **273 PASS**,
31.751s, no skips/Python warnings. Warning-as-error compileall softlab/examples/
tests and import/version0.3.0 exit0; git diff --check exit0. Informational backend
prints remain. No broader platform/lint/CI claim. Independent Designer iteration2
is next; Developer does not approve own repair or close findings.
