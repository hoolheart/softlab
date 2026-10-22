# M01-W03-001 independent test results

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Subject committed69045ec5d22ccdbf99325d9cc4185d96491e833b,
including implementationd48e925, guide1ab392a and exact-boundary repair341c55b.
Branch codex/m01-w03-stored-context; baseline integrated setupdev6d332ce.
Designer iteration2 APPROVED and closed CR-W03-01 before this testing gate.
Verdict: **PASS for independent local task verification and guide walkthrough**.
All Tester-owned scoped feature/fixture/regression failures are CLOSED by actual
green evidence below. Coordinator principle/formal gate, exact candidate CI/dev
integration and release-end architecture/PO acceptance remain pending.

## Executed commands/environment

Repository-root runtime commands use `.venv/bin/python -W error`, with
MPLBACKEND=Agg, MPLCONFIGDIR=/tmp/softlab-w03-mpl,
XDG_CACHE_HOME=/tmp/softlab-w03-cache. Local macOS26.6.2 arm64;
Python3.13.15 conda-forge/Clang19.1.7, softlab0.3.0. Actual version collection:
NumPy2.5.3, SciPy1.18.1, pandas3.0.6, Matplotlib3.11.2, Plotly7.1.0,
h5py3.16.0, PyVISA1.16.2, pyvisa-sim0.7.1, ipywidgets8.1.9, imageio2.37.4.
Versions came from sys/platform/importlib.metadata and import softlab; no package
or dependency configuration changed.

| Command after runtime prefix | Actual result |
| --- | --- |
| `-m unittest discover -s tests -p 'test_*.py'` | Exit0; **273 tests,31.972s,OK**, no skips. Includes unchanged W01/W02/legacy/Coordinator/resource fault coverage. |
| `-m unittest discover -s tests -p test_thermal_manifest.py -v` | Exit0; **13 methods,10.880s,OK**, no skips, including exact logical tick/source fractional subcases. |
| `-m compileall -q softlab examples tests` | Exit0; no diagnostics. |
| import/version collection via sys/platform/importlib.metadata | Exit0; softlab0.3.0 and actual versions above. |
| `/tmp/softlab-w03-guide-walkthrough.py` | Exit0; separate generation/inspection, changed configuration, legacy/malformed/version fixture commands and independent typed/schema/sample assertions PASS. |
| Python pathlib/re guide plus release_6 document checks | 19 Markdown files/35 explicit local links resolve; final newlines and no trailing whitespace pass before this report. |
| `git diff --check` / `git status --short` | Exit0, no diagnostics; clean before final records. |
| `git rev-parse HEAD '@{upstream}'` | Both69045ec5d22ccdbf99325d9cc4185d96491e833b before records. |
| `git diff --name-only 6d332ce HEAD -- softlab pyproject.toml` | Empty: no library/dependency/support configuration changes. |

No Python warnings, compilation errors, unexpected logging warnings, skips or
cache-permission diagnostics reported by named checks. Expected legacy failure
warnings are captured explicitly; backend connection prints are informational.
This is not certification of unconfigured linter/static-analysis/all-platform
warning absence. No source/tests changed by Tester during this verification.

## Independently executed guide

From repository root, use fresh paths under one automatically removed temporary
folder in place of fixed /tmp guide paths. Run normal generation and inspection
as distinct Python invocations, then repeat generation with documented
`--dt 0.5 --power 6 --points 5` and inspect in another new interpreter. Inspector
stdout parses as exactly one JSON object with available context, stored producer
manifest and numeric rows. Assertions check root schema/version/UUID, exact
configuration scalar types, declared column/dependent types, timing/snapshot
meaning, finite rows, exact logical boundaries and independent delayed-Euler
numeric oracle for both default8 and changed5 cases. Generated PNG exists.
Delivered HDF5 SHA256 remains unchanged after inspection.

Extracted and executed the guide's CONTEXT_FIXTURES heredoc verbatim except for
fresh source-path substitution; it creates only copies. Separate documented CLI
inspection confirms legacy exit0 with manifest null/context legacy_unavailable and
eight numeric rows; malformed and version999 exit1 with visible diagnostics and
no available-context stdout. Original source HDF5 hash unchanged. Every synthetic
walkthrough artifact was removed. No harness or product failure occurred.

Read guide for source digest byte scope/Git uncertainty, non-authentication and
non-locking limits, pre-persistence snapshot, no partial durable context, W02
outcomes/resources/reset/repeats and copy-only/backend limitations. Its commands
and API descriptions match actual independent execution; no prose-only claim
substitutes for fresh-process or numeric assertions.

## AC01–12 evidence and failure closure

| Acceptance | Actual evidence/outcome |
| --- | --- |
| AC01 | Complete closed root and required nested schema/model/equation/validity/units/config/procedure/columns/software/source/outcome assertions pass; manifest physically stored alongside data. |
| AC02 | Brand-new CLI interpreter reconstructs every stored manifest field and associated numeric rows; exact declared int/float/bool/container types checked without original live result. |
| AC03 | Real copied-file UUID/count/tick/units/clock/config tampering and actual numeric temperature/tick/source/time/power tampering rejected. New sub-tolerance fractional tick/source tests prove representable mutation and exact rejection while integral float storage remains accepted; corrupt input file hashes unchanged by inspection. |
| AC04 | Fresh default repeat distinct ID/equal config/numeric traces, changed dt/power/points recovered and independently recomputed delayed Euler fixture; guide confirms changed fresh-process result. |
| AC05 | Unsupported/wrong types, bool count, nonfinite values, nonstring keys, extra/missing keys, malformed/nontext/duplicate-key/nonfinite-token JSON rejected; declared real normalization and deterministic encoding pass. Encoding failure preserves cause/completed acquisition and prevents writer construction. |
| AC06 | Absent legacy context numerically inspectable with explicit unavailable manifest; present-invalid/malformed/nontext and unsupported schema/version visibly fail without downgrade/default repair. Separate guide processes confirm policy. |
| AC07 | Actual temporary source Git clean/dirty/unrelated-cwd fixture, missing-Git query fault with successful installed-style generation, unavailable digest reason and declared SHA256 byte scope pass. Reader cannot recapture producer identity; repeated inspection preserves source hash/identity. |
| AC08 | Stored completed-acquisition snapshot declares later stages not_yet_established; later plot failure retains verified storage while overall fails. Existing save/reload/nested cleanup faults preserve W02 stage/primary/structured-error outcomes; stop/model/sample partials remain memory-only. |
| AC09 | Full273 includes W01 numeric/schema/delay/UUID/plot/collision, W02 stop/ack/model/sample/reset/repeat/cleanup and three metadata characterizations; no library/backend format contract changed. |
| AC10 | Produced manifest removal fails delivery verification; real malformed/version/relationship copies fail inspection; source file integrity and ownership/fault matrix green. Save/reload may leave incomplete bytes; no atomic or byte-free promise inferred. |
| AC11 | Actual documented default/changed headless generation and fresh-process typed inspection plus verbatim legacy/malformed/version fixtures executed above; all sample/source-integrity assertions pass. |
| AC12 | Plan/design/owned review/test chain and local checks/environment/eleven-principle evidence recorded. Actual release-end architecture, coordinator principle/formal/exact candidate CI/dev integration and PO acceptance remain later gates, not marked complete by local test PASS. |

Tester closes original feature red4 failures/16 absent-surface errors from
M01-W03-001-red.md based on the now-green thirteen-method feature suite.
Tester closes both fractional-boundary regression failures from
M01-W03-001-review-regression.md by actual green subcases. The integer-truncation
and synthetic-cleanup fixture corrections9f956e7 retain real-mutation/writer-reached
assertions and pass in this committed full suite, so their provisional local
closures are now confirmed against committed code. No scoped test failure remains.
Designer-owned CR-W03-01 closure is the separate iteration2 approval, never a
Tester decision. No additional fixture correction was needed in this gate.

## Eleven-principle task evidence

| Principle | Current local evidence / remaining gate |
| --- | --- |
| 1 | Existing tu/huo/shui/jin ownership preserved; manifest example-local, no softlab changes. Actual W03 architecture closure remains Architect release-end work; planned envelope is not claimed current arch. |
| 2 | Characterization35d68f2 preceded design/code; legacy backend/caller contracts preserved by passing assertions. |
| 3 | Plan45ee0bb → Developer9a8347f → design125d6be/Architect21d2736 → redb4dcad3 → implementation/review/repair → Designer69045ec → this independent execution. |
| 4 | Setup integrated6d332ce before sole active implementation task; no closure task started or Done claim. |
| 5 | Tester owns assertions/results/failure closure; Designer closed review issue; coordinator/Architect/PO authorities preserved. UI N/A. |
| 6 | Subject/upstream equality and clean baseline verified; these completed records require immediate docs(log) commit/push before handoff. |
| 7 | Actual local full273/feature13/compile/import/guide/version/warning evidence; configured exact candidate Ubuntu3.9/3.13 CI still pending. |
| 8 | Existing R4/OBS/DEFECT debt explicitly open with bounded green measures; no silent waiver or new scoped failure/warning. |
| 9 | Synthetic temporary data/source Git fixtures and copy-only reads; no instrument access/shared-history mutation; owned cleanup and borrowed state assertions pass. |
| 10 | Scope example/tests/guide/process records; no dependency/support/storage-format/backend/framework change. |
| 11 | Reproducible commands/diff/links/scope and AC mapping complete locally; coordinator formal/principle verdict, exact candidate CI/dev, release architecture/PO acceptance remain pending. |

## Baseline disposition/limits

R4-BASE-SCHED-01 remains unfixed with private serial executor and legacy cancel/
status/drain characterization. R4-BASE-META-01 remains constructor loss, worked
around by explicit declared disk-text hydration. R4-BASE-HDF5-01 remains destructive
reader attrs, bounded by copy-only reopening/source-integrity tests. All local
measures regress green; manifest does not repair generic metadata/backends or
restrict arbitrary Parameter values. OBS-004/005/006/DEFECT-2 remain tracked
historical out-of-scope debt, with no new observed named-check failure/warning.
Any further prerequisite requires Architect/PO scope disposition.

Only local macOS Python3.13 executed. Exact candidate Ubuntu Python3.9/3.13 CI
must precede dev integration; prior setup CI is not this candidate's evidence.
Historical CI Node20/forced24 and Ubuntu26 notices remain disclosed baseline
platform annotations, not a whole-system warning waiver or new local warning.
No hardware/UI, durable partial persistence/resume, W04, universal provenance,
authentication/environment lock or package publication is certified. This PASS
does not accept the release or whole M01 milestone.
