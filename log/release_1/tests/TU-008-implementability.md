# TU-008 acceptance-plan implementability review

Reviewer: sw-tom. Verdict: **CHANGES REQUESTED** — one required correction
to the gate record (issue 1); the seven cases themselves are sound and
implementable as designed. Reviewed candidate: tip `c4989a5` on
`codex/tu-008-integration` (production unchanged; docs-only task-start and
evidence commits).

Reviewed `log/release_1/tests/TU-008.md`, `tests/test_tu_integration.py`
(7 cases), `log/release_1/compatibility.md` (OBS/DEFECT list),
`pyproject.toml`, `softlab/huo/process` public API (`count`, `scan`,
`run_process`, `get_scheduler`), `softlab/tu` public re-export surface,
`tests/test_common_proc.ipynb` (nest_asyncio usage precedent), and the
TU-007 implementability review as format precedent. I reproduced the
recorded evidence on the tip:

- `.venv/bin/python -m unittest tests.test_tu_integration` — **7 tests, 6
  pass, 1 fail** (case 7, the designed RED: `docs/tu_extensions.md`
  missing). Matches the gate record exactly.
- `.venv/bin/python -W error -m unittest discover -s tests -p 'test_*.py'`
  — **99 tests, 98 pass, 1 fail** (the designed RED only); zero warnings
  escalated, zero skips. Matches the gate record.
- `.venv/bin/python -W error -m compileall -q -f softlab tests` — **exit
  1**, failing on TWO files (see issue 1; the gate recorded only one).

## Answers to the six review questions

### 1. Do the 6 passing cases genuinely guard combined usage?

**Yes, with one coverage observation (non-blocking).** The cases pin
genuine cross-module interactions, not just presence:

- Case 2 pins the real integration seam: `station.describe()` performs
  zero acquisitions (counter via `_before_get`), `supports()` detects the
  unsupported capability, `prepare()` gates `initialized`, legacy `count()`
  through `run_process` records **legacy value-only raw floats** (would
  break if `read()`/`Reading` leaked into `Parameter.__call__` or into
  huo's recording path), exactly one acquisition per count, and
  `cleanup()` releases the resource. A broken integration at any of these
  seams turns this red.
- Case 3 pins `scan()` recording swept setter and getter values beside
  the new `read()`/`describe_reading()` contract on the **same**
  parameter — the two paradigms provably coexist.
- Case 4 pins the full lifecycle journey (idempotent prepare, safe
  repeated cleanup, `initialized` transitions) through a huo run.
- Case 5 pins the theory round trip against live delegation
  (`model.value()` sees `configure()` writes — single source of truth)
  and validator rejection leaving values intact.
- Case 6 pins the migration-free surface: plain set/get/call, legacy
  value-only count records, and the characterized OBS-005 `{}` fallback
  on `features`.

No missing combination vs the PRD acceptance table: every PRD criterion
row in TU-008.md's mapping has a vehicle. **Observation (non-blocking):**
cases 2–4 use a plain virtual `Device` subclass; `VisaHandle`/`VisaCommand`
new APIs (`abort`, `confirm_stop`, `timeout_seconds`, `describe_operation`)
are exercised only for class-level presence in case 1. The PRD criterion
is "virtual/simulated device usage", which the virtual device satisfies,
and the simulated-VISA behavior is pinned by `test_tu_visa_contracts`
(17 tests, TU-006). Acceptable as scoped; if sw-prod wants the Visa path
combined with huo inside TU-008 itself, that would be an added case, not
a defect of the current seven.

### 2. Case 7 (docs artifact RED): location, format, content checklist

`docs/tu_extensions.md` is the right location and Markdown the right
format. No `docs/` directory exists at the repository root today; creating
one for the user-facing extension guide is the natural layout (log/ stays
process records; README.rst stays the overview per DC-3). The automated
gate is deliberately a presence/token check; content correctness is DC-2's
manual walk, which is the correct split.

Concrete section checklist for `docs/tu_extensions.md` (to satisfy "Added
APIs, limitations and migration-free usage are documented" and case 7):

1. **Header**: title, one-paragraph scope ("TU-002–TU-007 additions in
   softlab 0.3.0"), pointer back to README.rst.
2. **Parameter measurement contract**: `Parameter.describe()`,
   `describe_reading()`, `read()` returning `Reading`
   (value/quality/error/acquired_at), `schema_version` 1 description
   shape; legacy `()`/`get()` value-only semantics explicitly retained.
3. **Device lifecycle**: `Device.describe()`, `prepare()`, `cleanup()`,
   `initialized`, `supports()` — including idempotent prepare and
   repeated-cleanup safety, and that `describe()` performs no I/O.
4. **Station**: `Station.describe()` aggregate description.
5. **VISA lifecycle**: `VisaHandle.initialized`, `supports()`, `prepare()`,
   `cleanup()`, `timeout_seconds` (seconds-based; None disables;
   resolved OBS-001 semantics — default 5000 ms forwarding), `abort()`,
   `confirm_stop()`, `AbortReport`; `VisaCommand.execute()`,
   `describe_operation()`.
6. **Theory model**: `TheoryModel.describe()`,
   `supported_configuration()`, `configuration()` (JSON-serializable),
   `configure()` (unknown-key `KeyError`, non-mapping `TypeError`,
   validation rejection leaves values intact), `evaluate_features()`
   (default lenient `{}` fallback; `strict=True` re-raises the original
   error object).
7. **Worked examples**: (a) describe → prepare → `huo.count()` → cleanup
   on a virtual device; (b) legacy `scan()` beside `read()`; (c) model
   configuration round trip. Examples must be runnable as written
   (copy-paste from the test fixtures is acceptable).
8. **Limitations** (explicit section, must name at minimum):
   OBS-001 resolved timeout units; OBS-004 init-value hazard
   (`QuantizedParameter`/`VisaParameter` non-None settable `init_value`);
   OBS-005 lenient swallow on `features` and `evaluate_features()`
   (opt-in strict is the escape hatch); OBS-006 delegated-name collisions
   (explicit `_attributes`/`device()` lookup remains); DEFECT-2 deliberate
   warning for neither-settable-nor-gettable parameters; description
   serialization limits (JSON-safe scalar values; ndarray attribute
   values are the model author's responsibility per the TU-007 design
   handoff).
9. **Migration-free statement**: existing code that calls no new API
   needs no changes — original import paths, value-only records from
   `count()`/`scan()`, legacy `features`, builder helpers
   (`register_device_builder`, `get_device_builder`,
   `set_default_station`, `default_station`) all unchanged (pinned by
   case 6 and the guard suites).
10. **Versioning note**: all description payloads are `schema_version` 1.

### 3. DEFECT-1 raw-string fix — safe, but the disposition scope is wrong

The raw-string fix (`'\w+(\.\w+)*@\w+(\.\w+)+'` → `r'...'`) is **safe and
zero-behavior-change**: it sits inside the `if __name__ == '__main__':`
example block of `parameter.py:580-581`, is never imported, and the raw
form is value-identical (the non-raw form already yields the same string;
only the warning differs). **However, the gate's defect list is
incomplete**: my forced compile of `softlab` + `tests` under
`-W error` (the exact P1 command) fails on **two** files:

- `softlab/tu/station/parameter.py:580` (recorded by the gate);
- `softlab/jin/validator/implements.py:470-471` (**not recorded** — two
  `ValPattern('\w+...')` lines in the `__main__` example block).

A disposition that fixes only `parameter.py` leaves P1 step 1 red at a
file the record never mentioned. This is issue 1 below. No other file in
`softlab/` or `tests/` trips the forced compile (verified).

### 4. P2 package-artifact inspection — sound; installation acceptable

The procedure is sound and correctly scoped: temporary build directory,
artifacts never committed, version cross-check against
`softlab.__version__` (0.3.0, from `VERSION.txt` via
`[tool.setuptools.dynamic]`; `package-data` includes `VERSION.txt`, and
`sdist`/`wheel` will contain `softlab/tu/*` under the declared
`packages.find` include). Installing `build` into `.venv` is acceptable
and preferred over leaving it manual: it is a build frontend, not a
runtime dependency, and `pyproject.toml` deliberately has no dev
dependency group — creating one for a single end-of-sprint procedure
would be unjustified config churn. Record in the evidence: the install
command, `python -m build` invocation (note `--no-isolation` is a valid
offline alternative since `.venv` already has setuptools), and the
artifact listing. One caution to record: `python -m build` defaults to
build isolation, which needs network access to fetch
setuptools/wheel — if the sprint-end environment is offline, use
`--no-isolation` and say so in the record.

### 5. huo count/scan usage in cases 2/3 — reflects real public API

Confirmed against `softlab/huo/process/common.py`:

- `count('acquire', None, None, device.signal, times=3)` matches the
  documented key-parameter pattern (name, group, record, then
  parameter args, `times` as a `Counter` keyword).
- `scan('sweep', [device.signal], None, None, setter, [10.0, 20.0, 30.0])`
  matches the signature (name, getters, group, record, then
  setter/values pairs).
- `run_process(proc, self._scheduler, verbose=False)` with
  `get_scheduler().start()/stop()` mirrors the notebook's
  `nest_asyncio` + scheduler pattern (nest_asyncio is only needed inside
  a running Jupyter event loop; in unittest the scheduler is started
  synchronously, which is correct and needs no nest_asyncio).
- No `huo` production change is required or implied by any case; the
  gate's claim that huo is exercised read-only holds.

### 6. PRD TU-008 acceptance consistency — complete

Every clause of the PRD TU-008 row maps to a vehicle: public imports
(case 1), representative count/scan (cases 2, 3, 6), virtual device usage
(cases 2–4), numerical model (case 5), documented APIs/limitations/
migration (case 7 + DC-1..DC-3), recorded regression/compile/import
checks (P1), package-artifact inspection (P2), final architecture +
evidence-referenced acceptance (DC-4). Nothing dangles unaddressed the
way OBS-002 did in TU-006. DC-4's "successor" wording for
`log/release_1/arch-review.md` should be pinned to a concrete file path
at task end so the release acceptance can link actual evidence.

## Issues

1. **(Required) DEFECT-1 disposition scope is incomplete.**
   `log/release_1/tests/TU-008.md` records the forced-compile blocker at
   `parameter.py:580-581` only. The same `-W error -f compileall` command
   also fails on `softlab/jin/validator/implements.py:470-471`
   (`ValPattern('\w+@\w+(\.\w+)+')` and
   `ValPattern('\w+(\.\w+)*@\w+(\.\w+)+')` in the `__main__` example).
   Update the gate record (and the DEFECT-1 disposition required from
   sw-tom) to cover both files; otherwise P1 step 1 stays red after the
   prescribed fix. The fix itself is identical in both places: raw
   strings, `fix(tu)`/`fix(jin)` commits, zero behavior change.

## Non-blocking observations

1. **Case 7 token check is a presence gate, not a content gate.** Tokens
   like `read` and `supports` are substrings that could pass via
   incidental prose (e.g. "reading"). That is acceptable given DC-2's
   manual walk, but the walk checklist should enumerate the exact
   signatures/semantics (the section checklist in question 2 above) so
   DC-2 is auditable rather than impressionistic.
2. **Visa path not combined with huo inside TU-008** (see question 1).
   Scoped acceptably; record the cross-suite disposition (TU-006 visa
   contract suite) in the final evidence so the gap is explicit, not
   silent.
3. **DC-4 file path should be pinned** at task start rather than left as
   "or successor".
4. **P2 `--no-isolation` fallback** for offline environments should be
   noted in the procedure (observation in question 4).

## Verification performed for this review

- Reproduced the gate's recorded evidence on tip `c4989a5`: integration
  module 7 tests (6 pass, 1 designed RED); full discover 99 tests (98
  pass, 1 designed RED, zero warnings escalated under `-W error`).
- Ran P1 step 1 exactly as written: `-W error -m compileall -q -f
  softlab tests` → exit 1 on **two** files (the basis of issue 1).
- Read `count`/`scan`/`run_process` signatures in
  `softlab/huo/process/common.py` and `process.py`; confirmed case 2/3
  call shapes match and no huo edit is needed.
- Checked `pyproject.toml` packaging config (dynamic version from
  `VERSION.txt`, `package-data`, `packages.find`) — P2's artifact
  expectations are consistent with the declared configuration.
- Confirmed no `docs/` directory exists yet; `docs/tu_extensions.md`
  will be a new top-level path (intended).

This review implements nothing, does not claim testing or principle-gate
completion, and does not amend the test cases. After issue 1 is corrected
in `TU-008.md` (a docs-only gate-record edit), no re-review of the seven
cases is required — the cases stand as designed.
