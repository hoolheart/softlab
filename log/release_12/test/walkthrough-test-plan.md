# Release 12 (M03 Phase 4) — Walkthrough / Comparison Test Plan

Owner: Tester (sw-mike) | 2026-11-03 Asia/Shanghai
Status: **DRAFT — for developer review (sw-tom); no test code and no source
changes before approval**
Branch: `codex/m03-p4-acceptance`
PRD: [log/release_12/prd.md](../prd.md) — **M03-AC05**, **M03-AC06** (Phase 4
row of `plan/milestone-03.md`)
Binding inputs:
- [p4-oracle-design.md](../design/p4-oracle-design.md) — **FROZEN** at
  `63ed461` (revised per review), architecture **APPROVED** at `d8d8744`.
  This plan quote-checks against that freeze and never re-derives, relaxes or
  silently edits any frozen oracle, tolerance, exclusion or step. Any
  deviation observed in execution reopens the freeze through review.
- [p3-storage-design.md](../design/p3-storage-design.md) §3.5.1 (rule 5
  ordering), §3.6/§4 (bootstrap inspection, fresh copies, F-A guard);
  [p1-boundary-decision.md](../design/p1-boundary-decision.md) §1.1, §2, §4
  (D1/D2, F-A…F-D dispositions).
- [characterization-results.md](characterization-results.md) — C1/C3/C5a/C5b
  evidence and the execution-record style reused in §W4.
- [storage-test-results.md](storage-test-results.md) — executed Phase 3
  baseline (storage seam green; reused unchanged per the evidence-reuse
  rule).
- [p4-oracle-design-review.md](../reviews/p4-oracle-design-review.md) — the 5
  findings this plan operationalizes as hard requirements.

Format conventions follow the Phase 1–3 plans: assertion-based, one defect
per failure, synthetic temporary data only, literals quote-checked against
the freeze, discrepancies flagged never guessed, unrun gates never marked
pass.

## Scope and ground rules

This is a **planning document only**. It defines the Phase 4 walkthrough and
comparison scenarios to be **executed after** sw-tom's Phase 4 implementation
(thermal integration already exists; signal §5.1 wiring and the
user-facing guidance update are Phase 4 deliverables — see Dependencies).
No test code is written and nothing under `softlab/` or `examples/` is
modified by the tester.

Every scenario runs on **synthetic data in `tempfile.TemporaryDirectory`
roots** (`prefix='softlab-m03-p4-*'`); no fixture touches the repository
tree, real instruments, or any pre-existing experiment data. Originals are
never loaded in place; all inspection goes through the Phase 3 bootstrap on
fresh byte copies (F-A guard, p3-storage-design §3.6) with per-run sha256
before/after preservation asserted (C3 discipline).

Two result artifacts are produced by execution of this plan:
1. This plan's scenario results, recorded in
   [walkthrough-results.md](walkthrough-results.md) (template in
   "Results recording" below) with **per-scenario verdicts and explicit
   traceability to M03-AC05 / M03-AC06**;
2. Verbatim environment/warning evidence inside that report per freeze §6.

### Frozen literals reused verbatim (never re-declared test-local copies)

All oracle values, tolerances and exclusions are **imported or transcribed
from the freeze at `63ed461`**, not re-derived at run time:

- §2.1 thermal v1 oracle: `tick=[1..8]`,
  `measurement_source_tick=[0..7]`, `sim_time_s=[1.0..8.0]`,
  `commanded_power_w=[10.0]*8`, eight frozen `temperature_degc` doubles,
  `timestamp` excluded; relations `measurement_source_tick == tick-1`,
  `sim_time_s == tick*1.0`, `completed_rows == requested_rows ==
  final_tick == 8`, `final_sim_time == 8.0`, manifest `version == 1`,
  `clock.measurement_delay_ticks == 1`, `clock.source_tick == 'tick-1'`.
- §2.2 thermal v2 oracle: same except `measurement_source_tick=[1..8]`,
  the post-step temperature vector, manifest `version == 2`,
  `clock.measurement_delay_ticks == 0`, `clock.source_tick == 'tick'`.
- §2.3 signal oracle: declared order `(timestamp, signal, reference)`
  (**non-lexicographic** — by-name reconstruction in envelope order is
  mandatory); `signal=[6,7,8]`, `reference=[3,4,5]`, caller-derived
  `difference=[3,3,3]` (never a stored column); read trace
  `[('signal',1),('reference',1),('signal',2),('reference',2),
  ('signal',3),('reference',3)]`; non-consumptive reads;
  `completed_rows == requested_rows == 3`.
- §3 tolerances: integers/config/identity/structural facts → exact `==`;
  `sim_time_s`, `commanded_power_w` → exact `==`; thermal
  `temperature_degc` → `math.isclose(actual, expected, rel_tol=0.0,
  abs_tol=1e-10)` element-wise. Nothing looser is permitted.
- §4 exclusions: applied **only** to the named nondeterministic fields
  (`timestamp` values/wall-clock fields, `execution_id`/`group_id` *values*,
  `source.digest`/`git_revision`/`git_dirty` values, environment-fact
  values, `thermal.png` bytes, cross-run file byte hashes). Every §4
  "separate assertion retained" check is executed **per run**.

### Dependencies on Phase 4 implementation (must exist before execution)

These are asserted against the implementation, never assumed. If any is
missing at execution time, the affected scenario is recorded **BLOCKED —
dependency absent** (a reported blocker, not a silent skip):

- **[DEP-1] Signal owned-persistence wiring** (freeze §5.1, all six
  required inputs): acquisition-success gate before any envelope/save;
  identity inputs (`execution_id = str(result.run_id)`, fresh distinct
  `group_id`); `build_run_group(name, envelope)` + public
  `DataGroup.add_record(record)` before `save_run_group`; envelope built
  via `build_signal_payload(points=3, offsets={'signal': 5,
  'reference': 2})` and `build_envelope(profile='signal', ...,
  requested_rows=3, completed_rows=3, ...)` with declared columns in
  declaration order; signal-owned `software`/`source` facts and an explicit
  HDF5 target path under the temp root; `save_run_group` returning an
  asserted-success `StorageReport` and owned resources disconnected.
  **§W3 is entirely dependent on DEP-1.**
- **[DEP-2] Unchanged thermal entry seam** (freeze §5.2 step 2): the real
  signature `execute(config, output_dir, *, stop=None, after_row=None,
  adapter='coordinated')` with adapter as the only call-side timing
  selector. The plan probes the signature; there is no `delay` parameter
  and none may be added by tests.
- **[DEP-3] User-facing run-record guidance** (sw-tom's Phase 4 docs
  scope): the guidance document(s) covering reopen/inspect/rerun of a
  supported record exist and describe the actual behavior. §W7 asserts
  against them once written; it is **BLOCKED — dependency absent** until
  they land.
- **[DEP-4] Phase 3 seams unchanged**: `tests/run_record_inspect_bootstrap.py
  <path> --profile {thermal|signal}` exits 0 with a well-formed JSON payload
  on stdout; `softlab.shui.run_storage.inspect_run_group` decode path and
  the Phase 2 codec literals are importable and identical to the frozen
  values.

## Scenarios

### W1 — Thermal v1 (`adapter='coordinated'`): original vs clean-session rerun

Traceability: **M03-AC05** (freeze §5.2, §5.3; oracle §2.1).

1. **Declare inputs** (driver source, before any execution): the §1 thermal
   v1 row — `ThermalConfig()` defaults (`initial_temperature=20.0`,
   `ambient_temperature=20.0`, `heat_capacity=10.0`, `conductance=1.0`,
   `dt=1.0`, `power=10.0`, `points=8`), adapter `'coordinated'` — plus the
   §2.1 oracle table, §3 tolerances, §4 exclusions.
2. **Session A (original recording):** fresh Python process; run
   `examples/thermal_experiment.execute(config, output_dir, adapter=
   'coordinated')` in temp root A. Assert `result.overall_success`;
   disconnect owned resources; record `execution_id_A`, `group_id_A`,
   artifact path `A.h5`. Expected outcomes follow from the adapter: delay 1,
   manifest v1, `source_tick='tick-1'` — these are asserted as outcomes,
   never passed as inputs.
3. **A save verification:** `A.h5` exists; record `sha256_A_before`.
4. **Session B (clean-session rerun):** a **separate fresh Python process**
   and separate temp root B; same declared inputs; produce `B.h5`,
   `execution_id_B`, `group_id_B`. Assert distinct identities:
   `execution_id_B != execution_id_A`, `group_id_B != group_id_A`.
5. **B save verification:** as step 3 for B (`sha256_B_before`).
6. **Separate-process inspection:** for each artifact, run
   `python tests/run_record_inspect_bootstrap.py <path> --profile thermal`;
   exit code 0; parse JSON payload from stdout; warnings fatal except the
   approved recorded baselines (freeze §6 item 4). Inspections run twice
   per original on **fresh copies** (F-A guard); originals never loaded in
   place; `sha256_A_before`/`sha256_B_before` unchanged throughout.
7. **Oracle comparison (per artifact):** decode the envelope; iterate the
   envelope's **declared column sequence** and retrieve each same-named
   stored dataset (p3 §3.5.1 rule 5 — disk enumeration order is
   non-semantic); compare sample arrays **by row index** against §2.1 under
   §3 (integers/`sim_time_s`/`commanded_power_w` exact `==`;
   `temperature_degc` via `math.isclose(rel_tol=0.0, abs_tol=1e-10)`);
   verify all §2.1 timing/structural relations and every §4 retained
   assertion per run (timestamp column present, unit `'s'`, finite values;
   `execution_id != group_id`; `group_id == str(DataGroup.id) ==` HDF5
   group name `==` on-disk `id` attr; `execution_id` in no stored UUID
   slot; source states `{state:'available', value:...}` or
   `{state:'unavailable', reason:<bounded code>}`; `thermal.png` exists).
8. **A-vs-B comparison:** all non-excluded columns match by row index under
   the same §3 tolerances; all §2.1 timing/structural relations agree
   across runs; **recorded-configuration comparison (freeze §5.3)**: stored
   `payload.configuration` equals all seven frozen `ThermalConfig` values
   exactly; recorded adapter equals `'coordinated'`; manifest `version`,
   `clock.measurement_delay_ticks`, `clock.source_tick` match the §2.1
   literals (including the corresponding legacy configuration/manifest
   relations where applicable). §4 excluded fields are never compared
   across runs; their per-run retained assertions already passed in step 7.

**Pass:** all of freeze §5.3 holds for W1. Any single failure → scenario
FAIL, defect filed to sw-tom, failure closure owned by tester.

### W2 — Thermal v2 (`adapter='direct'`): original vs clean-session rerun

Traceability: **M03-AC05** (freeze §5.2, §5.3; oracle §2.2).

Identical structure to W1 with: adapter `'direct'`; §2.2 oracle
(`measurement_source_tick=[1..8]`, post-step temperature vector); expected
outcomes delay 0, manifest `version == 2`, `clock.source_tick == 'tick'`;
recorded-configuration comparison against the v2 literals. Steps 1–8 and
pass criteria as W1, mutatis mutandis. W1 and W2 use **independent process
pairs** (A1/B1, A2/B2) — no shared temp root, driver state, or in-memory
object between them.

### W3 — Signal monitor: original vs clean-session rerun (DEPENDENT ON DEP-1)

Traceability: **M03-AC05** (freeze §5.1, §5.2, §5.3; oracle §2.3).

**Dependency gate:** if DEP-1 (freeze §5.1 six required inputs) is absent,
W3 is recorded **BLOCKED — dependency absent** with the missing inputs
itemized, and reported to sw-prod/sw-tom. No partial substitute (e.g.
re-deriving a stored artifact by hand) is permitted — the stored-path
round-trip of `[6,7,8]`/`[3,4,5]`/`[3,3,3]` through
`save_run_group`/`inspect_run_group` is precisely the Phase 4 evidence gap
(freeze §8 item 2) and must be executed, not simulated.

With DEP-1 present, steps 1–8 mirror W1/W2:

1. Declare §1 signal row: `points=3`, frozen offsets
   `{'signal': 5, 'reference': 2}`, §2.3 oracle (declared column order
   `(timestamp, signal, reference)` — non-lexicographic), §3, §4.
2. **Session A:** fresh process; `examples/signal_monitor.acquire(points=3)`
   followed by the DEP-1 wiring. Assert the **acquisition-success gate**
   before any envelope/save; assert `StorageReport` success; record
   `execution_id_A = str(result.run_id)`, fresh `group_id_A`, `A.h5`;
   disconnect owned resources.
3–5. Save verification and **Session B** clean-process rerun in a separate
   temp root with the same declared inputs; identities asserted distinct.
6. Separate-process inspection: `python tests/run_record_inspect_bootstrap.py
   <path> --profile signal`; exit 0; well-formed JSON; warnings fatal except
   approved baselines; two inspections per original on fresh copies;
   sha256 preservation.
7. Oracle comparison per artifact with **by-name reconstruction in envelope
   order** (mandatory — no backend repair is introduced): `signal ==
   [6,7,8]`, `reference == [3,4,5]` exact `==`; caller-derived
   `difference == [3,3,3]` recomputed from stored source samples and
   asserted **absent from stored columns**; per-tick read trace
   `[('signal',1),('reference',1),...,('signal',3),('reference',3)]`;
   non-consumptive reads; `completed_rows == requested_rows == 3`.
8. A-vs-B comparison of non-excluded columns/relations under §3, plus
   recorded-configuration comparison (freeze §5.3): stored
   `payload.points == 3`; stored `payload.read_policy` equals
   `{semantics: 'tick_plus_offset', consumptive: false, offsets:
   {'signal': 5, 'reference': 2}}` exactly; `scheduler.ownership ==
   'run_count_owned'`; `derived.difference.storage ==
   'caller_derived_not_stored'`.

**Pass:** all of freeze §5.3 holds for W3. Any deviation between the C5b
in-memory evidence and the executed stored path **reopens the freeze
through review** — it is reported, never silently edited into the oracle.

### W4 — Fresh-environment walkthrough

Traceability: **M03-AC06** (freeze §6, all five requirements).

Executed once, in a **genuinely fresh environment** — activating the
preexisting project `.venv` does **not** satisfy this scenario:

1. **Environment creation (actual, recorded):** run a real environment
   creation command, e.g. `python3 -m venv /tmp/softlab-m03-p4-freshenv`
   (or an equivalent isolated creation command), recording the selected
   interpreter path and evidence of isolation from existing site packages
   (e.g. `python -c "import sys; print(sys.prefix)"` and a probe that the
   project `.venv` site-packages are not visible). The creation command,
   its full output, and its exit code are recorded verbatim.
2. **Install current checkout (actual, recorded):** from the repository
   checkout, in the fresh environment, run `python -m pip install -e .`;
   record the resolved dependency set actually installed (pip output).
   **A network/package-index failure or unavailable interpreter is a
   recorded blocker/limitation — never a pass justified by falling back to
   an existing environment** (freeze §6 item 1).
3. **Documented command sequence (actual, recorded):** from the repo root,
   in the fresh environment, run and record verbatim (command, exit code,
   key output):
   - `python -m unittest discover -s tests -p 'test_*.py'` — **legacy
     suite green** is an explicit W4 assertion (full run, zero failures);
   - `python -m compileall -q softlab`;
   - `python -c "import softlab; print(softlab.__version__)"`;
   - the §5.2 rerun/inspection commands for W1, W2 and (if DEP-1 present)
     W3 — i.e. the W1–W3 sessions A/B driver invocations and the
     bootstrap inspection commands execute **in this fresh environment**.
4. **Versions actually observed:** Python, softlab, numpy, pandas,
   h5py/libhdf5, matplotlib, sqlite3 (module + library) as read **in the
   walkthrough environment**, in characterization-results execution-record
   style. Characterization-era values (Python 3.13.15, softlab 0.3.0,
   numpy 2.5.3, pandas 3.0.6, h5py 3.16.0 / libhdf5 2.2.0) are historical
   context only — the walkthrough records its own.
5. **Warnings verbatim:** every warning observed, categorized product /
   environment / CI-reporting artifact. Approved recorded baselines per the
   bootstrap `install_warning_policy`: the four pyparsing deprecations on
   Python 3.9, the pandas `pd.concat` FutureWarning on Python 3.9/pandas
   2.x, the sqlite3 `version` DeprecationWarning on Python ≥ 3.13.
   Recorded, never waived or suppressed; **any new warning fails W4 until
   dispositioned**.
6. **Limitations actually encountered** (missing optional extras,
   unavailable interpreters, creation/install failures) recorded as
   limitations, not silently scoped away.

**Pass:** items 1–3 succeeded with verbatim evidence, item 4's table is
complete, item 5 contains no undispositioned new warning, item 6 is honest.
If the fresh environment's Python differs from the characterization Python,
the freeze §8 item 3 note applies: cross-version exact-repr equality is not
asserted; only the §3 tolerance is — W1–W3 remain the arbiters.

### W5 — Clean-process walkthrough (no shared in-memory state)

Traceability: **M03-AC05** (freeze §5.2 steps 4 and 6; review finding 3).

Cross-cutting verification that the rerun and inspection evidence is
**process-level**, not object-level:

1. W1/W2/W3 sessions A and B each run in a **separate fresh Python process**
   (subprocess driver; distinct PIDs recorded in the results report). No
   driver process that recorded A also records or inspects B.
2. Inspection of every artifact runs via the bootstrap in **its own**
   process (`tests/run_record_inspect_bootstrap.py`), never via an
   in-process import of `inspect_run_group` against an already-decoded
   object.
3. **No shared in-memory state:** each process opens only its own temp
   root; environment variables that could carry state (e.g. softlab profile
   paths) are recorded; a control assertion shows the B process starts with
   no `softlab`-module state from A (fresh interpreter, no persisted
   pickles/shared caches crossing the boundary — no such mechanism exists
   in the plan and any discovered one is a defect).
4. Clean-process rerun determinism is demonstrated by W1–W3 step 8
   (A-vs-B agreement under §3) — the comparison is between independently
   produced artifacts, not between an artifact and its own in-memory echo.

**Pass:** W1–W3 executed as process-separated as described, with PIDs and
temp roots recorded; no inspection reused in-process decoding.

### W6 — Compatibility and debt verification

Traceability: **M03-AC05** ("existing v1/v2 behavior remain compatible"),
**M03-AC06** (legacy suite green), PRD "Limits and debt".

1. **Existing v1/v2 behavior:** the legacy thermal dual-write path and the
   M02-accepted v1 coordinated / v2 direct acquisition behavior remain
   unchanged by Phase 4 — verified by (a) the full legacy suite run in W4
   item 3 (zero failures), and (b) W1/W2's recorded configuration/manifest
   literals still matching the M02-accepted relations (including legacy
   configuration/manifest relations where applicable, freeze §5.3).
2. **Legacy suite green:** `python -m unittest discover -s tests -p
   'test_*.py'` exits 0 in the fresh environment (count of tests and the
   pre-existing/Phase 2/Phase 3/Phase 4 split recorded). A baseline-count
   regression is a defect.
3. **Remaining debt inventory re-confirmed** — each item is verified to
   still carry its established disposition, is **not** repaired, waived or
   silently promoted by Phase 4 changes, and its bounded local measure
   still holds:

   | Debt item | Established disposition (source) | Phase 4 re-confirmation |
   | --- | --- | --- |
   | R4-BASE-SCHED-01 / F-01, F-02 | Private generic scheduler routes completion through the singleton; avoid via the serial seam; generic repair separately scoped (PRD limits; p1-product-assessment) | Phase 4 wiring does not introduce the generic path; serial seam still used; suite green |
   | R4-BASE-META-01 | Metadata constructor behavior tracked; explicit metadata creation and disk hydration retained; repair separately scoped | Supported construction never depends on `DataGroup(meta=...)`; caller inputs not mutated (D1 evidence retained) |
   | R4-BASE-HDF5-01 | Legacy loader may remove reserved attributes; disposable-copy reads; repair separately scoped | F-A guard enforced in every inspection step (W1–W3 step 6); original-byte sha256 preservation asserted per run |
   | OBS-004 / OBS-005 / OBS-006 | Init-value hazard / lenient swallow / name collisions — avoid, retain fallback, retain escape hatch; no contract change | Unchanged; suite green; no Phase 4 dependency on affected paths |
   | DEFECT-2 | Deliberate programmer-error warning preserved for a neither-readable-nor-writable Parameter | Warning still fires as documented; no incidental change |
   | D1 / D2 | D1: additional R4-BASE-META-01 evidence, tracked; D2: SQLite empty plain-load meta, unsupported scope | Signal/thermal supported paths never depend on `DataGroup(meta=...)` or SQLite metadata persistence |
   | F-A / F-B / F-C / F-D | F-A: plain HDF5 loads unsupported (copy-only supported path); F-B: no arbitrary custom-key hydration, per-profile explicit recovery; F-C: `io.reload_group` excluded (`None` ≠ absence); F-D: `io.load_groups` excluded (empty ≠ empty database) | No supported flow in W1–W3 uses a plain original load, thermal's three-key hydration for signal data, `io.reload_group`, or `io.load_groups` — verified by inspecting the executed driver/inspection commands and the DEP-1 wiring |

4. **No new debt invisibility:** any Phase 4 code path observed to touch a
   routed-around path is reported as a defect, not worked around by the
   tests.

**Pass:** items 1–3 hold with evidence; debt table fully re-confirmed in
walkthrough-results.md.

### W7 — Guidance verification (DEPENDENT ON DEP-3)

Traceability: **M03-AC06** (PRD Phase 4 row: "update guidance").

**Dependency gate:** the user-facing run-record guidance is sw-tom's Phase
4 scope; W7 is **BLOCKED — dependency absent** until the guidance document
lands. Once present:

1. **Existence and location:** guidance for reopening, inspecting and
   rerunning a supported run record exists, is user-facing, and its
   location is recorded.
2. **Behavior match (procedure):** every command/procedure the guidance
   documents (inspect via the bootstrap, fresh-copy discipline, supported
   profiles, temp/synthetic-data practice) is executed exactly as written
   and observed to work — guidance that cannot be followed verbatim is a
   defect (doc bug reported to sw-tom).
3. **Behavior match (semantics):** guidance statements about scope match
   the freeze: acquisition-snapshot-only outcome stage; `execution_id` vs
   `group_id` distinctness; timestamp/UUID/environment exclusions; the
   supported HDF5-only backend; unsupported flows (plain loads,
   `io.reload_group`, `io.load_groups`, SQLite metadata) named as
   unsupported, not omitted.
4. **No overclaim:** guidance does not promise environment replay,
   physical validation, atomicity, failed-run persistence, or arbitrary
   reproduction beyond the recorded scope (PRD limits).

**Pass:** items 1–4 hold; every guidance claim traced to an executed
behavior in W1–W5.

## Hygiene (applies to every scenario)

- **Synthetic data only**, in `tempfile.TemporaryDirectory(prefix=
  'softlab-m03-p4-*')` roots; nothing written into the repository tree; no
  database/HDF5 artifacts committed.
- **Version-portable assertions:** no test depends on the characterization
  environment's exact versions; cross-version exact float reprs are not
  asserted (only the frozen §3 tolerance); version-dependent expectations
  (e.g. warning baselines) are gated on the Python/pandas versions actually
  observed in W4.
- **Verbatim warnings:** all warnings captured and recorded word-for-word,
  categorized product / environment / CI-reporting artifact; approved
  baselines recorded, never waived; new warnings fail the scenario until
  dispositioned.
- **Environment actually observed:** every version table and interpreter
  claim in the results report comes from commands run in the reported
  environment — never copied from the characterization record (freeze §6
  item 3 explicitly makes characterization values historical context only).
- **Warnings fatal** in inspection processes except the approved recorded
  baselines; inspection exit codes asserted.
- **Tester-owned failure closure:** every FAIL/BLOCKED item is filed to
  sw-tom with repro steps, tracked in the results report, re-tested after
  fix, and closed only by the tester.

## Results recording

All scenario outcomes are recorded in
[walkthrough-results.md](walkthrough-results.md) (created at execution
time), with **per-scenario verdicts** and the traceability matrix below.
Unrun scenarios remain **PENDING — not passed**.

### walkthrough-results.md structure (template)

```markdown
# Release 12 (M03 Phase 4) — Walkthrough Results
Owner: Tester (sw-mike) | <date> | Branch: codex/m03-p4-acceptance
Verdict: <ALL PASS / FAIL / BLOCKED>

## Execution record
<exact commands actually run, exit codes, verbatim — W4 items 1–3>

## Environment actually observed
<Python / softlab / numpy / pandas / h5py+libhdf5 / matplotlib / sqlite3 table>

## Warnings observed (verbatim, categorized)
<product / environment / CI-reporting artifact; baselines recorded, new ones dispositioned>

## Per-scenario verdicts
| Scenario | Description | Verdict (PASS/FAIL/BLOCKED/PENDING) | Evidence (paths, PIDs, temp roots, sha256) | Defects filed |
| W1 | Thermal v1 original vs rerun | | | |
| W2 | Thermal v2 original vs rerun | | | |
| W3 | Signal original vs rerun (DEP-1) | | | |
| W4 | Fresh-environment walkthrough | | | |
| W5 | Clean-process walkthrough | | | |
| W6 | Compatibility and debt verification | | | |
| W7 | Guidance verification (DEP-3) | | | |

## Debt re-confirmation (W6)
<the debt table with observed evidence per item>

## Limitations
<honest list: missing extras, unavailable interpreters, index failures, DEP gaps>
```

### AC traceability matrix

| Plan element | M03-AC05 | M03-AC06 |
| --- | --- | --- |
| W1/W2/W3 oracles, tolerances, exclusions, A-vs-B, recorded configuration (freeze §2–§5.3) | ✔ primary evidence | rerun/inspection commands also feed W4 |
| W4 fresh-environment creation/install/commands/versions/warnings/limitations (freeze §6) | rerun determinism cross-check | ✔ primary evidence |
| W5 process separation (freeze §5.2 steps 4/6) | ✔ | ✔ |
| W6 compatibility, legacy suite, debt inventory | ✔ | ✔ |
| W7 guidance vs actual behavior | | ✔ |

## Exit criteria for this plan (all required)

1. W1 and W2 **PASS** in full (freeze §5.3 — no partial pass).
2. W3 **PASS**, or **BLOCKED — dependency absent** with DEP-1 itemized and
   reported (a reported blocker, not a silent skip; signal evidence gap
   freeze §8 item 2 remains open and visible in the results report).
3. W4 and W5 **PASS** with verbatim environment/warning evidence.
4. W6 **PASS** with the complete debt table re-confirmed.
5. W7 **PASS**, or **BLOCKED — dependency absent** with the missing
   guidance reported to sw-prod.
6. Any deviation from the frozen oracle reopens the freeze through review
   — never a silent edit.
7. walkthrough-results.md complete with per-scenario verdicts, the AC
   traceability matrix filled, and every FAIL/BLOCKED carrying a filed
   defect with tester-owned closure.
