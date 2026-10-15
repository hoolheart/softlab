# Code Review — R3-003: Cohesive coordinated simulation

Reviewer: sw-celeste | Date: 2026-10-15 | Re-confirmed: 2026-10-11
Branch: `codex/r3-003-coordination` | Head reviewed: `d5abc9b`
(re-reviewed at fix head `1aa961f`, in sync with
`origin/codex/r3-003-coordination`; fix range `ee4339a..1aa961f`
touches only `softlab/tu/simulation/object.py` docstrings and
`tests/test_tu_coordination.py` cosmetics — 12 insertions, 4
deletions, zero behavior change)
(in sync with `origin/codex/r3-003-coordination`; code head `8199d2b` —
commits `b5ce572`/`d5abc9b` touch `log/` only, verified by
`git diff 8199d2b..d5abc9b --stat -- softlab tests` = empty)
Range reviewed: `5d8fc6a` (red tests), `9a65f0f` (mock.py), `a1015e0`
(coordination.py), `f4ab4d0` (object.py helpers+guards), `f8535d9`
(station.py+exports), `63251c2` (test greening), `8199d2b` (refactor).
Binding specification:
[r3-003-detailed-design.md](../design/r3-003-detailed-design.md)
(APPROVED at `e8d9841`, issues closed at `f5d8680` — including the
NORMATIVE `sim_dt` setter `_busy` guard at D6 step 5, the explicit
endpoint resolution rule, the corrected P3-2 mutating-ops list and the
all-outputs snapshot cardinality) | Test plan:
[r3-003-test-plan.md](../test/r3-003-test-plan.md) (Revision 2, 41
cases) + [r3-003-baseline.md](../test/r3-003-baseline.md) | Architect
review: [r3-003-design-review.md](r3-003-design-review.md) (proof
attacks + 6 rulings) | Convention:
[r3-002-code-review.md](r3-002-code-review.md)

## Verdict: APPROVED

- **Total issues: 3** (critical: 0, high: 0, medium: 1, low: 2) —
  **all 3 CLOSED** at fix head `1aa961f` (re-confirmation below)
- The implementation is faithful to the approved design
  element-for-element — all twelve design decisions, all five CHK-03-1
  proof mechanisms, and the normative D6-step-5 `_busy` guard are
  present and runtime-verified; all four kill-mutations are killed by
  exactly the designed tests; the `63251c2` greening commit is
  genuinely fixture-side (full audit below).

### Re-confirmation note (2026-10-11)

Fix commit `1aa961f` (`fix(tu): address r3-003 code review issues`,
range `ee4339a..1aa961f`) re-verified by sw-celeste on
`codex/r3-003-coordination` synced to
`origin/codex/r3-003-coordination` (head `1aa961f`):

| Check | Result |
| --- | --- |
| Fix diff scope (`git diff ee4339a..1aa961f --stat`) | `object.py` +11/−2 (docstrings only), `tests/test_tu_coordination.py` +4/−1 (cosmetics only) — no executable production code touched |
| Issue 1 — docstrings | **CLOSED** — both Errors sections now carry the `RuntimeError` entry (see per-item confirmation below) |
| Issue 2 — long line | **CLOSED** — line wrapped; `awk 'length > 80'` over all six touched files reports zero violations |
| Issue 3 — dead code | **CLOSED** — `if False` conditional removed; mid-run enroll rejection still pinned nearby |
| `python -m unittest discover -s tests -p 'test_*.py'` | **Ran 206 tests, OK** |
| `python -m unittest tests.test_tu_coordination` | **Ran 41 tests, OK** |
| `python -W error::Warning -m unittest discover …` | **Ran 206 tests, OK, zero warnings** |
| Behavior delta | **none** — assertion semantics, guard raises, and all pinned behaviors unchanged |

All issues resolved; the task proceeds to sw-mike's testing phase.

## What was executed (commands + results)

All commands from the repository root (or scratch worktrees under the
system temp dir) using the project `.venv` (Python 3.13.15), matching
AGENTS.md verification §1:

| Command | Result |
| --- | --- |
| `git fetch origin`; HEAD check | HEAD at `d5abc9b`, working tree clean, in sync with `origin/codex/r3-003-coordination`; code identical to `8199d2b` |
| `git diff --stat dev...HEAD` | 5 production files exactly as design-listed — `softlab/tu/simulation/mock.py` (+215, NEW), `coordination.py` (+1376, NEW), `object.py` (+207/−11, EDIT), `simulation/__init__.py` (+8, EDIT), `station/station.py` (+131/−, EDIT) — plus `tests/test_tu_coordination.py` (+1377, NEW) and `log/` records. Nothing else |
| `git diff dev...HEAD --stat -- softlab/huo softlab/jin softlab/shui softlab/mu` | **empty** — production changes confined to `softlab/tu` (CHK-03-1 item 1 satisfied) |
| `git diff dev...HEAD -- pyproject.toml setup.py` | **empty** — no dependency/config changes (R3-TC-08e satisfied trivially) |
| `git diff dev...HEAD --check` | clean, exit 0 |
| `git diff dev...HEAD --stat -- tests/` (excluding the new file) | **empty** — the 165 pre-existing tests are byte-untouched |
| `awk 'length > 80 …'` over all six touched Python files | one violation: `tests/test_tu_coordination.py:652` (82 chars) — Issue 2 |
| `python -m unittest discover -s tests -p 'test_*.py'` | **Ran 206 tests, OK** (165 pre-existing anchors unmodified + 41 new) |
| `python -W error::Warning -m unittest discover -s tests -p 'test_*.py'` | **Ran 206 tests, OK, zero warnings** |
| `python -m unittest tests.test_tu_coordination` | **Ran 41 tests, OK** — case count matches plan Revision 2 arithmetic (5+6+6+6+7+5+6 = 41) exactly |
| `python -m compileall -q softlab` | exit 0 |
| Import smoke + export identity (`Coordinator`/`Endpoint`/`MockControl`/`MockMeasurement` re-exported from `softlab.tu.simulation`; `import softlab` → `0.3.0`) | pass |
| Red-commit verification (worktree at `5d8fc6a`, run `tests.test_tu_coordination`) | **fails by `ImportError: cannot import name 'Coordinator' from 'softlab.tu.simulation'`** — the documented red mode; the TDD-red claim is truthful |
| Runtime probes (clean worktree at HEAD) | probe 1: a callback attempting `station.sim_dt = …` **during `prepare()`** is rejected with `RuntimeError`, interval unchanged, prepare succeeds — the normative D6-step-5 `_busy` half is functionally live, not just present in code; probe 2: mock/object name sharing (control `'A'` + object `'A'`) resolves per the D3 signal-presence rule and delivers correctly; probe 3: a failed `MockControl.write` leaves the pending store unchanged (validate-then-mutate) |
| Mutation checks (worktree at `d5abc9b`, 6 targeted production mutations, each reverted after the run) | 4 killed by exactly the designed tests; 2 survived — both analyzed and dispositioned below |

### Mutation results (vacuous-pass guard)

| # | Mutation applied (worktree) | Result |
| --- | --- | --- |
| M1 | Publish candidate outputs into the boundary structure during phase 2 (`step`): same-tick leak | **KILLED** — `test_01a`, `test_01e` (the designated same-tick guards) plus `02d`, `04f`, `06a`; 5 failures |
| M2 | Advance `self._tick` before the candidate phase: clock moves on a failed tick | **KILLED** — 14 failures including the designated `03c` and `06a` |
| M3 | Wrap the `evolve` callback exception in `RuntimeError(…) from exc` inside `_evolve_candidate`: identity break | **KILLED** — `06a` (`assertIs`), `03c` (`assertIs`), `04e` (`assertIs` on the propagated reentrancy failure) |
| M4 | Delete the `_busy` reentrancy check in `step` | **KILLED** — by exactly `04e` |
| M5 | Remove **all four** delivery-path copies (`_frozen_edge_value`, phase-2 delivery copy, `_step_candidate` overlay copy, phase-3 measurement copy) | **SURVIVED** — see Observation 1: the delivery path is defended by redundant layers and 05d's getter-mutation vector is copy-isolated by construction; the design's P1-3 copy list was verified present by inspection |
| M6 | Drop the `_busy` half of the `sim_dt` setter guard (D6 step 5) | **SURVIVED** — see Observation 2: the guard is implemented in code and runtime-probe-verified; the architect's *optional* 04e extension pinning it was not taken (explicitly permitted; no plan revision needed to add it at test phase) |

The 41 final tests are not ImportError-trivial: they assert real
behavior (golden traces, identities, counters, atomicity, determinism
across registration orders) and detect each designed failure mode
individually (M1–M4).

## Design compliance (element by element)

- [x] **Two-state lifecycle (D5)**: STOPPED/RUNNING; every graph
  operation checks `_prepared` first, `step` requires RUNNING, `reset`
  requires `_started` and lands STOPPED with `_started` retained
  (coordination.py:483, 538, 594, 636, 685, 723, 787, 842, 896, 946,
  1008, 1095, 1223); reset before first prepare rejected; STOPPED-state
  reset allowed once started. ✔
- [x] **Claim-at-enroll (D4)**: `enroll` check order stopped → busy →
  membership (`KeyError`) → duplicate (`ValueError`) → foreign claim
  (`RuntimeError`), claim recorded last (coordination.py:452–507);
  release only via `withdraw` with reference-absence gate
  (509–566).
- [x] **4-phase tick (D8)**: phase 1 freeze (1104–1110), phase 2
  per-participant candidates in sorted order with per-destination
  validated, copied deliveries and bound-input overlay (1111–1135),
  phase 3 measurement candidates + clock candidate with finite check →
  `OverflowError` (1136–1150), phase 4 publication — commit, snapshot
  rebuild, sample delivery, clock — clearly delimited "no user code"
  (1151–1161). Canonical sorted iteration everywhere
  (`_endpoint_key` never compares `None` with `str`, 165–168). ✔
- [x] **All-outputs snapshot cardinality (closed Issue 4)**:
  `_publish_snapshot` stores every participant's full output dict,
  edge sources or not (1358–1376). ✔
- [x] **Delayed edges read boundary-`k`**: deliveries read the frozen
  phase-1 copies only; candidate outputs are unreachable until
  publication (M1 kill is the empirical proof). ✔
- [x] **Tick-zero preparation (D9)**: `sim_dt` presence + live
  re-validation; exactly one `observe` per participant on committed
  states, no `evolve`, no factories; edge tick-zero delivery validation
  naming source/destination; publication of `S_0` + zero clock +
  RUNNING; measurements never delivered at prepare; failure publishes
  nothing (967–1035). ✔ (02a/02b)
- [x] **Staged control writes + pending readback (D2)**: `MockControl`
  validate-copy-in on write, copy-out on read, last-write-wins,
  `_busy` guard when enrolled (mock.py:91–123); never ticks (02c/02e).
- [x] **Delivered measurements**: `MockMeasurement` owns `_delivered`;
  constructor initial is the tick-zero sample; publication-only
  delivery (mock.py:142–215; coordination.py:1157–1159). ✔ (02d)
- [x] **Never-implicit-tick**: 02f counters pin zero `evolve`/`observe`
  from reads/writes/inspection; `coordinator` property never creates
  (station.py `coordinator` getter). ✔
- [x] **`bind_interval` explicit, validated, single-driver (D11)**:
  enrolled/input-declared `KeyError`, non-float `TypeError`,
  already-driven `ValueError`; delivery `float(sim_dt)` each
  transition; a `dt`-named input without binding receives no special
  treatment (03e pins both halves). ✔
- [x] **Atomic staged reset (D10)**: candidate phase (factories in
  declaration order + observe), edge re-validation against candidate
  outputs **and restored control initials**
  (`use_control_initials=True`, 1238–1240), publication: commits,
  `S_0`, control pending := copy of initial (accepted writes do not
  leak), measurement delivered := copy of initial, zero clock, STOPPED;
  failure leaves the prior network usable (06c/06e; M2/M3 kills).
  Original exception identity preserved by the zero-try/except rule on
  candidate paths (the only wraps are validation re-raises that
  preserve the class and add source/destination context — per design,
  identity is pinned on *callback* exceptions only). ✔
- [x] **Validate-then-mutate graph edits, no batch API (P3-1/D12)**:
  every edit validates fully and mutates last; 05a/05g pin
  bit-identical `connections` after each rejection;
  `assertFalse(hasattr(coord, 'connect_batch'))`. ✔
- [x] **int tick + `float(tick) * sim_dt` policy (D7)**: exact
  multiplication at each publication; 03f(b) asserts the exact Python
  expression after 10⁵ ticks of `1e-9`; overflow raises
  `OverflowError` as a failed tick before publication (03f(c);
  largest-finite-interval boundary verified). ✔
- [x] **Endpoint resolution rule explicit (closed Issue 2)**:
  `signal=None` → mock namespace, `str` → object signal; disjoint
  namespaces; direction/kind validated at resolution
  (coordination.py:370–436; class docstring 117–123); name sharing
  permitted and runtime-probe-verified (probe 2). ✔
- [x] **`sim_dt` setter rejects when prepared OR `_busy` (NORMATIVE D6
  step 5)**: station.py setter — `_validate_interval(value)` (D6 steps
  1–4, single-sourced in coordination.py:58–97, bool check first) then
  `coordinator.prepared or coordinator._busy` → `RuntimeError`. **Code
  verified** (not just docstring) and **runtime-probe-verified**
  (probe 1: callback-reached change during `prepare()` rejected,
  interval unchanged). ✔
- [x] **object.py additive-only**: the diff is purely additive — the
  `_active_coordinator` slot, six private helpers, three guards keyed
  on the slot — plus the verbatim `reset` → `_reset_candidate` +
  `_commit` extraction (loops byte-identical in the diff; same factory
  order, same identity, same build-then-swap). `evolve_once`/
  `observe_outputs` bodies unchanged apart from the `evolve_once`
  guard. Standalone paths behavior-identical: all 165 baseline tests
  pass unmodified; anchors 1–3, 7 hold. ✔
- [x] **station.py changes as designed**: two inert slots, property
  pair, read-only `coordinator`, `rm_object` guard resolving the
  member first with lenient `None` semantics preserved (station.py
  diff); the R3-002 forward-looking docstring is replaced by the
  delivered contract (test-plan risk note 6 disposition).
  `snapshot()`/`describe()` untouched. ✔
- [x] **`__init__.py`**: exactly the four new public names re-exported;
  import direction preserved (coordination/mock never import
  `station` — pinned by 08e and verified by inspection);
  `station.py` → simulation import direction unchanged; no cycle
  (import smoke). ✔

## Reentrancy/ownership coverage (checklist item 2)

- [x] `_busy` checked and held (set after operation-specific
  preconditions, cleared in `finally`) on all 13 design-enumerated
  operations: `prepare`, `step`, `reset`, `connect`, `disconnect`,
  `bind_interval`, `unbind_interval`, `enroll`, `withdraw`,
  `add_control`, `add_measurement`, `rm_control`, `rm_measurement` —
  verified body by body.
- [x] `MockControl.write` consults the coordinator link's `_busy`
  (mock.py:117–121); the station `sim_dt` setter consults
  `prepared or _busy`; `set_input` on a claimed object delegates to
  `_guard_set_input` (busy + driven-input checks,
  coordination.py:1264–1296; object.py:521–522).
- [x] Claim slot single-ownership: single mutable slot written only by
  `enroll`/`withdraw`; check-and-set under the single-threaded
  contract; cross-station conflict rejected with S1 left coherent
  (04c, including the one-coordinator-per-station permanence
  rejection).
- [x] `rm_object` ownership guard: member resolved first, claimed →
  `RuntimeError` naming the active coordination (04a); safe-detach
  sequence disconnect → withdraw → `rm_object` verified end-to-end
  (04b).

## Test-plan traceability (41/41 mapped, assertions as planned)

| Case | Test method | Pins |
| --- | --- | --- |
| 01a | `test_r3_tc_01a…` | chain one-edge-per-transition; C consumes B's **tick-0** output; A reaches C no earlier than 1→2 |
| 01b | `…_01b…` | fan-out identical value to both destinations |
| 01c | `…_01c…` | feedback consumes initialized boundary value; cycle permitted |
| 01d | `…_01d…` | **genuinely varies** build order `('A','B','C')` vs `('C','A','B')`, endpoint-registration order (control-first vs measurement-first), interleaves `station.objects`/`participants`/`connections` inspection; bit-identical 3-tick traces |
| 01e | `…_01e…` | per-tick golden trace from the design's derivation table, recomputed in-test |
| 02a | `…_02a…` | inspection pure (zero callbacks), explicit prepare publishes tick 0 / time 0.0, exactly one observe per participant |
| 02b | `…_02b…` | `assertIs` identity, no tick/time/state/ownership change on failed prepare, later healthy prepare works (injection at tick-zero observe — the design-review-recorded rebind) |
| 02c | `…_02c…` | write stages, readback shows pending, downstream/clock untouched, delivered at next boundary |
| 02d | `…_02d…` | tick-zero sample is the declared initial (−1.0 ≠ 100.0), reads never fetch/advance, deliveries at publication only |
| 02e | `…_02e…` | last-write-wins; intermediate value never delivered |
| 02f | `…_02f…` | reads/writes/inspection/unconnected-input write never tick, counters pinned |
| 03a | `…_03a…` | ordinary `Station(name)` + device with no simulation argument; shared interval governs all participants |
| 03b(+cont) | `…_03b…` | 10 invalid values → exact classes, station usable after each; Decimal rejected `TypeError`, `np.float64`/`Fraction` coerced to plain `float` |
| 03c | `…_03c…` | failed tick: clock identical before/after, `assertIs`, retry works |
| 03d | `…_03d…` | mid-run change rejected, unchanged interval governs, post-reset change succeeds and governs |
| 03e | `…_03e…` | explicit binding delivers `k × sim_dt`; name-inference absence pinned; contradictory bind/connect/set_input rejected; int-declared bind `TypeError`; unbind `KeyError` |
| 03f | `…_03f…` | `5e-324` accepted; largest finite admits exactly one tick then `OverflowError` with clock preserved; 10⁵-tick exact multiplication policy |
| 04a–04f | `…_04a…`–`…_04f…` | connected removal rejected; safe detach; dual-activation conflict with S1 coherent; standalone guards + driven-input rejection + positive unconnected-write sub-assertion; reentrancy (nested step/write rejected, propagation identity); two mock endpoints on one participant permitted |
| 05a–05g | `…_05a…`–`…_05g…` | 8 invalid endpoint/direction rows + non-Endpoint `TypeError`s + construction-time `Endpoint` validation, graph bit-identical after each; single-driver with original edge intact; prepare-time and tick-time incompatibility paths; ndarray fan-out isolation; unconnected-input carry-forward; STOPPED-only topology edits with post-reset success; per-edit atomicity + no batch API |
| 06a–06e | `…_06a…`–`…_06e…` | evolve-failure atomicity with pending-write survival and redelivery on retry; observe-failure atomicity; reset factory failure leaves network usable; full reset rebuild (clock, inputs, states, controls, measurement, feedback buffer) + re-run reproduces the trace; mid-staging multi-participant reset atomicity |
| 08a–08f | `…_08a…`–`…_08f…` | 165-test regression re-run in-process; compile/import gates; standalone compatibility probes; no device-hook execution + opt-in mocks; import-direction/dependency scan; CI matrix file presence (run evidence is sw-mike's results-document record) |

No vacuous passes found: every case asserts real behavior, and M1–M4
demonstrate the key cases are live.

## Fixture-correction audit (`63251c2` — highest-risk commit)

All 12 hunks audited against the designed semantics; every one is
genuinely test-side; none masks a production defect:

1. **03a expectation 11.0 → 13.0**: A is undriven (y = 1.0 at every
   boundary); B accumulates +1.0 per transition from 10.0 → 13.0 after
   3 ticks. The original counted one delivery. Fixture arithmetic
   error, corrected to the designed delay semantics.
2. **03b/03b-cont merge into one method**: preserves every assertion of
   both blocks verbatim; yields exactly the plan's 41-case count
   (03b cont. is a data block of 03b, not a 42nd case — Revision 2
   arithmetic).
3. **03e restructure**: the original attempted graph edits while
   RUNNING — the design's check order raises `RuntimeError` (RUNNING)
   before the pinned `ValueError`; the fix resets to STOPPED first and
   keeps every designed rejection (`ValueError` re-bind,
   `ValueError` edge onto bound input, `RuntimeError` set_input,
   `TypeError` int-declared bind) plus the reset-restores-initial
   assertion. Fixture misconception of the state machine.
4. **04c reorder**: duplicate/unknown enrollment checks moved before
   prepare (enrollment is STOPPED-only). Same misconception class.
5. **04d disconnect keys**: `disconnect` is dest-keyed by design; the
   original passed source endpoints (`Endpoint('A','y')`), which can
   only raise `KeyError`. Corrected to the dest keys; withdraw then
   legally succeeds after the referencing edges are gone.
6. **04e second half**: the original enrolled a new object into the
   still-RUNNING coordinator (would raise at `enroll`, never reaching
   its assertion) and never actually exercised reentrancy propagation.
   The rewrite uses a fresh network and is **stronger**: it pins
   `assertIs` identity of the propagated reentrancy `RuntimeError`
   (design P2-2) and zero-clock integrity.
7. **05c tick-time path**: at 0→1 the delivered value is the committed
   shape-(2,) `S_0` — compatible, so the transition succeeds and
   publishes the shape-(3,) output; 1→2 delivers it and fails
   atomically. The original expected failure at 0→1, contradicting
   boundary semantics (outputs carry no pinned spec; the destination
   spec is what fails, one boundary later). Corrected expectations
   match D8/D9 exactly and add the intermediate success assertions.
8. **05d fixture dtypes**: evolve result `x + u` is ndarray; declaring
   `x` as float `0.0` violates the pinned-spec contract at the first
   tick. Fixture now declares ndarray states. Pure fixture defect.
9. **05e restructure**: enrollment after prepare would raise
   `RuntimeError`; moved before prepare; the final assertion now reads
   the **destination's** log (the original read the source's, which
   can never observe the delivery). Fixture defect, corrected.
10. **05g disconnect target**: the original disconnected
    `Endpoint('A','u')` — an edge the test itself had just created —
    so it would have succeeded instead of raising `KeyError`.
    Corrected to a genuinely absent dest.
11. **06a expectation 10.0 → 11.0**: the failing attempt is the
    **second** step; B already committed 11.0 at tick 1. The original
    forgot the first committed transition.
12. **08e assertion rescoping**: the naive substring checks matched the
    modules' own docstring prose (`softlab.tu.station` appears in
    coordination.py's docstring *stating the rule*). The regex now
    pins actual import statements — the designed rule, correctly
    scoped, not weakened.

## Assertion-integrity audit

- [x] No existing test file touched (diff empty outside the new file);
  no pre-existing assertion weakened or removed.
- [x] No byte-overconstraint beyond the plan: error **classes** are
  asserted, not message texts, except the 05c source/destination
  naming (`assertIn('A'/'B'/'u')`) which the plan itself requires
  ("naming source/destination").
- [x] Test isolation: fresh stations/objects/endpoints per case; no
  module-global coordination state exists to leak (per-instance by
  design); `_default_station` untouched.
- [x] 01d is a genuine determinism case (varied build order, endpoint
  order, and inspection interleaving), not a self-comparison.

## Conventions (AGENTS.md)

- [x] English docstrings in the repo Args/Returns/Errors/Side-effects
  style on all new public API — gap found at `d5abc9b` (Issue 1),
  **fixed at `1aa961f`**
- [x] Type annotations on all new public API; signatures match the
  design's interface tables verbatim
- [x] Black line width 80 — one violation at `d5abc9b` (Issue 2),
  **fixed at `1aa961f`; zero violations re-verified**
- [x] No new dependencies; no `pyproject.toml`/`setup.py` change;
  stdlib (`numbers`/`math`/`typing`) + already-imported NumPy only,
  pinned by 08e
- [x] No unrelated changes; Release 1 debt untouched; `builder.py`,
  `device.py`, `parameter.py` byte-untouched
- [x] No compiler/lint warnings; suite clean under `-W error::Warning`
- [x] TDD records truthful: red commit fails by the documented
  ImportError; greening commit audited fixture-side

## Simplicity audit

- [x] Zero third-party dependencies — the design's dependency table is
  empty and the diff adds none (08e enforces mechanically).
- [x] No over-engineering: one coordinator per station with no
  retirement API (Ruling 6); no batch graph API (P3-1); no thread
  safety; the `_resolved` reference store is the design's disclosed
  single indirectly-exercised element; single-sourced interval rule
  (8199d2b removes a duplication the first cut had).
- [x] Public surface is exactly the designed four names plus the
  station/coordinator members — nothing more.
- [x] Data structures minimal: dest-keyed edge dict (single-driver by
  key uniqueness), one snapshot dict, one binding set — no entity
  beyond the design's table.

## Issues found

### [Medium] Issue 1: `evolve_once`/`reset` docstrings do not document the new coordination `RuntimeError`

- **Location**: `softlab/tu/simulation/object.py:567–577`
  (`evolve_once` Errors section; guard raise at 583–587) and
  `softlab/tu/simulation/object.py:701–702` (`reset` Errors section;
  guard raise at 708–712)
- **Description**: both public methods gained a guard that raises
  `RuntimeError` while the object is claimed by a coordinator, but
  their Errors sections were not updated. `set_input`'s docstring
  *was* updated with exactly this entry (507–510), so the three
  guarded methods are now inconsistent, and the repo convention
  (docstrings document parameters, returns, exceptions and
  side-effects) is violated for two of them.
- **Impact**: user-facing error behavior of public API is
  undocumented; the inconsistency also invites future drift.
- **Recommendation**: add the `RuntimeError` entry to both Errors
  sections, mirroring the `set_input` wording (enrolled in an active
  coordinated simulation; standalone operation rejected while
  claimed).
- **Owner**: sw-tom
- **Status**: CLOSED (2026-10-11, fix `1aa961f`) — confirmed: the
  `evolve_once` Errors section (object.py:569–577) now lists
  "RuntimeError --- while enrolled in an active coordinated
  simulation: standalone ``evolve_once`` is rejected while claimed
  (withdraw first)", and the `reset` Errors section (object.py:701–708)
  was restructured into the same bulleted form with the matching
  ``reset`` entry. Phrasing mirrors the `set_input` convention
  (enrollment in an active coordinated simulation; standalone
  operation rejected while claimed); the three guarded methods are now
  consistent and the docstring matches the actual guard raise at each
   site. No code line changed.

### [Low] Issue 2: 82-character line in the test file

- **Location**: `tests/test_tu_coordination.py:652`
- **Description**:
  `self.assertEqual(objs['A'].get_state('x'), 1.0 + objs['A'].get_input('u'))`
  exceeds the 80-column Black limit (82 chars).
- **Impact**: mechanical convention violation; the only one in the
  six touched files.
- **Recommendation**: wrap the line (e.g. bind
  `expected = 1.0 + objs['A'].get_input('u')` first).
- **Owner**: sw-tom
- **Status**: CLOSED (2026-10-11, fix `1aa961f`) — confirmed: the line
  is wrapped exactly as recommended (`expected = 1.0 +
  objs['A'].get_input('u')` bound first, then
  `self.assertEqual(objs['A'].get_state('x'), expected)`); assertion
  semantics unchanged; `awk 'length > 80'` over all six touched Python
  files now reports zero violations.

### [Low] Issue 3: dead-code conditional expression in `test_05f`

- **Location**: `tests/test_tu_coordination.py:983`
- **Description**:
  `coord.enroll('nope') if False else coord.withdraw('C')` — the
  `if False` branch is never evaluated; the line only ever calls
  `withdraw('C')`. (Mid-run enrollment rejection *is* genuinely
  pinned four lines later via `coord.enroll('E')`, so no coverage is
  lost — this is hygiene, not a hole.)
- **Impact**: dead code in a test reads as a leftover and obscures
  intent.
- **Recommendation**: replace with the plain
  `coord.withdraw('C')` call.
- **Owner**: sw-tom
- **Status**: CLOSED (2026-10-11, fix `1aa961f`) — confirmed: the
  `if False` conditional is gone; the line is now the plain
  `coord.withdraw('C')` inside the same `assertRaises(RuntimeError)`
  block. The mid-run enrollment rejection remains genuinely pinned
  four lines later via `coord.enroll('E')  # enrollment requires
  STOPPED too` — no coverage lost.

## Non-blocking observations (no action required for approval)

1. **M5 survival — delivery-path copy redundancy vs 05d's vector.**
   Removing *all four* delivery-path copies passes the full suite. The
   delivery path therefore carries redundant layers relative to the
   design's P1-3 list (`_frozen_edge_value` already returns an owned
   per-destination copy; coordination.py:1131 copies it again and
   `_step_candidate` copies a third time; the phase-3 sample copy at
   1143 is likewise redundant). Correctness is unaffected — every
   design-listed copy point is present (verified by inspection:
   control write/read, measurement read, per-destination fan-out,
   callback arguments, callback results, snapshot publication,
   reset/initial restoration) — and 05d's getter-mutation vector is
   copy-isolated by construction (getters copy out), so the case pins
   the end-to-end property but cannot detect delivery-path aliasing on
   its own. Either resolution is acceptable: keep the redundancy as
   defense-in-depth, or drop the two clearly duplicate copies
   (coordination.py:1131, 1143). If stronger pinning is ever wanted,
   a stateful-callback fixture (an `observe` that retains and later
   mutates its argument) would make the aliasing cases
   mutation-sensitive — tester-owned, optional, no plan change needed.
2. **M6 survival — the normative `_busy` half of the `sim_dt` setter
   is unpinned by the suite.** The guard is implemented per D6 step 5
   and verified live (probe 1: a callback attempting an interval change
   during `prepare()` is rejected and the interval is unchanged). The
   design review assigned verification of this line to code review and
   made the R3-TC-04e test extension *optional*; sw-tom implemented,
   this review verified. sw-mike may still add the one-callback probe
   at test phase (design-review Issue 1 closure: no test-plan revision
   required). Not a defect; recorded for the results document.
3. **Unpinned-but-implemented error rows**: endpoint-membership errors
   (`TypeError` wrong type, `ValueError` duplicate name, `RuntimeError`
   foreign enrollment) and the `rm_control`/`rm_measurement`
   edge-referenced rejection are implemented per the design's interface
   tables but not pinned by any of the 41 cases — consistent with the
   design's own traceability table, which maps D2 to the positive 04f
   case. Recorded for completeness.
4. **Cosmetic**: coordination.py:866 — one over-indented continuation
   line inside the `bind_interval` docstring. Harmonize when the file
   is next touched.

## Quality gates

- [x] No compiler errors/warnings; zero warnings under
  `-W error::Warning`
- [x] Regression: 206 tests OK = 165 pre-existing anchors (byte-
  untouched) + 41 new; single-file run of the new suite: 41 OK
- [x] Import smoke + export class identity; no import cycle
- [x] `git diff --check` clean; scope confined to the 5 design-listed
  production files + 1 new test file + `log/`; R3-AC-08 anchors hold
- [x] TDD red verified at `5d8fc6a`; greening commit audited
  fixture-side in full

## Approval

- [x] All issues resolved — 3/3 CLOSED at fix head `1aa961f`
  (re-confirmed 2026-10-11; fix diff is docstring/test-cosmetic only,
  suite 206 OK with zero warnings)
- [x] Design faithfully implemented (element-for-element, including
  the normative D6-step-5 `_busy` guard — code- and probe-verified)
- [x] Test plan faithfully realized (41/41, no vacuous passes,
  greening commit clean)
- [x] Code meets quality and simplicity standards
- [x] **APPROVED** — the task proceeds to sw-mike's testing phase.
  CHK-03-1 items 1–2 are discharged by this review (scope diff empty;
  the five design proofs verified realized in code); the docstring
  citations for CHK-03-1 item 3 are: setter rule + prepared/busy
  rejection at `softlab/tu/station/station.py` (`sim_dt` setter
  docstring), clock policy at
  `softlab/tu/simulation/coordination.py` (`sim_time`/`step`
  docstrings), binding semantics (`bind_interval` docstring),
  tick-zero measurement initial (`MockMeasurement` class docstring),
  reentrancy rule (`Coordinator` class docstring), safe detach
  (`withdraw`/`disconnect` docstrings), the reset
  external-side-effect limitation (`Coordinator.reset` docstring),
  and — added by the fix — the claimed-object standalone-operation
  rejection at `softlab/tu/simulation/object.py` (`evolve_once` and
  `reset` Errors sections).
