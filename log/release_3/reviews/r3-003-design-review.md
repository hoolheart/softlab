# Review — R3-003 detailed design

Reviewer: sw-jerry (Software Architect) | Date: 2026-10-11
Verdict: **APPROVED**
Reviewed artifact:
[r3-003-detailed-design.md](../design/r3-003-detailed-design.md) at commit
`92eaa62` on `codex/r3-003-coordination` (verified via `git fetch` /
`git rev-parse HEAD origin/codex/r3-003-coordination`; both `92eaa62`,
1267 lines, read in full).
Requirements: [prd.md](../prd.md) (R3-AC-01–06 in full; compatibility
portion of R3-AC-08) | Architecture authority:
[architecture-plan.md](../architecture-plan.md) (lines 9–94) | Approved
test plan: [r3-003-test-plan.md](../test/r3-003-test-plan.md) (Revision 2,
41 automated cases + CHK-03-1) | Characterization baseline:
[r3-003-baseline.md](../test/r3-003-baseline.md) (165 tests OK; seven
no-regression anchors) | Review convention:
[r3-002-design-review.md](r3-002-design-review.md)

## Issues

No blockers. Four minor items, none of which changes the verdict.

1. **[minor] The `sim_dt` setter is not covered by the `_busy`
   reentrancy guard — a callback-reachable window during `prepare()` and
   STOPPED-state `reset()`.** Location: design D6 step 5 and P2-2
   (design lines 249–281, 505–528). Setter step 5 rejects a mid-run
   change only when `self._coordinator.prepared` is True. During
   `prepare()` the coordinator is STOPPED with `prepared` still False
   until publication, yet phase 2 runs user `observe` callbacks; during
   a STOPPED-state `reset()` (allowed once started, D5) the candidate
   phase runs user factories and `observe` with `prepared` False. A
   callback invoked at those points could therefore legally change the
   interval mid-operation, and the setter is not among the 13
   `_busy`-guarded operations P2-2 enumerates. Impact assessment: no
   committed-state corruption is reachable (the interval is read live at
   each step — D6's no-caching rule; the clock published by
   prepare/reset is always zero; the new value is itself validated by
   the setter), so this is an unspecified behavior window, not an
   invariant break — but it is a genuine gap in the design's own "fixed
   mid-run / no staleness window" narrative and in the P2 coverage
   claim. Required change: either (preferred) extend setter step 5 to
   also reject when `self._coordinator is not None and
   self._coordinator._busy`, closing the window structurally; or
   document explicitly in the setter docstring that an interval change
   from a callback during prepare/stopped-reset is permitted and takes
   effect from the next transition. No test-plan impact: R3-TC-03d pins
   mid-run (RUNNING) rejection, which holds either way; if the guard is
   added, sw-mike may optionally extend R3-TC-04e's reentrant callback
   to attempt an interval change — this pins already-planned behavior
   and requires no test-plan revision (R3-002 Ruling-1-companion
   convention). Owner: sw-celeste; awareness sw-tom, sw-mike.
2. **[minor] Endpoint resolution rule is implicit; mock/object name
   sharing is unaddressed.** Location: D3 (design lines 148–175) and the
   `connect` interface (lines 931–937). The design never states the base
   resolution rule "an `Endpoint` with `signal=None` resolves in the
   mock-endpoint namespace; one with a `str` signal resolves to an
   enrolled object's declared signal". That rule is what makes the
   design unambiguous when a mock endpoint and an enrolled object happen
   to share a name (controls/measurements share one namespace per
   `add_control`'s duplicate check, but nothing prevents a mock named
   identically to an object). The design is saved by the implicit rule —
   `(name, None)` and `(name, 'u')` are distinct keys with disjoint
   resolution targets — but an implementer should not have to derive it.
   Required change: state the resolution rule explicitly in D3 and in
   the `connect` contract, and either document that mock/object name
   sharing is permitted (disambiguated by signal presence) or reject the
   collision at `add_control`/`add_measurement`. Either disposition is
   acceptable; the former is simpler and consistent with the Simplicity
   Mandate. Owner: sw-celeste.
3. **[minor, editorial] P3-2 miscounts the mutating operations.**
   Location: design line 561 — "Induction over the only two mutating
   operations closes the proof." The graph-mutating operations number
   more than two (`connect`, `disconnect`, `bind_interval`,
   `unbind_interval`, `add_control`/`add_measurement`,
   `rm_control`/`rm_measurement`, `enroll`, `withdraw`, plus the
   station-side `rm_object`). The induction itself is sound as
   structured — the invariant "no edge references a non-enrolled
   participant/endpoint" is *established* at `connect` (resolution
   fails with `KeyError` otherwise) and *preserved* because every
   removal is gated on reference absence and `rm_object` is gated on
   the claim — but the sentence as written is falsifiable and weakens
   an otherwise valid proof. Required change: reword to name the
   establish/preserve operations explicitly. Owner: sw-celeste.
4. **[minor, editorial] Snapshot cardinality wording tension.**
   Location: Overview (design lines 86–91: "mapping each edge source to
   the value committed at boundary k") versus D9 step 4 / D10 step 3
   ("`S_0` := tick-zero outputs" / "`S_0` := candidate outputs") and
   the `_snapshot` row of the data-structure table (line 1021: "object
   outputs by source Endpoint"). The Overview reads as
   edge-sources-only; D9/D10 read as full per-participant output dicts.
   I traced both readings and verified they are *semantically* safe
   either way: graph edits are STOPPED-only, `step` requires RUNNING,
   and every `prepare()` rebuilds `S_0` wholesale, so a newly connected
   edge's source is always re-snapshotted (and its tick-zero delivery
   re-validated) at the next `prepare()` before any `step` can read it.
   Still, the document should state one cardinality. Required change:
   state explicitly whether `_snapshot` holds all participant outputs
   or only current edge sources; I recommend all-outputs — it matches
   the D9/D10 text and keeps the publication invariant local
   ("snapshot = validated candidate outputs, full stop"). Owner:
   sw-celeste.

## Rulings on the six open questions (architect decisions, explicit)

### Ruling 1 — Reset lands STOPPED (Q-a, D5): **ACCEPT**

The two-state machine with reset as the only RUNNING→STOPPED transition
is not one option among several; it is the unique resolution forced by
the approved anchors:

1. **R3-TC-05f requires post-reset topology editability.** "Topology
   changes require a stopped/reset boundary … after a stopped/reset
   boundary the same topology change succeeds." If reset landed RUNNING,
   the graph would be frozen after reset and 05f's positive half would
   be unsatisfiable. The alternative the design rejects (reset lands
   RUNNING) is therefore not merely worse — it fails an approved case.
2. **R3-TC-06d requires the clock readable at zero after reset**, which
   the design satisfies by keeping `_started` True and republishing
   `(0, 0.0)` at reset publication (architecture-plan line 66:
   "publishes tick/time zero together"). STOPPED-with-readable-zero-clock
   is exactly the plan's "stopped/reset boundary" language (PRD line 88)
   made total: one graph-mutable state, one running state.
3. **Re-run reproducibility** (06d: prepare + steps reproduce the
   original trace) requires a fresh `prepare()` to rebuild `S_0` after
   topology edits — which only STOPPED permits. The minimal machine is
   the correct machine (Simplicity Mandate).

### Ruling 2 — Ownership claimed at `enroll`, not `prepare` (Q-b, D4): **ACCEPT — and strengthened: it is load-bearing, not merely preferable**

1. **The claim point is forced by the no-dangling invariant (P3).** The
   `rm_object` guard keys on `obj._active_coordinator`. If the claim
   were deferred to `prepare`, an object that is enrolled *and
   connected* (both legal in STOPPED before any prepare) could be
   removed from the station, leaving `_resolved` edge references
   dangling — the exact state P3-2's induction proves unreachable. The
   claim must therefore precede any state in which edges can exist,
   i.e. it must happen at `enroll`. The design's determinism argument
   (R3-TC-04c stable under all interleavings) is correct but secondary;
   the detach proof is the decisive one.
2. **R3-TC-02b compatibility holds as designed.** "No ownership
   activation" on failed preparation is satisfied as "failed prepare
   changes no ownership state" — the claim pre-exists and is untouched;
   the architecture plan's "reset retains coherent ownership … not
   permission for two stations to activate the same participant"
   (lines 69–71) reads identically.
3. **Cross-station dual activation is covered.** Verified against
   R3-002 semantics: nothing prevents one object instance from holding
   membership in two stations, so the single mutable claim slot on the
   object (not on any station) is the correct and only place the
   conflict can be decided. `RuntimeError` naming the conflict, with
   the owning simulation untouched (pure read for it), satisfies
   R3-TC-04c's "S1 remains coherent" clause.

### Ruling 3 — `Decimal` rejected, NumPy real scalars coerced (Q-c, D6): **ACCEPT**

1. **The rule is one documented line instead of an open-ended type
   enumeration** — `numbers.Real` keying is the minimal statement of the
   PRD's "strictly positive finite real duration (booleans rejected)"
   (line 57). Verified live on this branch's interpreter (CPython
   3.13.15): `isinstance(Decimal('0.5'), numbers.Real)` is `False`;
   `np.float64(0.5)` is `Real` (and subclasses `float`); `Fraction(1,2)`
   is `Real`; `True` is `Real` (hence the bool-first check order, also
   verified necessary); `0.5+0j` is not `Real`.
2. **The test plan authorizes exactly this disposition.** Revision 2's
   R3-TC-03b (cont.) pins only "rule documented and enforced" and
   defers the type-strictness choice to this design (test-plan lines
   162, 336–338); CHK-03-1 cites the setter docstring, which the
   design's Implementation Notes commit to. Both anchors are satisfied
   by rejection-with-`TypeError` for `Decimal`.
3. **Coercion-toward-double is the honest rule.** Storing
   `float(value)` with the representability caveat documented
   (a non-double real rounds to the nearest double) is what the
   multiplication policy (D7) then makes exact by construction; an
   accepted-but-unconverted `Decimal` would poison every downstream
   `float(tick) * sim_dt` with decimal/float mixed-arithmetic failures.
   Rejection is the correct side of the line.

### Ruling 4 — Mock endpoints are coordinator-owned ports, not station devices (Q-d, D2): **ACCEPT**

1. **This is the plan's wording, realized.** Architecture-plan lines
   18–19: "Graph mechanics should remain a focused simulation
   responsibility rather than inflate device/parameter abstractions";
   lines 22–27 assign the mock endpoints pending/delivered storage and
   demand the three readback roles be documented rather than
   `Parameter` behavior silently changed. D2 does exactly that —
   `device.py`/`parameter.py` byte-untouched, no device hook ever
   executed by the coordinator (R3-TC-08d holds by construction).
2. **The R3-001 bridge correction stays intact.** Standalone bridge
   readback reads authoritative object inputs; connected controls read
   pending stores; connected measurements read buffered deliveries.
   Three roles, three disjoint code paths, zero shared mutation —
   verified consistent with object.py's actual store ownership
   (`_inputs`/`_states`, lines 385–386) and with baseline §4 anchor 5.
3. **Membership/snapshot contracts are preserved.** `snapshot()`/
   `describe()` stay legacy device+object-free (device-only) per R3-002
   Ruling 2 and plan lines 93–94; mocks appear in no station listing.
   The deferred `before_set`/`before_get` wiring is correctly R3-004
   example scope — no approved R3-003 case requires it.

### Ruling 5 — Clock overflow raises `OverflowError` as a failed tick (Q-e, D7): **ACCEPT**

1. **Coherent with R3-AC-06, not in tension with it.** The clock
   candidate is computed in phase 3, before publication; a non-finite
   candidate raises `OverflowError` and phase 4 is never entered, so
   "factory/callback failure preserves clock" and "overflow preserves
   clock" share one mechanism — the publication-only-writer rule. AC-06
   is honored structurally, and R3-TC-03c/06a hold by the same proof
   (P5-4).
2. **The setter stays value-local**, which is the right separation: set
   time validates the *value* (finite, positive, real, non-bool); tick
   time validates the *arithmetic* (candidate product finite). Rejecting
   large-but-finite intervals at set time would conflate the two and
   would have to predict `tick` — an entity beyond necessity. Verified
   live: with the largest finite double, `1.0 * max` is finite and
   `2.0 * max` is not — the documented "exactly one successful tick"
   boundary is exact, not approximate.
3. **Exception class choice is conventional.** Python signals numeric
   range overflow with `OverflowError` (an `ArithmeticError`), e.g.
   int→float conversion overflow; reusing it needs no new exception
   type (dependency zero-trust applied to types). R3-TC-03f(c) pins
   "no silent `inf`/`nan` clock" and defers the mechanism to the
   documented design — satisfied.

### Ruling 6 — One coordinator per station, permanent (Q-f, D1): **ACCEPT**

1. **No requirement needs replacement.** Nothing in PRD R3-AC-01–06/08
   or the 41 approved cases requires retiring or swapping a station's
   coordinator; a fresh coordinated setup via a fresh station is a
   complete answer (tests construct fresh stations anyway — test-plan
   risk note 7's isolation discipline).
2. **A retirement API would be real complexity for zero requirement:**
   it would need ownership release across all enrolled participants,
   edge/binding teardown semantics, and an answer to "what does the old
   coordinator's clock mean afterwards" — all entities beyond necessity
   (Simplicity Mandate, applied at the architecture level).
3. **The permanence is enforceable at one line** (`RuntimeError` if
   `station._coordinator is not None`), keeps the station slots inert
   for ordinary use (R3-TC-03a anchor; baseline §4 anchor 7), and
   preserves the no-global-state property: all coordination state hangs
   off station instances.

## Proof-by-proof assessment (attacked, not paraphrased)

### P1 — Snapshot publication: **WITHSTANDS ATTACK**

- *Same-tick read paths exhausted.* Candidate outputs exist only as
  `step()` locals between phases 2 and 4; every edge read happens in
  phases 1–3 from `S_k` copies or frozen pending copies; publication is
  the sole writer. I searched for a side door and found exactly one
  candidate: a phase-2 callback of participant B calling
  `A.observe_outputs()`. It reads A's *committed* boundary-`k` states
  (publication has not run), so the delay semantics hold — and it is
  unguarded read access, which plan line 84 explicitly permits. It does
  invoke user code beyond the one-observe-per-tick accounting, but that
  is user-callback behavior in the same family as the existing
  "callbacks must not re-enter the same object" contract
  (object.py:246–247, verified): outside the guarantee boundary, not a
  design hole. No path reads a same-tick candidate. ∎ holds.
- *Copy list cross-checked against object.py store types.* The only
  value stores are `_inputs`, `_states`, `_pristine_inputs`,
  `_pristine_states` (specs and sources hold no mutable values), and
  the value contract admits only exact `int`/exact `float`/numeric
  `ndarray` (object.py:72–127, verified). Scalars are immutable; every
  ndarray crossing is listed: control write in/out, measurement read
  out, per-destination fan-out copies in phases 2–3, callback arguments
  and results inside the candidate helpers (`_validate_callback_result`
  copies in, object.py:792–803, verified), snapshot publication stores
  already-copied validated candidates, reset/pending/initial
  restoration copies. No store-to-store aliasing path exists;
  `_commit`'s no-re-copy rule is *safe* precisely because candidates
  are already owned copies (Implementation Notes pitfall d is correct,
  not risky). P1-3's "exhaustive" claim is accurate. ∎ holds.
- *Phase-1 freeze is load-bearing and correctly coupled to P2:* pending
  control values are sampled at phase 1 while `_busy` blocks mid-tick
  writes — remove either half and a write between freeze and delivery
  would break boundary-k purity. The design has both halves. ✔
- *All-or-nothing:* publication is attribute/dict rebinds only; the
  per-participant `_commit` loop cannot observe its own intermediate
  state because no user code runs in phase 4 and the contract is
  single-threaded (PRD exclusion). ∎ holds.

### P2 — Ownership and reentrancy: **WITHSTANDS ATTACK, with one documented gap (Issue 1)**

- *13-operation `_busy` coverage verified complete as enumerated:*
  prepare, step, reset, connect, disconnect, bind_interval,
  unbind_interval, enroll, withdraw, add_control, add_measurement,
  rm_control, rm_measurement — all graph/clock/user-code operations are
  on the list; `MockControl.write` consults the link; `set_input` on a
  claimed object delegates to `_guard_set_input` (busy + driven-input
  checks). Callback-reachable station operations re-checked:
  `rm_object` on an enrolled object is blocked by the claim guard at
  all times (mid-tick included); `add_object`/`build_object` mid-tick
  mutate membership only, never enrollment or the graph — correctly
  left unguarded; `snapshot()`/`describe()`/`object()` are pure reads.
  The single gap is the **`sim_dt` setter** (Issue 1): reachable from
  callbacks during prepare and STOPPED-reset because step 5 checks
  `prepared` but not `_busy`. Filed as minor; the preferred one-line
  guard closes it.
- *Check-and-set soundness:* the single-threaded contract (PRD
  exclusion, no thread safety added) makes the two-bytecode
  read-then-write claim window unreachable by construction; the proof
  states this assumption honestly. ✔
- *Standalone-operation prevention:* guards are first-statement,
  keyed on the claim slot, and unreachable when unclaimed — verified
  against the actual insertion points (object.py:511–515 `set_input`
  after the `KeyError` check; 539+ `evolve_once`; 649+ `reset`). ∎
  holds with the Issue-1 caveat.

### P3 — Safe detach: **WITHSTANDS ATTACK**

- *Validate-then-mutate verified:* every graph edit validates fully and
  mutates once, mutation last — the same structural atomicity R3-002
  shipped for `add_object` (mutation is the last statement; no
  try/finally dependence). The deliberate absence of a batch API makes
  R3-TC-05g's second disjunction true by construction. ✔
- *No-dangling induction sound* (modulo the editorial miscount, Issue
  3): established at `connect` (resolution `KeyError` otherwise),
  preserved because `rm_control`/`rm_measurement`/`withdraw` are gated
  on reference absence and `rm_object` on the claim. Ruling 2 records
  that the claim-at-enroll point is what makes the `rm_object` leg of
  this induction total. ✔
- *Unconnected-input carry-forward verified against the candidate
  semantics:* `inputs_c` = own store copy overlaid *solely* with
  delivered/bound values, so an unconnected input's entry is carried
  forward unchanged across transitions; reset restores it like any
  input (D10); between ticks `set_input` stages it through the guarded
  path (D4 positive sub-assertion, R3-TC-04d/05e). ✔

### P4 — Model-dt binding: **WITHSTANDS ATTACK**

- *Explicitness:* binding exists only via `bind_interval`; a model
  input literally named `dt` receives no special treatment — plan lines
  76–77 honored verbatim. ✔
- *Single-driver coherence in both directions:* `connect` rejects a
  bound destination; `bind_interval` rejects an edge-driven or
  already-bound input; `set_input` rejects bound inputs while
  coordinated. All three legs present. ✔
- *Float-declared validation at bind time is deterministic without
  `sim_dt`:* the delivered value is always `float(sim_dt)`, and since
  D6 stores the coerced double, `float(sim_dt)` is the identity — the
  delivered category is `float` regardless of the interval's value, so
  rejecting int/ndarray-declared inputs at bind time needs no clock.
  Sound. ✔
- *Coercion rule vs R3-TC-03b (cont.):* verified live (Ruling 3);
  rejection classes (`TypeError` for bool/non-Real, `ValueError` for
  non-finite/non-positive) match R3-TC-03b's "exact error classes per
  approved design; message names the invalid value category" — the
  design names categories. ✔

### P5 — Numeric clock limits: **WITHSTANDS ATTACK**

- *Bit-exactness of the multiplication policy verified live:*
  `sim_time(k)` is `float(k) * sim_dt` computed fresh at each
  publication; R3-TC-03f(b)'s assertion against the identical Python
  expression is therefore exact by construction (deterministic IEEE
  double multiplication; `float(10**6)` exact; verified
  `float(10**6) * 1e-9 == 10**6 * 1e-9`). Accumulation drift is
  designed out, not bounded empirically — the strongest honest policy.
  ✔
- *Overflow-as-failed-tick coherence with AC-06:* the candidate clock
  is computed in phase 3; the clock pair is written only in phase 4;
  hence a non-finite candidate is indistinguishable from any other
  failed tick — "factory failure preserves clock" and "overflow
  preserves clock" are one mechanism. Verified boundary: largest finite
  interval admits exactly one tick; `5e-324` is accepted (finite,
  positive); `float(k)` rounding for `k > 2^53` documented as
  representable-but-rounded and unreachable in practice. ✔
- *No-advance-on-failure:* the only clock write site is publication;
  every enumerated failure (callback, validation, overflow) raises in
  phases 1–3. ∎ holds.

## Point-by-point findings

### 1. Architectural compliance — PASS

- **Plan realization, member for member.** Checked every clause of
  architecture-plan.md lines 9–94 against the design: internal
  detached-candidate/validate-before-commit machinery with no public
  rollback (lines 9–12 → object.py private helpers, Overview) ✓;
  opt-in station-associated coordinator owning enrollment, connections,
  snapshots, samples, clock, lifecycle (13–18 → D1, Coordinator
  interface) ✓; station membership alongside devices plus the shared
  interval (15–17 → D6, R3-002 preserved) ✓; mock pending/delivered
  storage with the three documented readback roles and zero
  `Parameter` change (22–27 → D2) ✓; builder convention untouched
  (29–34 → scope guard: `builder.py` byte-untouched) ✓; one boundary
  snapshot per transition with frozen pending sources, detached
  fan-out-safe candidates, publication-only-on-success with no user
  code (36–55 → D8, P1) ✓; explicit tick-zero preparation staging
  initial mock samples without implicit first delivery, no partial
  commit, inspection purity (59–63 → D9, R3-TC-02a/02b/02d) ✓; staged
  coordinated reset publishing zero together, failure-intact,
  external-effect caveat, retained ownership (65–71 → D10, D4) ✓;
  station-owned explicit positive finite non-boolean interval, fixed
  mid-run, changeable at the reset boundary, explicit validated
  model-dt binding never inferred by name, callback signatures
  unchanged, representation/roundoff/overflow settled *before*
  implementation as the plan demands (73–78 → D6, D7, D11) ✓; ownership
  guards, staged unconnected-input writes, safe read access, release at
  the stopped boundary, stable membership references (81–86 → D3, D4,
  D12) ✓; directional single-driver connections with fan-out and
  permitted cycles, atomic invalid-edit rejection, connected-removal
  rejection, preserved cross-name collision and legacy snapshot/describe
  contracts (88–94 → D3, D12, P3, compatibility §2) ✓; scoped Release 2
  corrections untouched here (96–103 → scope guard; R3-001 owns them)
  ✓. **Nothing in the planned architecture is changed or missing.**
- **Module placement correct.** `mock.py` and `coordination.py` belong
  in `softlab/tu/simulation/` beside the object whose contract they
  extend; station keeps only the two inert slots, the interval
  property and the membership guard — graph mechanics stay a
  simulation responsibility (plan lines 18–19).
- **Import direction verified, no cycle.** `coordination.py` duck-checks
  the station (`_objects`/`sim_dt`/`_coordinator`) and never imports
  `tu.station`; `object.py` imports nothing new (the claim slot is an
  opaque `Any`); `mock.py`/`coordination.py` reuse `object.py`'s
  private helpers intra-package; `station.py` already imports the
  simulation package (verified at station.py:17–21) and gains no new
  direction. Verified against the current
  `simulation/__init__.py` (8 lines, imports only `object`/`builder`)
  — adding `mock`/`coordination` exports keeps the acyclic shape.
- **Five-element discipline held.** All runtime changes in
  `softlab/tu/` (two new files, three edited); `jin`/`huo`/`shui`/`mu`
  untouched; no `huo` process, scheduler or automatic advancement —
  CHK-03-1 item 1 is enforceable against this design.
- **R3-AC-08 compatibility verified against the actual code.** The
  `reset` refactor (guard + `_reset_candidate` + `_commit`) is
  semantics-preserving against object.py:649–704 (same factory
  invocations in declaration order, same exception identity — no
  try/except anywhere on the path — same build-then-swap rebinds);
  `evolve_once`/`observe_outputs` may share the candidate helpers with
  identical copy/validate sequences (object.py:539–584, 608–647);
  every guard keys on `_active_coordinator is not None`, unreachable
  without explicit `Coordinator.enroll`; station slots are inert;
  `rm_object` leniency for missing names is preserved (the guard
  resolves the member first) and the forward-looking docstring at
  station.py:205–208 (verified present) is correctly realized, not
  contradicted. The design's promised implementation order — object.py
  refactor first, full 165-test suite must stay green before any new
  module exists — is the right compatibility gate (I re-ran the suite
  at this head: 165 tests OK).

### 2. Golden traces — independently re-derived: **ALL MATCH**

From the stated semantics (destinations at k→k+1 consume `S_k`;
`S_0` = outputs of initial states; accumulator `x' = x + u`, `y = x`;
`u` declared 0.0):

- **Chain (01a/01e):** `S_0` = (A.y, B.y, C.y) = (1.0, 10.0, 100.0).
  0→1: B.u := 1.0, C.u := **10.0**; states (1.0, 11.0, 110.0).
  1→2: B.u := 1.0, C.u := 11.0 — A's output reaches C no earlier than
  this transition; states (1.0, 12.0, 121.0). 2→3: B.u := 1.0,
  C.u := 12.0; states (1.0, 13.0, 133.0). Matches the design's table
  (lines 1066–1071) row for row. ✔
- **Fan-out (01b):** both destinations receive the identical `S_k`
  value, each a fresh copy (P1-3). ✔
- **Feedback (01c):** with C→A added, at 0→1 A.u := 100.0 (C's tick-0
  output) → A.x = 101.0; no solver, no rejection. ✔
- **Measurement companion (02d):** initial −1.0 → after 0→1: 100.0 →
  after 1→2: 110.0; the measurement never leads the boundary. ✔
- **Bound integrator (03e):** `t' = t + dt` with `dt = float(0.5)`
  delivered at every transition ⇒ after k transitions `t = k × 0.5`
  exactly; k = 3 → 1.5. Matches. ✔

No mismatches found; the test-side computation of expectations from
declared initials is uniquely determined by the specified semantics.

### 3. Feasibility and simplicity — PASS

- **Implementable verbatim by sw-tom.** Signatures, error classes,
  guard insertion points (verified against the cited object.py/station.py
  lines), state tables, the D8/D9/D10 pseudocode, the canonical-sort
  helper, the docstring citation list for CHK-03-1, and the pitfalls
  list are all specified to implementation level. Pitfall (a)
  (bool-before-`Real`) is verified necessary, not merely cautious;
  pitfalls (b)–(g) each map to a pinned case or a proof dependency.
- **State machines are total.** Two coordinator states with explicit
  transitions; the FAILED_* pseudo-states in the diagram are correctly
  annotated as *no state change* (nothing to roll back because nothing
  was published); the connection lifecycle diagram matches D12's detach
  sequence. The only under-specified corner I found is the Issue-1
  setter window.
- **Error contracts are complete** at every interface table (classes
  plus the condition that triggers them); original-exception identity
  is preserved by the zero-try/except rule on candidate paths — the
  `assertIs` cases (02b, 06a–06c) hold by construction.
- **Minimal diff:** five files, two new; zero new dependencies (stdlib
  `numbers`/`math`/`typing`/`collections.abc` plus already-imported
  NumPy) — dependency zero-trust honored; `pyproject.toml`/`setup.py`
  diffs empty by scope guard (R3-TC-08e trivially passable).
- **Full traceability:** all 41 automated cases (01a–01e, 02a–02f,
  03a–03f incl. 03b cont., 04a–04f, 05a–05g, 06a–06e, 08a–08f — count
  re-verified against test-plan Revision 2's arithmetic: 5+6+6+6+7+5+6
  = 41) map to design elements; the single unmapped element
  (`_resolved`) is disclosed with its indirect-exercise justification,
  which I accept (stable-identity rule, plan lines 85–86).

### 4. Forward seam for R3-004 — PASS, with one scope note

- The design gives R3-004 everything the worked example and guide need
  without implementing any of it: standalone-constructible
  `MockControl`/`MockMeasurement` with the deferred `before_set`/
  `before_get` wiring path explicitly named (D2); the read-only
  `station.coordinator` property as the discovery surface; golden-trace
  values (§"Determinism replay values") that double as the example's
  expected output; and the CHK-03-1 docstring citation list that gives
  the guide its anchors.
- **Scope note for sw-camille/sw-prod (awareness, no action required
  for this task):** my R3-002 review (Ruling 2) described the additive,
  inert simulation description node as belonging to "the
  connected-simulation design (R3-003)"; this design defers it to
  R3-004 ("snapshot()/describe() stay legacy device-only … additive
  simulation description remains R3-004 business", lines 1006–1008).
  The deferral is *permissible* — plan lines 93–94 mandate only that
  legacy contracts be preserved and any simulation description be
  additive and inert; no approved R3-003 case pins the node, and
  R3-TC-08d actively pins the legacy shape — but R3-004's requirements
  must now explicitly pick it up (or consciously drop it) so the plan's
  "any simulation description" clause is not orphaned. Recorded here so
  the R3-004 PRD/test-plan step cannot silently miss it.
- **Test-binding note for sw-mike (no design change):** R3-TC-02b's
  precondition mentions "initial-state factory **or** tick-zero output
  evaluation" armed to raise; the design invokes no factories at
  `prepare()` (D9 step 2 — factories fire at construction/reset only),
  so the injection point for 02b is the tick-zero `observe`
  evaluation. This is the design's prerogative under test-plan risk
  note 1 (names/mechanisms rebind to the approved design); the results
  document should record the rebind.

## What was inspected

- `log/release_3/design/r3-003-detailed-design.md` (full, 1267 lines,
  at `92eaa62`)
- `log/release_3/architecture-plan.md` (full, 109 lines — clause-by-
  clause realization check)
- `log/release_3/prd.md` (R3-AC-01–06, R3-AC-08; behavioral statements
  lines 33–66, 68–106; scope guard)
- `log/release_3/test/r3-003-test-plan.md` (Revision 2, full — 41
  cases, CHK-03-1, risks 1–8) and `r3-003-baseline.md` (full — 165-test
  anchor, GAP inventory, seven no-regression anchors)
- `log/release_3/reviews/r3-002-design-review.md` (review convention)
- `softlab/tu/simulation/object.py` (full, 804 lines: value contract
  72–127, copy policy 197–219, `set_input` 479–515, `evolve_once`
  539–584, `observe_outputs` 608–647, `reset` 649–704, callback-result
  validation 737–803, name immutability 427–438, reentrancy contract
  246–247)
- `softlab/tu/station/station.py` (full, 303 lines: `__init__` slots
  57–67, membership methods 143–255, forward-looking `rm_object`
  docstring 205–208, simulation import 17–21, `_default_station` 257)
- `softlab/tu/simulation/__init__.py` (current exports and import shape)
- Verifications executed: `git fetch origin` + `git rev-parse HEAD
  origin/codex/r3-003-coordination` (both `92eaa62`); full regression
  suite at this head (165 tests, OK, exit 0); live interpreter checks
  on the branch venv (CPython 3.13.15): `Decimal` not `numbers.Real`,
  `np.float64`/`Fraction` are `Real`, `bool` is `Real`, `complex` not
  `Real`, `5e-324` finite and positive, `1.0 * DBL_MAX` finite /
  `2.0 * DBL_MAX` infinite, `float(10**6) * 1e-9 == 10**6 * 1e-9`;
  independent re-derivation of every golden-trace value.

All source reads were read-only; no production files and no design
document content were modified by this review. The design document's
own "Design Review" section (Status: PENDING) is left for the
coordinator/designer to close against this review.

## Verdict and next steps

**APPROVED.** The design realizes the planned coordination architecture
member for member without changing the planned Release 3 architecture,
any public contract, or any standalone behavior; the candidate/commit
split correctly lifts object.py's verified build-then-swap discipline
to the whole network; all five CHK-03-1 proofs withstand direct attack
(P1's copy list is genuinely exhaustive against the actual store types;
P2's one gap is the Issue-1 setter window; P3's induction is sound
modulo an editorial miscount; P4's coercion rule is verified against
the live interpreter; P5's bit-exactness and overflow boundary are
verified numerically); every golden-trace value re-derives exactly; and
the R3-004 seam is clean. All six open questions are ruled **ACCEPT**:
reset lands STOPPED (forced by R3-TC-05f/06d); ownership claimed at
`enroll` (load-bearing for the no-dangling proof, not merely
preferable); `Decimal` rejected via the documented `numbers.Real` rule;
mock endpoints as coordinator-owned ports (plan lines 22–27 realized);
`OverflowError` as a failed tick (one mechanism with AC-06 atomicity);
one coordinator per station, permanent (Simplicity Mandate). The four
minor issues (setter `_busy` window; endpoint resolution rule made
explicit; P3-2 wording; snapshot cardinality wording) are design-text
or one-line-guard fixes that do not block implementation — sw-celeste
should apply them when closing the review section. Per the serial
workflow, sw-tom may proceed with implementation once the minors are
recorded; sw-mike should record the R3-TC-02b injection-point rebind
(and, if Issue 1's guard is adopted, the optional R3-TC-04e extension
disposition) in the results document; sw-camille/sw-prod should carry
the simulation-description-node scope note into R3-004 planning.

## Issue closure — 2026-10-11 (sw-celeste)

All four minor issues are closed in the design document, committed as
`docs(log): close r3-003 design review issues` on
`codex/r3-003-coordination` (successor of review head `e8d9841`). The
verdict of this review (**APPROVED**) is untouched.

1. **[minor] `sim_dt` setter `_busy` window — RESOLVED.** The preferred
   resolution is adopted and made a required implementation behavior:
   D6 setter step 5 now rejects when the coordinator is `prepared`
   **or** `_busy`, closing the callback-reachable window during
   `prepare()` and STOPPED-state `reset()` structurally; P2-2, P4-4, the
   Station interface table row and the Implementation Notes docstring
   citation list updated to match. Closed by sw-celeste, 2026-10-11.
2. **[minor] Endpoint resolution rule — RESOLVED.** D3 and the
   `connect` contract now state the rule explicitly: `signal=None`
   resolves in the mock-endpoint namespace; a `str` signal resolves to
   an enrolled object's declared signal; the namespaces are disjoint.
   Mock/object name sharing is documented as permitted, disambiguated by
   signal presence — the simpler disposition per the Simplicity Mandate.
   Closed by sw-celeste, 2026-10-11.
3. **[minor, editorial] P3-2 miscount — RESOLVED.** The induction now
   names the establish operation (`connect`) and the preserve operations
   (`disconnect`, `unbind_interval`, `rm_control`/`rm_measurement`,
   `withdraw`, station-side `rm_object`; the non-removing
   `bind_interval`/`enroll`/`add_*` only add references between
   resolved, enrolled parties) explicitly; the induction structure is
   unchanged. Closed by sw-celeste, 2026-10-11.
4. **[minor, editorial] Snapshot cardinality — RESOLVED.** One
   cardinality stated: `_snapshot` holds all enrolled participants'
   output Endpoints (full per-participant outputs, edge sources or
   not) — the all-outputs disposition recommended by this review,
   matching the D9/D10 text. Overview, D9 step 4 and the
   data-structure table updated. Closed by sw-celeste, 2026-10-11.

The design's "Design Review" section is updated accordingly (Status:
APPROVED at `e8d9841`; the six architect rulings recorded as ACCEPT;
hand-offs recorded for sw-tom — the `sim_dt` `_busy` guard is a required
implementation behavior — and sw-mike — R3-TC-02b injection-point
rebind: factories do not fire at `prepare()`; the injection is the
tick-zero `observe` evaluation).
