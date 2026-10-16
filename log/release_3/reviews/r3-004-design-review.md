# Review — R3-004 detailed design

Reviewer: sw-jerry (Software Architect) | Date: 2026-10-16
Verdict: **APPROVED**
Reviewed artifact:
[r3-004-detailed-design.md](../design/r3-004-detailed-design.md) at commit
`f24b38f` on `codex/r3-004-delivery` (verified via `git fetch` /
`git reset --hard f24b38f`; 783 lines, read in full).
Requirements: [prd.md](../prd.md) — R3-AC-08 clauses 1–4 (prd.md:146) +
release-wide R3-AC-01–08 | Architecture authority:
[architecture-plan.md](../architecture-plan.md) | Approved test plan:
[r3-004-test-plan.md](../test/r3-004-test-plan.md) (Revision 2) | Baseline:
[r3-004-baseline.md](../test/r3-004-baseline.md) (GAP-1…GAP-6; 207-test
baseline — re-verified live at the review head: 207 tests, OK) | Review
convention: [r3-003-design-review.md](r3-003-design-review.md)

## Issues

No blockers. Two minor items, neither of which changes the verdict.

1. **[minor] A1 Stage-1 header self-reference and fill sequencing are
   not stated.** Location: §A1 (design lines 444–458) together with the
   Implementation Notes order (lines 695–703: arch.md edits at step 4;
   full suite at step 6; candidate CI only after push). Stage 1 requires
   the header to carry the "R3-004 candidate head SHA + candidate CI run
   ID (filled once the run exists — never before)" and "full suite 210
   tests green at the candidate head". A commit containing those facts
   necessarily post-dates the run that established them, so the cited
   run certifies an *ancestor* of the commit carrying the header — the
   same off-by-one the Release 2 precedent already embodies (the
   `d52217c` header cites `dev` tip `470e2c7`, an ancestor of `d52217c`
   itself; verified in git history). The precedent makes the convention
   acceptable, but the design never states it, and 09m3(a)/09m4's
   "recorded SHAs match the commits they certify" could be misread as
   demanding self-certification, which is unreachable. Required change:
   state the convention explicitly in A1 — Stage-1 facts are filled in
   the final delivery docs commit *after* the content-complete candidate
   run exists, citing that run's ID and head SHA (an ancestor of the
   fill commit); the "210 green" clause is filled in the same commit and
   never before the suite run at that head exists; the fill commit's own
   push/run is the final candidate evidence recorded by 09m4 in the
   results document, and Stage 2 reconciles everything at integration.
   No test-plan impact: 09m3(a)'s "same discipline" is satisfied by the
   stated convention, as `d52217c` demonstrates. Owner: sw-celeste;
   awareness sw-tom, sw-mike.
2. **[minor, editorial] E2 elides that `enroll` takes the member name,
   not the object.** Location: §E2 step 2 (design line 180) — "`enroll`
   A, B, C, D". `Coordinator.enroll(object_name: str)`
   (coordination.py:452, verified) resolves against station membership;
   passing the object itself raises `KeyError` (verified live during
   this review's scratch execution). The surrounding design is
   unambiguous to a careful reader — §E1 defines A–D as objects and the
   `connect` lines use `Endpoint('A','y')` string names — but the one
   enrollment line is exactly where an implementer can stumble. Required
   change: state "enroll by station member name (`'A'`, `'B'`, `'C'`,
   `'D'`)" in E2 step 2 or in the Implementation Notes. Owner:
   sw-celeste.

## Rulings on the five open questions (architect decisions, explicit)

### Ruling 1 — Mock endpoints AS the clause-2 "mock interaction" surface; no Device/Parameter hook wiring in the example (Q1): **ACCEPT**

1. **This is the architecture plan's wording, realized.**
   Architecture-plan lines 22–27 assign the mock endpoints the
   pending/delivered storage roles and demand the three readback roles
   be *documented* rather than `Parameter` behavior changed; lines
   18–19 forbid inflating device/parameter abstractions with graph
   mechanics. The PRD exclusion (prd.md:158-159) is explicit: "Device
   signal participation is opt-in for mock signals, not automatic
   execution of arbitrary device parameter hooks." Clause 2's "mock
   interaction" is an observable-behavior phrase, and the example
   observes it completely: pending staging with immediate readback,
   next-transition delivery, tick-zero measurement initial vs delivered
   samples, reads/writes never ticking. No R3-AC clause requires hook
   wiring, and the approved test plan (09a) requires exactly "one
   `MockControl` and one `MockMeasurement`" — nothing more.
2. **The r3-003 Q-d carry-over is closed consistently.** My R3-003
   Ruling 4's statement that "the deferred `before_set`/`before_get`
   wiring is correctly R3-004 example scope" was a *permissive*
   deferral (no approved R3-003 case requires it), not a mandate that
   R3-004 must wire hooks. The carry-over's substance — the three
   readback roles must be documented distinctly — is discharged by this
   design's G3 (§9 deferral resolution), G5 (mock-interaction and
   connected-vs-standalone rows), G6 (honesty guards) and A5 (bridge
   paragraph correction). The hook-wiring path remains available to
   users as §9's documented closure pattern, unchanged; duplicating it
   around mock endpoints would demonstrate no new semantics (the
   design's own argument, which I accept under the Simplicity Mandate).
3. **Scope coherence:** wiring endpoints into `Device`/`Parameter`
   would also sit awkwardly with D2 (endpoints are coordinator-owned
   ports, never station members) — a wired parameter would need a
   device the architecture deliberately does not create. ACCEPT.

### Ruling 2 — Two-stage arch.md header (Q2): **ACCEPT, with the Issue-1 clarification**

1. **The precedent is real and matches.** `d52217c`
   (`docs(arch): record release 2 simulation implementation`) landed the
   Release 2 architecture record on `dev` *post-integration*, citing the
   integration merge `9408c79`, suite size 135 and CI run 37298623628 —
   and citing `dev` tip `470e2c7`, an ancestor of the record commit
   itself (verified in history). Stage 2 of A1 is exactly this pattern
   replayed for Release 3; Stage 1 is its candidate-scope analogue.
2. **No overstatement risk, given the stated guards.** The clause
   "architecture describes implemented capability only after delivery"
   (R3-AC-08 clause 4) is honored because Stage 1 records only facts
   that exist at record time — R3-001/002/003 are integrated into `dev`
   (merges `6dc4d36`/`0bd97d5`/`2991a63` verified; dev CI run
   38820136267 verified `success` at `2991a63` via `gh run view`) — and
   the R3-004 facts are labeled candidate-scope with the CI citation
   gated on the run's existence (R3-001 REC-1 lesson, correctly carried).
   The wording requirement "names this the Release 3 candidate point" is
   the load-bearing honesty device; sw-tom must not let the Stage-1
   sentence read as an integration claim.
3. **The one gap is procedural, not conceptual:** the self-reference /
   fill sequencing convention is unstated (Issue 1). With that recorded,
   the two-stage discipline satisfies 09m3(a) at candidate test time.
   ACCEPT.

### Ruling 3 — Append §12/§13 without renumbering (Q3): **ACCEPT**

1. **The mechanical pin is verified, not assumed.**
   `tests/test_sim_user_guide.py:39` splits on the literal
   `'## 11. Executed example'` (read verbatim); §11 is the guide's last
   section (heading at simulation.md:327, file ends at :401 — verified).
   Appending §12/§13 after §11 preserves the split target exactly.
2. **The extraction indices are stable by ordering, not by luck.** The
   Release 2 harness takes the *first* ` ```python ` and first
   ` ```text ` blocks after the §11 heading; §11's own blocks precede
   any §12/§13 content by construction (§12 is inserted between §11 and
   §13, i.e. after §11's blocks). SIM-TC-08b therefore keeps passing
   with zero harness edits — R3-TC-09c becomes trivially true, which is
   the correct way to make a regression anchor cheap.
3. **The alternative is strictly worse.** Renumbering §11 would break
   the split literal for zero benefit and force an edit to a byte-stable
   Release 2 test file — churn the Simplicity Mandate forbids. The
   pedagogical oddity (executed example §11 before the conceptual §12)
   is a fair price and is honestly disclosed in the design. ACCEPT.

### Ruling 4 — Two new test files, zero edits to existing test files (Q4): **ACCEPT**

Test-plan Revision 2's 09g `tests/` row *permits* design-bound extension
of `tests/test_sim_user_guide.py`; it does not require it. The design's
disposition — `tests/test_sim_connected_guide.py` (09a/09b) and
`tests/test_arch_links.py` (09h), both new, the Release 2 harness
byte-untouched — satisfies the 09g enumeration ("lists only R3-004-scope
test changes…; no unrelated test edits") in its strictest reading and
keeps the Release 2 suite's risk surface at zero. The mirrored
extraction convention (same split-then-fence pattern applied to the
§13 literal) reuses the proven idea without inheriting its file. This
is the minimal-diff, single-responsibility choice. ACCEPT.

### Ruling 5 — Two ticks plus write/reset/re-run suffice for clause 2 (Q5): **ACCEPT**

Every clause-2 element is demonstrated by tick 2, and I verified each
one live (see "Golden trace" below): shared clock (`sim_time ==
float(tick) * sim_dt`: 0.0 → 0.5 → 1.0); chain delay (C.u = **10.0**,
not 11.0, at 0→1 — the previous-boundary rule made visible); no
same-tick propagation (A's output reaches C only at 1→2); delayed
feedback (A.u := 100.0 → A.x = 101.0; no solver, no rejection); mock
control interaction (pending readback 3.0 between ticks, delivered at
1→2, no tick on write); mock measurement interaction (tick-zero initial
−1.0 distinct by construction → 100.0 → 110.0); fan-out (C.y feeds both
A.u and probe); coordinated reset (clock zero, `prepared=False`, states
restored, pending write of 3.0 does not leak — `drive` reads its
constructor initial 2.0, `probe` reads −1.0); re-run determinism
(tick-1 trace reproduced exactly). A third tick would repeat the
established pattern without demonstrating anything new; the smallest
complete example is the right example for a user guide (Simplicity
Mandate). `bind_interval` is *not* required — clause 2 names "the
clock, delayed feedback, mock interaction and reset", and the clock is
demonstrated via `sim_dt`/`tick`/`sim_time`; §12 documents the binding
for 09m1. ACCEPT.

## Golden trace — independently re-derived and executed: **ALL MATCH**

I re-derived §E4 from the declared semantics (D8/D9/D10: destinations at
k→k+1 consume the committed boundary snapshot `S_k` plus frozen pending
controls; `S_0` = outputs of initial states at `prepare`, no delivery at
prepare, measurement tick-zero sample is the constructor initial;
`sim_time == float(tick) * sim_dt`; `reset` restores constructor
initials including control pendings and lands STOPPED):

- **Chain:** `S_0` = (A.y, B.y, C.y) = (1.0, 10.0, 100.0). 0→1:
  B.u := 1.0, C.u := **10.0** (B's tick-0 output, not its new 11.0);
  states (1.0→with feedback 101.0, 11.0, 110.0). 1→2: B.u := 101.0,
  C.u := 11.0 — A's tick-0 output reaches C no earlier than this
  transition. Matches the r3-003 canonical derivation
  (r3-003-detailed-design.md:1097-1131, read verbatim) row for row,
  extended consistently by the feedback edge.
- **Feedback:** at 0→1, A.u := C's tick-0 output 100.0 → A.x = 101.0;
  at 1→2, A.u := C's tick-1 output 110.0 → A.x = 211.0. No solver, no
  rejection. ✔
- **Control:** pending frozen at 2.0 for 0→1 (D.u := 2.0, D.x = 2.0);
  `write(3.0)` between ticks stages pending (readback immediate, no
  tick, D.x unchanged); 1→2 delivers D.u := 3.0 → D.x = 5.0. ✔
- **Measurement:** −1.0 (tick zero) → 100.0 (after 0→1) → 110.0 (after
  1→2); never leads the boundary. ✔
- **Reset:** clock zero, `prepared=False`, states (1.0, 10.0, 100.0,
  0.0), pending restored to 2.0 (the write of 3.0 does not leak), probe
  restored to −1.0, STOPPED. Re-run `prepare()`+`step()` reproduces the
  tick-1 trace exactly. ✔

**Live execution check (scratch run, not the deliverable):** I built
the E1/E2 scenario verbatim against the delivered implementation at
`f24b38f` (fresh `Station`, `sim_dt = 0.5`, five edges in the E1 order)
and executed the E2 spine. The captured output matches the §E5 recorded
block **line for line, value for value** — including
`delivered: A.u=100.0 B.u=1.0 C.u=10.0 D.u=2.0` at tick 1,
`delivered: A.u=110.0 B.u=101.0 C.u=11.0 D.u=3.0` at tick 2, the
between-ticks readback line (`tick still 1, D.x still 2.0`), the reset
block (`prepared=False`, pending 2.0, probe −1.0) and the re-run block.
The design's "design-time verification" claim (lines 246–248) is
independently confirmed. All printed values are exact small IEEE-754
halves/integers under `:.1f`; byte-reproduction carries no formatting
risk.

## Point-by-point findings

### 1. Rulings on the five open questions — recorded above

All five ruled **ACCEPT** (Q2 with the Issue-1 clarification). The Q1
ruling closes the r3-003 Q-d carry-over consistently with the
architecture plan: the mock endpoints *are* the opt-in mock signal
surface (plan lines 22–27; PRD exclusion prd.md:158-159); the three
readback roles are documented (G3/G5/A5) rather than `Parameter`
behavior being changed; no approved case or AC clause requires hook
wiring in the example.

### 2. Golden trace correctness — PASS

See the dedicated section above: re-derived from declared semantics
*and* executed against the delivered implementation; every cell of §E4
and every line of §E5 confirmed, including the two easy-to-miss values
(C.u = 10.0 not 11.0 at 0→1; the staged 3.0 delivered exactly at 1→2)
and the reset non-leak of the pending write. The test-side design (T1:
the in-test reference transition — not the implementation's output — is
the expectation source) correctly implements the 09a derivation rule
and pre-classifies a mismatch as a bug report (test-plan risk note 3).

### 3. Guide-section design — PASS

- **§13 as sole example location:** consistent with repo conventions —
  no `examples/` directory exists (baseline GAP-1, verified), and the
  §11 executed-example convention (simulation.md:327-401) is the
  established pattern. Single-source-of-truth holds: R3-TC-09b's
  identity clause is satisfied by construction because 09a and 09b
  extract the *same* guide code block (T1).
- **09b byte-reproduction:** the mirrored extraction convention
  (split on the exact heading literal, then single ` ```python ` /
  single ` ```text ` block) reuses the proven §11 mechanics
  (test_sim_user_guide.py:35-42, 86-93 — read verbatim) without editing
  the Release 2 harness; the trailing-newline comparison discipline
  (`rstrip('\n')` / `strip('\n')` plus `endswith('\n')`) matches
  SIM-TC-08b's. Determinism prerequisites are fully specified: `:.1f`
  formatting, literal A/B/C/D enumeration (no dict iteration), ASCII
  only (SIM-TC-08a's no-CJK check stays green), fresh `Station` (no
  module-global default), no NumPy/wall-clock/randomness.
- **Standalone/connected honesty (09m2):** G3 replaces the deferral
  paragraph (verified present at simulation.md:257-263) with the
  resolved split; the required statements match the delivered semantics
  exactly — pending-source control readback (mock.py:125-139),
  delivered-sample measurement reads with the tick-zero constructor
  initial (mock.py:142-170, 198-215; coordination.py:1136-1143 phase-3
  delivery — all read verbatim). G1's stale-export fix is real: guide
  lines 11–12 still claim `SimulatedObject` is "the only public name of
  the subpackage" (verified) while `__init__.py:3-15` exports eight
  names (verified). G6's four negative guards are the right honesty
  checklist.
- **No renumbering impact:** Ruling 3 — the §11 extraction split is
  preserved and the first-block indices are stable by section ordering;
  §12 snippets cannot confuse either harness.

### 4. arch.md A1–A10 edits — PASS, with Issue 1

- **Claim-by-claim traceability sound; anchors stable.** The production
  diff is empty by construction (zero-change design, and I confirmed the
  delivered capability suffices by executing the example at the review
  head), so every A9 anchor is stable from `2991a63` through
  integration. I spot-checked every A9 row against the code at
  `f24b38f`: eight-name export surface (`__init__.py:3-15` — all eight
  names on those lines) ✓; `Coordinator` class/:171-221 and
  `__init__` duck-check :223-259 ✓; lifecycle :181-186, clock readable
  after start :292-330 ✓; `prepare` :967-1035 ✓; `step` phases
  :1037-1163 with the S_k-only/freeze machinery at :1104-1135 ✓; clock
  multiplication policy :196-202 and atomic overflow :1144-1150 ✓;
  reset :1165-1258 with pending restoration :1247-1249 ✓; `Endpoint`
  :105-162 and resolution rule :370-436 ✓; single-driver/atomic graph
  edits :750-856 ✓; `bind_interval` :858-924 ✓; safe detach
  :509-566/:820-856 + station.py:324-331 ✓; `MockControl` mock.py:33-139
  ✓; `MockMeasurement` mock.py:142-215 ✓; `sim_dt` station.py:105-168 +
  `_validate_interval` coordination.py:58-97 ✓; membership
  station.py:100-102/:273-335 ✓; builder builder.py:19-101 +
  station.py:337-376 ✓; claim slot/guards object.py:390/:521-522/:586/
  :715 ✓; malformed callback-key errors object.py:947- ✓; legacy
  snapshot/describe station.py:190-239 ✓. Zero mismatches.
- **Two-stage header precedent applied correctly:** `d52217c` verified
  as post-integration on `dev` citing a neighboring certified point;
  Stage 2 replays it; Stage 1's factual basis is verified (merges
  `6dc4d36`/`0bd97d5`/`2991a63` on `dev`; dev CI run 38820136267
  `success` at `2991a63` via `gh run view`). The unstated fill
  sequencing is Issue 1.
- **GAP-4 bridge correction accurate:** arch.md:302-321 documents
  set/observation wiring but not the R3-001-corrected readback
  ("authoritative" appears only in the sim-spec sentence — verified);
  A5 adds exactly the corrected statement with the connected-mode
  cross-reference, preserving the existing wiring content. The
  correction matches the guide's §9 wording (simulation.md:248-255).
- **Exclusions/PLANNED resolution:** A6/A8 correctly convert the
  now-false exclusions ("a dedicated clock API", "multi-object
  scheduling") into *automatic*-only exclusions and add the deferred
  snapshot/describe node to tracked future work with its deferral
  reference; the [PLANNED] block (arch.md:452-462, verified single) is
  enumerated against delivered R3 capability rather than silently
  edited. Both directions of the 09m3(c) marker check are addressed.
- **A3 precision edit:** correct and conservative. Verified: the
  simulation package still imports nothing from `softlab` outside
  itself (coordination.py: stdlib + intra-package only, :37-55;
  mock.py: typing + object.py helpers, :26-30; object.py: stdlib +
  NumPy; builder.py: typing + object.py) while station.py imports the
  simulation package (station.py:17-24) — the one-way direction is
  simulation-independent-of-station, exactly as A3 states. Observation
  (no issue): the existing class-level sentence "imports nothing from
  `softlab`" remains literally true even after Release 3; the edit is
  a clarity improvement, not a correctness fix, and is safe either way.
- **A10 link-integrity rules safe for the 09h extractor:** the
  path-extension regex (`/` plus a known suffix) cannot match
  `path:line` citation spans, so plain-text citations cannot produce
  false dangling references; the prohibition on linking
  not-yet-existing files (r3-004-test-results.md by name in prose only)
  prevents the one self-inflicted 09h failure mode; the non-vacuous
  guard (collection non-empty, ≥1 `log/release_3/` target) prevents a
  silently-empty pass. Sound.

### 5. Scope guards — PASS

- **Zero production changes, verified sufficient:** the complete change
  inventory is docs + two new test files + arch.md + process records;
  `softlab/tu/`, packaging metadata and all existing test files have
  empty diffs by construction. Sufficiency is not asserted but
  demonstrated — the worked example runs on the delivered public API
  today (my scratch execution). No `snapshot()`/`describe()` schema
  change anywhere in the design (scope-gate §2 enforcement).
- **09g pins preserved:** the diff inventory matches Rev 2's pins
  exactly — empty `huo`/`jin`/`shui`/`mu`, empty `tu` (no design
  change), empty packaging, `tests/` listing exactly two new paths
  (the design explicitly declines the permitted harness extension —
  the strictest satisfying reading), no snapshot/describe schema
  change. Expected suite arithmetic 207 + 3 = 210 is consistent with
  the baseline (re-verified: 207 OK at `f24b38f`).
- **09m6 ratification wording fit for sw-prod/sw-camille:** the record
  is framed as a *recommendation to ratify*, names the ratifiers
  (sw-prod/sw-camille — explicitly "their call, not the designer's"),
  carries the full basis (gate analysis, byte-stable pins, PRD scope
  discipline, zero-production-change confirmation), and specifies the
  override path (bounded design addendum + new cases + test-plan
  revision before any implementation; 09g would need revision first).
  This is exactly the "explicitly pick up or consciously drop"
  disposition the r3-003 review carry-over (lines 507-519) demanded —
  the node is consciously dropped, with the decision recorded where
  09m6 looks for it (sprint-board.md). Release 1 debt
  (OBS-004/005/006, DEFECT-2) stays tracked and untouched. ✓

## What was inspected

- `log/release_3/design/r3-004-detailed-design.md` (full, 783 lines,
  at `f24b38f`)
- `log/release_3/architecture-plan.md` (full, 109 lines — Q1/Q2
  consistency check against lines 18–27, 93–94)
- `log/release_3/prd.md` (R3-AC-08 clause text at :146; scope and
  exclusion statements at :148-159)
- `log/release_3/test/r3-004-test-plan.md` (Revision 2, full — cases
  09a–09h/09m1–09m6, §2 scope gate, §4 matrix, risk notes) and
  `r3-004-baseline.md` (GAP-1…GAP-6)
- `log/release_3/reviews/r3-003-design-review.md` (full — Q-d
  carry-over wording at :197-198 and :507-519; review convention) and
  `r3-004-test-plan-review.md` (full — Rev 2 condition and closure)
- `log/release_3/design/r3-003-detailed-design.md:1097-1131`
  (canonical golden-trace derivation — read verbatim)
- `docs/user-guide/simulation.md` (full, 401 lines: stale export claim
  :11-12, §8 :224-229, §9 :231-311 with deferral :257-263, §10
  :312-325, §11 :327-401)
- `tests/test_sim_user_guide.py` (:18-19 path resolution, :35-42
  extraction, :86-93 exec/redirect, :39 split literal — read verbatim)
- `arch.md` (full, 462 lines: header :5-17, `tu` row :27, simulation
  section :210-330 with bridge :302-321 and exclusions :323-330,
  release-scope :426-450, [PLANNED] :452-462)
- Production anchors (all spot-checks listed in finding 4):
  `softlab/tu/simulation/{__init__,object,builder,mock,coordination}.py`,
  `softlab/tu/station/station.py` — every A9 row and every E3/E4/G3/G5
  inline citation verified at `f24b38f`
- Git history: merges `6dc4d36`/`0bd97d5`/`2991a63` on `dev`;
  precedent `d52217c` (post-integration arch record citing ancestor
  `470e2c7`); `gh run view 38820136267` → `success` at `2991a63`
- Verifications executed: `git fetch` + `git reset --hard f24b38f`;
  full regression suite at the review head (207 tests, OK, exit 0);
  **live scratch execution of the E1/E2 example program against the
  delivered implementation** — output matched the §E5 recorded block
  line for line (including the C.u=10.0-not-11.0 delay value, the
  staged-write delivery at 1→2, the reset non-leak and the re-run
  reproduction); negative check that `enroll` rejects a non-string
  (Issue 2).

All source reads were read-only; no production files and no design
document content were modified by this review. The design document's
own "Design Review" section (Status: PENDING) is left for the
coordinator/designer to close against this review.

## Verdict and next steps

**APPROVED.** The design delivers exactly the three surfaces R3-AC-08
clause 2/4 require and nothing more: a single executed connected
example in guide §13 whose golden trace I independently re-derived from
the declared semantics *and* reproduced byte-for-byte against the
delivered implementation at the review head; guide updates that resolve
the R3-001 deferral honestly (the stale export claim, the §9 deferral
paragraph and the §10 exclusion are all verified real staleness, and
the required replacement statements match the delivered semantics
citation-for-citation); and final architecture records with
claim-by-claim file:line traceability whose anchors I spot-checked to
zero mismatches under an empty production diff. All five open questions
are ruled **ACCEPT**: mock endpoints as the clause-2 mock-interaction
surface (closes the r3-003 Q-d carry-over consistently with plan lines
22–27 and the PRD exclusion); the two-stage arch.md header (matches the
verified `d52217c` precedent; no overstatement given the candidate
labeling and never-before-CI gating — with the Issue-1 sequencing
clarification); §12/§13 appended without renumbering (the §11
extraction split is preserved and stable by section ordering); two new
test files with zero edits to existing ones (the strictest satisfying
reading of 09g Rev 2); and two ticks plus write/reset/re-run (the
smallest complete demonstration of every clause-2 element). The two
minor issues (A1 Stage-1 fill-sequencing convention; E2 `enroll`
argument kind) are design-text fixes that do not block implementation —
sw-celeste should apply them when closing the review section. Per the
serial workflow, sw-tom may proceed with implementation once the minors
are recorded; sw-mike should note that 09m3(a)/09m4's SHA discipline is
satisfied by the Issue-1 convention (the header cites an ancestor's
certified run; the final head's run lives in the results doc);
sw-prod/sw-camille should expect the 09m6 ratification record in
sprint-board.md using the design's supplied wording.

## Issue closure (sw-celeste)

The verdict above is untouched; both minor issues are closed as
design-text fixes in the closure commit on `codex/r3-004-delivery`.

1. **[minor] A1 Stage-1 header self-reference/fill sequencing —
   RESOLVED.** Closed by sw-celeste, 2026-10-15. What changed: §A1
   Stage 1 of
   [r3-004-detailed-design.md](../design/r3-004-detailed-design.md)
   now states the fill-commit convention explicitly — Stage-1 facts are
   filled in the final delivery docs commit *after* the
   content-complete candidate run exists; the cited candidate CI run
   certifies an ancestor of the fill commit (the same off-by-one the
   `d52217c` precedent embodies); the "210 green" clause is filled in
   the same commit and never before the suite run at that head exists;
   the fill commit's own push/run is the final candidate evidence 09m4
   records in the results document, with Stage 2 reconciling at
   integration. This satisfies 09m3(a)'s "same discipline" as stated.
   Commit reference: `docs(log): close r3-004 design review issues` on
   `codex/r3-004-delivery`.
2. **[minor, editorial] E2 `enroll` argument kind — RESOLVED.** Closed
   by sw-celeste, 2026-10-15. What changed: §E2 step 2 now reads
   "enroll by station member name (`'A'`, `'B'`, `'C'`, `'D'`)" and
   notes that `Coordinator.enroll(object_name: str)` takes the name
   string and passing the object raises `KeyError`. Commit reference:
   `docs(log): close r3-004 design review issues` on
   `codex/r3-004-delivery`.
