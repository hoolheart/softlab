# TU-006 architecture design review

Reviewer: sw-jerry (architect). Candidate: `c313207` on
`codex/tu-006-visa-contracts` (`log/release_1/design/TU-006.md`, step-split
design). Verdict: **CHANGES REQUESTED** — three issues, all minor and
design-text level; no redesign, no re-litigation of accepted decisions.
Scope is detailed-design compliance with the Release 1 PRD (TU-006 row),
the approved TU-006 test contract and its implementability review (both
blocking rounds CLOSED), the TU-001 compatibility matrix
(OBS-001/OBS-002/OBS-003), the TU-004 lifecycle design being mirrored, the
Release 1 architecture constraints (`log/release_1/arch-review.md`), and
the five-module architecture. This is not a code review, test execution or
integration approval.

I independently re-read `softlab/tu/station/visa.py` at the branch tip,
`tests/test_tu_visa_contracts.py` (all 17 cases, including the case-8 fix
at `25cc5e1`), `tests/test_tu_contracts.py` (VisaContracts class, lines
197–233), `tests/test_tu_operations.py` (lines 15–30),
`tests/test_compatibility.py` (lines 131–147), and `tests/visa_sim.yaml`.
Source-level claims below are verified against those files.

## Assessment against review criteria

1. **Architecture fit — confirmed.** The change is additive-only in
   `softlab/tu/station/visa.py`: additive members on `VisaHandle`, one
   module-level `NamedTuple`, one constructor-signature change, one
   behavior fix (OBS-002), one delegation (`close()` → `cleanup()`). Only
   `tu` is touched; five-element boundaries hold. New imports are stdlib
   only (`threading`, `NamedTuple` from the already-imported `typing`);
   the dependency table's "(none)" verdict is accurate. The decision not
   to export `AbortReport` from `__init__.py` is justified (the
   acceptance suite accesses it only via returned attributes) and
   explicitly flagged for revisit at TU-008. No VISA backend behavior is
   claimed beyond evidence: interruption is honestly framed as
   backend-honored with the test modeling the release, manager release is
   attributed to PyVISA process teardown, and `@sim` (pyvisa-sim 0.7.1)
   is the only instrument-level verification. Verified against
   `visa_sim.yaml`: Weinschel (`GPIB::1::INSTR`) has the `*OPC? → 1`
   dialogue case 17's `confirm_stop()` needs; Keithley
   (`GPIB0::26::INSTR`) has `*IDN?` but no `*CLS`, consistent with the
   suite's `device_clear=False` on both `@sim` constructions.
2. **Step split A→B→C→D — sound, with one wrong green-condition
   prediction (Issue 3).** Ordering, independence and per-step green
   conditions are otherwise correct. Step A keeps the constructor
   signature unchanged and routes the legacy unconditional raw timeout
   assignment through `_acquire()` — guards 1 and 6 stay green because
   both construct with an *explicit* `timeout=5.0` (verified:
   `test_tu_visa_contracts.py` lines 128–129, 236–237). The early-green
   note for case 15 at step A is correct (cleanup sets `_resource=None`;
   the legacy operations already raise `RuntimeError('Invalid visa
   resource')`; `prepare()` exists from step A). The step-B test
   co-requisites are correctly identified and scoped: the case-8 fix
   (landed at `25cc5e1`, verified — `quick_resource.timeout == 1500`
   targets the second handle's mock) and the TU-001 supersession at
   `tests/test_tu_contracts.py:224`. I swept every `resource.timeout`
   assertion in the repo: line 224 is the **only** assertion of raw 5.0
   on default construction in the existing suites; `test_tu_operations.py`
   and `test_compatibility.py` construct with defaults but assert no
   timeout, so the handoff is complete, not partial. Guard-stay-green
   conditions are honest.
3. **TU-004 lifecycle adoption — confirmed, mirror-divergences
   legitimate.** All five divergences (eager failed-acquisition release
   with no `FailedPendingRelease`/`_needs_cleanup`, no cleanup-first rule,
   `supports('connection') = True`, no subclass hooks, the
   operations-serialized/lifecycle-single-threaded split per TU-004
   decision 3) are correct consequences of eager self-contained
   acquisition and are documented with rationale, not merely asserted.
   Failed-init cleanup is exactly-once with original exception identity:
   the `_acquire()` `try` is scoped strictly after the
   `MessageBasedResource` check (the legacy rejection path closes once
   and must not double-close), the cleanup `close()` is a single attempt
   whose own failure is logged and suppressed, and the `raise` is bare —
   matching case 4's two `assertIs` blocks. `borrow()` non-ownership,
   retained-manager `prepare()` re-open with stored-config replay
   (required for case 17's Keithley `\r` terminations and honored
   `device_clear=False` on re-open), and the borrowed-`prepare()`
   `RuntimeError` are all pinned. The `close()` blast-radius verification
   is performed, not claimed, and I re-verified it:
   `test_compatibility.py:147` closes once; `test_tu_operations.py:24`
   closes once via cleanup; `test_tu_contracts.py` registers
   `addCleanup(self.handle.close)` (line 206) *and* closes explicitly
   (line 219), but the `assert_called_once_with()` (line 220) runs before
   the cleanup fires and the second close is a no-op under delegation —
   the characterization stays green.
4. **Concurrency/interruption model — sound; one dead field
   (Issue 1).** `_serialized` preserves legacy call shapes exactly —
   verified against `visa.py` lines 150–178 and the pinnings in
   `test_tu_contracts.py` (`write(message=..., encoding=...)` keyword,
   `query(command, delay)` positional); the resource-validity check
   inside the lock is atomic with operation boundaries and performs zero
   I/O on rejection (case 15). `abort()` is flag-based, never touches
   `_op_lock`, never writes to the device — required by case 12's
   structure (the release event is set only after `abort()` returns) and
   correctly so. `AbortReport` never conflates request with confirmation
   (`stopped` always `False` from `abort()`; `handle.stopped` untouched);
   the report *shape* is contract-driven by case 12 and therefore not a
   simplicity violation. `confirm_stop()` delegates to
   `query('*OPC?', None)` (inheriting serialization, post-cleanup
   rejection and error identity for free), parses with
   `str(...).strip() == '1'` (the real `@sim` `"1"`-plus-`\r` round trip
   passes), latches `stopped`, and `cleanup()` resets the latch — the
   only reading consistent with cases 13 and 17. The thread-safety claims
   are honest: the unsynchronized `_in_flight` int read is documented as
   GIL-non-tearing with stale reads affecting only the advisory report
   flag, and lifecycle-vs-operation races are declared out of contract
   rather than policed.
5. **Timeout semantics — correct, one annotation inconsistency
   (Issue 2).** The `None` sentinel, the 5.0 s default (5000 ms on the
   resource), ×1000 in both directions, `None` disables both ways, and
   explicit-raw-wins precedence are pinned and are the only reading
   consistent with guards 1/6 (explicit `timeout=5.0`/`12.5` raw) and
   cases 7/8/17 (default construction → 5000 ms). OBS-001's
   no-silent-rescale constraint is intact: explicit raw forwarding is
   never rescaled on constructor, set or get paths.
6. **OBS-002 disposition, error identity, post-cleanup zero I/O —
   confirmed.** Step D routes `write_raw` to `resource.write_raw(message)`
   positionally, matching case 16's `assert_called_once_with(message)`
   and `assertIs` on the bytes argument; the `resource.write` divergence
   at `visa.py:171` is the recorded defect. Error identity is structural:
   bare `raise` everywhere, no wrapping, no `raise ... from`; the legacy
   `raise e` in `__init__` preserves object identity for `assertIs`.
   Post-cleanup `RuntimeError('Invalid visa resource')` with zero I/O is
   guaranteed by the check-inside-lock placement.
7. **Simplicity rejections and docstring mandates — mostly complete;
   three gaps below.** The rejected-alternatives audit gives a reason for
   every rejection. The verbatim docstring mandates (sentinel precedence,
   OBS-003 guarantee, request/confirmation separation, latch semantics,
   `close()` delegation semantics change, single-threaded lifecycle
   boundary) are unusually strong and remove developer guesswork. But the
   audit that rejects "a second lock for `_in_flight`/`_abort_requested`"
   never justifies *keeping* `_abort_requested` (Issue 1), and the step
   table contains a wrong gating claim (Issue 3).

## Issues (all required changes; all design-text level)

### Issue 1: `_abort_requested` is write-only dead state — remove it or name its consumer

The design's own pseudocode shows the flag is **never read to control
anything**: `abort()` sets it when an operation is in flight; the
`_serialized` `finally` clears it; no code path branches on it; it is
never communicated to the backend (decision 1 explicitly forbids device
I/O in `abort()`, and the report's `aborted` field is computed from
`_in_flight > 0`, not from the flag). Deleting the field and its two
assignment sites changes no observable behavior and breaks none of the
17 cases — the abort-report triad, the idle no-op, and error identity
are all functions of `_in_flight` alone. The Simplicity Mandate ("if
removal does not break any requirement, it does not belong") applies
directly, and it is inconsistent for the design to reject a second lock
as "an entity without necessity" while retaining a boolean whose only
effect is being cleared. **Requested change:** remove `_abort_requested`
from the state table, the `abort()`/`_serialized`/`borrow()` pseudocode
and the class diagram; if the designer believes a consumer exists (e.g. a
future backend-honored interruption path), name it and pin the
read semantics — otherwise delete. One paragraph of the "Flag
consumption" rationale is then also deleted.

### Issue 2: `timeout_seconds` constructor annotation contradicts the `None`-disables contract

The public surface and state table declare
`timeout_seconds: float = 5.0` / `_timeout_seconds: float`, while the
timeout construction contract states "`timeout_seconds=None` disables
the timeout (resource receives `None`)" and the `_acquire` pseudocode
implements `None if seconds is None else seconds * 1000`. The property
type is already correctly `Optional[float]`. **Requested change:**
annotate the constructor keyword and the `_timeout_seconds` field as
`Optional[float]` (signature `timeout_seconds: Optional[float] = 5.0`),
and update the class diagram accordingly. No acceptance case constructs
with `timeout_seconds=None`, so this is a contract-honesty fix, not a
test change.

### Issue 3: Step-C green condition wrongly lists case 17 as RED / "genuinely D-gated"

Case 17 (`test_sim_end_to_end_resource_outcomes`) exercises `borrow`
(step A), the seconds default and `timeout_seconds` readback (step B),
and `confirm_stop()`/`stopped` (step C) — it never calls `write_raw`, so
nothing in it depends on step D's production delta (the OBS-002 fix plus
docs dispositions). The design's own early-green note undercuts itself:
it says case 17 "exercises A+B+C together", which means it turns green
at step C, not D. After step C the full suite state is: cases 1–15 and
17 green, case 16 RED (the OBS-002 regression) — only case 16 is
genuinely D-gated. **Requested change:** correct the step-C row ("case
16 RED" only) and rewrite the early-green note to extend the case-15
treatment to case 17 (expected green at C, must stay green; step D
remains load-bearing because case 16 pins OBS-002 and the
`compatibility.md` dispositions land there). This is a prediction
accuracy fix — an early green never breaks the plan — but the gate
record must not assert a gating mechanism that does not exist.

## Disposition

Criteria 1–7 pass in substance; the three issues are minor, precisely
scoped, and require no redesign and no test-plan change. Once the
design text is corrected (delete `_abort_requested`; `Optional[float]`
annotation; case-17 gating correction), TU-006 may proceed to
implementation without a further full re-review — a focused confirmation
of the three corrections suffices. This review does not claim code
review, test pass, CI or integration; the implementation, independent
review, testing, principle and CI gates remain open. The blocking
test-contract finding (case-8 binding) is already resolved at `25cc5e1`
and is correctly pinned as a step-B prerequisite, not an implementation
task.

## Correction confirmation (commit 04dab95)

Focused confirmation of the three corrections only — no full re-review.

1. **Issue 1 (`_abort_requested`) — corrected.** The field is fully
   removed: state table row deleted, class diagram attribute deleted,
   sequence diagram notes now read "no state recorded" and "finally:
   `_in_flight` = 0", the `borrow()`, `_serialized` and `abort()`
   pseudocode sites deleted, decision 1 reworded to "reads the in-flight
   counter ... records no state" (the "Flag consumption" paragraph
   deleted; the second-lock rejection now correctly covers the counter
   alone), and the step-C row no longer lists the field. The single
   remaining mention in the simplicity audit is a proper rejection record
   ("considered and rejected as write-only dead state ... deletion breaks
   none of the 17 cases") — rationale, not a dangling reference. No other
   occurrences in the design.
2. **Issue 2 (`Optional[float]`) — corrected and consistent.** Intent
   summary, pseudocode constructor signature, `_timeout_seconds` state
   table row, class diagram attribute and the step-B row all now declare
   `timeout_seconds: Optional[float] = 5.0` / `_timeout_seconds:
   Optional[float]`, matching the property type and the
   `None`-disables contract. Repo-wide sweep of the design finds no
   remaining `float = 5.0` annotation.
3. **Issue 3 (case-17 gating) — corrected.** Step-C row newly green is
   "11, 12, 13 (17 also turns green — see note)" with green condition
   "case 16 RED"; step-D newly green is "16" only; the early-green note
   now correctly explains that case 17 exercises only A+B+C semantics
   (never `write_raw`), is expected green at step C and must stay green,
   and that only case 16 (OBS-002) plus the `compatibility.md`
   dispositions are genuinely D-gated.

**Verdict: APPROVED.** All three requested changes are correctly and
completely applied at the design-text level; no new issues found in the
changed hunks. TU-006 may proceed to implementation. Per the original
disposition, code review, test pass, CI and integration gates remain
open and are not claimed here.
