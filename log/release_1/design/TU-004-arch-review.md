# TU-004 architecture design review

Reviewer: sw-jerry (architect). Candidate: `3a2ecd0` on
`codex/tu-004-lifecycle` (`log/release_1/design/TU-004.md`). Verdict:
**CHANGES REQUESTED** — one transition gap must be pinned before
implementation. Scope is detailed-design compliance with the Release 1 PRD
(TU-004 row), the approved TU-004 test contract and its implementability
review, the TU-001 compatibility matrix, the TU-002/TU-003 architectural
decisions, and the five-module architecture; this is not a code review,
test execution or integration approval.

## Assessment against review criteria

1. **Architecture fit — confirmed.** The change is four public members,
   two protected hooks and two private boolean fields on the existing
   `Device` in `softlab/tu/station/device.py`. No new class, base class,
   module, constructor-signature change, `__init__.py` export change, or
   universal instrument hierarchy. Five-element boundaries are preserved
   (only `tu` is touched). The dependency table is empty and correct: two
   booleans and plain methods need no new imports. I independently verified
   the grounding claim: production contains **zero `Device` subclasses**
   (grep over `softlab/` finds only `isinstance` uses and `VisaHandle`,
   which is a standalone class, not a `Device` subclass), so the
   "subclass calls `super().__init__`" assumption has no hidden consumer
   and legacy breakage is impossible by construction. The TU-001
   invariants the design lists (pure container construction, delegation,
   snapshot/`describe` shapes, eager VISA open) are structurally
   untouched.
2. **State model soundness — one gap, see Issue 1.** The two-boolean
   model with invariant `_initialized ⇒ _needs_cleanup` is minimal and
   correct: the `(True, False)` cell is genuinely unreachable under the
   stated algorithms (the only path to `_initialized = True` passes
   through `_needs_cleanup = True`; the only path that clears
   `_needs_cleanup` clears `_initialized` first). The single release
   site, arming `_needs_cleanup` before the hook, discharging flags
   before `_cleanup_impl()`, lazy partial release, double-prepare
   no-op while initialized, and the repeatable prepare→cleanup cycle are
   all coherent and jointly satisfy cases 4–8. A state enum was rightly
   rejected.
3. **Capability detection — sound and TU-006-compatible.** The
   membership-test `supports()` with base vocabulary
   `{'prepare', 'cleanup'} → True`, everything else (including
   `'connection'` and `''`) → `False`, never raising, is consistent with
   case 3. The subclass extension rule (additive over
   `super().supports()`, side-effect-free, never raises) is the correct
   shape: since `VisaHandle` is not a `Device` today, TU-006 can wrap it
   in a `Device` subclass that additively advertises `'connection'`
   without contradicting any base behavior or changing the base result.
   The rejection of an enum (string vocabulary to avoid a universal
   instrument hierarchy) matches the PRD goal.
4. **Ownership/borrowed-resource semantics — provable.** A non-owner
   (`_needs_cleanup=False`) returns before the hook, so nothing is
   released — cases 5 and 8 follow directly from step 1 of `cleanup()`.
   Exactly-once after failed prepare follows from arming before the hook
   plus discharge-before-hook in `cleanup()`: the flag can be `True` at
   most until one `cleanup()` call consumes it. The failing-release-hook
   pinning (propagates, never implicitly retried) closes the only
   ambiguity the acceptance suite leaves open, and documenting rather
   than enforcing it is consistent with the design's style.
5. **Consistency with TU-002/TU-003 and OBS-003 — confirmed.** No
   interaction with the v1 `describe()` schema (TU-002) or
   `VisaCommand.execute()` (TU-003); neither file nor contract is
   touched. OBS-003 (eager-open lifecycle, failed-init cleanup gap,
   resource-manager ownership) is explicitly left as characterized legacy
   behavior for TU-006, matching the TU-001 matrix disposition and the
   implementability review §5. OBS-006 (delegated-name collision) gets
   the same documented-precedence disposition as TU-001 gave `child`/
   `device`, with explicit lookup via `parameter(key)`/`child(key)`
   remaining available — acceptable.
6. **Simplicity and docstring contract — complete.** Every implementability
   observation is resolved with the simplest option, and the rejections
   (state enum, locking, capability registry, context-manager sugar) are
   recorded with reasons. The docstring requirements cover the
   single-threaded statement (decision 3, TU-006 owns VISA concurrency)
   and the OBS-006 precedence note (decision 4) at both class and method
   level, in the surrounding `Args:/Returns:/Errors:/Side-effects:`
   style.

## Issue requiring change

1. **Re-preparation after a failed `prepare()` is an unpinned transition
   gap.** After `_prepare_impl()` raises, the device is in
   `FailedPendingRelease` (`_initialized=False, _needs_cleanup=True`).
   The design pins double-prepare as a no-op only *while initialized*
   (decision 2) and pins re-preparation only *after `cleanup()`*
   ("Re-preparation after `cleanup()` is fully supported"). But under the
   `prepare()` algorithm as written, calling `prepare()` again in
   `FailedPendingRelease` does not return early (`_initialized` is
   `False`) and re-runs the hook — a **second acquisition attempt while
   the first partial acquisition is still pending**. A single subsequent
   `cleanup()` then discharges the flag once, so two acquisition attempts
   are matched by one release, contradicting the design's own hook
   contract ("`_cleanup_impl()` invoked at most once per acquisition
   attempt"). The state-machine diagram has no outgoing `prepare()`
   transition from `FailedPendingRelease`, and the docstring requirements
   do not cover this case. No acceptance case exercises it, so the suite
   passes either way — this must be pinned explicitly, not left to
   chance. Either resolution is acceptable; pick one and state it in the
   design, the state diagram, and the `prepare()` docstring requirement:
   - **(a) Cleanup-first (recommended, simplest):** after a failed
     `prepare()`, the author must call `cleanup()` before re-preparing;
     re-preparation from `FailedPendingRelease` is outside the contract
     (documentation-only pinning, consistent with the failing-release-hook
     pinning). Or:
   - **(b) Re-entrant hooks:** re-preparation from
     `FailedPendingRelease` is permitted and runs the hook again; the
     hook contract then must state that `_prepare_impl()` must tolerate
     being re-entered after a failed attempt and that `_cleanup_impl()`
     releases the cumulative acquisition exactly once.

## Non-blocking observations

- The Mermaid class diagram omits `name`/`parent` from `Device`; fine as
  a focused view, no change needed.
- Decision 1's rationale correctly notes that lazy release keeps
  exception-identity propagation (case 6's `assertIs`) structurally
  trivial. Worth keeping exactly this structure during implementation:
  any `try`/`except` added later in `prepare()` would put that guarantee
  at risk.

TU-004 may proceed to implementation once Issue 1 is resolved in the
design document; no re-review of the full document is required if the fix
is limited to pinning that transition (design text, state diagram,
docstring requirements). This review does not claim code review, test
pass, CI or integration.
