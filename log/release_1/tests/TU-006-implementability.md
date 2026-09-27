# TU-006 test implementability review

Reviewer: sw-tom. Verdict: **CHANGES REQUESTED** (2 blocking issues, 4
non-blocking observations for the detailed design).
Reviewed candidate: `850ce75` on `codex/tu-006-visa-contracts`.

Reviewed the 16 acceptance cases and the proposed surface
(`supports`/`initialized`/`prepare`/`cleanup`/`borrow`,
`timeout_seconds`, serialization guard, `abort()` report,
`confirm_stop()`, post-cleanup `RuntimeError` policy) against
`softlab/tu/station/visa.py` at the branch tip, the approved TU-004
lifecycle design (`log/release_1/design/TU-004.md`), the TU-001
compatibility matrix (`log/release_1/compatibility.md`, OBS-001/OBS-003),
`tests/visa_sim.yaml` (pyvisa-sim 0.7.1), the Release 1 PRD TU-006 row,
and all in-repo `VisaHandle` consumers. I reproduced the recorded red
phase: `python -m unittest discover -s tests -p
'test_tu_visa_contracts.py'` from the repository root on `850ce75` yields
**4 pass, 12 fail (10 errors + 2 failures)**, matching the gate record —
10 `AttributeError`s for the missing contract API, the OBS-003
failed-init leak (`close` called 0 times), and the unserialized
interleave (`['start', 'start', 'end', 'end']`).

## Blocking issues

### Issue 1: Cases 1 and 8 contradict each other on the construction-time default — unreconcilable as written

Case 1 (compatibility guard, must stay green) constructs
`VisaHandle("TEST@sim")` with defaults and asserts
`resource.timeout == 5.0` — pinning the legacy raw forwarding of the
`timeout=5.0` constructor default. Case 8 constructs **the identical
object** (`make_resource(self, "TEST@sim")`, no keyword overrides) and
asserts both `handle.timeout_seconds == 5.0` **and**
`resource.timeout == 5000`. The same construction cannot write 5.0 to
the resource (case 1) and 5000 to the resource (case 8).

The contradiction is latent, not visible in the red run: case 8 errors
on `AttributeError: timeout_seconds` before reaching the
`resource.timeout == 5000` assertion. It will surface the moment
implementation goes green and will make cases 1 and 8 mutually
unpassable.

Both readings also collide with the compatibility matrix:

- Keeping the legacy raw default (resource gets 5.0) satisfies case 1
  but makes `timeout_seconds` read back `0.005` from any default-built
  handle, breaking cases 7 (initial `timeout_seconds == 5.0` readback),
  8, and 16 (`owned.timeout_seconds == 5.0` on a default-built `@sim`
  handle).
- Silently writing 5000 ms at default construction satisfies cases
  7/8/16 but breaks guard case 1 — and is precisely the "silent
  rescaling of existing callers" that OBS-001 forbids
  (`compatibility.md`: "Do not silently rescale existing callers"; the
  matrix row "Timeout constructor/default/set/get preserve raw numeric
  forwarding" is characterized legacy).

**Requested change.** sw-mike (with sw-celeste) must revise the test
plan to pick one explicit, PRD-compatible resolution, e.g.:

- **(a)** Introduce an explicit sentinel: change the constructor to
  `timeout: Optional[float] = None` (meaning "not specified") plus
  `timeout_seconds: float = 5.0`; when `timeout` is `None` the seconds
  default applies (resource gets 5000 ms), when `timeout` is given the
  legacy raw path forwards it untouched. Then update guard case 1 to
  construct with an explicit `timeout=5.0` (raw forwarding of an
  explicit value stays pinned) and let default construction take the
  seconds path. This is additive, keeps explicit legacy callers
  byte-compatible, and resolves OBS-001 without silent rescaling — but
  it is a constructor-signature semantic change that the architect
  should confirm.
- **(b)** Keep default construction raw (resource gets 5.0, case 1
  stands) and change cases 7/8/16 to expect a 0.005 s readback on a
  default-built handle, applying the documented seconds default only
  via the explicit `timeout_seconds` keyword. Weaker: the "default
  5.0 s" acceptance criterion becomes keyword-only.

Either way, the guard must pin *explicit* `timeout=` raw forwarding
(case 6 already does this for set/get) and the revision must state
which construction paths write milliseconds. The implementation cannot
proceed past step 2 (group B) until this is resolved.

### Issue 2: OBS-002 (`write_raw` calls resource `write`, not `write_raw`) has no acceptance coverage

`compatibility.md` assigns OBS-002 explicitly to TU-006: "`VisaHandle.
write_raw` calls resource `write`, not `write_raw` … Resolve explicitly
in TU-006 with a focused regression and compatible error handling." The
TU-006 gate document never mentions OBS-002, and none of the 16 cases
exercises a live `write_raw`: case 15 calls `handle.write_raw(b"V 1")`
only **after** `cleanup()`, asserting a `RuntimeError` with zero I/O —
which passes whether or not the OBS-002 defect is fixed.

**Requested change.** Add one focused case (group D, alongside case 15)
that performs a live mocked `write_raw` and pins the disposition:
either a regression asserting `resource.write_raw` is called with the
bytes (fix direction; then confirm the fix composes with case 15's
post-cleanup zero-I/O policy and identical-object error propagation),
or an explicit characterization case asserting the current
`resource.write(message=bytes)` forwarding as the certified legacy
behavior with a documented rationale. Leaving OBS-002 unaddressed would
let the PRD/arch-review commitment ("require targeted regression and
explicit behavior decision") dangle into release acceptance.

## Non-blocking observations (for the detailed design to pin)

1. **`abort()` must never wait on the operation serialization lock.**
   In case 12 the blocked read holds the guard while waiting on a
   release event that the test sets only **after** `handle.abort()`
   returns; if `abort()` tried to take the same per-handle lock it
   would block ~10 s and the case would fail. Sound design:
   `abort()` only records the abort request and in-flight state
   (flag-based) and returns the report immediately; actual
   interruption is backend-honored (the test models this by releasing
   the read). Pin in-flight tracking (which operations count), the
   report type (at least `aborted`/`stopped` attributes), and that an
   idle abort returns `aborted=False`.
2. **`close()` relationship to `cleanup()`.** The gate requires
   non-owner `cleanup()`/`close()` to never close a borrowed resource,
   but only `cleanup()` is exercised. Recommend `close()` delegate to
   `cleanup()` (idempotent, ownership-aware) and note the resulting
   change to legacy double-`close()` semantics — no production caller
   depends on double-close (only `VisaParameter`/`VisaCommand` hold
   handles and never call `close()`), so the blast radius is zero, but
   the docstring should record it.
3. **`prepare()` re-open needs a resource-manager lifetime decision.**
   OBS-003 leaves resource-manager ownership unspecified. To re-open
   after `cleanup()`, the handle must either retain the manager created
   in `__init__` or recreate it per `prepare()`. Either is
   implementable (`@sim` tolerates both); the design must pick and
   state it, and must keep `initialized=False` + defined
   `RuntimeError` behavior if re-open itself fails.
4. **`confirm_stop()` parsing and `stopped` persistence.** Pin how the
   `'*OPC?'` response is parsed (the cases use exactly `"0"`/`"1"`;
   recommend `strip()` + equality so the real `@sim` `"1\r"` round trip
   in case 16 passes), the behavior on unexpected values, and whether
   `stopped` remains `True` after a later failed `confirm_stop()` (not
   asserted; state the choice).

## Per-group feasibility notes

### Group A (cases 2–5): feasible — the TU-004 contract maps cleanly onto the eager-open reality

- **`supports`/`initialized`**: pure membership test over
  `('connection', 'prepare', 'cleanup')` plus an internal
  `_initialized` boolean; zero I/O by construction. Note the deliberate
  divergence from the `Device` base vocabulary (which answers
  `'connection' → False`): a `VisaHandle` *is* a connection, so
  `True` is correct here; the design should document this mirror, not
  inherit (the gate already frames it that way). Case-sensitive unknown
  names fall out of plain string equality.
- **`prepare()`/`cleanup()` mapping**: with eager open, successful
  `__init__` ≡ successful `prepare()` (`_initialized = True` at the
  end of `__init__`); `prepare()` while initialized is the TU-004
  decision-2 no-op; `cleanup()` is TU-004's release with
  clear-before-hook discharge, so double-cleanup is a no-op and
  `prepare()` after `cleanup()` re-runs acquisition (repeatable cycle).
  This matches TU-004 outcomes without inheriting `Device` — correct,
  since TU-004's lazy failed-prepare release (decision 1) and
  cleanup-first re-prepare pinning (decision 5) model *deferred*
  acquisition and simply do not apply to an eager constructor; the
  design docstring should state that mapping explicitly.
- **Failed-init cleanup (case 4, OBS-003)**: wrap the post-acquisition
  tail of `__init__` (`clear()` when `device_clear`, then the
  timeout/termination assignments) in `try/except`: on any exception,
  close the just-acquired resource exactly once, then re-raise the
  **identical object** (bare `raise` after close, never `raise ... from`
  wrapping). Both failure scenarios in case 4 are implementable: the
  `clear()` `side_effect` and the `timeout` property setter that raises
  both trigger the same path. Two guards: (i) the pre-acquisition
  non-message `TypeError` rejection path already closes the resource
  once and must not be double-closed by the new wrapper — scope the
  `try` strictly after the `MessageBasedResource` check succeeds;
  (ii) if `close()` itself is the failing step, "exactly once" still
  holds (one attempt); pin in the design.
- **`borrow(resource)`**: sound and non-intrusive. Implement as a
  classmethod constructing a handle instance without running the
  open path, with an `_owns_resource = False` flag; `initialized` is
  `True`; adoption performs no clear/configuration writes (the test
  asserts `resource.clear.assert_not_called()` on a mock that records
  everything); non-owner `cleanup()` flips state but never calls
  `resource.close()`; the owner keeps full use (case 5 ends with the
  owner querying the borrowed mock). Default address-based
  construction sets `_owns_resource = True`. Address bookkeeping for a
  borrowed handle: `address` may be unavailable — no case asserts it;
  design should state the behavior (recommend exposing the resource's
  address if any, else `''`).

### Group B (cases 7–9): feasible **after Issue 1 is resolved**; OBS-001 handling is otherwise correct

- The `timeout_seconds` property with ×1000 conversion in both
  directions and `None` disabling both ways composes with the legacy
  raw `timeout` property untouched — they are two properties over the
  same `_resource.timeout`, and case 7's final block explicitly guards
  that setting the legacy path still forwards raw (12.5 stays 12.5).
- Constructor keyword `timeout_seconds=` is additive `**kwargs`-free
  surface; case 8's keyword path (1.5 → 1500) has no interaction with
  the legacy `timeout` parameter as long as the design pins precedence
  when both are given (recommend: explicit `timeout_seconds` wins;
  state it, no case combines them).
- Case 9's identical-object timeout error is free: the property never
  wraps I/O; the mock's `read.side_effect` propagates unchanged
  through the existing call path. The `@sim` half of case 9 (round trip
  with explicit 5.0 s on a real resource) is sound — the simulator has
  no delay support, so nothing disturbs the dialogue.
- The only defect is the construction-default contradiction of Issue 1,
  which infects the initial readback assertions of cases 7 and 16 as
  well. Everything else in the group is implementable as specified.

### Group C (cases 11–13): feasible; consistent with TU-004's single-threaded contract

- TU-004 decision 3 declares the *Device* lifecycle single-threaded and
  explicitly assigns VISA concurrency to TU-006, so a per-handle
  `threading.Lock` around resource calls creates no cross-contract
  conflict. Case 11's strict `[start, end, start, end]` serialization
  is achievable by holding one lock for the duration of each public
  operation (`query`, `read`, `write`, …); single-threaded semantics
  and error identity are unchanged because the lock adds no wrapping.
  The lock must live in `VisaHandle` (not in the resource), and the
  group-D pre-check should acquire the lock before the
  `_resource is None` test (or test first — either way, no I/O on the
  rejected path; case 15's `resource.assert_not_called()` holds).
- **Abort report semantics (case 12)** are implementable and correctly
  strict: `abort()` sets a request flag, returns a report with
  `aborted=True`, `stopped=False`, leaves `handle.stopped` `False`,
  and issues **no write** (`resource.write.assert_not_called()` pins
  this — so the design must not implement abort as a device halt
  command; software-request-only). The aborted call's underlying error
  object propagates unchanged once the backend honors the request
  (modeled by the test's release event). Idle abort returns
  `aborted=False` — requires tracking in-flight state; see observation
  1 for the must-not-block-on-lock constraint.
- **`confirm_stop()` via `'*OPC?'` (case 13)**: implementable as
  exactly one `self._resource.query('*OPC?', None)` (positional delay
  `None`, matching the mock's `assert_called_once_with("*OPC?", None)`),
  `True` only on a device-confirmed completion, setting `stopped=True`
  on success and leaving it `False` on `'0'`. Unstated assumptions to
  pin (observation 4): response parsing (strip + compare, so the real
  `@sim` `"1\r"` in case 16 passes), unexpected-response behavior, and
  `stopped` persistence across a later failed `confirm_stop()`.
  Transport errors propagate unchanged — free, since no wrapping is
  introduced.
- **Case 10 (blocking guard)** is already green and stays green: the
  lock and abort machinery must not alter single-threaded latency or
  the `read(encoding=None)` call shape.

### Group D (cases 15–16): feasible; `@sim` soundness confirmed against the simulator YAML

- **Post-cleanup `RuntimeError` with zero I/O (case 15)**: all five
  operations already raise `RuntimeError('Invalid visa resource')` when
  `self._resource` is `None`; the new `cleanup()` sets `_resource =
  None` (and discharges state before any hook, per TU-004), so the case
  passes with `resource.assert_not_called()`. `prepare()` recovery then
  re-acquires. Sound.
- **Error identity policy**: no path in the new surface may wrap; all
  cases use `assertIs`, which forbids `raise ... from` chains that
  substitute objects. Direct propagation preserves identity trivially;
  the only risk is an over-eager `try/except` in `__init__` cleanup
  (group A) — re-raise bare.
- **`@sim` case 16**: verified against `tests/visa_sim.yaml` —
  `GPIB::1::INSTR` (Weinschel) has the `*OPC? → 1` dialogue needed by
  `confirm_stop()`, and `GPIB0::26::INSTR` (Keithley) has `*IDN?` but
  **no** `*CLS` dialogue, which is why both `@sim` constructions in
  the suite pass `device_clear=False` — consistent and correct.
  `resource.session is not None` after non-owner cleanup holds on
  pyvisa-sim 0.7.1 (the session attribute survives as long as the
  resource is not closed), and the owner's subsequent `query("*IDN?")`
  works on the still-open session. Real 5000 ms on the resource is
  readable via `timeout_seconds` once Issue 1 fixes the default path.
- The one gap is OBS-002 coverage (Issue 2) — add the case here.

## Case dependencies and recommended step split

Strict order **A → B → C → D** is achievable and is the recommended
split; each step makes exactly its group's RED cases green while all
prior groups and the four guards stay green:

1. **Step A — lifecycle and ownership** (cases 2–5): internal
   `_initialized`/`_owns_resource` state, `supports`, `prepare`,
   `cleanup`, `borrow`, failed-init cleanup wrapping the post-
   acquisition `__init__` tail, `close()` delegating to `cleanup()`.
   Constructor signature untouched in this step (Issue 1 does not gate
   step A; but scope the failed-init wrapper so the timeout/termination
   assignments it covers remain the legacy raw assignments for now).
2. **Step B — timeout units and defaults** (cases 7–9): **gated on
   Issue 1**; `timeout_seconds` property + constructor keyword +
   default application per the revised construction contract, legacy
   raw path untouched.
3. **Step C — blocking, serialization, interruption** (cases 11–13):
   per-handle operation lock, in-flight tracking, flag-based `abort()`
   with report, `confirm_stop()` via `'*OPC?'` with pinned parsing,
   `stopped` flag. Depends on A's `initialized`/cleanup state (the
   rejected-path pre-check) but not on B.
4. **Step D — error causes and `@sim` outcomes** (cases 15–16, plus the
   Issue 2 OBS-002 case): post-cleanup zero-I/O rejection across all
   five operations (needs A's `cleanup()` semantics and C's lock), and
   the end-to-end `@sim` walk (needs A's `borrow`/`prepare`, B's
   seconds default, C's `confirm_stop`/`stopped`).

Group independence claim from the gate holds **except** that case 16
(group D) exercises `borrow`, `confirm_stop`, `stopped`, and
`timeout_seconds` together, so D's validation genuinely requires A, B,
and C landed — the gate's own incremental list (A 2–5, B 7–9, C 11–13,
D 15–16) already encodes exactly this ordering.

## PRD / TU-004 consistency check

- PRD TU-006 row: lifecycle adoption compatible (A), timeout
  units/defaults documented and verified (B, once Issue 1 picks the
  documented contract), blocking + concurrency + interruption (C),
  original causes available (D + all `assertIs` cases), abort request
  vs confirmed physical stop not conflated (case 12's
  `aborted=True/stopped=False/no-write` triad plus case 13's
  device-confirmed-only `stopped=True` — the strongest part of the
  plan). Nothing contradictory beyond Issue 1; Issue 2 is a missing
  commitment, not a contradiction.
- TU-004 contract: mirrored outcomes (`supports`/`prepare`/
  `initialized`/`cleanup` semantics, idempotence, exactly-once release,
  identical-object propagation, non-owner safety) are all preserved;
  the eager constructor makes the failed-init cleanup *eager* rather
  than TU-004's lazy decision-1 release, and `supports('connection')`
  answers `True` where the virtual `Device` answers `False` — both
  correct mirror-divergences the design should document.

## Verification performed for this review

- `.venv/bin/python -m unittest discover -s tests -p
  'test_tu_visa_contracts.py'` (Python 3.13.15, `MPLCONFIGDIR`/
  `XDG_CACHE_HOME` under `/tmp`): 16 tests, **4 pass / 12 fail (10
  errors + 2 failures)**, matching the gate record; the 4 guards are
  the green cases.
- Inspection of `visa_sim.yaml` confirming `*OPC?` exists only on the
  Weinschel device and `*CLS` exists on neither — consistent with the
  suite's address choices and `device_clear=False` usage.
- Grep of in-repo `VisaHandle` consumers: only `VisaParameter` /
  `VisaCommand` / `VisaIDN`, which write/query and never `close()` —
  so the `close()`-delegates-to-`cleanup()` recommendation and the
  constructor changes have no hidden production caller.

This review did not run the acceptance suite as a pass criterion (12
cases are expected RED), does not implement anything, and does not
claim detailed-design approval, independent review, or testing gates;
those remain pending. Re-review of the test plan is required after
Issues 1 and 2 are resolved.
