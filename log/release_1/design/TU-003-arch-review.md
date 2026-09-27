# TU-003 architecture design review

Reviewer: sw-jerry (architect). Candidate: `0047a50` on
`codex/tu-003-operations` (`log/release_1/design/TU-003.md`). Verdict:
**APPROVED**. Scope is detailed-design compliance with the Release 1 PRD
(TU-003 row), the approved TU-003 test contract and its implementability
review, the TU-001 compatibility matrix, the TU-002 architectural decisions,
and the five-module architecture; this is not a code review, test execution
or integration approval.

## Assessment against review criteria

1. **Release architecture consistency.** The change is two instance methods
   on the existing `VisaCommand` in `softlab/tu/station/visa.py` — no new
   class, base class, module, constructor change or `__init__.py` export
   change. No universal instrument hierarchy is introduced. Five-element
   responsibilities are preserved (`tu` keeps device abstraction; no `huo`,
   `shui`, `mu` or `jin` touch). No new runtime dependency: the only cited
   imports (`typing.Any`, `typing.Dict`) already exist in `visa.py`
   (verified at lines 3–8).
2. **TU-002 consistency.** The design explicitly forbids a `VisaCommand`
   `describe()` override and keeps schema v1 byte-for-byte semantically
   unchanged. `describe_operation()` as a separate lookup with its own
   `schema_version` namespace is coherent: the two schemas answer different
   questions (structure/permissions vs. side-effect semantics) and version
   independently, which leaves TU-005 free to extend either without
   reinterpreting the other. The four-field contract (`schema_version`,
   `name`, `effect`, `executions_per_call`) is minimal and JSON-compatible.
3. **`execute()` direct-write decision.** Sound, and the rationale is the
   correct one: aliasing `get()` would re-enter the overridable `before_get`
   hook, making the explicit path's side-effect count and error surface
   depend on subclass hook behavior. Direct `self._handle.write(self._cmd)`
   plus `return self._value` keeps the contract ("exactly one write, stored
   value, unwrapped errors") independent of the legacy hook chain. Against
   the TU-001 matrix: execution-count semantics are preserved (one write per
   call, matching legacy `get()`; two calls write twice) and the denied-set
   gate is untouched (`Parameter.set` raises before any hook, zero writes).
   One latent corner exists — see observation below — and the design's
   categorical "always permitted" rule resolves it.
4. **Error semantics and edge cases.** Complete relative to the PRD and the
   six acceptance cases: denied `set` raises `RuntimeError` before any write
   (case 3/5); transport failure propagates the identical exception object
   with no `try`/`except` (case 5's `assertIs`); unavailable resource raises
   the handle's `RuntimeError` after exactly one attempted write (case 6);
   repeated calls are counted (case 1); `describe_operation()` performs zero
   I/O even on an unavailable resource (case 2). The no-wrapping decision is
   consistent with the TU-001 "direct communication error propagation"
   contract.
5. **Simplicity.** Minimal surface: two methods, a fresh dict literal per
   call, no copy helper, no schema package, no error-wrapping utility — the
   design explicitly records these rejections. Every element earns its keep.
6. **Clarifications.** None blocking; one non-blocking observation below.

The three implementability observations are resolved correctly: detached
fresh-dict guarantee with an explicit test-phase handoff to sw-mike for the
mutation-independence assertion (the gap is recorded rather than silently
exempted), positional single write matching both case 1 and case 6 call
shapes, and the direct-write decision documented above.

## Non-blocking observation

`VisaCommand.__init__` forwards `**kwargs` to `Parameter`, so
`VisaCommand(..., gettable=False)` is constructible. On such an instance the
legacy `get()` raises `RuntimeError` before any write while the designed
`execute()` still writes. The design's stated rule — "no permission check in
`execute()`; always permitted on a `VisaCommand`" — is categorical and
already covers this case, and no acceptance case constructs it, so it is not
a defect. For documentation completeness, the `execute()` docstring required
by the design should state explicitly that the explicit path is not gated by
the legacy `gettable`/`settable` permissions, so a reader comparing the two
invocation paths does not have to infer it.

TU-003 may proceed to implementation after its remaining applicable gates.
This approval does not claim code review, test pass, CI or integration; the
test-phase mutation-independence addition (design decision 1) remains owed
to sw-mike.
