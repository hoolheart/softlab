# TU-003 test implementability review

Reviewer: sw-tom. Verdict: **ACCEPTED** for implementability.
Reviewed candidate: `1c6d97c` on `codex/tu-003-operations`.

Reviewed the six acceptance cases and the proposed surface
(`VisaCommand.execute()`, `VisaCommand.describe_operation()`) against the
existing `Parameter` invocation path and `VisaHandle`/`VisaCommand` in
`softlab/tu/station/visa.py`. Both methods can be added on `VisaCommand`
alone, inside `softlab/tu/`, with no constructor, base-class or dependency
changes and no touch to the legacy `get()`/`__call__`/`snapshot()`/`describe()`
paths, so the TU-001 compatibility baseline (including
`legacy_command_reads_execute_once_and_writes_denied` and the TU-002 v1
`describe()` schema) is structurally preserved.

Feasibility notes per case:

1. Explicit path: implementable. `VisaCommand` stores `init_value=True`, so
   "write once, return stored value" is satisfied by a single
   `handle.write(self._cmd)` plus returning the stored value; the asserted
   `write(message="*RST", encoding=None)` call shape matches the existing
   `VisaHandle.write` forwarding.
2. `describe_operation()`: implementable as a pure method returning a fresh
   dict (`schema_version`, `name`, `effect`, `executions_per_call`); the
   JSON round-trip and command-string exclusion assertions are consistent
   with a hand-built dict and zero handle I/O.
3. Legacy compatibility: passes on unchanged code; snapshot is a pure dict,
   `()`/`get()` each route through `before_get` (one write), and denied
   `set` raises `RuntimeError` in `Parameter.set` before any hook runs.
4. TU-002 `describe()`: passes on unchanged code; the base `describe()` is
   pure and never includes `cmd`, and the follow-up legacy call still writes
   exactly once.
5. Denied set plus transport error: implementable; the
   `assertIs(raised.exception, error)` assertion forbids exception wrapping,
   which matches the gate record's "propagate unchanged" contract.
6. Unavailable resource: implementable; `Mock(spec=VisaHandle)` passes the
   constructor's `isinstance` check (verified), and the positional
   `write("*RST")` assertion matches a direct `handle.write(self._cmd)`
   call before the handle's `RuntimeError`.

Mock approach: sound and hardware-free. Patching
`softlab.tu.station.visa.visa.ResourceManager` resolves to the pyvisa module
attribute (verified) and is restored by `addCleanup`; the `TEST@sim` address
exercises the `@lib` parsing branch without a backend. No new runtime
dependencies.

Non-blocking observations for the design/test phases (not implementability
issues):

- The gate record calls the `describe_operation()` dict "detached", but no
  case asserts mutation independence between successive calls; the designer
  should either keep the dict freshly built per call or ask the tester to
  add a detachment assertion.
- Case 6 pins `execute()` to a positional `handle.write(self._cmd)` call;
  the designer should not route it through keyword arguments or extra
  handle methods (e.g. `query`), which would also violate the "single
  command write" contract.
- Whether `execute()` aliases `get()` or performs the write directly is an
  open design choice both cases allow; the designer should pick one
  explicitly (direct write keeps the contract independent of overridable
  hooks).

Coverage against the PRD criteria is complete: explicit path (case 1),
discoverable side effects without execution (cases 2, 4), legacy
compatibility (cases 3, 4) and error semantics (cases 5, 6). The reported
4-error/2-pass red phase with `AttributeError` as the sole red cause is
appropriate. This review did not run the acceptance tests and does not
claim implementation or final test approval; independent review,
implementation and integration gates remain pending.
