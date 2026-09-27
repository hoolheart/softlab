# TU-004 test implementability review

Reviewer: sw-tom. Verdict: **ACCEPTED** for implementability.
Reviewed candidate: `692b422` on `codex/tu-004-lifecycle`.

Reviewed the nine acceptance cases and the proposed surface
(`Device.supports`, `Device.prepare`, `Device.initialized`,
`Device.cleanup`, protected `_prepare_impl`/`_cleanup_impl`) against the
existing `Device` base class in `softlab/tu/station/device.py`, the
`Delegated` mixin in `softlab/jin/misc/delegated.py`, and the TU-001
compatibility baseline. I also reproduced the recorded red phase:
`python -m unittest discover -s tests -p 'test_tu_lifecycle.py'` from the
repository root on `692b422` yields **8 errors, 1 pass** (`AttributeError`
for the missing API in every error), matching the gate record.

## 1. Additive, no-op-by-default feasibility — confirmed

All four public members plus the two hooks can be added on `Device` alone,
confined to `softlab/tu/`, with no constructor-signature change:

- Real methods and properties take precedence over `Delegated.__getattr__`
  (delegation only fires on failed normal lookup), so `supports` /
  `prepare` / `cleanup` / `initialized` resolve directly.
- Lifecycle state is a private flag initialized in `Device.__init__`; the
  existing `str(name)` / delegation setup is untouched, so construction
  remains pure container setup and a plain `Device` stays virtual.
- I verified there are **no `Device` subclasses in production**
  (`softlab/tu` and dependents), so no legacy subclass needs modification
  and the "subclass calls `super().__init__`" assumption carries no hidden
  consumer.
- The TU-001 baseline (`nested_paths_delegation_ownership_and_removal`,
  `batch_settings_preserve_sequence_order`, the v1 `describe()` schema,
  eager VISA open/clear) is structurally unaffected: nothing in the
  proposed surface touches `Parameter`, `snapshot`, `describe`,
  `set_parameters`, or `VisaHandle`.

## 2. State machine coherence

The not-initialized → prepare → initialized → cleanup → not-initialized
cycle, with failure paths, is coherent and implementable as a small base
class state flag:

- `prepare()` runs `_prepare_impl()`, sets ready only on success, and
  re-raises the hook exception unwrapped (`assertIs` in case 6 pins
  exception identity, forbidding wrapping).
- `cleanup()` invokes `_cleanup_impl()` at most once per successful
  preparation (or per failed preparation that acquired partial state),
  and never when nothing was acquired. Cases 4, 5, 7, 8 jointly pin this
  gating precisely.
- Hook signatures (`_prepare_impl()` / `_cleanup_impl()`, no args,
  `None` returns) are stated in the gate and used consistently in every
  mock subclass; no unstated signature assumption.

Unstated but test-tolerant points (non-blocking, for the designer to pin
in the detailed design):

- **Eager vs. lazy failure cleanup**: after `_prepare_impl()` raises, the
  base class may call `_cleanup_impl()` immediately or defer to the first
  `cleanup()` call. Cases 6–7 pass under either; the designer must pick
  one explicitly.
- **Double `prepare()`**: whether re-preparing an initialized device
  re-runs the hook is unstated. No case depends on it; document the
  semantics.
- **Thread-safety**: the contract is implicitly single-threaded. No case
  exercises concurrency, consistent with the compatibility matrix note
  that concurrency is deferred; the design should state this assumption
  (TU-006 owns VISA concurrency).

## 3. Mock approach — sound and hardware-free

All resources are `unittest.mock.Mock`; no simulator backend and no
hardware are needed. The `patch("softlab.tu.station.visa.visa.ResourceManager")`
target resolves to the `pyvisa` module attribute (verified by import), is
used purely as an I/O sentinel — `Device` never imports visa, so nothing
under test is actually patched — and is restored by unittest teardown. No
new runtime dependencies.

## 4. PRD criteria coverage — complete

Virtual no-op connection (case 1), explicit readiness (case 2), ownership
and non-owner safety (cases 5, 8), repeated cleanup idempotence (cases 4,
7), failed preparation/initialization (cases 6, 7), borrowed/shared
resources (case 8), unsupported capability detection (case 3), and the
legacy compatibility guard (case 9) all map to the TU-004 PRD row. No
contradictions found; nothing missing.

## 5. OBS-003 / VISA deferral — confirmed

Nothing in these cases forces VISA changes now. No case constructs a
`VisaHandle`; the `ResourceManager` patch is only an activity sentinel on
an unrelated module attribute. `VisaHandle`'s eager-open lifecycle, its
failed-initialization cleanup gap, and resource-manager ownership
(OBS-003) remain characterized legacy behavior for TU-006, which may
adopt this opt-in contract compatibly later.

## Feasibility notes per case

1. Virtual device / no meaningless connection: implementable; base
   `supports('connection') -> False`, base `prepare()` a pure no-op; the
   post-prepare `snapshot()` assertion holds on existing code paths.
2. Explicit readiness: implementable as a property over a private flag;
   the False/True/False sequence is exactly one flag transition each way.
3. Unsupported capability detection: implementable as side-effect-free
   string matching; `""` is covered by the "unknown → False" rule.
4. Idempotent cleanup after success: implementable; base gating plus the
   subclass counter asserts hook invoked exactly once across two calls.
5. Cleanup before successful prepare: implementable; initial state
   releases nothing, hook never invoked.
6. Failed preparation: implementable; original exception identity
   preserved by direct re-raise, readiness flag never set.
7. Failed preparation then cleanup: implementable (see eager/lazy note
   above); both orders of "release exactly once, then idempotent" are
   testable.
8. Borrowed resource: implementable; same gating as case 5 proves a
   non-owner never closes the borrowed mock.
9. Legacy guard: passes against unchanged code today (verified as the
   single green case); every asserted behavior is an existing public
   method.

## Non-blocking observations

- **Delegated-name collision (OBS-006 category)**: the four new public
  names become real attributes and would shadow a delegated parameter or
  child with the same name. No such names exist in the repo today, and
  explicit lookup (`device.parameter("supports")`) remains available;
  the designer should record this in the new-API docstring.
- The gate's `supports` capability vocabulary (`'prepare'`,
  `'cleanup'`, `'connection'`) is case-pinned only for those literals;
  the designer should define whether subclasses extend it via override
  only, and keep base behavior `False` for everything else.

This review did not run the acceptance suite as a pass criterion and does
not claim implementation or final test approval; detailed design
approval, independent review, and testing gates remain pending.
