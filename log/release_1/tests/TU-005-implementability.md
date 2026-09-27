# TU-005 test implementability review

Reviewer: sw-tom. Verdict: **ACCEPTED** for implementability.
Reviewed candidate: `84716ac` on `codex/tu-005-measurements`.

Reviewed the ten acceptance cases and the proposed surface
(`Parameter.read() -> Reading`, `Parameter.describe_reading()`,
optional `uncertainty`/`calibration` constructor information) against
`softlab/tu/station/parameter.py`, `softlab/tu/station/visa.py`, the
TU-001 compatibility matrix, the TU-002 v1 `describe()` schema, the
TU-003 `describe_operation()` namespace precedent, and the `huo` value-only
consumption path (`huo/process/common.py:AtomJob` and `parse_setters*`).
I also reproduced the recorded red phase:
`python -m unittest discover -s tests -p 'test_tu_measurements.py'` from
the repository root on `84716ac` yields **8 errors, 2 pass**
(`AttributeError` for the missing `read` / `describe_reading` API in every
error), matching the gate record.

## 1. Additive, `tu`-only, legacy `get()` semantics intact — confirmed

All proposed members are additions to the existing `Parameter` base class,
mirroring the TU-003 pattern:

- `read()` and `describe_reading()` are new methods; `get()`, `__call__()`,
  `set()`, `snapshot()`, and TU-002 `describe()` are not touched. Legacy
  `get()` byte-identical semantics is preserved trivially because the new
  code never re-enters or reorders the legacy path.
- New constructor information (`unit`, `value_type`, `shape`, `channel`,
  `uncertainty`, `calibration`) is additive keyword surface with `None`
  defaults. `Parameter.__init__` takes them directly; `VisaParameter`
  forwards `**kwargs` to the base, so the keywords compose with the
  existing chain without signature changes.
- No `Parameter` subclass in production overrides `get()` except
  `ProxyParameter` (which forwards to its target), so no legacy subclass
  needs modification. `QuantizedParameter`'s codec-based chain is inherited
  unchanged and flows through any shared acquisition helper.
- The `huo` compatibility contract holds: `AtomJob.body` and the
  `parse_setters*` helpers consume `para()` value-only calls. Since
  `__call__` and `get()` are untouched, count/scan retain numeric
  value-only behavior (matrix row "count/scan retain numeric value-only
  behavior"). Production changes stay confined to `softlab/tu/`.

## 2. Exact legacy acquisition chain reuse — implementable, no duplication

`read()` can reuse the exact legacy chain (permission check →
`before_get` → encoder) without duplicating it, and with exactly one
acquisition per call:

- Recommended shape: `read()` performs the gettable permission check
  itself (so `gettable=False` raises `RuntimeError` **before** any
  acquisition, satisfying case 10), then wraps `self.get()` in
  `try/except Exception`. On success it stamps `acquired_at` and returns
  `quality='ok'`; on failure it returns `quality='failed'` with the
  original exception object on `reading.error` (direct `except ... as e`
  preserves identity — `assertIs` in case 7 pins this and forbids
  wrapping).
- Because `get()` itself is the chain, acquisition counts stay identical:
  `VisaParameter.before_get` performs exactly one
  `handle.query("MEAS?", None)` per `get()` (positional, `delay=None`),
  so `read()` inherits exact-once acquisition for free (cases 7 and 8 pin
  `assert_called_once_with("MEAS?", None)`). No parallel acquisition path
  is invented, exactly as the gate requires.
- Case 8 confirms no caching is asserted: `read()` then a legacy `get()`
  each perform one query (two total, `reset_mock` in between). This
  matches the current per-call hook semantics (`before_get` re-queries
  every time).

## 3. `Reading` type — implementable; several unstated assumptions to pin

A small result class with `value`, `acquired_at`, `quality`, `error`,
`uncertainty`, `calibration` is straightforward. The tests pin behavior,
not representation, so all of the following are **non-blocking** items for
the detailed design to state explicitly:

- **Time source**: the tests bound `acquired_at` between `time.time()`
  calls around `read()`, so a naive `time.time()` epoch float taken
  *after* acquisition satisfies every case. State this choice; no
  timezone object or clock injection is needed (the bounded-window
  assertion makes the source self-certifying).
- **Quality vocabulary**: only `'ok'` and `'failed'` are pinned. State
  whether the vocabulary is closed (two literals) or open; tests tolerate
  either as long as the two literals are exact.
- **Failed-read field states**: what `value` and `acquired_at` hold on a
  failed acquisition is unstated (`None` is the natural choice); state it.
- **Exception scope**: "the acquisition step raises" — state whether the
  `try` covers the full post-permission chain (`before_get` **and**
  encoder, the recommended reading, and the one case 7 exercises) or only
  the hook. Covering the whole chain is strictly more useful and remains
  test-compatible.
- **Mutability / equality**: whether `Reading` is a frozen value object
  or a mutable container is unstated; no case depends on it. Prefer a
  frozen dataclass-like value object for safety.
- **Location**: `Reading` should live in `softlab/tu/` — either beside
  `Parameter` in `station/parameter.py` (consistent with
  `describe_operation`'s placement beside its class) or a small
  `station/reading.py`. Either is additive; the designer picks. No
  cross-五行 dependency is introduced.

## 4. `describe_reading()` namespace/versioning — consistent

- Own namespace with `schema_version` 1, `name`, and optional
  `unit`/`value_type`/`shape`/`channel` (`None` when undeclared) follows
  the TU-003 precedent exactly: versioned description in a separate
  lookup, never a reinterpretation of TU-002 `describe()` v1. Case 4
  guards that v1 keeps exactly its six fields — structurally guaranteed
  since `describe()` is untouched.
- Sharing the literal keys `schema_version` and `name` across three
  namespaces (`describe`, `describe_operation`, `describe_reading`) is
  the pattern the TU-003 architecture review already blessed ("leave
  TU-005 free to extend either without reinterpreting the other").
- Case 6 pins that `describe_reading()` never stringifies the stored
  value (the `Opaque` repr raises) and is JSON-clean — satisfied by a
  field-only dict with no value access, matching the gate's "zero device
  I/O, never reads the stored value".

## 5. Explicit serialization limit — implementable, zero imposition

- `json.dumps` of a `Reading` raises `TypeError` under the default encoder
  as long as `Reading` defines no JSON fallback, and the stored value is
  never coerced or restricted. Case 6's `assertIs` pins identical object
  identity through `get()` and `read().value`, which forbids defensive
  copying. This is the correct reading of "explicit serialization limit":
  the operation fails loudly rather than the library restricting stored
  values, and it composes with the TU-002 case-3 opaque-object contract.

## 6. Proxy forwarding, denied-read parity, OBS-004 avoidance — sound

- `ProxyParameter` overrides `set`/`get` to forward; adding a matching
  `read()` override that forwards to `self._obj.read()` mirrors the
  TU-001 proxy contract (case 9 only requires value/quality parity with a
  direct read — delegating returns the target's `Reading`, so parity is
  automatic). Note the proxy copies `gettable` at construction, so a
  denied target stays denied through the proxy without extra code.
- Case 10's denied-read parity is pinned on a plain `Parameter`; the
  pre-acquisition permission check in `read()` (see §2) makes
  `RuntimeError` precede any hook invocation, exactly matching legacy
  `get()`.
- OBS-004 avoidance is by construction: every `VisaParameter` in the
  suite is constructed with `settable=False` and no `init_value`, so the
  base-`__init__`-before-hook-fields defect is not triggered and is not
  asserted as behavior. It remains a recorded defect for disposition,
  unchanged.

## 7. PRD criteria coverage — complete, no contradictions

PRD TU-005 row coverage: richer reading results with acquisition time and
quality (cases 1, 8); failed-acquisition quality with preserved error
(case 7); optional unit/type/shape/channel descriptions (case 3);
optional uncertainty/calibration reference (case 5); existing value-only
reads and arbitrary values keep working (cases 2, 4, 6); explicit rather
than imposed serialization limits (case 6); simple value access retained
for existing callers (cases 2, 8 and the untouched `huo` path). Nothing
missing; nothing contradictory. The two legacy-compatibility guards
(cases 2, 4) genuinely pass against unchanged code, as verified in the
red run.

## Feasibility notes per case

1. Rich read time/quality: implementable via permission pre-check +
   `try: self.get()`; `acquired_at` stamped inside the `time.time()`
   window; legacy `get()`/`__call__` untouched (verified value-only).
2. Legacy guard: passes against unchanged code today (verified as one of
   the two green cases); all asserted behavior is existing public API.
3. Reading description: implementable as a fresh dict literal with
   `schema_version` 1 and echo/`None` optional fields; `assert_json`
   holds because all six values are str/int/None/list-of-int.
4. TU-002 v1 guard: passes against unchanged code (verified green); the
   six-field set assertion matches the current `describe()` exactly.
5. Uncertainty/calibration: implementable as stored constructor
   information copied onto the `Reading`; `None` default on plain
   parameters; flows through `VisaParameter`'s `**kwargs` if ever needed.
6. Nonserializable value: implementable with no coercion and no stored
   value restriction; `Reading` exposes the identical object; the
   `TypeError` falls out of the default JSON encoder.
7. Failed acquisition: implementable; `except Exception as e` preserves
   `assertIs(result.error, error)` identity; exactly one
   `query("MEAS?", None)` because `get()` runs the hook once; legacy
   `get()` re-raises the identical object afterward (side-effect-free
   mock makes the second call deterministic).
8. Single acquisition on VISA: implementable; `read()` performs one
   positional query, encodes to `3.5`, timestamps, and a subsequent
   `get()` performs exactly one more (mock reset between assertions).
9. Proxy forwarding: implementable with a one-line `read()` override on
   `ProxyParameter`; returns the target's `Reading`, giving value/quality
   parity by construction.
10. Denied read: implementable; gettable check precedes the `try`, so
    `RuntimeError` matches legacy `get()` with zero acquisitions.

## Non-blocking observations (for the detailed design to pin)

1. `Reading` field states on failure (`value`/`acquired_at`), quality
   vocabulary closure, mutability, and `try`-scope (hook-only vs full
   post-permission chain) are unstated; §3 lists the recommended
   resolutions. None affect any acceptance case.
2. `describe_reading()` on `VisaParameter`/`QuantizedParameter` is not
   exercised; the base implementation satisfies the contract, but the
   designer should state whether subclasses enrich it (e.g. a channel
   default) — an additive decision, not a gate issue.
3. `acquired_at` on a successful read is stamped after acquisition; the
   design should state this explicitly so the timestamp means "completion
   of the acquisition chain", which is the only reading consistent with
   the bounded-window assertion when hooks are slow.

This review did not run the acceptance suite as a pass criterion and does
not claim implementation or final test approval; detailed design
approval, independent review, and testing gates remain pending.
