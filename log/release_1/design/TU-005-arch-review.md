# TU-005 architecture design review

Reviewer: sw-jerry (architect). Candidate: `9ef0cd2` on
`codex/tu-005-measurements` (`log/release_1/design/TU-005.md`). Verdict:
**APPROVED** — TU-005 may proceed to implementation. Scope is
detailed-design compliance with the Release 1 PRD (TU-005 row), the
approved TU-005 test contract (`log/release_1/tests/TU-005.md`) and its
implementability review (ACCEPTED with three non-blocking observation
groups), the TU-001 compatibility matrix, the TU-002/TU-003/TU-004
architectural decisions, and the five-module architecture. This is not a
code review, test execution or integration approval.

I independently re-read `softlab/tu/station/parameter.py`,
`softlab/tu/station/visa.py`, `softlab/tu/station/__init__.py`, and the
ten acceptance cases in `tests/test_tu_measurements.py` at the candidate
commit; the source-level claims below are verified against those files,
not taken on faith from the design.

## Assessment against review criteria

1. **Architecture fit — confirmed.** The change is additive-only in
   `softlab/tu/station/parameter.py` (one frozen dataclass, two methods
   on `Parameter`, six optional constructor keywords, one `read()`
   override on `ProxyParameter`) plus one additive export line in
   `softlab/tu/station/__init__.py`. Five-element boundaries hold: only
   `tu` is touched; `huo`'s `AtomJob` value-only path is undisturbed
   because `get()`/`__call__` keep identical code and the design invents
   no parallel acquisition path. New imports are stdlib only (`time`,
   `dataclasses.dataclass`, `List` from the already-imported `typing`
   module); the dependency table's "(none)" verdict is accurate. The
   additive export does not disturb the TU-001 "public exports retain
   identity" contract, which pins identity of existing names, not
   absence of new ones. Verified against source: `VisaParameter` and
   `VisaCommand` genuinely end in `**kwargs` forwarded to
   `super().__init__` (visa.py lines 191–219, 280–295), so the six
   keywords compose with zero VISA signature changes, exactly as
   claimed; `QuantizedParameter`'s fixed signature legitimately stays
   untouched (no requirement, correct rejection).
2. **Chain reuse — sound.** `read()` = gettable pre-check (outside the
   `try`) + `try: value = self.get()` / `except Exception as error`. The
   pre-check placement is correct: it reproduces legacy `get()`'s
   `RuntimeError(f'Parameter {self.name} is not gettable')` — verified
   identical message and type at parameter.py line 297 — before any hook
   or I/O (case 10), and the design's rationale (permission denial is a
   programming error, not an acquisition outcome) is the right
   architectural distinction. The whole-chain `try` scope (hook *and*
   encoder) correctly resolves implementability observation 3's
   recommendation: a hook-only scope would arbitrarily split one
   acquisition chain. The note that the permission check inside the
   `try` is structurally dead but not special-cased is the right call —
   removing it would fork the chain. `BaseException` is never caught
   (`except Exception` only), so `KeyboardInterrupt`/`SystemExit`
   propagate exactly as from `get()` — explicitly stated and correct.
   Exact-once acquisition is inherited for free: `read()` delegates to
   `get()`, and `VisaParameter.before_get` performs exactly one
   positional `handle.query(get_cmd, delay)` per call (verified at
   visa.py line 255), matching cases 7–8's
   `assert_called_once_with("MEAS?", None)`.
3. **Reading model — sound.** All three implementability observation
   groups are pinned with the recommended resolutions, and each pinning
   is justified rather than merely chosen: post-completion
   `time.time()` stamp (the only reading consistent with the
   bounded-window assertions under slow hooks; no clock injection —
   self-certifying); failed-read field states (`value=None`,
   `acquired_at=None`, original exception object by identity,
   `uncertainty`/`calibration` still populated because declarations are
   not acquisition products) — the per-field rationale is honest
   (`None` is the only non-fabricated state; stamping a failure time
   would silently change the field's meaning); closed two-literal
   quality vocabulary as plain `str` (consistent with TU-004's string
   capability vocabulary; an `Enum` would be an entity without
   necessity); frozen dataclass with field values not deeply frozen
   (case 6's identity pinning forbids copying; freezing removes record
   falsification, e.g. rewriting `quality` while `error` is set).
   Placement beside `Parameter` in `parameter.py` follows the TU-003
   placement precedent; a separate module for one small class was
   rightly rejected.
4. **`describe_reading()` — consistent with TU-002/TU-003.** Own
   versioned namespace (`schema_version` 1), sharing only the
   TU-003-blessed `schema_version`/`name` keys, never reinterpreting
   `describe()` v1 — structurally guaranteed since `describe()` is
   untouched (case 4). Zero-I/O is pinned precisely: touches only
   `self._name` and the six declared fields, never invokes validators,
   codecs, hooks or `str`/`repr` on any runtime value — which is exactly
   what case 6's repr-raising opaque object requires. Fresh dict literal
   per call follows TU-003 decision 1. The no-enrichment posture for
   subclasses (no automatic `channel` from a VISA address, proxy
   describes its own — all-`None` — declarations) is correct: it keeps
   the declared/inferred line clean, no acceptance case requires
   otherwise, and it is explicitly additive for a future task.
   Constructor metadata is stored verbatim with no validation and no
   copying; the consequence (non-JSON declarations make the description
   non-JSON-serializable) is stated as the caller's explicit choice,
   consistent with the serialization-limit philosophy.
5. **Serialization limit, proxy forwarding, permission parity —
   confirmed.** Explicit and loud: `Reading` defines no JSON fallback of
   any kind, so `json.dumps` raises `TypeError` under the standard
   encoder for *every* reading, with case 6 pinning the opaque case;
   stored values are never restricted or copied (identity pinned by
   case 6's `assertIs`); `describe_reading()` is the JSON-safe
   inspection path that never carries the value. `ProxyParameter.read()`
   is one-line forwarding to `self._obj.read()`, so the returned
   `Reading` is the target's own object and value/quality/error parity
   is automatic (case 9). Permission parity is by construction and
   verified against source: `ProxyParameter.__init__` copies
   `obj.gettable` (parameter.py lines 386–388), and the target's own
   pre-check runs inside `target.read()`, so a denied target stays
   denied through the proxy with zero extra code — identical structure
   to legacy proxy `get()` forwarding.
6. **Simplicity rejections, docstring mandates, handoff — complete.**
   The rejected-alternatives audit (quality `Enum`, separate module,
   clock injection/`time.monotonic`, defensive copying,
   `QuantizedParameter` pass-through, subclass enrichment, `to_json()`
   helper) gives a reason for every rejection; none is a bare assertion.
   The docstring requirements are unusually strong: exact contract text
   is mandated verbatim for the quality vocabulary, the permission path,
   and the failure path, plus the `Parameter` class docstring's
   `Public methods:` update — this removes developer guesswork on the
   load-bearing contracts. OBS-004 is correctly analyzed as untouched
   (new keywords are pure field assignments after the `init_value`
   block, installing nothing hooks touch) and the test-phase handoff to
   sw-mike is correctly scoped: the case-7 extension pins exactly the
   failed-read field states the design fixes but no acceptance case
   asserts, and is deferred to the test phase rather than implementation
   — the same pattern the TU-004 re-review approved.

## Non-blocking observations

1. **Generated dataclass dunders touch field values.** With
   `@dataclass(frozen=True)` and default `eq=True`, the generated
   `__repr__` reprs all fields and the generated `__hash__` hashes the
   field tuple. A `Reading` carrying an opaque value (case 6's
   repr-raising object) therefore has a `repr()` that raises, and
   hashability depends on the runtime value (unhashable stored values
   make `hash(reading)` raise `TypeError`). No acceptance case exercises
   either, and the behavior is consistent with the design's
   loud-failure, never-copy posture — but the class docstring mandate
   currently lists immutability and the serialization limit without
   noting the generated-dunder consequence. Recommend (non-blocking) one
   sentence in the `Reading` class docstring: generated
   `__repr__`/`__hash__` operate on field values, so a value whose
   `repr`/`hash` raises makes the corresponding reading operation raise.
2. **`shape` echo-by-reference vs. TU-002 detachment.** TU-002's
   `describe()` returns a fully detached dict (metadata is recursively
   copied by `_validate_metadata`); `describe_reading()` echoes `shape`
   by reference, so caller-side mutation of the declared list would leak
   into later descriptions. The design justifies this explicitly
   (posture set by case 6's no-copying rule; documented read-only) and
   the asymmetry is deliberate, not accidental. Acceptable as designed;
   no change requested.
3. **Concurrency.** `read()` inherits the legacy chain's single-threaded
   assumption and the design does not restate it. This is consistent
   with TU-004 decision 3 (concurrency out of contract, VISA concurrency
   owned by TU-006); no pinning is required here. Noted only so the
   test phase does not invent threaded cases.

## Disposition

All seven review criteria pass; the three implementability observation
groups are resolved with the recommended options; no acceptance case is
left unimplementable or ambiguous; no compatibility invariant from the
TU-001 matrix is disturbed. No issue requires a design change. TU-005
may proceed to implementation. This review does not claim code review,
test pass, CI or integration; the independent-review, testing, principle
and CI gates remain open.
