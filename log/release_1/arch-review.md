# TU-001 architectural review

Reviewer: sw-jerry (architect). Candidate: `9c84b35` on
`codex/tu-001-characterization`. Verdict: **APPROVED** for the TU-001
characterization architecture gate. This is an architecture/feasibility review,
not a code review, test result, detailed-design approval or release acceptance.

The compatibility matrix, PRD and characterization cases cover the interfaces
that later opt-in contracts must preserve: parameter call/hook behavior, device
composition and builders, station identity, VISA construction and command side
effects, model fallback, ndarray mapping and public exports. The cases keep
production unchanged and fit the existing five-module boundary: `tu` defines
device/model behavior; `huo` executes, `shui` persists, `mu` coordinates uses,
and `jin` supplies utilities. No new framework or dependency is needed for
TU-001. Direct source checks support the matrix's six observations.

| Observation | Architectural disposition |
| --- | --- |
| OBS-001 timeout units | Open TU-006 decision; preserve existing forwarding until the compatibility impact is assessed. |
| OBS-002 raw write | Open TU-006 defect candidate; require targeted regression and explicit behavior decision. |
| OBS-003 failed init/ownership | Open TU-004 lifecycle and TU-006 VISA decisions; no cleanup guarantee inferred from current code. |
| OBS-004 subclass initialization | Open TU-005/TU-006 defect candidate; reproduce before correction. |
| OBS-005 model fallback | Existing `features` fallback is characterized; TU-007 must expose failures through a new strict path. |
| OBS-006 delegation/cycles | Open TU-004 policy decision; explicit lookup remains available today. |

Approval means TU-001 can proceed to its remaining independent review, test,
principle and CI gates. It does not close OBS-001–006, approve a later detailed
design, or authorize integration. The matrix's claimed local 20-test pass is
tester evidence; this review did not rerun it. Hardware and concurrency remain
unverified.

# Release 1 end-of-release architecture record

Architect: sw-jerry. Date: 2026-10-01. Basis: `dev` tip `e2d31ce`, where
TU-001–TU-008 are all integrated and the full suite is 99 tests green
(`log/release_1/tests/TU-008.md` final evidence). The TU-001 review above is
preserved unchanged; this section is appended per DC-4 of the TU-008 design
(`log/release_1/design/TU-008.md`). Project-root `arch.md` has been updated to
describe the implemented state below (commit `docs(arch): describe release 1
implemented behavior`); every statement traces to merged production code and
merged tests, cross-checked against `docs/tu_extensions.md`.

## What shipped, per task

- **TU-001 (characterization):** `log/release_1/compatibility.md` and
  `tests/test_tu_contracts.py` characterize existing behavior on unchanged
  production code; they remain the baseline the extensions were verified
  against.
- **TU-002 (descriptions, schema v1):** `Parameter.describe()`,
  `Device.describe()` and `Station.describe()` return versioned
  (`schema_version: 1`) JSON-compatible descriptions. Descriptions never read
  cached values, never invoke validators/codecs/hooks and perform no device
  I/O; the built-in device/station traversal never invokes subclass
  `describe()` overrides. Metadata accepts exact built-in
  dict/list/str/int/float/bool/None with str keys and finite floats;
  unsupported types raise `TypeError`, nonfinite floats and self-referential
  containers raise `ValueError`. Pinned by `tests/test_tu_descriptions.py`.
- **TU-003 (explicit operation path):** `VisaCommand.execute()` performs the
  declared operation with the handle's serialization guard and
  identical-object error propagation; `describe_operation()` returns a
  `schema_version` 1 description of side-effect semantics and executes
  nothing. Legacy `VisaCommand` get-executes-once/write-denied behavior is
  unchanged. Pinned by `tests/test_tu_operations.py`.
- **TU-004 (lifecycle):** `Device` gains `prepare()`/`cleanup()`/
  `initialized`/`supports()`. `prepare()` runs `_prepare_impl()` once and is a
  no-op when already initialized; `cleanup()` runs `_cleanup_impl()` exactly
  once per acquisition attempt and repeated calls are safe no-ops; borrowed
  resources held by a non-owner are never released. `supports()` is
  side-effect-free: base answers `True` for `'prepare'`/`'cleanup'`, `False`
  otherwise. The contract is single-threaded. Pinned by
  `tests/test_tu_lifecycle.py`.
- **TU-005 (measurement contract):** `Parameter.read()` returns a frozen
  `Reading` dataclass (`value`/`quality`/`error`/`acquired_at`, plus echoed
  `uncertainty`/`calibration` declarations); `quality` is the closed
  vocabulary `{'ok', 'failed'}`; on failure `error` is the original exception
  object, identity preserved. `describe_reading()` returns a
  `schema_version` 1 description of the constructor-declared
  `unit`/`value_type`/`shape`/`channel` metadata and performs no acquisition.
  Legacy `()`/`get()` value-only semantics are unchanged. Pinned by
  `tests/test_tu_measurements.py`.
- **TU-006 (VISA adoption):** `VisaHandle` adopts the lifecycle contract
  (`prepare()` re-opens through the retained resource manager; a cleaned-up
  borrowed handle raises `RuntimeError` on re-open; `close()` is an alias of
  `cleanup()`), the seconds-based `timeout_seconds` property (two-way
  conversion; `None` disables; default construction forwards 5000 ms; explicit
  raw `timeout` forwarded unchanged, never rescaled — OBS-001 resolved), the
  advisory `abort()`/`AbortReport` surface (bookkeeping only, no I/O;
  `stopped` always `False`) and the device-confirmed `confirm_stop()` latch
  via `*OPC?`. `write_raw` now routes to `resource.write_raw` (OBS-002 fixed);
  `_acquire()` is the single acquisition site with exact-once failure cleanup
  (OBS-003 closed). Pinned by `tests/test_tu_visa_contracts.py` with the
  `@sim` simulator.
- **TU-007 (theory surface):** `TheoryModel` gains `describe()` (schema v1,
  no evaluation), `supported_configuration()`, `configuration()`
  (JSON-serializable exactly when attribute values are), `configure()`
  (three-phase: non-mapping `TypeError`, unknown-key `KeyError`, pre-validation
  with no partial application) and `evaluate_features(strict=False)` (lenient
  `{}` fallback on `Exception`; `strict=True` re-raises the original error
  object). Legacy `features` and fixed-shape ndarray `Mapping` behavior are
  unchanged. Pinned by `tests/test_tu_theory.py`.
- **TU-008 (integration/docs):** DEFECT-1 fixed (raw-string regex literals in
  two `__main__` example blocks, zero behavior change); `docs/tu_extensions.md`
  shipped as the user-facing extension guide; README pointer added. P1
  (regression/compile/import), P2 (package-artifact inspection) and the DC-2
  documentation walk recorded in `log/release_1/tests/TU-008.md`.

## Observation dispositions

Resolved (recorded for transparency; full wording in
`log/release_1/compatibility.md` and `docs/tu_extensions.md` §8):

- **OBS-001 — resolved (TU-006):** timeout sentinel contract landed; default
  construction forwards 5000 ms, explicit raw `timeout` is never rescaled,
  `timeout_seconds` converts in both directions.
- **OBS-002 — fixed (TU-006):** `write_raw` routes to `resource.write_raw`
  with identical bytes, forwarded return and preserved error identity.
- **OBS-003 — closed (TU-006):** single acquisition site; post-acquisition
  failure closes the resource exactly once and propagates the original
  exception; the resource manager is retained for the handle's lifetime.

Known limitations / technical debt (open; workarounds documented in
`docs/tu_extensions.md` §8, dispositions in `log/release_1/compatibility.md`):

- **OBS-004:** `QuantizedParameter`/`VisaParameter` initialize the base before
  installing fields used by setting hooks/codecs; a non-None settable
  `init_value` can fail. Workaround: avoid non-None settable `init_value` on
  these classes.
- **OBS-005:** `features` and `evaluate_features(strict=False)` swallow
  evaluation exceptions into `{}`; the opt-in `strict=True` path re-raises.
- **OBS-006:** delegated attribute names may collide with methods (`child`,
  `device`); explicit `_attributes`/`device()` lookup remains the escape
  hatch.
- **DEFECT-2:** constructing a neither-settable-nor-gettable `Parameter`
  emits a deliberate, documented programmer-error warning; it is not silenced.
- **Description serialization limits:** description payloads contain JSON-safe
  scalar values; ndarray attribute values in model configuration are the model
  author's responsibility.

## Architect confirmation

`arch.md` (repository root) now describes the implemented Release 1 state in
the present tense: the new `tu` public APIs, `schema_version` 1 description
namespaces, the error-identity policy, and the known limitations above. The
pre-release "Proposed evolution" section is replaced by an implemented-behavior
record plus an explicitly-marked future-work note. No claim is made beyond the
merged implementation and the recorded green gates.
