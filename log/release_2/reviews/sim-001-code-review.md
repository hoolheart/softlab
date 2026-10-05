# Code Review — SIM-001: deterministic simulated-object foundation

Reviewer: sw-celeste | Date: 2026-10-05
Branch: `codex/sim-001-simulation-foundation` | Range reviewed:
`99da38c..4823c1b` (implementation commits `4eab94d`, `0364080`, `6914475`;
phase-record commit `4823c1b` touches `log/release_2/sprint-board.md` only)
Binding specification:
[sim-001-detailed-design.md](../design/sim-001-detailed-design.md)
(APPROVED, minors closed at `917cfd7`) | Requirements:
[prd.md](../prd.md) (SIM-AC-01–08) | Test plan:
[sim-001-test-plan.md](../test/sim-001-test-plan.md) (APPROVED, rev 2)

## Verdict: APPROVED

- **Total issues: 0** (blocker: 0, minor: 0)
- One informational observation (non-blocking, no fix requested) recorded
  under Simplicity Audit.

## What was executed (commands + results)

All commands from the repository root using the project `.venv`
(Python 3.13), matching AGENTS.md verification §1:

| Command | Result |
| --- | --- |
| `git branch --show-current` | `codex/sim-001-simulation-foundation` |
| `git diff --stat 99da38c..4823c1b` | 6 files: `softlab/tu/simulation/object.py` (+794), `softlab/tu/simulation/__init__.py` (+3), `softlab/tu/__init__.py` (+1), `docs/user-guide/simulation.md` (+381), `log/release_2/test/sim-001-implementation-notes.md` (+98), `log/release_2/sprint-board.md` (±1) |
| `git diff 99da38c..HEAD -- softlab/huo softlab/jin softlab/shui softlab/mu` | **empty (0 lines)** — CHK-06-1 satisfied |
| `git diff 99da38c..4823c1b -- pyproject.toml` | empty — no dependency changes (SIM-TC-07d) |
| `git diff 99da38c..4823c1b --check` | clean, exit 0 |
| `awk 'length > 80 …'` over `object.py`, `simulation/__init__.py` | no Python line exceeds 80 chars (Black width; only markdown table rows in the guide exceed 80, which is expected and consistent with the design document itself) |
| `python -m compileall -q softlab` | exit 0 |
| Import smoke: `softlab.tu.simulation`, `softlab.tu`, top-level `softlab`; class-identity assertion `softlab.tu.simulation.SimulatedObject is softlab.tu.simulation.object.SimulatedObject` | all pass, version `0.3.0` (SIM-TC-07c pattern) |
| `python -W error::Warning -m unittest discover -s tests -p 'test_*.py'` | **Ran 99 tests, OK, zero warnings** — baseline regression intact (SIM-TC-07a/07b; warnings-as-errors covers SIM-TC-07e evidence) |
| Re-executed the guide §11 example verbatim (copied code block to a scratch script outside the repo) twice | output **byte-for-byte identical** to the guide's recorded "Actual output" block and identical across the two runs (SIM-TC-08b) |
| Reviewer adversarial spot-check script (scratch, not committed): evolve-once counting, pending inputs, caller-container/return/callback-arg aliasing, failed `set_input`/`evolve`/`observe` atomicity and exception identity (`is` checks), armed-factory reset failure (SIM-TC-05i scenario) incl. post-failure usability and successful second reset, dtype/shape pinning, rejected categories (`bool`, `np.float64`, list, `None`, object-dtype), int/float cross-category rejection, cross-role duplicate names, `KeyError` on unknown names, evolve-result key validation, declaration-order name properties | **all checks passed** |

## Design compliance

- [x] Implementation matches the detailed design — every §2.3 public API
  member present with the designed signature: constructor (§2.1),
  `name`/`input_names`/`state_names`/`output_names` properties,
  `set_input`, `get_input`, `evolve_once`, `get_state`,
  `observe_outputs`, `reset`. No convenience aliases added (design §8
  prohibition respected).
- [x] Value contract (§2.4): closed three-category list enforced by
  `_classify_value` with `bool` checked **before** `int`
  (object.py:105–108, per design §8), NumPy scalars rejected with the
  designed redirect message (object.py:109–113), exact `int`/`float`/
  `np.ndarray` checks via `type(v) is …`, numeric-dtype check via
  `np.issubdtype(v.dtype, np.number)`; per-variable spec pinning
  (scalar category; ndarray dtype+shape) in `_validate_against_spec`
  (object.py:130–194) with `TypeError`/`ValueError` split as designed.
- [x] Copy-in/copy-out (§2.4): single `_copy_value` mechanism
  (`np.array(v, copy=True)`) applied at every boundary — construction
  (two independent copies for literals: pristine + working,
  object.py:424–425), `set_input` (515), `get_input`/`get_state`
  (537/606), callback arguments in `evolve_once` (573–580) and
  `observe_outputs` (641–644), callback results in
  `_validate_callback_result` (793), reset candidates (727–729).
- [x] Pending-input publication at the evolve boundary (§2.3, adopted
  recommendation 1): one input store; `set_input` stores and never
  evolves; inputs persist across evolutions. Verified by spot-check.
- [x] Evolve-exactly-once (§3.1): `self._evolve(` appears exactly once in
  the module (object.py:581), inside `evolve_once`; `self._observe(`
  exactly once (645), inside `observe_outputs`. No invocation from
  construction, inspection or reset. Verified by counter spot-check.
- [x] Build-then-swap atomicity (§2.5, §3.2): `evolve_once` validates
  into a local dict and rebinds `self._states` only after full
  validation (582–584); `reset` builds `new_inputs`/`new_states` in
  locals and rebinds only after both loops complete (687–698);
  `set_input` validates and copies before the single-entry replacement
  (513–515); `observe_outputs` performs no mutation at all.
- [x] Factory-declared initial values (§2.1): factories invoked once at
  construction, re-invoked per `reset` in declaration order; pristine
  copies retained only for literals (`_resolve_initial`,
  object.py:388–425) — matches closed review item 2.
- [x] Original-exception identity (§2.5): no `try`/`except` anywhere
  around user callbacks or factories; identity verified with `is`
  assertions for evolve, observe and factory exceptions.
- [x] Error model (§2.5 table): `TypeError`/`ValueError` split,
  `KeyError` naming the unknown variable, key-set mismatch `ValueError`
  naming missing/extra — all confirmed by spot-check.
- [x] Module structure and exports (§5): `object.py` imports only
  `collections.abc`, `typing`, `numpy` — **zero `softlab` imports**
  (circular import impossible structurally; executable identity check
  passed); `simulation/__init__.py` re-exports only `SimulatedObject`;
  `tu/__init__.py` gains `simulation` in alphabetical position
  (`station`, `simulation`, `theory`) with no class names added.

## Test-plan traceability (walked against the code)

| Case | Implementable? | Basis |
| --- | --- | --- |
| SIM-TC-01a | ✓ | name properties return declaration-order tuples (441–477); roles are distinct dicts; cross-role duplicate rejected (327–348) |
| SIM-TC-01b | ✓ | ndarray category + dtype/shape pinning (118–123, 180–193) |
| SIM-TC-01c | ✓ | constructor validates category of every argument before any object state matters; constructor raise ⇒ no object escapes |
| SIM-TC-01d | ✓ | `KeyError` naming the variable (511–512, 535–536, 604–605) |
| SIM-TC-01e | ✓ | rejected categories (105–127) + `set_input` atomicity (513–515) |
| SIM-TC-02a/02b | ✓ | single `self._evolve(` call site; counter-verified |
| SIM-TC-02c | ✓ | memoryless idiom: `evolve` may ignore `previous_states`; holding state supported |
| SIM-TC-02d | ✓ | `dt` as ordinary input; no clock/I/O imports exist in the module |
| SIM-TC-02e | ✓ | `observe_outputs` reads committed post-swap state only |
| SIM-TC-03a/03b/03c | ✓ | inspection never invokes callbacks; no caching (G recomputed per call); pending-input semantics counter-verified |
| SIM-TC-03d | ✓ | module has no I/O, scheduler or clock access |
| SIM-TC-04a/04b/04c | ✓ | reset restores literal pristines + fresh factory values (687–698); construction double-copy; per-instance stores only; no module-level mutable state |
| SIM-TC-04d | ✓ | failed evolve leaves store unswapped; reset then restores |
| SIM-TC-05a/05b | ✓ | verified: validate-before-mutate; `is`-identity of propagated exception; snapshot equality |
| SIM-TC-05c/05d/05e | ✓ | verified: caller-container mutation, returned-value mutation, adversarial in-place mutation and later mutation of retained callback args all leave committed state intact |
| SIM-TC-05f | ✓ | closed-list rejections verified (`bool`, NumPy scalar, list, `None`, object-dtype) |
| SIM-TC-05g | ✓ | guide §7 states the external-side-effects caveat verbatim per design |
| SIM-TC-05h | ✓ | raising `G` propagates identically, nothing mutated; reset-then-observe path works (05i spot-check exercised it) |
| SIM-TC-05i | ✓ | armed-factory scenario executed by reviewer: identical `RuntimeError` object, pre-reset snapshot intact (inputs/states/observations), object fully usable, second reset succeeds and restores the initial condition |
| SIM-TC-06a–06e | ✓ | bridge is test-side closures per §4; the object exposes exactly the surface the closures need (`set_input`, `observe_outputs`, bound no-arg `evolve_once`); no production dependency on `Device`/`Parameter`/`huo` |
| CHK-06-1 | ✓ | `git diff 99da38c..HEAD -- softlab/huo softlab/jin softlab/shui softlab/mu` is empty (recorded above) |
| SIM-TC-07a–07e | ✓ | 99 tests OK, zero warnings under `-W error::Warning`; compile exit 0; import identity check; `pyproject.toml` untouched; no Release 1 debt implicated |
| SIM-TC-08a/08b/08c | ✓ | guide topic checklist complete (see below); executed example reproduced byte-identically; docstrings sampled below |

## User guide review (`docs/user-guide/simulation.md`)

Design §6 topic checklist — all present and accurate against the code:

construction (§2), evolve contract incl. pending inputs and the
memoryless idiom (§3), observation (§4), reset + failure atomicity (§5),
error table matching design §2.5 row-for-row (§6 error table), supported
value contract and ownership/aliasing rules (§6), deterministic-callback
contract **with the external-side-effects caveat** (§7), optional time
context (§8), mock-device vs simulated-object distinction (§1), bridge
pattern with the hook-binding constraint (**never `hook_before_set`**)
and the **two-gate validation asymmetry** stated explicitly with the
sim-spec-authoritative consequence (§9), known limitations matching
design §7 (§10), executed example (§11) — re-run by this reviewer, output
byte-for-byte equal to the recorded block and reproducible across runs.

## Conventions (AGENTS.md)

- [x] English docstrings with `Args:`/`Returns:`/`Errors:`/
  `Side-effects:` in the repo's `- name ---` bullet style (matches
  `softlab/tu/station/parameter.py` convention) on the constructor,
  every public method/property, and private helpers
- [x] Full type annotations on all signatures (py39-compatible
  `typing` names)
- [x] Black line width 80 on all Python files (verified mechanically)
- [x] `__init__.py` export order alphabetical; absolute imports matching
  existing subpackage style
- [x] Validators signal failure by raising (validate-raises idiom
  preserved)
- [x] No new dependencies; no `pyproject.toml` change
- [x] No unrelated changes; Release 1 debt untouched
- [x] No compiler or lint warnings (`compileall` clean; suite clean
  under warnings-as-errors)

## Simplicity audit

- [x] No third-party dependency beyond already-required NumPy
- [x] No over-engineering: three private helpers + one plain class; the
  private `_ValueSpec` NamedTuple is the minimal pinning record; no
  abstraction lacks a caller
- [x] Data structures minimal: plain dicts for stores/specs/sources, a
  tuple for output names
- [x] Public surface exactly the designed one — nothing added

**Informational observation (non-blocking, no fix requested):**
`_reset_value` copies a literal pristine twice (object.py:727 and 729 —
copy for validation, then copy for commit). This is a harmless,
negligible-cost redundancy that keeps the factory and literal paths
structurally uniform; removing it would special-case literals for no
measurable benefit. Recorded for completeness; the code is approved as
written.

## Quality gates

- [x] No compiler errors / warnings
- [x] Baseline regression: 99 tests OK, zero warnings
- [x] Import smoke and class-identity check
- [x] Guide example re-executed, byte-identical
- [x] CHK-06-1 verified by `git diff` inspection (this reviewer's
  checklist item — recorded here for the test-results record)

## Approval

- [x] Implementation faithful to the approved design
- [x] Code meets quality and simplicity standards
- [x] **APPROVED — ready for testing by sw-mike** (SIM-TC execution
  phase). CHK-06-1 is discharged by this review and should be recorded
  as verified in the test-results document.
