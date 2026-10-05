# Review — SIM-001 detailed design

Reviewer: sw-jerry (Software Architect) | Date: 2026-10-05
Verdict: **APPROVED**
Reviewed artifact: [sim-001-detailed-design.md](../design/sim-001-detailed-design.md)
at commit `7ccac0d` on `codex/sim-001-simulation-foundation` (verified via
`git branch --show-current` / `git log`).
Requirements: [prd.md](../prd.md) (SIM-AC-01–08) | Planned architecture:
[tasks.md](../tasks.md) | Approved test plan:
[sim-001-test-plan.md](../test/sim-001-test-plan.md) |
Architecture baseline: [arch.md](../../../arch.md)

## Issues

No blockers. Three minor items, none of which changes the verdict.

1. **[minor] §4.1 — document the two-gate validation asymmetry in the
   bridge.** The input parameter's own validator (e.g. `ValNumber()`) and the
   object's per-variable specification are independent gates: the parameter
   validator runs first (`Parameter.set` order: permission → validate →
   decode → `before_set` → store, verified in
   `softlab/tu/station/parameter.py:396-407`), and the object may still
   reject a value the parameter accepted (e.g. a `float` passed to an
   `int`-pinned input). The `before_set` placement already guarantees the
   parameter store is untouched on sim-side rejection, so parameter and
   object cannot disagree — but the user guide (§6 guide checklist) should
   state explicitly that the object's specification is authoritative and
   that a sim-side rejection surfaces as the object's `TypeError`/
   `ValueError` propagating out of `Parameter.set`. Requested change: add
   one sentence to the guide-topics list in §6 (and to §4.1) covering this;
   no API change.
2. **[minor] §2.1/§2.5 — clarify pristine copies for factory-declared
   variables.** §2.1 says construction retains "a pristine copy retained for
   `reset()`" of each mutable initial value, while §2.5/§3.2 correctly
   specify that reset *re-invokes the factory* for factory-declared
   variables (the pristine copy is only used for literal-declared ones).
   The behavior is unambiguous in the failure-path contract, but the §2.1
   sentence reads as if pristine copies are retained for factories too.
   Requested change: one clarifying sentence in §2.1 (pristine copies are
   retained for literal-declared initial values; factory-declared variables
   are rebuilt by re-invocation at reset). Editorial only.
3. **[minor] §6 SIM-TC-05i handoff — record the follow-up artifact.** The
   resolution correctly instructs sw-mike to reclassify SIM-TC-05i-PENDING →
   SIM-TC-05i [PLAN] with the exact contract table. Make the resulting
   test-plan revision an explicit tracked follow-up (test plan revision 2)
   so the SIM-AC-05 gate checklist has a concrete document to point at.
   Requested change: none to the design; noted here so the coordinator and
   sw-mike schedule the revision before implementation is claimed complete.

## Per-dimension findings

### 1. Architectural fit — PASS

- Placement in `softlab/tu/simulation/` matches the `[PLANNED]` architecture
  in `tasks.md` exactly (focused package, exported through `tu`'s package
  initializer, independent of `Device`/`TheoryModel`/scheduling/storage).
- Five-element separation respected: the simulated object is an experimental
  object, which is `tu` (earth) territory per arch.md's module table. The
  bridge to `huo` lives entirely in tests/examples and uses only existing
  `huo` public API (`count`, `scan`, `run_process`, `get_scheduler`); zero
  `huo`/`shui`/`jin`/`mu` production changes. The design's explicit refusal
  to reuse `jin.validator` (§8) is correct — the §2.4 closed three-category
  contract is simpler than adapting general validators and avoids a new
  `tu → jin` edge inside the package.
- Circular-import risk: none by construction. `object.py` imports only
  `typing` and `numpy`; `softlab/tu/__init__.py` currently imports
  `(station, theory)` and the proposed `(station, simulation, theory)` tuple
  is alphabetical, matching the existing convention (subpackage imports in
  `tu/__init__.py`, class names in subpackage initializers — verified
  against `softlab/tu/__init__.py` and `softlab/tu/station/__init__.py`).
  The SIM-TC-07c executable identity check mirrors the Release 1
  `public_exports_retain_class_identity` pattern. Sound.
- SOLID/IDD: single public abstraction with one responsibility (own and
  evolve declared variables); callbacks are injected contracts defined
  before implementation; the closed value contract is segregated from the
  evolution contract. Dependency direction is strictly one-way (bridge
  closures reference the object, never vice versa).

### 2. Compatibility — PASS

- Release 1 behavior preserved: production diff is one new subpackage plus
  one line in `tu/__init__.py`; nothing existing is touched. OBS-004
  (avoided by using base `Parameter` without `init_value`), OBS-005
  (`TheoryModel` untouched), OBS-006 (delegated-name discipline stated for
  examples) and DEFECT-2 (bridge parameters always settable and/or gettable)
  are each addressed in §7 with the correct avoidance strategy.
- Bridge pattern verified against real APIs:
  - `Parameter.__init__` accepts `before_set: Callable[[Any, Any], None]`
    and `before_get: Callable[[Any], Any]`
    (`parameter.py:211-213`); `set()` order is permission → validate →
    decode → `before_set(old, new)` → store → `after_set`
    (`parameter.py:396-407`), exactly as the design claims, so the
    before-set placement genuinely prevents parameter/object divergence.
  - `get()` does `self._value = self._before_get(self._value)`
    (`parameter.py:409-417`), so the design's "before_get's return replaces
    the stored value" is accurate, and the `before_get=lambda old:
    sim.observe_outputs()['y']` wiring is the established precedent
    (`tests/test_tu_integration.py:170`, `tests/test_compatibility.py:70`,
    `tests/test_tu_measurements.py:56`).
  - `huo` hook claims verified in `softlab/huo/process/common.py`:
    `AtomJob.__init__` accepts all four hooks (`common.py:157-160`);
    `body()` invokes `hook_before_set`/`hook_after_set` only when setters
    exist and not dry-run, and `hook_before_get`/`hook_after_get` only when
    getters exist and not dry-run (`common.py:255-274`); the terminal
    sweep-exhaustion execution sets `is_dryrun(True)` (`common.py:428-430`),
    so hooks fire **exactly once per non-dry-run point** and never in the
    dry run — evolve count == point count holds as designed.
  - The never-`hook_before_set` constraint is correct: in `body()` the
    before-set hook fires *before* the setter loop (`common.py:257-262`),
    so evolving there would consume the previous point's input — the
    off-by-one the test plan warned about.
  - The design's call shapes match real signatures:
    `count('acquire', None, None, meas_param, times=N, ...)` matches
    `count(name, group, record, *args, **kwargs)` (`common.py:495-532`);
    `scan('sweep', [meas_param], None, None, ctrl_param, values, ...)` matches
    `scan(name, getters, group, record, *args, **kwargs)` with list-of-
    `Parameter` getters handled by `parse_getters` (`common.py:56-64`,
    `618-677`). These mirror the existing calls in
    `tests/test_compatibility.py:76-82` and
    `tests/test_tu_integration.py:183-207`.
  - `Device.add_parameter(para, visible=True)` exists
    (`device.py:440`); plain base `Device` performs zero I/O per arch.md.
- Error-identity policy: §2.5's "identical original exception object
  propagates — never caught, wrapped or replaced, including `BaseException`"
  is consistent with the Release 1-wide policy (`Reading.error`).

### 3. Contract completeness vs PRD — PASS

- SIM-AC-01: distinct role namespaces, cross-role duplicate prohibition,
  closed value contract with documented `TypeError`/`ValueError` rejections
  (§2.1, §2.4). Covered.
- SIM-AC-02: `x_next = evolve(u, x_previous)`, `y = G(x_next)` is the exact
  callback split; evolve-once semantics (§3.1) and the memoryless holding-
  state idiom (§2.2) cover dynamic and memoryless models; time/`dt` as
  ordinary inputs/states with no clock API (§2.6). Covered.
- SIM-AC-03: inspection/observation never invoke `evolve`; `set_input`
  stores without evolving (pending-input publication decision, §2.3 —
  resolves test-plan open question 1); no wall-clock access anywhere.
  Covered.
- SIM-AC-04: build-then-swap reset restores declared initial inputs/states
  including time values (§2.5, §3.2); determinism contract + copies give
  replay equality; double-copy construction gives instance isolation
  (SIM-TC-04c). Covered.
- SIM-AC-05: failure atomicity is uniform build-then-swap across all four
  mutating/observing operations (§2.5): validate-and-copy fully precede the
  single-entry input replacement; evolution commits by swap after full
  validation; observation mutates nothing; reset builds all candidates in
  local dicts before two plain attribute rebinds. Exception identity
  preserved at every site. Copy-in/copy-out/callback-argument copies are one
  uniform mechanism (§2.4). External side effects explicitly out of scope
  with a guide requirement (SIM-TC-05g). Covered.
- **SIM-TC-05i resolution (reset atomicity) is sound.** The build-then-swap
  reset makes the failure branch provably residue-free (no mutation precedes
  the swap), and the factory-declared initial value is a clean, public-API
  failure-injection point — the concrete case table in §6 is executable
  without monkeypatching internals, pins exception identity, pre-reset
  snapshot equality, continued usability, and second-reset success. This
  fully discharges test-plan open question 4.
- SIM-AC-06: bridge via existing hooks only, explicit advancement named
  (`hook_before_get` for count; `hook_after_set` or `hook_before_get` for
  scan; never `hook_before_set`), shared-object sharing between devices,
  zero `huo` edits, no sleeps-as-steps. Covered, and the hook claims are
  code-verified (dimension 2).
- SIM-AC-07: additive-only, structural + executable circular-import checks,
  `pyproject.toml` untouched. Covered.
- SIM-AC-08: guide topic checklist and docstring requirements are enumerated
  and handed to sw-tom with executed-example specs (scalar accumulator +
  ndarray vector integrator). Covered.
- Value contract implementable with stdlib + NumPy only: `type(v) is int` /
  `is float` / `is np.ndarray` exact-type checks (bool correctly rejected
  before the int check, per §8), `np.issubdtype(v.dtype, np.number)`,
  dtype/shape pinning from the initial value, `np.array(v, copy=True)` at
  every boundary. No third-party need. The §6 traceability table maps every
  planned test case (including CHK-06-1/2 as review-gate items) to a design
  element; no orphan cases found.

### 4. Simplicity — PASS

Two-file package, one public class, three module-private helpers, zero new
dependencies, no base classes/mixins, no adapter class, no `Process`
subclass. Every PRD exclusion (feedthrough, solvers, clocks, stochastic
frameworks, background workers, persistence, hardware, UI) is restated as a
design non-goal in §7. The single generality beyond sw-tom's recommendations
— factory-declared initial values — is directly justified: it makes the
SIM-TC-05i reset-failure branch executable through the public API instead of
by monkeypatching internals, and it doubles as the documented
initial-condition mechanism. This is earned complexity, not speculative.
The closed value contract deliberately rejects general sequences/objects
rather than growing a coercion framework — aligned with the Simplicity
Mandate. The honest-limitations paragraph (§7) states the strictness costs
(int/float pinning, fixed ndarray shapes, all-outputs observation) instead
of hiding them.

### 5. Testability — PASS

All four open questions from the approved test plan are resolved with
binding decisions: (1) pending-until-evolve input publication; (2) uniform
copies in/out; (3) concrete stepping hooks named per flow with the ordering
constraint; (4) the full reset atomicity contract plus a concrete
SIM-TC-05i case table. Error types are pinned per failure site (§2.5
table), callback invocation counts are counter-checkable (§3.1), and every
planned test case maps to at least one design element (§6). sw-tom can
implement `tests/test_tu_simulation.py`,
`tests/test_tu_simulation_integration.py` and `tests/test_sim_user_guide.py`
from this document without guessing. The §8 implementation notes (build
order, exact-type checks before `isinstance`, two independent construction
copies, no convenience aliases) remove the remaining ambiguity.

## What was inspected

- `log/release_2/design/sim-001-detailed-design.md` (full, 721 lines, at
  `7ccac0d`)
- `log/release_2/prd.md` (SIM-AC-01–08, scope guard, debt section)
- `log/release_2/tasks.md` ([PLANNED] architecture section)
- `log/release_2/test/sim-001-test-plan.md` (approved plan incl. revision 1,
  open questions 1–4, sw-tom recommendations 1–4)
- `arch.md` (module boundaries, Parameter set/get order, OBS-004/005/006,
  DEFECT-2, error-identity policy)
- `softlab/tu/__init__.py` and `softlab/tu/station/__init__.py` (export
  conventions)
- `softlab/tu/station/parameter.py` (`__init__` hook signatures, `set()` /
  `get()` order, `init_value` behavior, DEFECT-2 warning site)
- `softlab/tu/station/device.py` (`Device.add_parameter`)
- `softlab/huo/process/common.py` (`AtomJob` hook storage and `body()` hook
  ordering/dry-run guards, `AtomJobSweeper._perform_sweep` dry-run
  termination, `Counter`/`Scanner`/`count`/`scan` signatures,
  `parse_getters`/`parse_setters_with_values`)
- `tests/test_tu_integration.py`, `tests/test_compatibility.py`,
  `tests/test_tu_measurements.py` (existing `before_get` wiring, count/scan
  and scheduler start/stop precedents)

All source reads were read-only; no production files were modified by this
review.

## Verdict and next steps

**APPROVED.** The design is architecturally sound, compatible with the
Release 1 baseline, complete against SIM-AC-01–08, appropriately minimal,
and directly implementable from the approved test plan. The three minor
items above (guide sentence on two-gate validation, pristine-copy wording,
test-plan revision tracking) may be addressed during implementation and the
SIM-TC-05i test-plan reclassification without blocking sw-tom's start.
Per the serial delivery sequence in `tasks.md`, sw-tom may proceed with
implementation of the approved design; sw-mike should apply the SIM-TC-05i
reclassification as test-plan revision 2.
