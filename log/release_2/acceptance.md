# Acceptance Document — Release 2 (SIM-001 simulation foundation)

## Acceptance Information
- **Reviewer**: sw-camille (Product Owner)
- **Date**: 2026-10-05
- **Release**: 2
- **Task**: SIM-001 — deterministic simulated-object foundation
- **Branch reviewed**: `dev` (integrated at merge `9408c79`; arch.md
  actual-implementation update `d52217c`; release principle inspection
  `470e2c7`)
- **Requirements baseline**: `log/release_2/prd.md` (SIM-AC-01 through
  SIM-AC-08, all Must)

## Review scope and evidence examined

I read the actual artifacts directly; no report was taken at face value
without corroborating source.

| Evidence | Reference | What I verified first-hand |
| --- | --- | --- |
| Requirements | `log/release_2/prd.md` | All eight acceptance criteria, scope boundary (production confined to `softlab/tu/`), exclusion list, gate applicability |
| Implementation | `softlab/tu/simulation/object.py`, `softlab/tu/simulation/__init__.py`, `softlab/tu/__init__.py` | Full read of the 794-line implementation: declared input/state/output namespaces, closed value contract, copy-in/copy-out at every boundary, evolve-once-only semantics, build-then-swap commit/reset, identical-exception propagation, no clock/wall-clock access, NumPy + stdlib only, single public export |
| Test results | `log/release_2/test/sim-001-test-results.md` | 135 tests OK, exit 0, verbatim outputs; `-W error::Warning` gate green; per-case table mapping all 41 case IDs to executed tests; per-AC coverage summary; no Release 1 debt warning fired |
| Baseline | `log/release_2/test/sim-001-baseline.md` | Fresh 99-test baseline recorded BEFORE production changes (`86cddb4`), predating first production commit `4eab94d`; Release 1 debt dispositions explicit |
| Test plan | `log/release_2/test/sim-001-test-plan.md` | Revision 2, approved; developer review issues closed at `f1bbcd2` |
| User guide | `docs/user-guide/simulation.md` | Full read: construction (§2), evolve contract incl. pending inputs and memoryless idiom (§3), observation (§4), reset + failure atomicity (§5), value contract + ownership + error table (§6), deterministic-callback contract (§7), optional time context as model input/state (§8), mock-device vs simulated-object distinction (§1), Device/Parameter bridge with hook-binding constraint (§9), known limitations (§10), executed deterministic example with verbatim output (§11) |
| Integration evidence | `tests/test_tu_simulation_integration.py` | Device/Parameter bridge over a shared object; `huo` `count` with `hook_before_get` and `scan` with `hook_after_set` (never `hook_before_set`); exactly one evolve per point; delays default to zero — no real sleep interpreted as a simulation step; manual-replay cross-checks; scheduler started/stopped per test; `huo` reached via public names only |
| Architecture record | `arch.md` (updated at `d52217c`) | Simulation section describes the actual implemented contract only; exclusions restated as remaining excluded; `[PLANNED]` items clearly separated from implemented scope; Release 1 debt still tracked, not silently repaired |
| Test suite sources | `tests/test_tu_simulation.py`, `tests/test_tu_simulation_integration.py`, `tests/test_sim_user_guide.py` | Present and referenced by the results record |
| Code review | `log/release_2/reviews/sim-001-code-review.md` | APPROVED at `dfa535c`, zero issues; CHK-06-1 (zero `huo`/`jin`/`shui`/`mu` production edits) verified by empty diff; executed guide example reproduced byte-identically by the reviewer |
| Design + design review | `log/release_2/design/sim-001-detailed-design.md`, `log/release_2/reviews/sim-001-design-review.md` | Design approved at `3d6cb13`; three non-blocking minors closed via `917cfd7`/`0aa3908` |
| Principle inspections | `log/release_2/principle_compliance_report.md` | Task-completion inspection PASS (`cd90ff6`); release-completion inspection PASS (`470e2c7`) with findings re-verified via git/CI commands |
| Sprint board | `log/release_2/sprint-board.md` | SIM-001 Done at merge `9408c79`; release acceptance and `main` promotion correctly recorded as pending |
| CI | Run `37298623628` (per principle report, verified by inspector via `gh run view`) | `status=completed`, `conclusion=success`, both matrix jobs (Python 3.9, 3.13) success, before the `dev` merge |

## Test report verification
- [x] All unit/integration tests pass: 135 tests, OK, exit 0 (baseline 99 + 36 new SIM-001 tests), recorded verbatim in `sim-001-test-results.md`
- [x] Warning gate green: `python -W error::Warning -m unittest discover` → OK, zero warnings; no OBS-004/OBS-005/OBS-006 or DEFECT-2 warning fired; nothing silently waived
- [x] Compile check: `python -m compileall -q softlab` exit 0
- [x] Import smoke: `import softlab` → `0.3.0`; public class identity `softlab.tu.simulation.SimulatedObject is softlab.tu.simulation.object.SimulatedObject` holds
- [x] CI: run 37298623628 success on Python 3.9 + 3.13 before integration into `dev`
- [x] Integration tests cover the PRD user flows: bridge control/observation, shared object, huo count/scan with explicit stepping

## Per-criterion verdict (SIM-AC-01 – SIM-AC-08)

| ID | Requirement (user need) | Verdict | Evidence verified first-hand |
| --- | --- | --- | --- |
| SIM-AC-01 | Distinct input/state/output roles | **MET** | `object.py` construction enforces three declared namespaces with cross-role name uniqueness; closed value contract (exact `int`/exact `float`/numeric `np.ndarray`) with documented `TypeError`/`ValueError` for invalid declarations, `KeyError` for unknown variables, dtype/shape pinning with named mismatch errors. Guide §2/§6 document the contract; scalar and ndarray examples both present in guide §11. Tests SIM-TC-01a–01e pass. |
| SIM-AC-02 | Dynamic and memoryless evaluation | **MET** | `evolve_once()` implements `x_next = evolve(u, x_previous)` with exactly one `evolve` invocation per call (callback-counter pins in SIM-TC-02a/02b) and outputs derived from resulting state (02e); memoryless idiom documented (guide §3 holding-state pattern) and tested (02c); `dt` represented as ordinary model input in the executed example — no dedicated clock API exists (02d). |
| SIM-AC-03 | Read without unintended evolution | **MET** | Code read confirms `name`/`input_names`/`state_names`/`output_names`/`get_input`/`get_state` invoke no callbacks and mutate nothing; `observe_outputs` touches no committed store; `set_input` validates and stores without evolving. Tests 03a–03d pin this, including no hardware/scheduler access on read. |
| SIM-AC-04 | Repeatable preparation | **MET** | `reset()` rebuilds the complete initial condition (pristine literal copies + fresh factory invocations) including time-valued inputs/states; replay of the same deterministic sequence reproduces identical observations (04b, plus scan-run determinism in 06e); independently constructed objects share no mutable container (04c; module holds no module-level mutable state — confirmed by code read); reset succeeds after a failed evolution (04d). |
| SIM-AC-05 | Predictable failures and ownership | **MET** | Original callback exception objects propagate with identity preserved (`assertIs`-pinned, `__cause__` is None); build-then-swap atomicity verified by code read for `set_input`, `evolve_once` and `reset` (failed operations leave committed state bit-identical; object remains fully usable after failed `reset`). Copy-in/copy-out at every boundary prevents caller-supplied, returned and callback-argument aliasing (05c–05e). Unsupported value categories are rejected with documented, named errors (05f). External callback side effects explicitly placed outside rollback guarantees (05g; guide §7). Tests 05a–05i pass. |
| SIM-AC-06 | Existing experiment interfaces | **MET** | `tests/test_tu_simulation_integration.py` uses plain `Device`/`Parameter` with `before_set`/`before_get` closures — no adapter class, no `Process` subclass; one object shared between a control device and an observation device (06c). `huo` `count` (hook_before_get) and `scan` (hook_after_set) integrations evolve exactly once per point (5/5 and 4/4) with recorded values equal to manual replay; delays default to zero, so no real sleep is interpreted as a simulation step; advancement location is explicitly defined (never `hook_before_set`). CHK-06-1: zero `huo`/`jin`/`shui`/`mu` production edits (empty diff, independently re-verified); CHK-06-2: `huo` imported via public names only. |
| SIM-AC-07 | Compatibility and imports | **MET** | Fresh pre-change baseline (99 tests, `86cddb4`) re-run green after implementation: full suite 135 tests OK including all Release 1 characterization/regression assertions; warning gate green with no debt warning firing; new public imports verified without circular imports (class-identity assertion); `pyproject.toml`/`setup.py` untouched (zero new required dependencies). CI green on Python 3.9 and 3.13. |
| SIM-AC-08 | Usable, honestly bounded delivery | **MET** | English docstrings on the constructor, every public method/property and private helpers (Args/Returns/Errors/Side-effects style, verified in code read). User guide covers every required topic: construction, `evolve`, observation, reset, errors, ownership, optional time context as model input/state, and the mock-device/object distinction — with an executed deterministic example whose verbatim output the code reviewer reproduced byte-for-byte. Guide §10 honestly lists known limitations and out-of-scope items. `arch.md` was updated to the actual implementation only after delivery (`d52217c`), with `[PLANNED]` items clearly separated. Full unittest/compile/import/warning/CI evidence was recorded before integration. |

## Scope-discipline finding

**PASS — no unauthorized scope expansion.**

- Production changes are confined to `softlab/tu/` (new
  `softlab/tu/simulation/` package plus one export line in
  `softlab/tu/__init__.py`); all other changes are in `tests/`,
  `docs/user-guide/`, `arch.md` and `log/release_2/`, exactly as the PRD
  permits.
- `git diff` over `softlab/huo`, `softlab/jin`, `softlab/shui`,
  `softlab/mu` is empty (verified by code reviewer and both principle
  inspections).
- PRD exclusions are respected and explicitly restated in guide §10 and
  arch.md: no solvers, no dedicated clock/automatic wall-clock
  advancement, no background workers, no thread safety, no multi-object
  scheduling, no stochastic/noise framework, no calibration/fitting, no
  model persistence, no hardware access, no UI, no direct input→output
  feedthrough.
- No new dependencies; `pyproject.toml`/`setup.py` untouched; no
  Python-support change.
- Release 1 debt (OBS-004/005/006, DEFECT-2) remains tracked and was not
  silently repaired or waived.

## PRD gate-list verification

| Gate (required by PRD) | Status | Evidence |
| --- | --- | --- |
| TDD test-plan review (developer review) | DONE | Plan `5caabc7`; review `313ae9f`; issues 1–4 closed by reviewer at `f1bbcd2` |
| Detailed design + architect review | DONE | Design `7ccac0d`; review APPROVED `3d6cb13`; minors closed `917cfd7`/`0aa3908` |
| Independent code review | DONE | `dfa535c`, APPROVED, zero issues (sw-celeste) |
| Tester execution | DONE | 135 tests OK, zero-warning gate green (`sim-001-test-results.md`, sw-mike) |
| Task principle inspection | DONE | PASS `cd90ff6` |
| CI before `dev` integration | DONE | Run 37298623628 success (3.9 + 3.13), before merge `9408c79` |
| arch.md actual-implementation update | DONE | `d52217c` |
| Release-completion principle inspection | DONE | PASS `470e2c7` |
| UI/Figma and real-hardware gates | N/A | PRD-declared not applicable; no UI change, no hardware access |
| Release acceptance | THIS DOCUMENT | — |

## User experience assessment
- [x] The guide reads as a user document, not an implementation dump: it opens with the conceptual distinction a user needs (object vs. mock device), and each contract is stated with its rationale
- [x] Error behavior is documented in a single error table with concrete exception types and messages that name the offending variable — a user is never left guessing
- [x] The two-gate validation asymmetry (parameter validator vs. object specification, sim-spec authoritative) is called out explicitly — exactly the kind of subtle interaction that would otherwise frustrate users
- [x] The hook-binding constraint (never `hook_before_set`) prevents a real off-by-one pitfall a user would otherwise hit silently
- [x] The executed example is honest: verbatim recorded output, byte-for-byte reproducible, covering both scalar and ndarray models, evolution, observation and reset
- [x] Limitations are stated plainly (strict scalar pinning, fixed ndarray shape/dtype, all-outputs observation) rather than discovered by the user

## Issues found

### Issue 1: Stale directory reference in arch.md
- **Severity**: Minor (non-blocking)
- **Related Requirement**: SIM-AC-08
- **Description**: `arch.md` (test-suite paragraph, ~line 415) cites evidence in `log/release_2/tests/`; the actual directory is `log/release_2/test/` (singular). All evidence files are present and correctly referenced elsewhere.
- **Expected Behavior**: Path reference matches the real directory.
- **Actual Behavior**: One-character path mismatch; content itself is accurate.
- **User Impact**: Negligible — a reader following the link pattern finds the directory immediately adjacent.
- **Recommendation**: Correct in the next docs commit; does not gate this acceptance.

## Acceptance Conclusion

### Pass Conditions Verification
| Condition | Status |
| --- | --- |
| ALL acceptance criteria (SIM-AC-01–08, all Must) met | YES |
| ALL tests passing (135, zero warnings, warning-gate green) | YES |
| CI green on both matrix versions before integration | YES |
| Documentation complete, accurate and user-facing | YES |
| Architecture record reflects actual implementation only | YES |
| Scope discipline: production confined to `softlab/tu/`, exclusions respected | YES |
| User experience acceptable | YES |

### Overall Verdict

**Status: ✅ ACCEPTED (PASS)**

**Summary**: Release 2 delivers exactly what the PRD promised and nothing
more. Every Must criterion is met with evidence I verified first-hand
against the code, tests, guide and architecture record — not merely
reported. The simulation foundation is genuinely usable: the contract is
explicit, failures are predictable and honestly documented, the bridge to
existing `Device`/`Parameter` and `huo` workflows requires no production
changes outside `tu`, and the excluded scope stayed excluded. The single
finding is a Minor documentation path typo that does not affect the
delivered behavior or the user's ability to find the evidence. Release 2
is accepted; `main` promotion is unblocked from the Product Owner side.

**Signed**: sw-camille, Product Owner

## Reconciliation addendum (2026-10-06, R3-001)

The CI references above cite run 37298623628 as "before the `dev`
merge". Verified via `gh run view`: run 37298623628 ran on branch
`dev` at head `9408c79` — i.e. *after* the merge. The pre-merge
evidence is run **37298431248** (branch
`codex/sim-001-simulation-foundation`, head `cd90ff6`, both matrix
jobs success, 2026-10-05T10:43:54Z, before the dev push of `9408c79`
at 10:45:40Z). The substance — green CI on Python 3.9 + 3.13 before
integration — stands with the corrected run reference; both runs are
green. This addendum corrects the reference only; the verdict and its
evidence are otherwise unchanged, and no `main` promotion is recorded
or implied.
