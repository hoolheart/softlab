# R3-004 baseline — current state vs needed evidence

Tester: sw-mike | Date: 2026-10-16
Branch: `codex/r3-004-delivery` @ `92e128d` ("docs(log): mark r3-003 done and
open r3-004"), created from dev `2991a63` ("chore(log): integrate r3-003
coordination into dev"); working tree clean except this task's new test
records.
Plan: [r3-004-test-plan.md](r3-004-test-plan.md) (Revision 1)

## 1. Environment and verification baseline (actually run, this head)

| Item | Value | Evidence |
| --- | --- | --- |
| Python | 3.13.15 (`.venv` interpreter invoked directly) | `sys.version` via venv |
| softlab version | `0.3.0` | `.venv/bin/python -c "import softlab; print(softlab.__version__)"` → `0.3.0`, exit 0 |
| Platform | macOS (darwin) | env |
| Full suite | **Ran 207 tests in 1.125s — OK**, exit 0 | `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'` (re-run; matches r3-003 results §1: 207 = 165 pre-existing + 41 R3-003 + 1 test-only 04e extension) |
| Warning gate | **Ran 207 tests in 1.116s — OK**, exit 0 under `-W error::Warning` | zero emitted warnings (informational backend "Connected to …" prints are not warnings) |
| compileall | exit 0, no output | `.venv/bin/python -m compileall -q softlab` |
| Import smoke | `0.3.0`, exit 0 | see above |

All commands run from the repository root on 2026-10-16 against head
`92e128d`. Verified items above are re-verifiable by the recorded commands.

## 2. Worked-example characterization (R3-AC-08 clause 2)

**Capability exists; the user-facing worked example does not.**

- The connected-simulation capability is fully delivered and tested:
  `Coordinator`, `Endpoint`, `MockControl`, `MockMeasurement` exported from
  `softlab.tu.simulation` (softlab/tu/simulation/__init__.py;
  identity pinned in r3-003-test-results.md §4); 42 test methods in
  `tests/test_tu_coordination.py` pass in-suite, including per-tick golden
  traces (R3-TC-01e) and determinism under reversed registration order
  (R3-TC-01d).
- **There is NO executable user-facing connected example.** Characterization:
  - No `examples/` directory exists in the repository (directory listing).
  - `docs/user-guide/simulation.md` ends at line 401; its only executed
    example is §11 "Executed example" (lines 327-401), which is
    **standalone-only** (`SimulatedObject.evolve_once`, no station,
    coordinator, mock endpoints, connections or `sim_dt`).
  - `grep -nic "coordinator|connected|sim_dt|mock control|mock measurement"`
    over the guide returns **2** — both are the R3-001 deferral paragraph
    (simulation.md:257-263: "A coordinated multi-participant simulation
    (planned Release 3 work)… intentionally not stated here"). Zero mentions
    of `Coordinator`, `MockControl`, `MockMeasurement`, `sim_dt`, `Station`,
    builders or connections as usable guidance.
  - No guide code block exercises the connected API; no recorded connected
    output exists anywhere in `docs/`.
- **Guide byte-reproduction convention exists only for the standalone
  example:** `tests/test_sim_user_guide.py:35-100` extracts the §11 code
  block and recorded output block, executes, and asserts byte-equality plus
  repeat-run identity (SIM-TC-08b). The same convention **cannot** cover a
  connected example yet — there is no connected section to extract.
- **Deterministic golden-trace source available:** the R3-003 design's
  "Determinism replay values" (design/r3-003-detailed-design.md:1097-1131)
  independently derives the canonical trace (chain A→B→C, x = 1.0/10.0/100.0;
  transition 0→1 delivers B.u=1.0, C.u=**10.0** (not 11.0); states after
  publication 1.0/11.0/110.0; feedback companion values; measurement tick-zero
  −1.0 → 100.0 → 110.0; `sim_time == float(tick) * sim_dt`). This derivation
  is the ready-made expected-value source for the worked example's golden
  trace; it must not be replaced by copying implementation output.
- The R3-003 design explicitly reserved this: worked example and guide are
  "R3-004 scope" (design/r3-003-detailed-design.md:59), mock-to-Device/Parameter
  wiring "is deferred to the R3-004 example" (line 1291-1295).

## 3. Guide status (R3-AC-08 clause 2 / R3-TC-09m1/09m2 baseline)

- §9 "Bridging to Device and Parameter (mock devices)" (simulation.md:231-311)
  carries the **R3-001-corrected** standalone bridge wording: authoritative
  input-store readback (`before_get=lambda stored: sim.get_input('u')`,
  line 245), the set/get order paragraph (240-255), and the standalone-vs-
  connected deferral paragraph (257-263). Verified in r3-001-test-results.md
  §"R3-TC-07t" — PASS with citations; the connected half is deliberately not
  stated.
- §11 (327-401) standalone executed example, byte-pinned by SIM-TC-08b
  (passing in the 207-suite).
- Sections 1-8, 10 cover standalone construction/evolve/observation/reset/
  value-contract/limitations; **no section covers coordinated simulation**.
- Guide sections that must exist for clause 2 (shared clock, delayed edges/
  feedback, mock interaction, coordinated reset) are all **absent** — see
  GAP-1/2.

## 4. Architecture status (R3-AC-08 clause 4 / R3-TC-09m3 baseline)

`arch.md` (462 lines) describes **Release 1 + Release 2 only**:

- Header (arch.md:5-8) pins "the implemented state at the Release 2
  integration point (2026-10-05, `dev` tip `470e2c7`… full suite 135 tests
  green, CI run 37298623628…)" — stale for Release 3.
- `tu` row (arch.md:27) ends at "Release 2 adds the `simulation` subpackage
  with `SimulatedObject`" — no R3-001 corrections, R3-002 object
  membership/builders, R3-003 coordination.
- Simulation section (arch.md:210-330) documents `SimulatedObject`
  standalone only. **Zero matches** in the whole file for
  `Coordinator|MockControl|MockMeasurement|sim_dt` (grep -c → 0) and for
  `build_object|add_object|ObjectBuilder` (no matches) — the delivered
  R3-002/R3-003 capability is entirely absent from the architecture record.
- Bridge paragraph (arch.md:302-321) documents the set wiring and
  `observe_outputs` observation wiring but **not** the R3-001-corrected
  control-readback semantics (authoritative input store; guide §9 lines
  240-255 has them; the arch record does not).
- Reset wording (arch.md:277-283) already carries the R3-001-corrected
  "no equivalence to fresh construction is claimed" text — the R3-001 arch
  edit landed (design sites C-5/C-6); consistent.
- Planned-vs-implemented markers: exactly one **[PLANNED]** block
  (arch.md:452-462) covering non-R3 future work (services, schedulers,
  solvers, thread safety…) — accurately describes *undelivered* capability;
  no marker falsely describes delivered R3 work, but delivered R3 work is
  wholly unmentioned (the inverse gap: implemented capability not described).
- Verified: arch.md has not been touched on dev since `d52217c` ("docs(arch):
  record release 2 simulation implementation") plus `670523b` — i.e. no
  Release 3 architecture update exists anywhere.

## 5. Release-wide evidence inventory (R3-AC-01–08)

| Criterion | Per-task evidence (exists) | Release-level evidence (missing at baseline) |
| --- | --- | --- |
| R3-AC-01 | r3-003 results 01a–01e, PASS | re-run inside 09d at release head |
| R3-AC-02 | r3-003 results 02a–02f + doc citations §7 | re-run inside 09d |
| R3-AC-03 | r3-003 results 03a–03f + §6 items 4-5 | re-run inside 09d |
| R3-AC-04 | r3-002 results (membership/builders 17 methods); r3-003 04a–04f + 08a/08c | re-run inside 09d |
| R3-AC-05 | r3-003 results 05a–05g | re-run inside 09d |
| R3-AC-06 | r3-003 results 06a–06e | re-run inside 09d |
| R3-AC-07 | r3-001 results 07a–07t + CHK-08-1 | re-run inside 09d; guide §9 re-check (09m2) |
| R3-AC-08 | r3-001 08a–08d; r3-003 08a–08f + CHK-03-1 | **connected example (09a/09b), guide additions (09m1/09m2), final arch (09m3/09h), candidate CI (09m4), matrix (09m5) — all missing** |

CI evidence status: dev CI run 38820136267 SUCCESS at `2991a63` (r3-003
integration, sprint-board.md:123) certifies dev, **not** this task branch.
No CI run exists yet for `codex/r3-004-delivery` — candidate CI (R3-TC-09m4)
is pending by definition.

## 6. Gap inventory (cited)

| Gap | Description | Evidence | Needed for |
| --- | --- | --- | --- |
| GAP-1 | No connected worked example artifact (executable program + recorded output) | simulation.md ends at :401 with standalone-only §11; no `examples/` dir; guide grep for connected API = 2 deferral lines only (simulation.md:257-263) | R3-AC-08 clause 2; R3-TC-09a |
| GAP-2 | No guide connected section; byte-reproduction convention covers §11 only | test_sim_user_guide.py:39 splits on `'## 11. Executed example'`; no connected code/output blocks exist in guide | R3-AC-08 clause 2; R3-TC-09b/09m1 |
| GAP-3 | arch.md has no Release 3 content at all | arch.md:5-8 (Release-2-pinned header), :27 (tu row ends at R2), :210-330 (standalone-only simulation section); 0 grep matches for Coordinator/MockControl/sim_dt/build_object | R3-AC-08 clause 4; R3-TC-09m3 |
| GAP-4 | arch.md omits the R3-001-corrected bridge readback semantics | arch.md:302-321 documents set/observation wiring only; corrected readback lives in guide simulation.md:240-255 and docstrings (r3-001 results §"R3-TC-07t") | R3-TC-09m3(b) |
| GAP-5 | No R3-004 candidate CI; no R3-004 test results; scope-gate ratification (plan §2) not yet recorded | branch `92e128d` has no CI run; sprint-board.md:131-133 states the carry-over is "NOT pre-authorized" | R3-TC-09m4/09m6 |
| GAP-6 | Release-wide acceptance matrix not compiled | per-AC tables exist only inside each task's results doc; no cross-release matrix | R3-TC-09m5 |

## 7. Baseline verdict for planning

The tree at `92e128d` is green (207 tests OK, zero warnings, compile/import
clean) and the connected capability is delivered and unit-tested, but every
R3-004-specific evidence artifact is **absent** (GAP-1 through GAP-6). No
production defect is asserted by this baseline; the gaps are missing
deliverable artifacts, which is exactly what R3-004 must produce. Scope-gate
recommendation (defer the additive snapshot/describe simulation node beyond
Release 3) and its justification are recorded in the test plan §2 and require
sw-prod/sw-camille ratification (R3-TC-09m6).
