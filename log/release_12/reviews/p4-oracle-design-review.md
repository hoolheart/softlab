# Release 12 (M03 Phase 4) — Oracle / Rerun Design Architecture Review

## Review Information

- **Reviewer**: sw-jerry (Software Architect)
- **Date**: 2026-11-03 Asia/Shanghai
- **Branch**: `codex/m03-p4-acceptance`
- **Reviewed design**: `log/release_12/design/p4-oracle-design.md` at commit
  `9b6be84c174da9a8c9a5e23c1fe6d5425ae08902` (`9b6be84`).
- **Binding inputs reviewed**: `plan/milestone-03.md`; Release 12 PRD
  **M03-AC05/AC06**; the Phase 1 boundary decision and ordering addendum;
  characterization C1/C3/C5a/C5b; Phase 2 construction contract; Phase 3
  storage design §2.2, §3.5.1, §3.6 and §4; and the executed Phase 3 storage
  results.
- **Source/seams checked**: `examples/thermal_experiment.py`,
  `examples/signal_monitor.py`, `softlab/huo/process/bounded.py`,
  `softlab/shui/run_record.py`, `softlab/shui/run_group.py`,
  `softlab/shui/run_storage.py`, and
  `tests/run_record_inspect_bootstrap.py`.
- **Method**: static design/source review plus independent arithmetic
  recomputation from manually transcribed frozen model parameters. The oracle
  recomputation used only the Python standard library, did not import softlab
  or either example, and did not read a stored result. A separate signature
  inspection confirmed the actual client entry points. No implementation was
  changed, no acquisition/storage workflow was run, and no test suite was
  executed for this review.

## Verdict

**CHANGES REQUESTED.**

The numeric oracle freeze itself is sound: both thermal tables and the signal
oracle independently recompute exactly, the tolerance policy is honest and
non-masking, and the exclusions retain the required structural assertions.
The three explicitly flagged evidence gaps are appropriate Phase 4 execution
work rather than reasons to require execution evidence before this design
freeze.

Approval is blocked instead by the rerun/walkthrough contract. The frozen
thermal invocation is not callable as written; the signal procedure relies on
client-owned integration that does not yet exist without plainly freezing that
prerequisite and its required construction/gate steps; and the procedure runs
only one acquisition, so it does not demonstrate the required original-run
versus clean-session-rerun comparison. Recorded-configuration comparison and
genuinely fresh environment setup also need to be made explicit.

**Findings: 5 total — High: 3, Medium: 2.**

## Findings

### 1. High — The frozen thermal rerun call uses a nonexistent `delay` keyword

**Locations**: design §5 step 2 (lines 120–126);
`examples/thermal_experiment.py:840-843`.

The freeze specifies:

```python
execute(config, output_dir, delay=1|0)
```

The actual signature is:

```python
execute(config, output_dir, *, stop=None, after_row=None,
        adapter='coordinated')
```

There is no `delay` parameter. Following the frozen procedure therefore raises
`TypeError` before acquisition. The adapter-to-delay mapping already exists and
is the correct public seam: `adapter='coordinated'` selects delay 1/manifest
v1, and `adapter='direct'` selects delay 0/manifest v2.

**Required fix**: replace the frozen call with
`execute(config, output_dir, adapter='coordinated')` and
`execute(config, output_dir, adapter='direct')`, respectively. Keep delay and
source-tick values as expected outcomes, not as a second call-side input that
could diverge from the adapter.

### 2. High — The signal rerun is not an end-to-end procedure on the current client, and its Phase 4 prerequisite is not stated plainly

**Locations**: design §1 signal row, §5 steps 1–3, §8 gap 2;
`examples/signal_monitor.py:11-40`; `softlab/huo/process/bounded.py:25-46`;
Phase 3 design §2.1–§2.3; Phase 1 architecture assessment §Phase 4.

Today `signal_monitor.acquire(points=3)` returns only
`(CountResult, DataRecord, differences, reads)`. The module has no owned
persistence entry point, no saved output path, no source/software capture, and
no returned `DataGroup`. By contrast, the frozen sentence moves directly from
`acquire()` to `build_signal_payload` / `build_envelope` / `save_run_group` as
though the client already possessed the required stored-record workflow.

The existing library seams are sufficient building blocks, but the caller must
still supply or perform all of the following, none of which is frozen in the
procedure:

1. Check the actual acquisition boundary before constructing a completed-run
   envelope (`CountResult.overall_success`, or explicitly equivalent
   acquisition/terminal/drain/loop/cleanup checks); saving must not occur for a
   failed, cancelled, or incomplete acquisition.
2. Retain `str(result.run_id)` as `execution_id`, create a distinct persistent
   `group_id`, and construct the owned group with `build_run_group`.
3. Attach the acquired `DataRecord` through `DataGroup.add_record` before
   `save_run_group`, as required by Phase 3 §2.1–§2.2.
4. Provide the required `software` and signal-specific `source` facts and an
   explicit HDF5 target path. Thermal's private source helper cannot simply be
   assumed to be signal-owned integration.

A pre-implementation design may legitimately depend on Phase 4 client wiring.
The defect is that the freeze neither says plainly that this wiring is a
**Phase 4 implementation prerequisite** nor freezes the above call-site
boundary sufficiently for the rerun to be implemented without new design
choices. Section 8 gap 2 correctly defers executed confirmation of the stored
oracle, but that is not the same as stating the missing client capability and
construction dependency.

**Required fix**: label the signal steps as the target procedure enabled by
Phase 4 signal-example integration, not an existing end-to-end client
capability. Freeze the completed-acquisition gate, identity sources,
`build_run_group` plus record attachment, software/source ownership, target
path, and save-report/cleanup pass conditions. No backend repair or new shared
framework is needed.

### 3. High — The supposed rerun procedure performs only one acquisition and never compares an original recording with a clean-session rerun

**Locations**: design §4 original-file/rerun-file exclusion, §5 steps 2–5;
`plan/milestone-03.md` product outcome (lines 14–29), Phase 4 row (line 97),
and evidence-matrix item 5 (lines 124–129).

Section 5 creates one artifact in process A and inspects that same artifact in
process B. Process B performs no acquisition. There is no original recording
input, no second acquisition in a later clean session, and no comparison
between original and rerun samples. Comparing that single run to an independent
oracle is valuable and necessary, but it does not by itself execute the
milestone's “compare a clean-session rerun to the original samples” outcome.

The exclusion table already speaks of an “original-file” versus “rerun-file”
hash, which makes the omission especially visible: those two runs are never
created by the frozen steps. Process A being named “rerun execution” does not
establish an earlier session or an original artifact.

**Required fix**: freeze an actual two-session workflow: create/save the
original with the §1 inputs; independently rerun the same inputs in a separate
clean process/session; safely inspect the stored artifacts; compare each run
to the independent §2 oracle and compare their deterministic sample/timing
relations under §3, while applying §4 only to the named nondeterministic
fields. Retain per-run identity and original-byte assertions. If an oracle-only
single-run workflow is intentionally proposed instead, that is a milestone/PRD
scope change requiring explicit Product/Architect review, not a wording
substitution in this design.

### 4. Medium — The pass criteria do not explicitly compare recorded configuration to the frozen inputs

**Locations**: design §1, §3, §5 step 5 and pass criteria;
`plan/milestone-03.md` Phase 4 row (“compare recorded configuration and
deterministic sample relationships”).

Step 1 declares the inputs, but step 5 requires only §2 sample/timing/
structural relations and §4 retained assertions. Neither §2 nor §3 requires
exact equality of the stored thermal `payload.configuration` to all seven
frozen `ThermalConfig` values, nor a direct stored-signal `points`/read-policy
comparison to the frozen signal inputs. Codec decoding establishes internal
validity and some frozen literals; it does not establish that an arbitrary
valid recorded thermal configuration equals the configuration selected for
this reference scenario.

Sample comparison is not a substitute for this declaration check. The
acceptance workflow must show that the inputs used to derive the oracle are the
inputs actually recorded for the run.

**Required fix**: add explicit stored-record assertions for the complete
frozen input declarations: thermal configuration and adapter (and the
corresponding legacy configuration/manifest relations where applicable), and
signal `points`, offsets, non-consumptive read semantics, scheduler ownership,
and source-versus-derived policy. Keep environment/source observation values
under the existing exclusions; do not exclude deterministic configuration.

### 5. Medium — AC06 requirements allow activation of an existing environment instead of a genuinely fresh installation

**Locations**: design §6 item 1 and §8 gap 1; PRD **M03-AC06**;
`plan/milestone-03.md` evidence-matrix item 5; the established M02 fresh-
environment gate in `log/release_11/test/test-plan.md` TP-W04-04.

The walkthrough must record “environment activation” and subsequent checks,
but it never requires creation of a new isolated environment, installation of
the current checkout/declared dependencies, or evidence that preexisting site
packages were not reused. Activating `.venv` and running the listed commands
could therefore satisfy the literal checklist without supplying a fresh-
environment walkthrough.

The demands/observations distinction is otherwise truthful: §6 is clearly a
requirement list, historical versions are labelled as historical, and §8.1
states that no walkthrough has yet run.

**Required fix**: require and record the actual environment-creation and
installation commands and outcomes, the selected interpreter path, isolation
from existing site packages, and installation of the current checkout with its
declared dependencies. A network/package-index failure or unavailable
interpreter must remain a recorded blocker/limitation, not become a pass based
on the existing project environment. Retain the existing command, version,
warning, and limitation records.

## Independent oracle recomputation

### Thermal model

I manually transcribed only the documented inputs and recurrence:

```text
T0 = 20, Tambient = 20, C = 10, G = 1, dt = 1, P = 10, points = 8
Tnext = T + dt * (P - G * (T - Tambient)) / C
```

For these inputs the exact decimal recurrence simplifies to
`Tnext = 0.9*T + 3`. I evaluated the documented operation sequence once with
Python binary64 floats and independently with 50-digit `Decimal` arithmetic.
No implementation function or current output supplied an expected sample.

The binary64 coordinated/pre-step sequence was:

```text
[20.0, 21.0, 21.9, 22.709999999999997, 23.438999999999997,
 24.095099999999995, 24.685589999999994, 25.217030999999995]
```

The binary64 direct/post-step sequence was:

```text
[21.0, 21.9, 22.709999999999997, 23.438999999999997,
 24.095099999999995, 24.685589999999994, 25.217030999999995,
 25.695327899999995]
```

Both lists compare **exactly equal, element for element and bit-for-bit**, to
§2.1 and §2.2. Maximum absolute difference from the frozen lists is `0.0`.
The exact decimal sequences are:

```text
pre-step:  [20, 21, 21.9, 22.71, 23.439, 24.0951, 24.68559, 25.217031]
post-step: [21, 21.9, 22.71, 23.439, 24.0951, 24.68559, 25.217031,
            25.6953279]
```

For either adapter, the largest `abs(Decimal.from_float(binary64) - exact)` is
approximately `5.851461537531577e-15 degC`, over four orders of magnitude
below the frozen `1e-10` absolute tolerance. The smallest adjacent Euler-step
change in the frozen eight-step scenario is approximately
`0.47829690000000014 degC`, confirming the document's “at least 0.47”
non-masking rationale.

The timing oracles also follow directly: ticks are 1 through 8, source ticks
are `tick-1` for coordinated and `tick` for direct, simulation times are
`tick*1.0`, and commanded power is exactly 10.0 at every row.

### Signal model

Using ticks `[1, 2, 3]`, `signal_offset=5`, and `reference_offset=2`, direct
integer evaluation gives:

```text
signal    = [tick + 5 for tick in ticks] = [6, 7, 8]
reference = [tick + 2 for tick in ticks] = [3, 4, 5]
difference = signal - reference                  = [3, 3, 3]
```

These exactly match §2.3. The expected read order is signal then reference at
each tick 1, 2, 3. The oracle is derived from declared inputs/read semantics,
not from a stored object or a rerun's output.

## Tolerance and exclusion assessment

- **Tolerances: accepted.** Exact equality is justified for integer columns
  and for the frozen exactly representable `dt=1.0`/`power=10.0` relations.
  `math.isclose(..., rel_tol=0.0, abs_tol=1e-10)` for temperature is consistent
  with characterized evidence and is far too small to mask a wrong recurrence,
  adapter delay, row shift, or channel/column substitution. Nonfinite actual
  temperatures do not compare close to the finite oracle.
- **Exclusions: accepted as a field policy.** Wall-clock values, fresh UUID
  values, environment/source observation values, plot bytes, and cross-run HDF5
  byte hashes are legitimately excluded from value equality. Timestamp
  presence/unit/row-count/finiteness, execution-versus-group UUID distinctness,
  group identity relations, source availability structure, environment
  recording, artifact existence, and within-run original-byte preservation are
  retained. No deterministic sample or required configuration field needs to
  be added to the exclusion table. Finding 3 concerns the missing two-run
  procedure that would apply these exclusions, not the exclusion choices
  themselves.
- **Ordering: accepted.** Iterating envelope declarations, retrieving stored
  columns by name, and comparing arrays by row index correctly handles signal's
  non-lexicographic declaration without a backend repair.

## Evidence-gap disposition

| Design gap | Review disposition |
| --- | --- |
| **1 — walkthrough not yet executed** | **Non-blocking at design freeze.** It is plainly a Phase 4 deliverable. Finding 5 must still be corrected so the eventual execution is genuinely fresh and adequately recorded. |
| **2 — exact signal values not yet executed through the integrated stored path** | **Non-blocking as execution debt; no executed pass may be claimed yet.** Phase 3 A2.8/A3.1 did execute generic signal-profile save/inspection and fresh-process inspection with different synthetic sample values, so the storage seam has evidence; neither run closes the exact `acquire()` integration oracle. Finding 2 requires the remaining client wiring dependency to be explicit. Any observed deviation must reopen review as the design states. |
| **3 — Python 3.9 float determinism not yet re-executed** | **Non-blocking.** The oracle comparison is tolerance-based, not repr-based; the frozen binary64 values independently recompute on the review interpreter, and `1e-10` is far above the recomputed rounding error while remaining non-masking. Actual supported-interpreter evidence remains a Phase 4/CI observation, not something to invent now. |

None of these three gaps requires Phase 4 execution evidence before the design
can be approved. Findings 1–5 do require document correction and re-review
before the freeze becomes binding implementation/test input.

## Gate disposition

Do not use `9b6be84` as the approved Phase 4 rerun/test-plan input. Correct the
five findings, then re-review the affected design diff. This review approves
the numeric values, tolerances, ordering rule, and exclusion policy, but it is
not test approval, implementation authorization beyond the existing Phase 4
scope, test execution, fresh-environment evidence, or M03 acceptance.
