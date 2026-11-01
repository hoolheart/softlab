# Release 12 (M03 Phase 3) — Storage Design Architecture Review

## Review Information

- **Reviewer**: sw-jerry (Software Architect)
- **Date**: 2026-11-01 Asia/Shanghai
- **Branch**: `codex/m03-p3-storage`
- **Reviewed design**: `log/release_12/design/p3-storage-design.md` at commit
  `ae83c2a`.
- **Binding inputs**:
  - `log/release_12/design/p1-boundary-decision.md`, especially §1.1, §2,
    and the D1/D2/F-A/F-B/F-C/F-D dispositions in §4;
  - `log/release_12/test/storage-test-plan.md` at `30f48a9`, with developer
    approval recorded at `b31d687`, including A1–A9 and P3P-01–P3P-07;
  - `log/release_12/prd.md`, Phase 3 and **M03-AC03/M03-AC04**;
  - `plan/milestone-03.md`, Phase 3 and evidence-matrix items **3** and **4**.
- **Source checked**: `softlab/shui/data/hdf5.py`, `data/base.py`,
  `data/backend.py`, `softlab/shui/backend.py`, `run_group.py`,
  `run_record.py`, `softlab/huo/process/bounded.py`, package initializers,
  `examples/thermal_experiment.py`, `examples/signal_monitor.py`, and the
  existing thermal test/bootstrap call sites.
- **Method**: static architecture, contract, source-fidelity, fault-seam, and
  testability review. No implementation or design files were changed, and no
  runtime tests or new probes were executed for this review. The design's
  reported probes were assessed as evidence, not rerun.

## Verdict

**CHANGES REQUESTED.**

The design selects the right narrow storage boundary and is largely faithful
to the characterized backend: HDF5-only owned writes, disposable-copy
preflight and single-load hydration, explicit legacy absence, additive thermal
wiring, acquisition-snapshot-only outcomes, and no atomicity or failed-run
persistence claims. The two-entry-point module, lack of new dependencies, and
nine-name package export delta are proportionate.

Approval is nevertheless blocked by a frozen disconnect-fault mechanism that
cannot execute as described, an unapproved relaxation of the mandatory
Python 3.9 evidence gate, incomplete report/diagnostic contracts, and an
on-disk ordering interpretation that changes the currently approved contract.
The fresh-process protocol and cleanup literals also need correction before
tests can be written without guessing.

**Findings: 6 total — High: 4, Medium: 2.**

## Findings

### 1. High — A4.3's double-disconnect mechanism is disabled by the design's own cleanup guard

**Locations**: design §2.2 steps 5–6, §6.1, §6.2 A4.3, §6.3;
`softlab/shui/backend.py:121-125`; `softlab/shui/data/hdf5.py:43-47`;
approved plan A4.3 and universal assertion (c).

The frozen injection asks an external actor to call `backend.disconnect()`
between the primary operation and owned cleanup, after which the owned
cleanup supposedly performs a second disconnect and observes the real
`None.close()` failure. That sequence cannot occur under the frozen entry
point:

1. A successful first disconnect sets `self.file = None` in
   `HDF5DataBackend.disconnect_impl()` and sets the status to `NotConnected`
   in `DatabaseBackend.disconnect()`.
2. The design's cleanup runs only when `backend.file is not None or
   backend.connected`. Both are then false, so the second disconnect is
   skipped. No error delta is generated and no cleanup-only failure is
   reported.
3. Moving the external disconnect earlier does not fix the row: disconnecting
   before the primary status check makes `just_succeeded` false and converts
   the outcome into a primary failure, not the required successful primary
   result with independent cleanup failure.

Consequently, the quoted 3.13 double-disconnect observation is not evidence
that the frozen `save_run_group`/`inspect_run_group` path reaches that stage.
In addition, plan A4.3 explicitly describes delivery of the primary
**inspection outcome**, while design §6.3 pins only a save-side complete
artifact; the intended inspection/scratch-cleanup coverage is not frozen.

**Required fix**: freeze an exact real-seam injection point that occurs after
primary success has been established while leaving owned cleanup able to
execute, and define whether A4.3 exercises save, inspection, or both. Pin the
corresponding artifact and scratch outcomes. Empirically verify the revised
mechanism at the named stage on Python 3.9 and 3.13 before design freeze. If
no such seam verifies, record A4.3 as **BLOCKED** with that evidence as the
approved plan requires; do not substitute a mock that bypasses the real path.

### 2. High — Python 3.13-only probes do not satisfy P3P-05's pre-freeze evidence gate

**Locations**: design §6.2 and §10 A4/A8; approved plan P3P-05 (lines
123–127), A4 failure-stage mandate (lines 388–405), and placeholder table.

The approved plan requires every frozen A4 mechanism to be empirically
verified at its named stage **on both Python 3.9 and Python 3.13 before
design freeze**. The design instead verifies only on 3.13.15 and moves the
3.9 obligation to candidate-commit CI before integration. That is a material
change to the gate, not merely an environment limitation:

- detailed-design approval and subsequent test authoring would occur with
  unverified fault expectations;
- candidate CI runs after implementation and test expectations have already
  been frozen, too late to prevent a wrong-stage or vacuous seam from being
  built into the phase;
- the design cannot unilaterally replace the approved plan's pre-freeze
  requirement with a later integration requirement.

The local absence of Python 3.9 is a reason to obtain CI or scratch-environment
evidence now, not to waive the requirement. The rejected/vacuous-seam evidence
on 3.13 is useful and should be retained, but it is not portable evidence.

**Required fix**: execute the final frozen mechanisms on both supported
interpreters before design approval and record commands, actual
Python/h5py/libhdf5 versions, failure stage, backend status/error evidence,
and artifact state for each A4 row. Reclassify wrong-stage rows and reject
vacuous rows as the plan mandates. Alternatively, obtain an explicit reviewed
amendment to the approved plan's gate; a unilateral CI-time statement in this
design is insufficient. Repeat this evidence after finding 1 is resolved.

### 3. High — `StorageReport` is not defined for every failure, and primary diagnostics are never populated

**Locations**: design §2.2 steps 0–6, §2.5, §3.1–§3.4, §4.3, §6.1;
approved plan A4 universal assertion (c), A3.3, and P3P-03/P3P-04.

The diagnostic contract is internally inconsistent in three ways:

1. Save-side report bookkeeping starts at step 4, but step 0
   (`UnsupportedBackendError`), step 1 (missing-key `StorageSaveError`), and
   step 3 (cross-check `StorageSaveError`) raise before a report exists. This
   contradicts §2.5's rule that every raised storage exception carries
   `.report`.
2. Codec errors deliberately propagate unwrapped and therefore have no
   specified `.report`, yet A4 universal assertion (c) requires independently
   retrievable primary and cleanup diagnostics for every row, including A4.1
   and invalid-reload rows. The fresh-process failure schema also includes a
   `report` while listing `RecordDecodeError` and `RecordValidationError` as
   possible `error_type` values. No rule explains how that payload is
   produced while preserving the frozen codec exception type.
3. The save algorithm initializes `primary=()` and appends only cleanup
   messages. Step 6 attaches the assembled report to a primary exception but
   never appends a primary diagnostic. Thus `primary_ok=False` can coexist
   with an empty `primary` tuple, contrary to the stated diagnostic purpose
   and leaving tests without a frozen content rule.

The inspection narrative has the same ambiguity for preflight, decode,
cross-check, cleanup, and original-byte safety failures: it does not define a
complete report-construction/finally ordering equivalent to a frozen save
algorithm.

**Required fix**: construct the report at entry before any contractually
reported raise, and freeze report attachment for every storage exception,
including unsupported-backend and pre-connect validation failures. Define how
unwrapped codec failures expose the required primary/cleanup evidence without
changing their exception types, or explicitly amend and re-review the plan's
universal assertion and CLI schema. Freeze how `primary` is populated and the
inspection-side report/finally ordering so every success and failure payload
has deterministic `primary_ok`, `primary`, `cleanup_ok`, and `cleanup` values.

### 4. High — The by-name ordering rule is technically sound for an oracle, but it does not satisfy the currently approved stored-order wording

**Locations**: boundary decision §1.1 sample-schema row; Phase 2 design §1.3;
approved plan A1.2 and A2.5; Phase 3 design §2.2 step 3c, §3.4–§3.5, §10 A1;
`softlab/shui/data/hdf5.py:109-117,147-164`.

The source-fidelity part of the new finding is correct. The writer creates
one dataset per declared column without `track_order`; subsequent HDF5 key
enumeration and legacy reload are name-ordered, not declaration-ordered.
Signal's `(timestamp, signal, reference)` declaration therefore reloads as
`(reference, signal, timestamp)`. Each dataset's attributes and row-array
order remain independently available.

The proposed honest rule—order-sensitive validation against the in-memory
record before save, then by-name schema association on disk—is sufficient for
Phase 4's numeric/sample oracle **only if** that oracle iterates the envelope's
declared column sequence, retrieves each same-named dataset, and compares
sample arrays by row index. It must not treat `StoredRecordFacts.columns` or
reloaded `DataRecord.columns` order as declaration order. Under that explicit
rule, signal's tick/offset and derived-difference checks remain implementable
without a backend repair.

However, the current binding documents say more than that: the boundary
requires envelope order to match **stored column order**, Phase 2 repeats that
cross-check, and approved A1.2 explicitly asserts that stored column order
equals `record.columns` order. Design §10 cannot unilaterally convert those
assertions into by-name equality merely by calling the reading a design
freeze. Physical HDF5 enumeration order cannot meet the existing literal for
signal.

**Required fix**: obtain an explicit Product Owner/Architect clarification of
the ordering boundary and a direct reviewed test-plan alignment. Define the
envelope sequence as the canonical stored semantic order, HDF5 enumeration as
non-semantic, by-name attribute association as the disk cross-check, and
row-index array comparison as the sample-order check. Freeze the Phase 4
by-name reconstruction rule described above. The alternative—persisting
physical creation order—would be a separately reviewed storage change and
must not be introduced as an opportunistic backend repair. Until one of these
paths is approved, the design is not compliant with the inputs under review.

### 5. Medium — Disconnect/cleanup message templates are not frozen, and the claimed legacy failure surface uses different literals

**Locations**: design §2.2 cleanup pseudocode, §2.4 step 3, §2.5, §6.1;
`examples/thermal_experiment.py:319-338`; approved plan P3P-02 and A7.1.

P3P-02 requires frozen save/connect/disconnect/inspection message templates.
The taxonomy table has no disconnect/scratch-cleanup templates. The cleanup
pseudocode instead names lowercase flags such as `backend still connected`
and `file handle still open`, while the legacy thermal `_disconnect()` uses
`Backend still connected`, `Backend disconnect left its file handle open`,
and `Raw handle close failed: ...`.

The example then raises `RuntimeError('; '.join(report.cleanup))` while the
design claims the legacy failure messages are preserved exactly. Those claims
cannot both hold with the documented literals. Scratch-cleanup diagnostics
are likewise not assigned an exact template. Backend-inner exception text may
remain version-dependent, but the owning prefixes and flags must not be left
for implementers or testers to invent.

**Required fix**: freeze exact templates for disconnect error deltas, still-
connected and open-handle flags, raw-handle close failure, and scratch
cleanup. Where thermal routes these messages through its legacy writer
surface, use the existing capitalization and text exactly, or explicitly
review a compatibility change rather than claiming exact preservation.
Continue to treat embedded h5py/backend exception text as runtime data and
assert only the frozen prefix/template around it.

### 6. Medium — The fresh-process JSON protocol omits required stdout handling and does not mirror the established warning policy

**Locations**: design §4.1–§4.4; `softlab/shui/data/hdf5.py:38-41`;
`examples/thermal_experiment.py:944-956`; `tests/thermal_cli_bootstrap.py`;
`softlab/__init__.py` and `softlab/jin/vis/plot.py`; approved plan A3/A8.

Two test-instrument details are not implementable as written:

1. `HDF5DataBackend.connect_impl()` prints its connection message to stdout.
   The inspection entry point performs a real connect on the disposable copy,
   but the proposed bootstrap has no stdout redirection and promises a JSON
   payload on stdout. Normal inspection will therefore emit a connection line
   before the JSON, so a parent cannot treat stdout as the frozen JSON
   document. The existing thermal CLI already handles this exact behavior
   with `redirect_stdout(sys.stderr)` around `inspect_stored()`.
2. The design allows only the pandas baseline allowance and describes it as
   the same single allowance used by `thermal_cli_bootstrap.py`. In fact, the
   existing policy also contains four exact pyparsing/matplotlib allowances.
   The new child's import of `softlab.shui.run_storage` necessarily initializes
   top-level `softlab`, whose eager `jin.vis` import imports matplotlib, so it
   has not been established that those known import-time warnings are
   irrelevant to this child.

**Required fix**: freeze test-side redirection of backend operational stdout
to stderr (and the corresponding `contextlib` import), leaving stdout for the
single JSON payload and stderr for diagnostics/tracebacks. Mirror every
applicable exact reviewed warning allowance, including the required category
and module matching, or provide both-interpreter probe evidence showing that
the pyparsing allowances cannot apply to this import graph. Keep every
unapproved warning fatal and record observed warnings; do not add broad
suppression.

## Confirmed coverage and positive decisions

Subject to the fixes above, the following decisions are clear and appropriate:

- **P3P-01**: `DataGroup.add_record()` branch (a) is the minimal attachment
  mechanism; the two exact entry signatures, thermal hydration keys, empty
  signal hydration set, and raw-text legacy-context meaning are explicit.
- **Thermal integration**: retaining `str(CountResult.run_id)` at the existing
  `after_count` seam is source-faithful and preserves its distinction from
  `DataGroup.id`. The additive `_load_group` hydration delta correctly leaves
  legacy-only files on the pre-change path and lets `_verify_round_trip`
  compare the additive key on both sides for new files. Dual-write wiring and
  the A2.8 library-level signal round trip remain in Phase 3; actual signal
  example wiring remains Phase 4 work.
- **Safety and scope**: original-file legacy loads are structurally excluded;
  preflight and capture occur on a fresh disposable copy, followed by at most
  one legacy load and explicit hydration. D1/D2 and F-A/F-B/F-C/F-D are routed
  around rather than silently repaired. SQLite and generic-io paths are not
  certified.
- **Outcome and failure scope**: stored outcomes remain the completed-
  acquisition snapshot with later stages `not_yet_established`; runtime save/
  cleanup observations stay outside the record. The design explicitly avoids
  atomic-publication, rollback, recovery, and failed/partial-run durability
  claims.
- **Simplicity and traceability**: one `shui` module with two client entry
  points, direct use of existing h5py only for the mandated preflight, and a
  minimal explicit export delta are justified. The A1–A9 freeze table maps to
  PRD M03-AC03/AC04 and milestone evidence items 3–4, subject to the contract
  gaps identified above.

## Gate disposition

Implementation and executable Phase 3 tests should not begin on the current
freeze. Revise the design for findings 1–6, obtain the ordering/test-plan
alignment required by finding 4, and re-review the affected design and plan
diffs. P3P-05 is not closed until the final fault mechanisms have actual
Python 3.9 and 3.13 stage evidence at the required pre-freeze gate.
