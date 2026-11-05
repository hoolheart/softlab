# Acceptance Document — Release 12 (M03 reproducible run record)

## Acceptance Information

- **Reviewer**: sw-camille, Product Owner
- **Date**: 2026-11-05
- **Release**: 12 / M03
- **Review branch and supplied revision**: `codex/m03-p4-acceptance`, `90dc1dc`
- **PRD**: `log/release_12/prd.md`
- **Review type**: final product acceptance; review only, no product-code changes
- **Method**: requirement-to-evidence tracing plus static inspection of the delivered codec, group/storage seams, client wiring, walkthrough assertions, guide, and architecture record. I did not execute tests or runtime workflows and do not claim an independent runtime rerun. Git/CI/worktree operations were not available in this review environment.

## Product Outcome Assessment

The delivered implementation and the W1–W3 evidence demonstrate the core bounded workflow in the recorded project environment: supported synthetic thermal coordinated/direct and signal results can be produced, reopened through the safe separate-process inspection path, and compared with independent frozen expected values. The records expose the reviewed client declarations, units, observed software/source identity, acquisition-only outcome, samples, and distinct execution/group identities.

The release nevertheless **does not meet the complete PRD acceptance contract**. The approved fresh-environment walkthrough was narrowed in execution, while its result and principle inspection still claim a pass. Documentation also contains a false data-safety claim and is not fully current. The product outcome is therefore demonstrated only partially, not accepted for release.

## Test and Review Report Verification

| Phase / gate | Recorded evidence | Acceptance assessment |
| --- | --- | --- |
| Phase 1 characterization | 47 characterization tests; 405 total tests, compile/import checks; C1–C6 all characterized; independent evidence-decision review ACCEPTED | Evidence supports AC01. Historical CI failure corrections and warnings are recorded, not hidden. |
| Phase 2 schema/codec | 54/54 schema tests; 459 total tests; code review APPROVED with zero defects; candidate CI recorded green on Python 3.9/3.13 at `cb2c8ca` | Evidence supports AC02. |
| Phase 3 storage/inspection | 48/48 storage tests; 507 total tests on Python 3.9.23 and 3.13.15; design and code reviews APPROVED | Evidence supports AC03/AC04 within the HDF5-only, new-run, copied-inspection boundary. |
| Phase 4 comparison/walkthrough | 521 total tests reported OK in the project environment; W1/W2/W3 original-versus-rerun comparisons PASS; code review issue closed | Supports AC05 and the project-environment portion of AC06. It does not satisfy the missing fresh-environment commands/scenarios identified below. |
| Fresh-environment W4 | Real venv creation, isolation, editable install, versions, and one thermal coordinated production/inspection workflow recorded | **Incomplete and overclaimed.** The results expressly admit that full unittest discovery, compileall, the exact import smoke command, and W1–W3 A/B sessions did not run in the fresh environment. |
| Independent reviews and failure closure | Phase design/code reviews end APPROVED; earlier findings have owner-closed dispositions | Reviewed evidence is reusable for unchanged content. Acceptance findings in this document remain open and are not waived by those historical approvals. |
| UI gates | No UI work | Not applicable. |
| Compilation/warnings | Executed compileall checks report no output/errors; actual runtime warnings are recorded | No silent zero-runtime-warning claim is accepted. The pandas, sqlite3, pyparsing, and deliberate DEFECT-2 records retain their stated applicability. |

## Acceptance Criterion Mapping

| Criterion | Concrete evidence | Status |
| --- | --- | --- |
| **M03-AC01** | `test/characterization-test-plan.md` and `test/characterization-results.md` cover legacy v1/v2 readers and validation, safe direct HDF5 inspection on disposable copies with hashes, HDF5/SQLite capabilities, production callers, both clients' field ownership, and independent Euler and tick/offset oracles. `design/p1-boundary-decision.md` selects one additive HDF5 JSON envelope before schema implementation and contains explicit Product Owner concurrence. `reviews/p1-evidence-decision-review.md` independently accepts the evidence and decision. | **PASS** |
| **M03-AC02** | `design/p2-schema-design.md` freezes `softlab.run-record`, format/payload versions, closed required fields, bounded values/limits, absent/null/unknown/unavailable semantics, and error taxonomy. `run_record.py` implements strict allowlisting, canonical deterministic JSON, duplicate/nonfinite/unsupported/version rejection, and separate identities without stringification fallback. `test/schema-test-results.md` records 54/54 schema tests and 459 total tests; the sprint board records exact-candidate CI on both configured interpreters. | **PASS** |
| **M03-AC03** | Phase 2/3 designs and `run_record.py`/`run_group.py` preserve closed thermal and signal declarations, model/procedure/timing, ordered semantic columns and units, software/source facts, counts, and acquisition-only outcome. `run_storage.py` checks group identity, attachment, columns, and row count. Execution and group UUIDs remain separate; persistence/plotting/cleanup/overall remain `not_yet_established`. Storage A1/A5 and walkthrough W1–W3 supply stored-artifact evidence. | **PASS** |
| **M03-AC04** | `design/p3-storage-design.md` selects HDF5-only owned writes and copied, read-only preflight plus one legacy load and explicit hydration. `run_storage.py` and `tests/run_record_inspect_bootstrap.py` implement separate-process inspection, legacy-absence versus invalid-present behavior, original re-hashing, and independent primary/cleanup reports. Storage A2–A4/A6 cover repeated inspection, malformed/unsupported reloads, serialization/connect/save/disconnect faults, multi-group selection, SQLite rejection, and excluded generic io helpers; 48/48 tests and the 507-test runs are recorded on both interpreters. | **PASS within the recorded new-run/HDF5 scope**. This does not certify generic metadata round-trip, original-file plain loads, SQLite, or same-ID atomic publication. The guide's broader failed-save safety sentence is separately rejected in Issue 2. |
| **M03-AC05** | `design/p4-oracle-design.md` freezes both thermal adapters and the signal scenario, independent numeric/timing tables, exact comparisons plus thermal `abs_tol=1e-10`, exclusions, and a two-session procedure. Architecture independently recomputed the oracle values and approved the revised freeze. W1/W2/W3 create distinct A/B artifacts in fresh processes, inspect each in separate processes, compare each to the independent oracle, and compare A with B. Legacy suites and the 521-test project-environment run are recorded green. | **PASS for clean-session rerun and compatibility evidence**. The missing fresh-environment repetition is assessed under AC06 rather than hidden here. |
| **M03-AC06** | Fresh venv creation/isolation, editable installation, dependency/version records, and one thermal coordinated workflow exist. Project-environment unittest/compile/import checks and independent code review pass. However, `test/walkthrough-results.md` lines 107–125 and 208–218 explicitly admit that the fresh environment did not run full unittest discovery, compileall, the exact import smoke check, or W1–W3 A/B sessions. The approved `design/p4-oracle-design.md` §6 item 2 and `test/walkthrough-test-plan.md` W4 item 3 require those executions in the fresh environment. Documentation defects also remain open. | **FAIL — Issue 1 blocks acceptance; Issues 2–4 also require documentation correction.** |
| **M03-AC07** | Four principle inspections are recorded PASS, and prior phase candidate revisions/CI links are recorded. This document maps all phases and debt, but acceptance is REJECTED. Inspection 4's statement “no gate skipped” is contradicted by the W4 deviation. Final acceptance-candidate CI, integration to `dev`, promotion to `main`, promoted-state verification, authoritative closure, and clean-worktree verification have not been supplied and remain pending. | **NOT MET / RELEASE REMAINS OPEN**. Later closure steps must not be represented as already passed. |

## Ordering Addendum Assessment

The Phase 1 boundary addendum is **accepted as interpretation-only**:

- It adds no client, backend, field, version, persistence, recovery, or environment capability.
- The envelope sequence remains the canonical semantic declaration and is still checked order-sensitively against the in-memory record before save.
- Physical HDF5 enumeration is treated as non-semantic; read-side verification still requires equal column-name sets and exact four-attribute matches by name.
- Sample ordering remains observable by row index, and the approved Phase 4 oracles reconstruct by declared name rather than enumeration order.
- Thermal's six declarations are already lexicographic, so legacy v1/v2 behavior is unaffected.

The addendum and the corresponding test-plan deltas are explicit and reviewed; this is not a silent broadening or a backend repair.

## Limits and Debt Assessment

| Item | Acceptance disposition |
| --- | --- |
| R4-BASE-SCHED-01 / F-01 / F-02 | Tracked avoidance remains: supported clients use `run_count` and its owned serial seam; no generic scheduler repair or new dependency is claimed. |
| R4-BASE-META-01 / D1 | Constructor metadata loss/mutation and the non-string-key failure remain characterized debt. Supported construction uses an empty group plus encoded text through `set_meta`, not `DataGroup(meta=...)`. |
| R4-BASE-HDF5-01 / F-A | Destructive plain HDF5 loading remains unfixed. Supported inspection uses a fresh disposable copy, preflight, at most one legacy load, explicit hydration, and original-byte re-verification. |
| D2 | SQLite's empty plain-load metadata behavior remains tracked; SQLite is explicitly unsupported for this record. |
| F-B | Custom-key loss is not repaired or generalized away. Only `run_record` and the declared per-profile legacy keys are captured/hydrated. |
| F-C / F-D | `io.reload_group` and `io.load_groups` remain excluded; their `None`/empty outputs are not treated as absence or successful inspection. |
| OBS-004 / OBS-005 / OBS-006 | Existing avoid/fallback/escape-hatch dispositions remain; no M03 path silently changes them. |
| DEFECT-2 | The deliberate neither-readable-nor-writable `Parameter` warning remains documented and was observed again; it is not suppressed or silently closed. |
| **`set_meta(None)` mismatch — separate assessment** | Phase 1 C2.2 establishes that the key remains with `None` on an existing key and is created with `None` on an absent key, contrary to the removal docstring. The mismatch remains tracked and unrepaired. Supported records do not use this method for absence/null; JSON null stays inside validated encoded text. This is non-blocking only under that explicit avoidance, not a waiver. |
| Scope exclusions | No environment/dependency lock, physical validation, real-instrument/VISA qualification, secret collection, arbitrary object/callable serialization, general provenance framework, backend rewrite, retry/rollback, durable partial recovery, later-outcome history, or atomic publication is accepted as delivered scope. |

Nothing in the accepted implementation evidence silently repairs or broadens those items. Issue 2 is a separate documentation overclaim about save safety and must not be used to enlarge the proven boundary.

## Guide and Documentation Review

### Maintainer guide

`docs/run-record.md` accurately describes the supported clients, HDF5-only backend, closed additive envelope, acquisition-only outcome, distinct identities, copied inspection, major exclusions, signal source-versus-derived relation, and the location of the frozen rerun oracle. W7 supplies executed evidence for its thermal/signal production examples and both bootstrap profiles, with substitutions and harness necessities recorded.

The guide is **not acceptable as a whole** while Issue 2's categorical failed-save safety statement remains. Its accurate statements do not cure that contradictory claim.

### Other documentation

- `arch.md` is not current for M03; see Issue 3.
- `examples/signal_monitor.py` contains a contradictory read-semantics docstring; see Issue 4.
- The general README need not enumerate every feature; no separate README acceptance defect is asserted here.
- Several historical plan/design headers retain DRAFT/PENDING wording even though later review sections record approval. Those stale labels are administrative inconsistencies, not the basis of this rejection; the substantive evidence and final review verdicts were used instead.

## Issues Found

### Issue 1: Approved fresh-environment AC06 execution was narrowed but still reported as PASS

- **Severity**: **Blocker**
- **Related requirement**: M03-AC06; Phase 4; frozen oracle design §6 item 2; approved walkthrough plan W4 item 3 and W6 item 2
- **Expected behavior/evidence**: In the genuinely fresh environment, record actual execution of full unittest discovery, `compileall`, the import smoke command, and the W1–W3 original/rerun sessions and inspections for the approved scope, with versions, warnings, limitations, and exact commands. Unrun gates must remain pending.
- **Actual behavior/evidence**: `test/walkthrough-results.md` expressly states that only the minimal thermal coordinated CLI plus bootstrap inspection ran in the fresh venv. Full unittest discovery, compileall, the exact import smoke, and W1–W3 A/B sessions ran only in the project environment or not at all in the fresh venv. Nevertheless, W4 is marked PASS, the AC traceability matrix claims items 1–6, and `principle_compliance_report.md` Inspection 4 says “no gate skipped.” The committed results also summarize session PIDs and driver forms without supplying the complete verbatim command transcript required for every W1–W3 session.
- **User impact**: A maintainer lacks the required proof that the complete supported thermal coordinated/direct and signal workflow independently repeats from the fresh documented environment. The report's PASS wording gives false confidence that this gate was completed.
- **Required correction**: Keep M03-AC06 open. Execute and record the missing fresh-environment commands and all three A/B scenarios on the frozen acceptance candidate, including exact commands, outcomes, versions, warnings, skips, and limitations. Install/record any test dependencies actually needed by the full suite rather than hiding environment failures. If a different scope is intended, obtain the necessary reviewed requirements/plan change instead of unilaterally narrowing execution. Tester-owned closure must correct the W4/AC traceability and affected principle-inspection overclaims before re-review.

### Issue 2: Guide falsely guarantees that a failed save never deletes or truncates the target

- **Severity**: **Major**
- **Related requirement**: PRD Limits/debt; M03-AC04 truthful failure evidence; documentation accuracy gate
- **Expected behavior**: Describe only the evidenced boundary: new-run HDF5 writes, no rollback or atomic publication, and no same-ID deletion/recreation guarantee. Do not imply protection of pre-existing target content that the writer does not provide.
- **Actual behavior**: `docs/run-record.md` §6 states “a failed save never deletes or truncates the target.” The unchanged HDF5 writer (`softlab/shui/data/hdf5.py` `save_impl`, lines 73–76) deletes an existing same-ID group before recreating it. A subsequent save failure can therefore lose the previous group content. The recorded A4.2 empty-new-file and A6.3 successful same-ID probes do not establish the guide's categorical failure-safety claim.
- **User impact**: A maintainer could mistakenly rely on M03 to preserve existing artifact content after a failed save, contrary to the explicit no-atomicity/no-rollback boundary.
- **Required correction**: Correct the guide and any matching storage docstring/design wording to state the actual target/content risk and the unsupported same-ID case. Do not introduce a backend repair under acceptance; any stronger guarantee requires separately reviewed scope, implementation, and failure tests. Re-run the affected guide/content validation after correction.

### Issue 3: Architecture record does not describe the delivered M03 public API and ownership

- **Severity**: **Major**
- **Related requirement**: M03-AC06/AC07 documentation and release-checklist requirement for current architecture/guide records
- **Expected behavior**: `arch.md` reflects the current implemented state, including the `shui` ownership and public seams delivered by this release, with supported limits identified truthfully.
- **Actual behavior**: `arch.md` ends its implemented execution history at M02-W04. Its `shui` boundary lists the older backend/data/profile capabilities but does not describe `run_record.py`, `run_group.py`, `run_storage.py`, their public exports, or the M03 additive-record/safe-inspection boundary. Phase 2/3 design references to the planned additions being marked in `arch.md` are not borne out by the current file.
- **User impact**: A maintainer reading the architecture record cannot discover the actual M03 module responsibilities and public persistence/inspection contracts from the authoritative architecture document.
- **Required correction**: Update `arch.md` to record the implemented M03 modules, public entry points, ownership, HDF5-only/copy-inspection scope, and explicit limits, linking the maintainer guide as appropriate. Perform direct content/link/diff validation; do not fabricate new runtime evidence for a factual documentation correction.

### Issue 4: Signal example docstring contradicts the verified non-consumptive read policy

- **Severity**: **Minor** documentation defect; correct before acceptance re-review
- **Related requirement**: M03-AC05 declared read semantics; documentation accuracy
- **Expected behavior**: The example describes the same non-consumptive `tick + offset` semantics characterized in C5b, frozen in the signal payload, and stated in the guide.
- **Actual behavior**: The module docstring begins “The source is intentionally consumptive,” while `read()` only records the channel/tick and returns `state['tick'] + offset`; it consumes no queued value. The tests, stored payload (`consumptive: False`), oracle, and guide all say non-consumptive.
- **User impact**: The client’s own documentation gives a maintainer the opposite mental model for a central rerun/oracle assumption.
- **Required correction**: Correct the stale docstring without changing acquisition behavior, and verify consistency with the guide and frozen payload. No new runtime claim is needed for the text-only correction; existing behavioral evidence remains applicable.

## Acceptance Conclusion

### Pass Conditions Verification

| Condition | Status |
| --- | --- |
| AC01 characterization and evidence decision | YES |
| AC02 strict bounded schema/codec | YES |
| AC03 stored declarations, units, identity, samples, and stage honesty | YES |
| AC04 owned HDF5 storage, safe inspection, and tested failures within scope | YES, subject to correcting the broader guide overclaim |
| AC05 independent oracles, compatibility, and clean-session A/B comparison | YES |
| AC06 complete fresh-environment/independent-repeat and documentation gate | **NO** |
| AC07 acceptance/principle mapping and final release closure | **NO — acceptance rejected; later closure gates pending** |
| All required reviews/tests applicable to unchanged evidence | Recorded pass, except for the missing AC06 execution identified above |
| Documentation complete, current, and accurate | **NO — Issues 2–4** |
| User experience acceptable within the claimed scope | **NO — the evidence/report overclaim and save-safety documentation are not acceptable** |
| Exact final-candidate CI / promotion / promoted checks / clean worktree | PENDING; none is claimed as passed |

### Overall Verdict

**Status: FAIL — REJECTED**

Release 12 is **not accepted** and M03 must remain **OPEN**. AC01–AC05 have substantive supporting evidence within the recorded scope, but AC06 is not satisfied and its current PASS/no-gate-skipped presentation is contradicted by the recorded execution deviation. The documentation issues must also be corrected. Final candidate CI, integration, promotion, promoted-state checks, and clean-worktree/closure verification remain pending and cannot proceed as release closure on this rejection.

Re-review must cover Issue 1's new execution evidence and corrected reports, Issue 2–4 documentation corrections, and any affected evidence applicability. No blocker or major issue is waived.

**Signed**: sw-camille, Product Owner

## Commit / Closure Status

This acceptance document was written to `log/release_12/acceptance.md`. No commit, push, CI, merge, promotion, or clean-worktree verification was performed because git tooling is unavailable in this review environment. The document therefore requires committing/pushing by the coordinator before it can serve as committed release evidence.
