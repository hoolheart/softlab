# PRD — M02-W02: bounded shared count execution

Owner: Product Owner | Date: 2026-10-30 Asia/Shanghai  
Status: **OPEN — requirements defined; implementation and acceptance pending**  
Process namespace: `release_9` (not a package version).

## Product outcome and prerequisite

A maintainer can use a small shared, bounded serial execution seam for distinct
count acquisition policies without copying executor machinery. The existing
thermal CLI remains unchanged in this release. A synthetic paired
signal/reference monitoring client and a bounded reference integration fixture
exercise the seam; the latter represents both existing thermal timing policies.
The client owns its acquisition and derived-value policy, configuration and
records. It is distinct from the thermal model and exposes unintended extra
reads through consumptive inputs. The fixture is not a second public runner.

The accepted [W01 closure](../release_8/closure.md) establishes the prerequisite
and selects the minimal additive executor boundary described in the
[evidence decision](../release_8/design/evidence-decision.md), with its
[Architect review](../release_8/reviews/evidence-decision-review.md) and
[Product Owner acceptance](../release_8/acceptance.md). Those records supersede
historical preparation statements that W01 is still open. The present user
request starts W02. The [M02 plan](../../plan/milestone-02.md) defines the
release outcome and its relationship to later increments.

## Scope and acceptance criteria

| ID | Acceptance criterion |
| --- | --- |
| W02-AC01 | Independently reviewed tests and detailed design define the additive `huo` API, placement, supported inputs, results, exception handling, ordering, ownership and terminal transitions. Architect approves placement and the extent of any acquisition-loop extraction. Production code does not import the example prototype. |
| W02-AC02 | The executor implements the existing public `Scheduler` interface for serial bounded work on a privately owned event loop, with one unchained, nonthreaded action per point. Supported sync and awaitable action behavior is specified and tested. Existing public imports and `Process`, `count`, `scan`, `run_process`, legacy reset/abort and append-on-repeat behavior remain compatible. No scheduler reset API or scan extension is added. |
| W02-AC03 | Ordered traces demonstrate command, advance, reads and complete-row commit for the distinct synthetic client and both reference timing policies. Caller policy controls observation; stateful inputs detect unintended extra reads. The executor does not embed thermal, derived-value, persistence or plotting policy. |
| W02-AC04 | Tests cover normal completion, stop before the first cycle, stop after a known completed row, and a late request after completion. Request and acknowledgment are distinct; no later cycle starts after acknowledgment, and late requests do not rewrite success. Cancellation is cooperative and does not claim physical termination or preemption. |
| W02-AC05 | Advance and sample/read failures before and after an advance retain the primary cause and completed rows. An incomplete row is never committed; a successful advance followed by failed sampling may leave the final boundary beyond the row count. Consumed input is not rolled back. Terminal results distinguish completion, failure and acknowledged cancellation. |
| W02-AC06 | Supported terminal paths attempt owned-work drain, scheduler stop and owned-loop closure on success, cancellation and failure. Cleanup failures are reported independently from the primary failure, and one cleanup failure does not suppress the remaining attempts. Borrowed thread loops and the global scheduler are neither stopped nor closed and remain usable. Unsupported borrowing is rejected before side effects. |
| W02-AC07 | Tests isolate rejection of chained and threaded actions, re-commit to a used point, commit before start or after stop, unsupported borrowed/global execution and synchronous use inside a running loop, before action side effects where applicable. Unknown points and failed actions return `False`; bounded wait timeout returns `None`, distinct from failure. The seam avoids the defective private generic scheduler path. |
| W02-AC08 | Independent code review and tester-owned execution and failure closure cover the implemented behavior and compatible existing callers. Full applicable unittest discovery, compile and import checks record actual commands, environment, warnings, failures and limitations. The W01 reduction of a characterization-only review does not apply to W02. |
| W02-AC09 | Product acceptance maps these criteria to actual evidence. Principle inspection, relevant architecture and usage documentation, scope and diff checks are complete. No unrun gate is represented as passed. |
| W02-AC10 | Freeze the code and required records, commit and push the exact integration candidate, and obtain passing CI for that revision before integration to `dev`. Promote accepted content to `main`, verify promoted revision/tree and required checks, record merge revisions and CI URLs in one closure summary, and verify a clean worktree. Only then mark this release **CLOSED**. |

Detailed design may select a narrower supported interface only where it still
satisfies all criteria above; unsupported capabilities must be explicit and
verified. Cleanup and truthful primary/cleanup failure reporting are W02 release
obligations and cannot be deferred to W03.

## Exclusions and tracked debt

W04 owns migration of the shipped thermal CLI and milestone acceptance. W03
owns further lifecycle refinement and any scheduler-level reset decision; W02
does not add automatic retry, rollback, durable resume or persisted partial
results. M03 owns general provenance work, M04 hardware contracts, M05 scan
extensions and M07 recovery. No physical instrument access, VISA qualification,
new dependency, broad scheduler repair, storage repair, new framework or global
scheduler replacement is authorized here.

R4-BASE-SCHED-01 and the generic control-point timeout finding F-02 remain
tracked baseline defects; the new seam must avoid the former and implement its
own tested timeout contract. R4-BASE-META-01, R4-BASE-HDF5-01,
OBS-004/005/006 and DEFECT-2 retain their existing dispositions, with no silent
waiver or unrelated repair. Historical pandas and Actions warnings are recorded
as historical; report observations in the environments actually tested. Use
synthetic data and temporary paths, and document any relevant baseline blocker
before a gate is called passed.

## Gates and ownership

This is one serial release task. Product Owner owns requirements and
acceptance; Architect owns feasibility and detailed-design review; Developer
reviews the test plan and implements; Designer owns independent code review;
Tester owns test execution and test-failure closure. Reviewers close their own
findings. UI/Figma gates do not apply. Evidence may be reused for unchanged
content with applicability stated; affected failures and changes require
appropriate rechecks. The [project principles](../../principles.md) and
[release checklist](../templates/release-checklist.md) govern the finish line.
