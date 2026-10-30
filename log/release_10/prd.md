# PRD — M02-W03: lifecycle and repetition

Owner: Product Owner | Date: 2026-10-30 Asia/Shanghai
Status: **REVIEWED — independent requirements review approved; implementation and release acceptance pending**
Process namespace: `release_10` (not a package version).

## Product outcome and prerequisite

A maintainer using the bounded count seam can understand what happened when
acquisition, scheduling, inspection, reset or cleanup fails, and can perform
fresh independent runs without losing terminal evidence or implicitly repeating
work. The result must distinguish completed in-memory rows from the last
observed acquisition boundary. A successful advance followed by a failed sample
can leave the boundary beyond the row count; no rollback is implied.

[M02-W02](../release_9/acceptance.md) accepted the additive `run_count` and
`SerialScheduler` boundary with owned resources and basic cleanup. Its
[closure](https://github.com/hoolheart/softlab/blob/4cbe0c4/log/release_9/closure.md) records the promoted revision and remaining
debt. The [M02 plan](../../plan/milestone-02.md) assigns this release lifecycle
fault depth and repeatability; migration of the shipped thermal CLI remains W04.
The current runner requires an initially empty record, creates its own scheduler
and invokes process reset before running; this PRD requires a reviewed contract
for those actual paths, not an assumption that legacy reset is safe in all states.

## Acceptance criteria

| ID | Observable acceptance criterion |
| --- | --- |
| W03-AC01 | An independently reviewed lifecycle contract and tests define terminal states, exception precedence, inspection and cleanup diagnostics, owned-resource responsibilities, preflight and interruption behavior, reset eligibility and failed-reset behavior. The design identifies supported inputs and any changes to W02's public API and explains compatibility with existing callers. |
| W03-AC02 | Fault injection covers scheduler creation/start/dispatch or join, acquisition and state inspection, reset, and each applicable owned cleanup step. Primary failure identity and independently observable secondary failures are retained without suppressing later cleanup attempts. Flags reflect observed drain and closure; acquisition success with cleanup failure is not overall success. An interruption is re-raised after cleanup with available terminal evidence, including the documented fallback when attachment fails. |
| W03-AC03 | Complete rows and last successfully observable boundary remain truthful on failure, cancellation and interruption. Tests include advance failure and sample failure after advance, partial rows, an inspection failure alongside a primary failure, and no fabricated row or rollback. If the boundary cannot be observed, the contract identifies that limit rather than asserting a value. |
| W03-AC04 | Cooperative stop request and acknowledgment remain distinct. Tests cover interruption and stop before a row, after a row and after completion, and no new cycle after acknowledgment. Software cancellation makes no claim of preempting blocking code or stopping physical work. |
| W03-AC05 | Owned work is drained and owned scheduler/loop cleanup attempted on all supported terminal paths. Borrowed/global scheduler and thread-loop resources remain usable. If borrowed execution remains unsupported, it is rejected before execution side effects and the rejection is tested; ownership transfer must not be implied. |
| W03-AC06 | For the bounded-run path, reset is permitted only under documented terminal and drain conditions; legacy `Process.reset()` behavior is not changed by this requirement. Tests exercise ineligible reset and injected reset failure, preserve prior terminal evidence, and prove that neither a failed reset nor a new invocation silently retries acquisition. The design may retain fresh-process-only use rather than add a scheduler reset API, provided the eligibility and failure contract is explicit and observable. |
| W03-AC07 | Two fresh runs use independent process, record, model and execution resources and distinct run UUIDs; terminal evidence from the first survives the second. Deterministic traces and completed numeric rows match when timestamps and UUIDs are excluded. Define where the run UUID resides without requiring persistence or a general provenance schema. |
| W03-AC08 | Automatic retry remains absent unless a specific idempotent boundary and observable effects are independently justified and tested, with product and architecture approval before implementation. There is no implicit resume, replay or rollback. |
| W03-AC09 | Existing public `Process`, scheduler, `count`, `scan`, and W02 client behavior remain compatible within their characterized contracts. Independent regression tests include relevant W01/W02 characterization and the synthetic client; tests do not require the W04 thermal CLI migration. Any discovered baseline blocker receives an explicit scoped disposition instead of an unreviewed repair or waiver. |
| W03-AC10 | Usage guidance and public docstrings describe supported ownership, failure evidence, stop limits, reset/fresh-run requirements and retry decision. Independent code review and tester-owned failure closure, product acceptance and principle inspection map every applicable criterion to evidence and report actual warnings and limitations. |
| W03-AC11 | Freeze required code and records and obtain passing CI for the exact integration candidate before `dev` integration. Promote accepted content to `main`, verify promoted state and required checks, record final revisions, CI links, scope limits and remaining debt in one closure summary, and verify a clean worktree before marking **CLOSED**. |

## Scope and debt disposition

This release covers lifecycle refinement of the W02 serial bounded-count seam,
its tests and guidance, using synthetic data and temporary paths. No physical
instrument operation is authorized. W04 owns both thermal adapter migrations and
milestone acceptance; M03 owns broader provenance, M04 hardware contracts, M05
scan extensions and M07 durable partial results and recovery. General scheduler
repair, storage repair, new dependencies, hard real-time guarantees, automatic
retry by default and a general execution framework are outside this scope.

| Tracked item | W03 disposition |
| --- | --- |
| R4-BASE-SCHED-01 / F-01 | Concrete private generic scheduler routes completion through the singleton. Continue to avoid that path using the W02 serial seam; generic repair is separately scoped. |
| F-02 | Generic control-point timeout does not meet its documented behavior. Preserve the serial seam's independently tested timeout contract; do not claim the generic issue fixed. |
| R4-BASE-META-01 | Metadata constructor behavior remains tracked; retain explicit metadata creation and disk hydration where relevant. Repair is separately scoped. |
| R4-BASE-HDF5-01 | Legacy loader can remove reserved attributes; continue disposable-copy reads where relevant and use temporary synthetic data. Repair is separately scoped. |
| OBS-004 | Avoid non-None settable initial values on affected Quantized/VISA subclasses; no initialization contract change. |
| OBS-005 | Retain legacy feature fallback and opt-in strict evaluation; do not change error behavior incidentally. |
| OBS-006 | Retain explicit lookup escape hatch and existing cycle limitation. |
| DEFECT-2 | Preserve the deliberate programmer-error warning for a Parameter that is neither readable nor writable. |

These items remain open and are not waived. Record any impact on the actual
selected paths and separately scope a blocker before proceeding. Historical
pandas concatenation, GitHub Actions Node runtime and local cache warnings must
be reported where observed, with environment and evidence; do not claim that a
warning was fixed or absent in an untested environment.

## Gates and ownership

This is one serial release task. Product Owner owns acceptance; Architect owns
feasibility and design review; Developer reviews the test plan and implements;
an independent reviewer owns code-review closure; Tester owns execution and
test-failure closure. UI gates do not apply. Before implementation prepare and
review the test plan and detailed design, including compatibility and reset
semantics. Execute applicable unittest discovery, compile and import checks;
record actual commands, environments and limitations. Reuse evidence only for
unchanged content with applicability stated. The [principles](../../principles.md)
and [release checklist](../templates/release-checklist.md) govern completion;
unrun or failed gates cannot be reported as passing.
