# Development principles

These project rules adapt the requested `onepiece:sw-prod-workflow` to the
user-approved Python-library workflow. Together with AGENTS.md, they take
precedence over conflicting skill ceremony, generic Rust/Flutter preferences and
direct-to-main task merges. Preparation was recorded
in `log/release_0/`; the subsequently authorized compatible `tu` work is scoped by
[Release 1 requirements](log/release_1/prd.md).

1. **Preserve the five-element architecture.** `tu` defines device/model
   abstractions; `huo` executes processes; `shui` persists data/configuration;
   `jin` supplies reusable utilities; `mu` coordinates application use cases.
   Describe actual implementation in `arch.md`; label proposals explicitly.
2. **Characterize before changing public behavior.** Read callers and add
   assertions for existing contracts before implementation. Preserve import
   paths, parameter return types, validation, codecs, hook ordering, command
   side effects, snapshots and model behavior unless an explicit breaking
   change and migration have been approved. Arbitrary parameter values need
   not become JSON-serializable merely because descriptions are serializable.
3. **Use documented TDD gates.** Requirements/acceptance criteria → tests and
   developer review → detailed design and architect review → implementation
   → independent review → testing → principle inspection → integration.
   Documentation/CI bootstrap uses document/configuration validation rather
   than artificial production-code tests; record applicability explicitly. Scale
   gates to impact: behavioral changes retain independent review and testing;
   configuration changes need relevant configuration checks; factual status/link/
   hash corrections need direct consistency and diff checks, not a full role chain.
4. **Keep one active task.** Task branches start from `dev` and integrate into
   `dev` only after gates pass. Use `codex/<task>` by default. `main` is the
   release branch; promotion requires release acceptance. Never rewrite shared
   history or overwrite user changes. The bootstrap is one task across roles.
5. **Respect role and review ownership.** The coordinator manages gates;
   product owner defines requirements/acceptance; architect owns architecture
   and decomposition; designer owns detailed design/code review; developer
   implements; tester owns tests/results. Only reviewers close review issues
   and testers close test failures. UI design applies only to UI changes. Delegate
   only when expertise or independence is needed; administrative confirmation
   does not require every role. Reuse valid evidence for unchanged content.
6. **Keep evidence committed and pushed.** Store release/task evidence under
   `log/release_x/`, with templates under `log/templates/`. Commit/push a completed
   phase's related artifacts together before handoff, using `docs(log):` for log
   commits. Freeze code and required records before final candidate CI. Report
   resulting merge hashes/CI URLs in the final handoff; do not create a new commit
   merely to record the previous commit's check result. Record final facts together
   without circular self-hashes or recursive gate cycles. Never represent an unrun
   gate as passing. One authoritative closure summary is sufficient.
7. **Verify the relevant environment.** Run unittest discovery, compileall and
   an import smoke check for Python changes; CI repeats the configured checks.
   Record Python/dependency versions, warnings, skips and limitations. Rerun checks
   for affected changes, failures or unresolved risks; reuse unchanged evidence
   with its applicability stated. Pure documentation uses content/link/diff checks,
   not fabricated production tests. Missing required components or failed checks
   block the affected gate. Notebook
   existence and examples are not test evidence.
8. **Distinguish baseline defects from regressions.** Record pre-existing
   failures/warnings explicitly; no silent waiver or automatic issue closure.
   New failures/warnings must be resolved. Existing debt needs a tracked task
   and explicit disposition before claiming the affected release gate passes.
   Do not expand preparation into unrelated fixes. Zero-warning claims require
   evidence from the named checks, not inference about unconfigured linters.
9. **Protect instruments and experimental data.** Use synthetic data, temporary
   storage and VISA simulation in routine checks. Real instrument access needs
   explicit task authorization. Define resource ownership; release owned
   resources after success/failure. Software cancellation is not proof of
   physical termination.
10. **Prefer minimal compatible extensions.** Optional capabilities must not
    burden virtual devices. Add no framework, dependency, universal domain
    hierarchy or supported-Python change without a concrete need and explicit
    explanation. Preserve the existing Python/setuptools project.
11. **Finish with reproducible evidence.** CI must pass for the integration
    candidate before merging to `dev`; release acceptance is additionally
    required before `main`. Inspect principles at task start/completion and
    release completion. Verify diff hygiene, scope and clean status; report
    limitations honestly. Do not call a release accepted merely because its
    preparation task has completed. Define closure upfront: required independent
    gates and acceptance complete, exact candidate CI passed, promotion and required
    promoted-state checks verified, and worktree clean. Then mark CLOSED and stop.
    Do not add intermediate merge/reconciliation CI gates or evidence-only tasks;
    automatically triggered runs need no recursive records. Administrative closure
    corrections do not reopen the release. Investigate real failures or reporting
    lag with bounded, decision-relevant checks; communicate new findings instead
    of repeating unchanged waiting status.
