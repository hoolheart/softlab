# M02-W02 principle inspection

Inspector: Product Owner, with evidence from independent role owners  
Date: 2026-10-30 Asia/Shanghai  
Verdict: **PASS for applicable implementation and local-evidence principles**;
release integration and closure checks remain pending.

1. Five-element responsibilities: the additive executor and runner belong to
   `huo`; acquisition policy stays with clients. Architecture and usage guidance
   describe the implemented scope.
2. Characterization and compatibility: the accepted W01 characterization is
   retained, and Tester reports full local regression passing. No legacy reset,
   count or scan API change is claimed.
3. Documented gates: reviewed requirements, test plan and design preceded
   implementation; independent code review closed its two findings and Tester
   owns the local result. UI design is inapplicable.
4. One task and branches: the active W02 work is on the designated `codex/`
   branch; `dev` integration and `main` promotion remain future gates.
5. Role ownership: Architect reviewed design, Developer implemented, Designer
   closed code-review findings, Tester reported execution, and Product Owner
   assesses acceptance. Administrative work is proportionate.
6. Evidence: process records exist and the coordinator will batch commit and
   push the frozen candidate. Exact-candidate CI and final closure facts must not
   be inferred from working-tree evidence.
7. Environment: 328 tests with `-W all`, compile and import pass locally under
   Python 3.13.15; Python 3.9 and candidate CI await actual results. Initial cache
   warnings and the temporary-cache rerun are recorded by Tester.
8. Baseline debt: known scheduler, metadata, HDF5, OBS and DEFECT-2 dispositions
   remain tracked; historical pandas/Actions warnings are not silently waived.
9. Resource safety: tests use synthetic clients; owned loops are cleaned up and
   borrowed/global resources are isolated in the tested paths. No physical or
   forced-process termination claim is made.
10. Minimal extension: no new dependency, scan extension, generic scheduler
    repair or new scheduler reset API is introduced.
11. Reproducible closure: final candidate CI before integration, promotion and
    required promoted-state checks, recorded hashes/URLs, final diff hygiene and
    clean worktree remain outstanding. This inspection does not mark CLOSED.

See [acceptance](acceptance.md), [test results](test/execution-results.md),
[code review](reviews/code-review.md), and the
[release checklist](../templates/release-checklist.md) for evidence and remaining
gates. Reinspect affected portions if behavior or relevant evidence changes.
