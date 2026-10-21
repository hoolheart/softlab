# M01-W01 product delivery summary

Owner: sw-camille (Product Owner). Updated 2026-10-21 Asia/Shanghai.
Kickoff 2026-10-18; task integrated into dev and weekly acceptance PASS 2026-10-21.

Delivered one documented headless synthetic thermal experiment: explicit delayed
coordinated acquisition, eight deterministic observations, fresh HDF5 identity and
verified reopen, and a temperature PNG. Researchers can run the repository-root
command twice with fresh directories and compare numeric results while keeping
UUID and wall time distinct. See [guide](../../docs/user-guide/thermal-experiment.md)
and [complete product acceptance](acceptance.md).

Independent evidence: 229 warning-as-error local tests and compile/import checks,
two CLI runs with storage/image/collision checks, approved code review and all
 eleven task/preacceptance principles. Integrated dev is
`c89ffe6689484f1ea1eaf7088e8e2592fa21908d`; its exact CI explicitly passes both
Python 3.9 and 3.13. [Integration](integration.md) preserves the earlier candidate
job API inconsistency without weakening the exact dev compatibility evidence.

The three recorded scheduler/metadata/HDF5 library defects remain open with tested
example-local measures; historical OBS/DEFECT debt remains unchanged. The guide
explains safe copied-file inspection, non-atomic storage, failure diagnostics,
best-effort interruption and no physical accuracy/stop claims. No generated
experiment outputs or dependency/library behavior changes are delivered.

Remaining closure: final release-state principle reconciliation, closure-document
candidate CI and follow-up dev integration; only then any approved main promotion.
No main promotion or package publication has occurred in this acceptance phase.
No next task starts here. Hardware and M01-W02–04 remain outside this weekly
acceptance, and the entire M01 milestone is not yet accepted.
