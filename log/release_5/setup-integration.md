# M01-W02 setup integration

Owner: sw-tom (Developer/integrator). Date: 2026-10-22 Asia/Shanghai.
State: **setup integrated; closure-record CI/reconciliation pending**.

Coordinator formally approved saved/pushed setup artifacts at `0255e485`:
PO requirements, Architect feasibility/decomposition/start inspection,
Designer APPROVED report at `e5d87dd`, and Tester PASS report at `0255e485`.
HEAD equaled upstream and the working tree was clean at delegation. Reviewer
and Tester retain issue closure authority; no issues were raised in setup.

The configured Compatibility workflow runs on codex branches and dev with
Python 3.9 and 3.13, installing declared dependencies plus pyvisa-sim, then
regression, compile and import checks. No workflow or runtime code is changed.
Exact candidate CI must pass both jobs before dev integration. Main remains
release-acceptance gated; implementation remains Backlog until setup records
are fully integrated. No runtime/test/design work is performed by this role.

## Executed integration evidence

Exact candidate `da6e4d79eda5c7459c225eb3e0b917b03a34eb64` passed
[Compatibility run 39690046771](https://github.com/hoolheart/softlab/actions/runs/39690046771):
overall completed/success and both Python 3.9/3.13 jobs explicitly
completed/success. `gh run watch --exit-status` exited 0; refreshed JSON
confirmed exact head and both conclusions before merge. Checks included the
configured regression, compile and import checks; no local runtime is claimed.

Fetched dev baseline `815c2825cce038313322fc66bb96ccc7a7847207` was clean and
current. No-ff merge `caaa57c03007bdad44be2c8eb2dd921bf33c8a70` was pushed to
origin/dev. Setup is Done on the board; implementation remains Backlog until
these closure records pass exact candidate CI and reconcile into dev. No branch
was deleted; main, source, tests and configuration are unchanged.

Runner annotations reported existing checkout@v4/setup-python@v5 Node.js 20
deprecation (forced Node.js 24) and ubuntu-latest migration to Ubuntu 26.
These platform notices are retained as baseline CI environment limitations;
no zero-warning or unconfigured lint claim is made, and no out-of-scope workflow
upgrade was performed. Reviewer/Tester findings are not closed by integrator.
Final closure-candidate and reconciliation hashes/runs are reported at handoff
rather than fabricated as circular self-references here.
