# M01-W03 closure CI runner platform blocker

Tester: sw-mike. Date: 2026-10-24 Asia/Shanghai.
Candidate399df915583ee156d2de6ba713aabbae3440a78c,
[run40106490821](https://github.com/hoolheart/softlab/actions/runs/40106490821).
Status: **OPEN — closure integration/promotion blocked**.
No configuration/source/test/dependency edits. Architect/PO scope review next.

## Actual primary evidence

Queried `gh run view 40106490821 --json headSha,status,conclusion,jobs` and
fetched both named job logs. Exact head matches; run completed/failure.

- [Python3.9 job120909146681](https://github.com/hoolheart/softlab/actions/runs/40106490821/job/120909146681):
  completed/failure at actions/setup-python@v5. Runner Ubuntu26.04.1,
  image ubuntu-26.04, image release ubuntu26/20261012.163. Error:
  `The version '3.9' with architecture 'x64' was not found for Ubuntu 26.04.`
  Local tool cache also lacked3.9. Checkout succeeded. Cache configuration,
  dependency installation, environment collection, VISA check, regression,
  compile and import all SKIPPED. No Python3.9 runtime behavior was tested.
- [Python3.13 job120909146822](https://github.com/hoolheart/softlab/actions/runs/40106490821/job/120909146822):
  now completed/success (supersedes earlier ongoing state). Actual log records
  Python3.13.16 and275 tests/26.235s/OK; API confirms dependency/VISA/regression/
  compile/import and completion success. This cannot substitute for missing3.9.

Current .github/workflows/ci.yml uses runs-on ubuntu-latest and unchanged matrix
['3.9','3.13'], checkout@v4/setup-python@v5, existing install/check commands.
Prior accepted/verified W03 candidate logs ran Python3.9.25 on Ubuntu24.04;
this new ubuntu-latest resolution moved to26.04. No code/dependency regression is
established by a setup failure before those steps execute.

Read official [actions/python-versions manifest](https://github.com/actions/python-versions/blob/main/versions-manifest.json)
via GitHub contents API, saved /tmp/w03-platform-python-manifest.json. At actual
inspection, versions3.9.25 and3.9.24 list Linux x64 targets22.04 and24.04;
no3.9 Linux x64 artifact has platform_version26.04. This directly corroborates
the setup action's platform-specific availability error, not a guessed version.
Official runner identity is recorded in the job's
[included-software image](https://github.com/actions/runner-images/blob/ubuntu26/20261012.163/images/ubuntu/Ubuntu2604-Readme.md)
and [image release](https://github.com/actions/runner-images/releases/tag/ubuntu26%2F20261012.163).
Manifest availability is time-specific, not an indefinite upstream guarantee.

## Bounded proposal — not implemented

Propose changing only runs-on from ubuntu-latest to **ubuntu-24.04** for the
existing compatibility job. This restores the previously demonstrated runner
family with an officially listed3.9 x64 artifact and preserves both Python3.9/
3.13 matrix entries and every install/VISA/regression/compile/import check.
No dropping Python3.9, supported-version change, dependency upgrade/pin, action
upgrade, warning-policy expansion or production modification. This bounds the
named CI platform; it does not certify every Ubuntu release or lock dependencies.

Require Architect/PO/coordinator authorization before CI edit, independent review
of the minimal diff, then actual exact replacement-candidate both-version setup/
regression/compile/import success before integration. Only actual execution may
close this platform gate; prior local/full checks or3.13 success cannot waive it.
No repeated speculative CI retries or broader online investigation needed.

Known parser/pandas baseline warning debt and prior dispositions remain open.
This failed job separately prints Node punycode deprecation and Node20-forced24
platform notices; no warning-free CI claim or new filter is proposed. Existing
PO product acceptance does not authorize skipping the required CI/promotion gate.
Integrator paused dev/main; Tester performs no merge or policy change.

Actual investigation commands also checked clean baseline/status and
`git diff --check` before immediate docs(log) evidence commit/push. Saved job logs:
/tmp/w03-closure-platform-py39.log and /tmp/w03-closure-platform-py313.log.
These are remote-log/official-artifact findings, not a local3.9 runtime test or
completed replacement-platform check. Failure remains OPEN pending reviewed fix.
