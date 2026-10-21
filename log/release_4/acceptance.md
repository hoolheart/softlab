# M01-W01 release acceptance

Product Owner: sw-camille. Date: **2026-10-21 Asia/Shanghai**.
Verdict: **PASS — M01-W01 bounded reference experiment accepted**.
Accepted implementation is integrated dev `c89ffe6689484f1ea1eaf7088e8e2592fa21908d`.
Process records reviewed on task branch at `c522b03469a125fdd565a7bc988f60d6c6f879db`.
Kickoff was 2026-10-18; execution resumed and acceptance completed 2026-10-21.
This is weekly product acceptance, not M01 completion, main promotion or publication.

## Product outcome and method

The [activated PRD](prd.md) promised one headless synthetic experiment connecting
existing model, acquisition, data, persistence and plotting primitives. The
[guide](../../docs/user-guide/thermal-experiment.md) supplies setup and the exact
repository-root command, inputs, stored schema, delayed-observation interpretation,
outputs and safe repeated inspection. Product inspection of that guide and the
[example](../../examples/thermal_experiment.py) establishes a coherent user journey:
a fresh output directory produces announced HDF5/PNG paths and a fresh UUID only
after verified reopening and cleanup. The first observation at one simulated
second is correctly the initial 20 degC, not an instantaneous thermometer claim.

This acceptance reviews committed requirements, delivered guide/example and owned
independent evidence; the Product Owner did not rerun tests, implement, design or
close technical findings. Runtime evidence is owned by the Tester in
[test results](test/m01-w01-001-test-results.md), including 229 tests under
`-W error`, compile/import, two documented CLI runs, disk/image inspection and
collision refusal. The 210 legacy tests remain included. Those local runs are
historical 2026-10-18 evidence, not mislabeled as new runs on acceptance day.

## Complete requirement traceability

Every row is PASS for this bounded delivered scope, based on the independent
Tester's corresponding AC/NFR row plus product document inspection.

| PRD requirement | Accepted evidence / observable result |
| --- | --- |
| FR-01 / AC-01 | Documented headless root CLI executed twice; announced HDF5/PNG; no instruments. |
| FR-02 / AC-02 | Invalid scalar/sign/stability/count matrix fails before graph creation or final artifacts. |
| FR-03 / AC-03 | Eight steps/eight rows; literal trace and independent Decimal oracle; no warmup/reset. |
| FR-04 / AC-04 | Ordered schema, units and finite values checked; time 1–8, source boundaries 0–7; Unix timestamp separately explained. |
| FR-05 / AC-05 | Fresh UUID, declared disk-derived text metadata and decoded configuration/timing inspected; repeats differ in identity. |
| FR-06 / AC-06 | Independently reopened owned copy matches identity/schema/values; source digest/reserved attributes preserved; failed/corrupt backend outcomes rejected. |
| FR-07 / AC-07 | Plot consumes reopened record, carries seconds/degC, matches data; decoded nonempty PNG; owned figures closed. |
| FR-08 / AC-08 | Occupied/file/symlink preflight refusal; sentinel and delivered source bytes preserved; no acquisition. |
| FR-09 / AC-09 | Model/sample/backend/corruption/plot failures remain unsuccessful and visible; no fabricated rows/retry; incomplete output identified. |
| FR-10 / AC-10 | Owned scheduler/loop/backend/figure cleanup and borrowed-state isolation verified; final boundary inspected, no teardown steps/reset; CR-01 drain/close failure regressions pass. |
| FR-11 / AC-11 | Two fresh in-process and CLI runs have equal numeric traces, eight rows each, different UUIDs and reopenable files. |
| FR-12 / AC-12 | Tester followed guide command; product review confirms setup/model/layers/timing/output/failure/limitations and inspection instructions. |
| NFR-01 | Full 229-test regression includes legacy contracts; no library imports/hooks/codecs/storage implementation changed. |
| NFR-02 | No dependency/Python-range change or simulation sleeps; actual Python 3.9/3.13 CI confirmed below. |
| NFR-03 | Default eight-step bound and termination checked; observed CLI durations recorded, no SLA claimed. |
| NFR-04 | Typed English example helpers and five-element boundaries inspected and independently reviewed. |
| NFR-05 | Temporary synthetic outputs; no generated experiment data, PNG, environment or notebook outputs tracked. |

The lifecycle requirements are included in AC-09/10: sampling failure after a
committed tick is not rolled back; failed acquisition does not save; storage is
not atomic; plotting failure can retain verified HDF5 while command fails.
Cleanup obstruction is reported rather than falsely claiming released resources.
Best-effort interruption does not promise synchronous preemption or physical stop.

## Independent gates, integration and CI disposition

[Code review](reviews/m01-w01-001-code-review.md) is APPROVED; CR-01 was closed
by sw-celeste, and the associated runtime regression by sw-mike. Tester contract
corrections TEST-01/02/03 remain Tester-closed. [Principle inspection](principle_compliance_report.md)
records all eleven principles PASS at task/preacceptance applicability; actual
architecture was reconciled at `af1dc74`. No reviewer/tester issue is reopened or
closed by this product acceptance.

[Integration](integration.md) records candidate `6bdff20` run
[39617262798](https://github.com/hoolheart/softlab/actions/runs/39617262798)
overall success but an inconsistent candidate Python 3.13 per-job API status.
That inconsistency is retained, not rewritten as explicit candidate job success.
For the exact accepted dev merge, run
[39617920072](https://github.com/hoolheart/softlab/actions/runs/39617920072)
is completed/success and both Python 3.9 and 3.13 jobs explicitly completed/success.
This stronger exact integrated-head evidence resolves the product compatibility
acceptance concern without claiming the candidate API inconsistency disappeared.
CI executes its configured checks; only named local checks establish `-W error`.

## Baseline debt disposition and limits

Accept the explicitly reviewed/tested bounded compatibility measures without
closing library debt: R4-BASE-SCHED-01 uses the example-only serial public-action
scheduler; R4-BASE-META-01 uses explicit metadata creation and disk-only hydration;
R4-BASE-HDF5-01 uses disposable-copy reads preserving the delivered file. Broader
repairs remain deferred to separately authorized, characterized work. Historical
OBS-004/005/006 and DEFECT-2 retain their existing tracked status outside unchanged
public paths. Writable temporary caches resolved baseline environment diagnostics.
No observed new warning/failure remains in named checks; no unconfigured lint or
universal library correctness claim is made. The guide exposes the destructive
legacy reader limitation and safe inspection path to users.

No hardware/firmware, physical qualification, UI/Figma, W02 cancellation/resume,
W03 manifest, W04 adapter, new supported API/framework, package version/tag or
publication is accepted or authorized here. M01-W02–04 and the whole first
milestone remain future scope. Final closure-document principle reconciliation,
exact closure candidate CI and follow-up dev integration remain required before
any main promotion; this PASS satisfies the Product Owner acceptance prerequisite
but does not claim those later actions have occurred.
