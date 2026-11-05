# M03 architecture review

Reviewer: independent architecture reviewer | 2026-10-31 Asia/Shanghai
Verdict: **APPROVED**

Reviewed [M03 plan](milestone-03.md), [roadmap](roadmap.md), [plan index](README.md),
`softlab/shui/data/base.py`, HDF5 and SQLite backends, thermal manifest handling,
bounded count result and station/parameter descriptions. Verified the M02
closure record exists at local `e827e7f`. This is source and planning review,
not new runtime evidence or acceptance.

## Findings

- **M03-AR-01 — decision fallback conflicts with later increments.** W01 can
  reject a common schema and select separate client-specific records, but W02
  assumes an accepted smallest schema/codec for supported clients and W04
  requires both clients to demonstrate the selected scope. Define the fallback:
  Product and Architect must revise affected deliverables and milestone exit
  before W02, or explicitly state which separate records and comparisons are
  accepted. Do not treat selection of no common seam as an automatic pass or
  proceed under contradictory acceptance language.
- **M03-AR-02 — second client currently has no persistence or reload contract.**
  `examples/signal_monitor.py` returns in-memory result, record, differences and
  read trace; it does not persist or reconstruct a runnable setup. W01 must
  characterize that actual limitation and specify who constructs the distinct
  client's stored record and rerun inputs, the selected backend, field origins,
  and an independent sample oracle before W03/W04 acceptance. Do not infer a
  clean-session record or replay capability from the current client alone.
- **M03-AR-03 — sequence must preserve disk-read safety in every increment.**
  W01 requests actual backend/source round-trip evidence and W03 requires faults
  preserve originals. HDF5's current loader pops live reserved attributes and
  constructor metadata is dropped; an ordinary direct read can modify the file
  and cannot establish lossless metadata. Make explicit that W01 characterization
  uses disposable copies and independently inspects disk metadata, not merely a
  `DataGroup` returned by the legacy loader. If no safe path exists, treat it as
  a scoped decision/blocker before claiming any supported persistence boundary.

## Scope assessment

The plan correctly separates persistent group and in-memory execution UUIDs,
completed-acquisition snapshots from later lifecycle outcomes, absent versus
invalid manifests, and structural setup descriptions from runtime values. It
recognizes that HDF5 stringifies metadata and SQLite does not currently persist
arbitrary group metadata, and does not promise general backend parity, hardware
qualification, arbitrary Python serialization or durable recovery. The four
increment structure is plausible once the fallback and evidence above are
resolved. Project gates and baseline dispositions are identified; exact candidate
CI, promotion and fresh-session claims remain future work.

## Independent re-review and closure

The revised plan requires an approved W02–W04 and exit revision if W01 rejects a
common schema (M03-AR-01); identifies the signal monitor's in-memory limitation
and requires W01 to assign stored-record/rerun ownership, backend, field origins
and independent oracle (M03-AR-02); and mandates disposable HDF5 copies with
independent on-disk metadata inspection and a blocker decision when no safe
read path exists (M03-AR-03). The architecture reviewer closes all three
findings. This approval is for planning only and does not certify implementation,
execution, CI or milestone acceptance.
