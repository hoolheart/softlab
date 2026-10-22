# M01-W03 architecture envelope — [PLANNED]

Architect: sw-jerry. 2026-10-22 Asia/Shanghai. PRD subject affe67b;
baseline dev410bc16. **FEASIBLE as bounded example-local work**; executable
characterization, approved tests and detailed design remain gates. This is an
architecture decision/envelope, not detailed interfaces or implemented capability.

## Representation decision after backend review

Choose **one explicit JSON-text manifest envelope in existing DataGroup metadata**,
written via set_meta before the existing HDF5 save. No sidecar. HDF5DataBackend
save_impl (softlab/shui/data/hdf5.py) writes every metadata value with str(value)
as a group attribute; passing a dict directly loses JSON typing/encoding. A
strictly encoded JSON string is preserved by this path without a backend format
change. One HDF5 artifact also avoids introducing cross-file pairing and missing
sidecar failure modes; its own save is still not atomic or guaranteed byte-free
on failure. The manifest and numeric data share a group/run identity that must
be checked after decoding. No multi-artifact durability promise.

Actual legacy loader aliases attrs and pops identity fields before constructing
DataGroup; base.py constructor does not populate _meta from its metadata input.
Preserve W01/W02 copy-only reload and explicit disk-text hydration. Current example
_load_group reads only _META_KEYS ('example','configuration','timing'); detailed
design must extend the example-local capture/hydration/inspection path to include
the envelope when present, without making it mandatory for historical files.
Capture raw presence before decoding: absent legacy context, corrupt present
metadata and unsupported schema/version are distinct outcomes. Never hydrate from
reader defaults or silently repair a mismatch. The copied loader must leave the
delivered original bytes/attributes untouched. SQLite parity is excluded.

Read sources/callers: softlab/shui/data/{hdf5,base,backend}.py,
examples/thermal_experiment.py (_META_KEYS, _load_group, _reload_group,
_verify_round_trip, _acquire, execute/run), tests/test_thermal_example.py metadata/
corruption/reload assertions, tests/test_thermal_execution.py outcome/cleanup
callers, W02 acceptance/architecture and milestone plan. These are static source
findings, not executed characterization. Tester must verify assumptions before
implementation. No prerequisite currently requires library repair.

## Ownership and snapshot boundary

Manifest schema belongs solely to the thermal example, with closed identifier/
version and declared fields. Designer specifies exact nesting/names, strict type
validation/encoding/decoding and diagnostics after test-plan approval; no arbitrary
user metadata serializer, Python object constructor or reusable provenance layer.
Use only schema-declared JSON scalars/containers, finite numbers and string keys;
bool must not pass integer count checks. Reject malformed/unsupported present
content and identity/count/schema/timing inconsistency explicitly.

Required meaning: full declared ThermalConfig/model/procedure and validity limits,
ordered record columns/units; run UUID; actual requested/completed counts and
committed tick/time; acquisition/source-boundary relation, dt and previous-boundary
delay; wall timestamp meaning separate from simulation clock; softlab/Python and
reference source identity. Timing relationships must be validated against loaded
sample columns (not merely compared with reader constants). Valid changed
configuration must retain its own dt/count/power/model values and interpretation.

Capture source identity at production, not inspection: declared covered bytes
for any digest, actual available revision/worktree status or explicit unavailable
identity/reason. Scope Git commands to the reference source repository; an unrelated
working directory must not become its provenance. Dirty state cannot claim clean
revision as complete identity. Git absence must not break ordinary installed
source use. Exact identity surface and test injection belong to detailed design;
no dependency lock/calibration/full-environment reproducibility claim.

Attach the envelope only after completed acquisition and before persistence.
Its named snapshot is **completed acquisition before save/reload/plot/cleanup**:
record completed acquisition/counts/boundary, explicitly leaving later outcomes
unknown/not yet established. Never predeclare verified persistence or overall
success. Existing current RunResult remains authoritative for later operations.
Cancelled/failed acquisitions keep complete partials in memory with persistence/
plot not attempted and no durable manifest. Existing run success return and
W02 operational failure/cleanup contracts remain compatibility constraints.

Independent fresh-process inspection must recover stored typed fields and numeric
record without original group/model/result. It verifies identity, actual counts,
column order/units and clock/delay/sample consistency; missing legacy context is
explicitly unavailable while numeric inspection remains possible. Recovered
source/software identity stays the producer's captured identity. One documented
headless run plus fresh-process command provides reproducible evidence.

## Scope and debt

Keep tu/huo/shui/jin responsibilities and actual arch.md unchanged during setup.
Implementation touches example/guide/tests and process records, followed by actual
architecture closure. Retain unfixed R4-BASE-SCHED-01/META-01/HDF5-01 and
OBS-004/005/006/DEFECT-2; private executor, text hydration and copy-only reads must
regress. No backend repair/migration, SQLite parity, framework, dependency/Python
change, partial resume/retry, W04/hardware/UI or publication. A demonstrated
prerequisite blocks affected implementation for Architect/PO scope disposition.
