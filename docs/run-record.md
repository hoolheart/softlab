# Run records: producing, inspecting, and comparing supported stored runs

Maintainer-facing guide for the additive run record shipped under M03
(softlab 0.3.0). It explains what a *supported stored run record* is, how
the two supported clients produce one, how to inspect one safely, and
what is explicitly not supported. For the full contract — closed schema,
numeric limits, error taxonomy, tolerance table, and the frozen
original-versus-rerun procedure — read the design documents referenced
below; this guide intentionally does not duplicate the schema.

## 1. What a supported stored run record is

A supported stored run record is the combination of:

- one HDF5 artifact written by the owned save seam
  (`softlab.shui.run_storage.save_run_group`) through an owned
  `HDF5DataBackend`;
- the existing `DataGroup`/`DataRecord` samples and column datasets on
  disk (the record is *additive*; it supplements, never replaces, the
  stored samples);
- one strict JSON-text envelope under the single shared metadata key
  `run_record` (`softlab.shui.run_record.METADATA_KEY`), validated by
  the closed codec (`JsonRunRecordCodec`);
- a closed client payload (`profile: 'thermal'` or `profile: 'signal'`).

The supported scope is deliberately narrow (joint boundary decision,
`log/release_12/design/p1-boundary-decision.md`):

- **Backend: HDF5 only.** SQLite persistence of run records is
  unsupported (tracked debt D2).
- **Clients: thermal coordinated (manifest v1), thermal direct (manifest
  v2), and the signal monitor** (`examples/signal_monitor.py`).
- **Outcome: the `completed_acquisition_before_persistence` snapshot
  only.** The stored record claims acquisition `completed` with equal
  requested/completed rows; `persistence`, `plotting`, `cleanup`, and
  `overall` are `not_yet_established`. Failed, cancelled, or partial
  runs are never persisted as supported records, and a stored record is
  never rewritten with later outcomes.
- The thermal legacy manifest (`thermal_manifest`) keeps its closed
  v1/v2 contract and is dual-written unchanged; the additive record does
  not reinterpret it.

## 2. Producing a run record

### 2.1 Thermal

Use the example's owned entry point (it performs acquisition, envelope
construction, save, reopen verification, plotting, and cleanup, and
refuses to persist anything but a completed acquisition):

```sh
MPLBACKEND=Agg python examples/thermal_experiment.py \
    --output-dir /tmp/thermal-run-01 --adapter coordinated
```

The artifact is `<output-dir>/thermal.h5`. `direct` selects the v2
timing policy; the adapter is the only call-side timing selector.

### 2.2 Signal monitor

The signal example owns a two-step seam: acquire, then persist through
the shared Phase 2/3 builders. All steps run in synthetic temporary
space; no fixture touches the repository tree:

```python
from pathlib import Path
import tempfile

from examples.signal_monitor import acquire, persist

with tempfile.TemporaryDirectory(prefix='softlab-run-') as root:
    result, record, differences, reads = acquire(points=3)
    saved = persist(result, record, Path(root) / 'signal.h5')
    print(saved.execution_id, saved.group_id, saved.path)
```

`persist` enforces the wiring contract:

1. **Acquisition-success gate.** `result.overall_success` is checked
   before any envelope is built or any save is attempted; a failed,
   cancelled, or incomplete acquisition raises and never reaches the
   storage seam.
2. **Identities.** `execution_id = str(result.run_id)` (the bounded
   execution UUID) and one freshly generated, distinct `group_id`.
3. **Group construction and attachment.** `build_run_group(name,
   envelope)` (empty group, id taken from the envelope) followed by the
   public `DataGroup.add_record(record)` before `save_run_group`.
4. **Envelope.** `build_signal_payload(points=3, offsets={'signal': 5,
   'reference': 2})` and `build_envelope(profile='signal', ...)`, with
   the declared-order columns `timestamp, signal, reference` taken from
   the acquired record.
5. **Owned facts.** The example supplies its own `software` and
   signal-owned `source` availability facts (exact file-bytes digest plus
   best-effort git revision/dirty state) and one explicit HDF5 target
   path.
6. **Save and cleanup.** `save_run_group` returns a `StorageReport`
   whose `primary_ok` and `cleanup_ok` are asserted; the seam owns the
   backend's verified disconnect.

Thermal's private source-capture helper is *not* reused: signal owns its
capture, and thermal's is scoped to its own reference file.

## 3. Safely inspecting a run record

Never load an original artifact in place: a plain HDF5 load is
destructive (tracked defect F-A), and thermal's plain load drops custom
metadata keys (F-B). The supported inspection path is
`softlab.shui.run_storage.inspect_run_group`, which hashes the original,
works only on a disposable byte copy, runs a read-only `h5py`
preflight, performs at most one legacy load, hydrates the captured
texts, re-verifies the original bytes, and reports primary and cleanup
diagnostics separately.

For a separate-process inspection (the form used by tests and
walkthroughs), use the bootstrap CLI from the repository root; it prints
one JSON payload on stdout and exits `0` on success:

```sh
python tests/run_record_inspect_bootstrap.py /path/to/signal.h5 \
    --profile signal
python tests/run_record_inspect_bootstrap.py /path/to/thermal.h5 --profile thermal
```

Warnings are fatal in the child except the recorded approved baselines.
`--group-id` selects a group explicitly; without it the artifact must
contain exactly one stored group. Repeat inspections on **fresh copies**;
the originals are never opened by a legacy loader.

## 4. What the envelope contains (high level)

- **Common outer layer** (`namespace: 'softlab.run-record'`, format
  version 1): profile, the two UUID identities, the ordered record
  declaration (name + columns with truthful units), software
  (softlab/Python versions), source availability facts (digest, git
  revision/dirty, each `available` with value or `unavailable` with a
  bounded reason), the acquisition-only outcome, and the payload.
- **Thermal payload**: exact `ThermalConfig` values, the frozen model
  declaration, procedure/timing literals (including the v1/v2
  delay/source-tick distinction), and the acquisition boundary.
- **Signal payload**: `points`, the frozen non-consumptive
  `tick + offset` read policy (offsets `signal: 5`, `reference: 2`),
  scheduler ownership, and the declaration that `difference = signal -
  reference` is caller-derived and never a stored column.

The full closed schema, limits, and validation rules are in
`log/release_12/design/p2-schema-design.md`; storage and inspection
behavior is in `log/release_12/design/p3-storage-design.md`.

## 5. Identity semantics: execution UUID vs group UUID

- `execution_id` identifies the bounded *execution* (one `run_count`
  run); it is captured as `str(result.run_id)` and appears in **no**
  stored UUID slot.
- `group_id` identifies the *persistent artifact*: it equals
  `DataGroup.id`, the HDF5 group name, and the on-disk `id` attribute.
- The two are generated independently, must differ (the codec rejects
  equality), and are fresh for every run — never compare them across
  runs and never backfill one from the other. Thermal's legacy manifest
  `run_id` is the *group* identity and must not be relabelled as the
  execution identity.

## 6. Known limitations and tracked debt

Honest status; none of these is repaired or waived by the run-record
work:

- **SQLite unsupported** (D2): run records persist on HDF5 only;
  selecting SQLite requires a separately reviewed storage/schema
  decision.
- **No atomicity**: no rollback, retry, or transactional publication.
  The supported writes are new-run HDF5 writes; the unchanged HDF5
  writer deletes an existing same-ID group before recreating it, so a
  failed save over an existing same-ID group can leave the previous
  group content lost (delete-then-recreate). When preserving prior
  content matters, write to a fresh target path.
- **No failed-run persistence**: only the completed-acquisition snapshot
  is supported; failed, cancelled, and partial runs leave no supported
  record.
- **No later-outcome updates**: persistence/plotting/cleanup/overall
  stay `not_yet_established` forever; there is no outcome history.
- **F-A** (plain HDF5 load is destructive): mitigated only by the
  disposable-copy inspection path; plain or repeated loads of an
  original are unsupported.
- **F-B** (plain load drops custom keys): mitigated by the explicit
  capture-and-hydrate seam; arbitrary custom-key round-trip is not
  promised, and thermal's legacy hydration keys are not imposed on
  signal.
- **F-C / F-D** (generic `io.reload_group`/`io.load_groups` defects):
  generic reload/list is excluded from the supported path.
- **D1** (`DataGroup(meta=...)` constructor defects): construction
  bypasses the constructor's metadata path entirely (empty group plus
  `set_meta`).
- **Column order on disk** is lexicographic by HDF5 enumeration and is
  non-semantic; envelope declared order is authoritative and comparison
  associates columns by name in declared order.

## 7. Comparing runs: the frozen oracle

For independent expected values, tolerances, exclusions, and the
two-session original-versus-rerun procedure (recording A and a clean
rerun B, each inspected in a separate process), use the oracle freeze:

- `log/release_12/design/p4-oracle-design.md` — frozen oracle tables
  (thermal v1/v2, signal), tolerances (e.g. `temperature_degc` at
  `abs_tol=1e-10`, integer columns exact), exclusions (timestamps, UUID
  values, environment facts), and the §5 clean-session procedure.

Freeze deviations are never edited silently: they reopen through review.
