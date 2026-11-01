# Phase 3 Storage/Inspection Test Plan — Developer Review (implementability)

Reviewer: sw-tom (Developer) | 2026-11-01 Asia/Shanghai
Branch: `codex/m03-p3-storage`
Input: [log/release_12/test/storage-test-plan.md](../test/storage-test-plan.md)
(commit `f25c497`) against
[log/release_12/design/p1-boundary-decision.md](../design/p1-boundary-decision.md)
(option (a), §1.1, §2 ownership/NON-support, §4 D1/D2/F-A/F-B/F-C/F-D, §6
items 5–8), [log/release_12/prd.md](../prd.md) (Phase 3 row, **M03-AC03**,
**M03-AC04**), [p2-schema-design.md](../design/p2-schema-design.md) (§9
deferral list), [characterization-results.md](../test/characterization-results.md)
(D1/D2, F-A/F-B/F-C/F-D, CI corrections), and the current source:
`softlab/shui/data/hdf5.py`, `softlab/shui/data/io.py`,
`softlab/shui/backend.py` (`catch_error`), `softlab/shui/data/base.py`
(`DataGroup` constructor), `softlab/shui/run_group.py`
(`build_run_group`), `examples/thermal_experiment.py` (`execute`,
`_load_group`/`_reload_group`, `inspect_stored`).

Method: source-level inspection only — no test code written, no source
modified.

## Verdict

**CHANGES REQUESTED** — the plan's scope discipline, placeholder policy,
safe-read path structure, three-way distinction, fault-matrix universal
assertions, unsupported-paths exclusions, legacy-compatibility and
portability layers are all sound and implementable against the current
codebase **without silent repairs** (positive confirmations below). Five
findings must be closed in plan text first. Findings 1–3 are correctness
issues that would make specific planned assertions unimplementable or
vacuous as written; findings 4–5 are coverage ambiguities against the
plan's own scope statement and design §9's Phase 3/4 split. All fixes are
additive plan-text clarifications or placeholder extensions; no
re-planning and no scope change to the decided boundary is needed.

## Findings

### Required fixes

1. **[High] A1 has no source of record datasets — `build_run_group`
   returns an empty group.** The plan's central A1 assertions (A1.2)
   require the stored artifact to contain record datasets with the
   declared ordered columns, a stored row count equal to
   `outcome.completed_rows`, and sample values matching a
   construction-time oracle. But the Phase 2 seam
   (`build_run_group(name, envelope)`, `softlab/shui/run_group.py`)
   deliberately constructs an **empty** `DataGroup` — no `DataRecord`,
   no rows — and the plan never states where the record datasets come
   from. As written, A1's sample/schema/row-count assertions have no
   implementable input. Fix (either branch, plan-text only): (a) state
   that A1 fixtures attach the record themselves via the existing public
   `DataGroup.add_record(...)` API before calling the owned save
   (columns taken from the envelope's `record.columns`, rows synthetic
   fixture values known at construction time — this is fully
   implementable today and keeps Phase 3 save-path scope clean); or (b)
   extend **[P3P-01]** to freeze a sample-attachment argument/seam in the
   owned save entry signature. Without one of these, the plan's most
   important area is not executable.

2. **[High] A4.2/A4.3 candidate injection seams may fail at the wrong
   stage or not at all — empirical verification must be mandatory.**
   The fault matrix's own injection discipline ("no mocks that skip or
   replace the real code path") is binding and correct, but two of the
   plan's example parentheticals do not hold against the real backend:
   - *"target path is an existing directory"* (`hdf5.py:39`,
     `h5py.File(path, 'a')`) raises at **connect**, not save — it would
     silently test an A4 connect-class fault inside the A4.2 save-class
     row (the matrix has no connect-failure row; connect-status checks
     live only in A1.3).
   - *"parent directory is removed before save"*: on POSIX, h5py holds
     an open file descriptor; after the parent directory is removed the
     path is unlinked but writes via the descriptor proceed into the
     unlinked inode — the save can succeed silently, making the probe
     vacuous.
   - Similarly for A4.3: h5py `File.close()` is idempotent, so "file
     handle externally closed" may not fail at disconnect; "scratch
     removed before disconnect" does not fail a descriptor `close()` on
     POSIX either.
   Fix: **[P3P-05]** must require that every frozen mechanism is
   **empirically verified to fail at the named stage** on both supported
   interpreters before design freeze (a seam that fails at the wrong
   stage reclassifies the row; a seam that does not fail is vacuous and
   invalidates universal assertion (a)). The plan text's example
   parentheticals for A4.2/A4.3 should be marked as unverified
   candidates. One verified-save-failure real seam exists today for
   reference: connect to a valid path, externally `close()` the h5py
   handle, then call the real `save_group` — `save_impl`
   (`hdf5.py:55`) operates on a closed file and fails through
   `catch_error` with the error recorded in `backend.errors`
   (`backend.py:169-188`), which is exactly the observable the plan's
   universal assertions require.

3. **[Medium] Universal assertion (b) "no artifact appears at the target
   path" is only true for pre-connect faults.** `connect_impl` opens
   `'a'` and creates the target file at **connect** — before save. Any
   fault after a successful connect (A4.2 save failure, A4.3 disconnect
   failure) leaves an empty or partially written HDF5 artifact at the
   target path. Asserting non-existence there would be false; asserting
   nothing would hide exactly the "what is and is not guaranteed" truth
   the universal assertions demand. Fix: scope the "no artifact" claim
   explicitly to pre-connect faults (A4.1), and pin per-row post-connect
   artifact-state expectations (e.g. "empty file exists, byte content is
   exactly the connect-time superblock, no group/dataset/record content"
   or "removed by cleanup", whichever the frozen design **[P3P-03]**/**
   [P3P-05]** chooses — but one of them must be chosen and asserted).

4. **[Medium] Example dual-write integration is claimed in scope but
   pinned in no A-row.** The plan's scope paragraph claims Phase 3
   includes "the example-level integration that dual-writes the additive
   record alongside the unchanged legacy thermal manifest", and design
   §9 assigns example wiring to "Phase 3/4 integration work". Yet no
   planned check runs `thermal_experiment.execute()` on a synthetic
   config and asserts, via the safe path, that the stored artifact
   carries **both** `thermal_manifest` and `run_record`, that the legacy
   manifest bytes/versions (1/2) and eleven-field contract are
   unchanged, and that the additive envelope decodes. A1.2's coexistence
   bullet covers only test-constructed fixtures. Fix (either branch):
   add an explicit dual-write row (natural home: A1 or A7 — e.g. run
   both adapters on synthetic temp dirs, inspect via A2's safe path,
   assert coexistence + legacy-content equality), **or** state verbatim
   that example wiring is deferred to Phase 4 and narrow the scope
   paragraph to match design §9. The current ambiguity means the scope
   claim cannot be evidenced by the plan's own verdicts.

5. **[Low] Signal-profile Phase 3 coverage is unstated.** The scope
   paragraph lists "signal-monitor owned persistence (conditional)"
   among the design §9 items this plan picks up, but no A-row names the
   signal profile; A2.4's signal clause is an absence-assertion
   ("does not impose thermal's legacy keys on the signal profile") with
   no positive signal fixture to apply it to. Decision §1 makes signal
   support conditional on its owned persistence path existing. Fix: one
   sentence recording which applies — either a named signal-profile row
   (construct via the Phase 2 seam with `PROFILE_SIGNAL`, save, inspect,
   assert hydration carries `run_record` and no imposed legacy keys), or
   an explicit recorded deferral of the signal client to Phase 4 with
   the conditional clause cited.

### Placeholder completeness (review question 4)

The seven placeholders **[P3P-01…07]** are the right freeze set — nothing
is over-scoped (each is genuinely a design decision the tests cannot
assert without). Two additions/extensions are required, both falling out
of the findings above:

- **[P3P-01] extension:** freeze (a) the sample/record attachment
  mechanism per finding 1, (b) the per-profile hydration key set that
  A2.4 asserts (thermal: the three `_META_KEYS` plus `thermal_manifest`;
  signal: none — the empty set is a frozen literal too), and (c) what
  "legacy context still available" means for A3.3's absence outcome at
  the chosen seam placement (raw manifest text vs decoded legacy
  envelope — manifest decoding currently lives only in the example
  layer, so this interacts with **[P3P-06]**).
- **[P3P-05] extension:** the mandatory empirical failure-stage
  verification per finding 2, including the wrong-stage reclassification
  rule.

No new placeholder beyond these extensions is needed; P3P-02/03/04/07
are correctly scoped as-is.

### Minor observations (non-blocking)

6. **A1.1 "save succeeds only after the completed-acquisition gate"**
   conflates two layers. The completed-acquisition gate is a **call
   site** (example-layer) property per decision §2; a `shui`-level owned
   save function cannot observe it — it only sees that the codec already
   rejected failed/cancelled envelopes (A5.2's negative probes). If the
   example dual-write row from finding 4 lands, the gate assertion
   belongs there ("`execute()` constructs and saves the record only
   after `result.acquisition == 'completed'`"); A1.1 should then be
   reworded to assert what the save entry point can actually observe
   (envelope rejected before connect vs saved). Wording only.

7. **A5.2 "no second `set_meta` on the outcome key"** — there is no
   `outcome` metadata key; the outcome lives inside the single
   `run_record` text. The recorder assertion should be "no `set_meta`
   call on any key after the save's attribute write" (or, at the
   example layer, "no manifest/record rewrite after persistence"),
   otherwise the guard is unimplementable as phrased. Wording only.

8. **A2.2 preflight list is exactly right** for backend-written files
   (`id`, `name`, `timestamp`, `backend`; `hdf5.py:78-86`), and A4.8's
   hand-built fixture correctly covers the missing-attr case. Noted as
   verified, no action.

## Verified as implementable without silent repairs (positive confirmation)

1. **Owned HDF5 write path.** `HDF5DataBackend.save_impl`
   (`hdf5.py:65-94`) writes `str(value)` attributes and per-char C2.3
   already-encoded JSON text survives verbatim; the envelope text from
   `build_run_group` flows through unchanged. Every A1.2 on-disk
   assertion (group name == `str(group_id)`, `id` attr, no
   `execution_id` in UUID slots, `run_record` text decodable by
   `decode_envelope`, ordered columns/units/rows) is checkable via
   `h5py.File(copy, 'r')` exactly as planned. Legacy `thermal_manifest`
   coexistence is a `set_meta` addition on the same group — no legacy
   writer change.
2. **Additive-key hydration without legacy behavior change (F-B).** The
   mechanism exists verbatim in-tree as a pattern: `_load_group`
   (`thermal_experiment.py:674-693`) reads `attrs[...]` **before** the
   single legacy `load_group` and re-`set_meta`s afterwards. The Phase 3
   inspection path can do the same for `run_record`: capture
   `attrs['run_record']` on the copy, at most one `load_group`, then
   hydrate. Files without the key are untouched — absence is
   `key not in attrs` on the disposable copy, so legacy-only files
   behave exactly as before (A3.3 absence case; A7.2). Thermal's three
   `_META_KEYS` plus manifest and signal's empty legacy-key set are
   both expressible per profile, satisfying the F-B condition ("not
   arbitrary custom-key hydration, no thermal keys imposed on signal").
3. **F-A guard is enforceable.** `load_data_group_from_hdf5_group`
   (`hdf5.py:128-144`) pops `name`/`id`/`timestamp`/`backend` from the
   live attrs proxy and persists the pops on close ('a' mode) — the
   characterized destructive chain. At-most-one-load-per-copy plus
   fresh-copy re-inspection is therefore not conservative excess but
   the only safe repeat strategy; the plan's A2.1/A2.3 byte-hash and
   call-inventory assertions exactly match the real hazard.
4. **Backend failure observability matches the plan's model.** All
   backend methods are wrapped by `catch_error` (`backend.py:169-188`):
   failures return sentinel values (`False`/`None`/`[]`) and are
   recorded in `backend.errors`, never raised. So "frozen error type
   **[P3P-02]** raised/surfaced, never logged-and-swallowed, never
   `None`-means-absence" (universal assertion (a)) is implementable
   **only** at the new Phase 3 seam layer, which must convert recorded
   backend errors into its frozen taxonomy — the plan consistently
   places the assertion there (A1.3, A4), which is correct.
5. **SQLite/io.py exclusion is truthful (review question 3).** D2
   re-verified in source: `sqlite3.py` schema persists only `sample`/
   `run_id`, and the reconstructed meta dict is dropped by the
   `DataGroup(meta=...)` constructor loop (`base.py:434-438` writes
   into `self._meta` only via the defective path — net observable `{}`,
   matching C2.4). F-C re-verified: `io.py:121` passes `group.id` (a
   `uuid.UUID`) to `load_group`, which `h5py` rejects ("Accessing a
   group is done with bytes or str"), the error swallowed by
   `catch_error` → `reload_group` returns `None` (io.py:119-124). F-D
   re-verified: `io.py:95-98` maps `load_group` over `list_impl`
   output whose ids are row tuples on sqlite → every load fails →
   `[]`. A6's "explicitly rejected / not the supported path" framing is
   therefore accurate, and the A6.1 guard is implementable as a
   backend-type check inside the new seam (no `sqlite3.py` change).
6. **Three-way distinction matches on-disk reality (review question
   2).** Backend-written files have **all-text** attrs
   (`hdf5.py:81-82`), so "present non-text" cannot occur on files the
   supported writer produced — it arises only on raw-`h5py` fixtures,
   exactly as A3.3 states and C3 characterized (native `numpy.int64`/
   `numpy.float64` on hand-built files). Malformed/wrong-namespace/
   unsupported-version/over-size are all text-level cases fed to the
   frozen Phase 2 decode path, whose frozen templates the plan quotes
   verbatim (`run_record: malformed JSON: ...`, `version: unsupported
   version (supported: 1)`, `run_record: encoded size exceeds limit
   131072 bytes` — all present in design §8.4). Legacy absence is the
   only non-error outcome, matching decision §6 item 6.
7. **Version portability and no-autospec discipline are sound (review
   question 5).** A4's injection-discipline paragraph correctly carries
   the Phase 1 lesson (CPython gh-75988: 3.9 autospec does not delegate
   to `wraps`; recording spies patched without `autospec=True`), and
   A8.4 requires both-interpreter evidence with conditional warning
   expectations (the 3.13-only `sqlite3.version` pattern). No planned
   assertion depends on interpreter-specific behavior; `h5py`
   idiosyncrasies called out in finding 2 are environment facts to be
   pinned by **[P3P-05]**, not portability violations in the plan.

## Closure conditions

This review closes when the plan text addresses findings 1–5 (additive
edits plus the two placeholder extensions), after which no further
developer input is anticipated for findings 6–8. The executable suite
remains gated on the reviewed Phase 3 detailed design freezing
**[P3P-01…07]** (as extended) per the plan's own gate-linkage section.
