# TU-008 architecture design review

Reviewer: sw-jerry (architect). Candidate: `b65bd81` on
`codex/tu-008-integration` (`log/release_1/design/TU-008.md`). Verdict:
**CHANGES REQUESTED** — two design-text-level issues, both minor and
precisely scoped; no redesign, no test-plan change, no re-litigation of
accepted decisions. Scope is detailed-design compliance with the Release 1
PRD (TU-008 row and acceptance-status section), the revised TU-008 test
gate (`log/release_1/tests/TU-008.md`), its implementability review
(sw-tom, issue 1 resolved by the revised gate; the 10-section
`docs/tu_extensions.md` checklist adopted as binding), the TU-001
compatibility matrix (OBS-001..OBS-006, DEFECT dispositions), the
TU-002..TU-007 detailed designs as authoritative behavior sources, and the
five-module architecture. This is not a code review, test execution or
integration approval.

Independent verification performed for this review (branch tip `b65bd81`,
`.venv`, Python 3.13.15):

- Reproduced P1 step 1 exactly as specified:
  `.venv/bin/python -W error -m compileall -q -f softlab tests` exits 1,
  failing on **exactly** the two files/lines the design cites
  (`softlab/tu/station/parameter.py:580`,
  `softlab/jin/validator/implements.py:470`), both inside
  `if __name__ == '__main__':` example blocks, with the exact "before"
  text quoted in the design. No other file trips the forced compile.
- Verified the documented API surface against live objects:
  `Parameter.describe/describe_reading/read` (a `read()` returns
  `Reading(value, acquired_at, quality, error, uncertainty,
  calibration)`), `Device.describe/prepare/cleanup/initialized/supports`,
  `Station.describe`, `VisaHandle.initialized/supports/prepare/cleanup/
  timeout_seconds/abort/confirm_stop`, `VisaCommand.execute/
  describe_operation`, `TheoryModel.describe/supported_configuration/
  configuration/configure/evaluate_features` — all present;
  `describe()` payloads carry `schema_version: 1`; the `VisaHandle`
  sentinel contract matches the design (default `timeout_seconds=5.0`
  forwarded as 5000 ms, explicit raw `timeout` wins and is never
  rescaled, `None` disables); DEFECT-2 warning site confirmed at
  `parameter.py:262-263`. Builder helpers
  (`register_device_builder`, `get_device_builder`,
  `set_default_station`, `default_station`) importable;
  `softlab.__version__` is 0.3.0.
- Full suite at the tip: 99 tests, 98 pass, 1 fail (the designed RED
  case 7) — matches the gate record.
- Confirmed no `docs/` directory exists at the repository root, and that
  `AbortReport` is importable from `softlab.tu.station.visa` but is
  **not** re-exported at the `softlab.tu.station` package level
  (verified `ImportError`; integration case 1 imports it from the
  `visa` module).

## Assessment against review criteria

1. **Release-fit — confirmed.** The design constrains
   `docs/tu_extensions.md` to implemented behavior only (present tense,
   no plans or rejected alternatives), enforced by the DC-2 walk against
   live objects. The Limitations section honestly separates resolved
   observations (OBS-001/002/003, recorded for transparency with their
   TU-006 resolutions matching `compatibility.md`) from open limitations
   (OBS-004 init-value hazard, OBS-005 lenient swallow with the strict
   escape hatch, OBS-006 delegated-name collisions, DEFECT-2 deliberate
   warning, description serialization limits per the TU-007 handoff).
   Nothing is silently dropped. No scope expansion: no `mu`/`huo`
   changes (huo exercised read-only, as the gate records), no runtime or
   tooling dependency changes (`build` install is a recorded procedure;
   `pyproject.toml` untouched), no dev-dependency group — consistent
   with the implementability review's endorsement.
2. **DEFECT-1 fix — confirmed.** The edits are confined to the two
   `__main__` example blocks (reproduced above), the raw form is
   value-identical to the non-raw form (CPython retains the invalid
   escape literally; only the `SyntaxWarning` differs), and the
   zero-behavior-change argument is correct. The two scoped commits
   (`fix(jin)` / `fix(tu)`) satisfy the gate's dedicated-commit
   requirement with nothing riding along. P1 step 1 with mandatory `-f`
   (exit 0, empty output, environment recorded; sw-tom self-check plus
   sw-mike authoritative re-run) is the binding verification, and the
   design correctly notes cached bytecode masks the warning without
   `-f`.
3. **Step split + boundary — clean in structure; two gaps (Issues 1 and
   2).** The TU-008 vs. release-end division is correct: TU-008 delivers
   the DEFECT-1 fix, the user guide and the optional README pointer, and
   records P1/P2/DC-2/DC-4 as verification/handoff records; the
   `arch.md` update is explicitly the architect's release-end gate after
   integration on `dev`, and the `prd.md` acceptance-status update is
   sw-camille's. The design usefully pins the release-end content so the
   gate is auditable — but the content list is incomplete on OBS
   dispositions (Issue 1) and the pinned DC-4 evidence path collides
   with an existing record (Issue 2).
4. **`docs/tu_extensions.md` outline — complete and accurate.** All ten
   sections trace to named authoritative sources (designs for semantics,
   merged test files for runnable form); every API named in the outline
   exists on `dev` with the semantics the outline states (verified
   above); the migration-free statement's verbatim wording is accurate
   against the merged surface and is pinned by gate case 6 plus the
   guard suites; the versioning note matches the implemented
   `schema_version: 1`. One unpinned import path (Observation 1).
5. **P1/P2/DC responsibility split — confirmed.** sw-tom self-checks P1
   step 1 at step 1; sw-mike runs everything authoritatively at the
   testing gate; evidence lands in the final evidence section of
   `log/release_1/tests/TU-008.md`, matching the DC-2 checklist's own
   wording. P2 evidence requirements are complete (install command,
   build invocation with explicit `--no-isolation` recording for offline
   environments, `dist/` listing, `VERSION.txt` presence in both
   artifacts, wheel-metadata version cross-check against
   `softlab.__version__`, temporary build directory, never-committed
   confirmation). The DC-4 record's pinned path, owner and pending
   status are correctly scoped as a handoff record, not an executed
   edit.
6. **Clarifications needed** — the two numbered issues below; both are
   design-text level.

## Issues (all required changes; all design-text level)

### Issue 1: the release-end `arch.md` content spec omits the open OBS-004/OBS-006 dispositions and DEFECT-2

The "Content the release-end `arch.md` update must add" list covers the
TU-002..TU-007 surfaces and names the OBS-001/002/003 resolutions (TU-006
bullet) and the OBS-005 `{}` fallback (TU-007 bullet), but never names
**OBS-004** (`QuantizedParameter`/`VisaParameter` base-`__init__`-before-
hook-fields init-value hazard), **OBS-006** (delegated-name collisions
with the explicit `_attributes`/`device()` escape hatch) or **DEFECT-2**
(the deliberate neither-settable-nor-gettable warning). The design's own
stated purpose for this list is to make the release-end gate auditable,
and the PRD acceptance clause is "final architecture describes
implemented behavior" — the open hazards *are* implemented behavior, and
the design itself mandates them as mandatory entries in the user guide's
Limitations section. An architecture record that is silent where the
user documentation is explicit would fail honest release-end audit.
**Requested change:** extend the release-end content list with one
bullet requiring the update to record the open observation dispositions
— OBS-004, OBS-006 and DEFECT-2 — as known limitations/technical debt
with their documented workarounds, consistent with
`docs/tu_extensions.md` section 8 and `log/release_1/compatibility.md`
(an explicit cross-reference to those two documents is acceptable
wording). One bullet; no other text changes.

### Issue 2: the pinned DC-4 evidence path `log/release_1/arch-review.md` already holds the TU-001 review — state the coexistence convention

The design pins the release-end review evidence path to
`log/release_1/arch-review.md`. That file already exists and holds the
TU-001 architecture review (verdict APPROVED, TU-001 characterization
scope). The design does not state whether the release-end record is
appended to that file or replaces it, leaving the DC-4 record and the
release-acceptance evidence link ambiguous — and an overwrite would
destroy TU-001 gate evidence. **Requested change:** state explicitly
that the release-end architecture update/review record is **appended**
to `log/release_1/arch-review.md` as a new section, preserving the
existing TU-001 review content. One sentence in "Step split and role
boundaries".

## Non-blocking observations

1. **`AbortReport` import path unpinned in the outline.** Integration
   case 1 imports `AbortReport` from `softlab.tu.station.visa`; it is
   not re-exported at the `softlab.tu.station` package level (verified
   `ImportError`). Outline section 5 names `AbortReport` without a
   documented path. The write-against-live-objects rule and the DC-2
   walk will catch a wrong path at the gate, so this is not blocking —
   but one clause in the section-5 row ("documented at its actual
   public path `softlab.tu.station.visa`") would pre-empt a review
   cycle.
2. **Design header cites tip `32dd5a7`; the reviewed candidate is
   `b65bd81`** (the design commit itself). Content is unchanged between
   them; noted for traceability only.

## Disposition

Criteria 1, 2, 4 and 5 pass; criterion 3 passes in structure with two
text-level gaps. Once the two corrections land (docs-only edits to the
design text: the OBS-disposition bullet for the release-end content
list, and the append-not-replace sentence for the DC-4 evidence path),
TU-008 may proceed to implementation without a further full re-review —
a focused confirmation of the two corrections suffices. This review does
not claim code review, test pass, CI or integration; the implementation,
independent review, testing, principle and CI gates remain open.

## Focused confirmation of corrections (candidate `e1e7a40`)

Verifier: sw-jerry (architect). Scope: the two requested corrections
only; no re-review.

1. **Issue 1 — resolved.** The release-end `arch.md` content list in
   `TU-008.md` now carries one bullet requiring the open observation
   dispositions — OBS-004 (init-value hazard), OBS-006 (delegated-name
   collisions with the `_attributes`/`device()` escape hatch) and
   DEFECT-2 (deliberate neither-settable-nor-gettable warning) — to be
   recorded as known limitations / technical debt with their documented
   workarounds, explicitly permitting a cross-reference to
   `docs/tu_extensions.md` section 8 and
   `log/release_1/compatibility.md`, and stating the record must not be
   silent where the user documentation is explicit. Matches the
   requested change (one bullet, no other content-list changes).
2. **Issue 2 — resolved.** `TU-008.md` now states explicitly,
   immediately after the pinned DC-4 evidence path (within "Step split
   and role boundaries" → "What TU-008 delivers vs. what the
   release-end arch update adds"), that the release-end architecture
   update/review record is **appended** to `log/release_1/arch-review.md`
   as a new section and that the existing TU-001 review content is
   preserved (no replacement). Matches the requested change.

The header candidate-chain correction (`b65bd81` → the correction
commit) also disposes of non-blocking Observation 2. Non-blocking
Observation 1 (`AbortReport` import path in outline section 5) stands as
previously recorded; it remains non-blocking and is caught by the DC-2
walk if mishandled.

**Confirmation verdict: APPROVED.** Both required corrections land as
requested; no new issues. TU-008 may proceed to implementation. All
downstream gates (implementation, independent review, testing,
principle check, CI, release-end architecture update) remain open.
