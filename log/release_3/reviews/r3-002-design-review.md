# Review — R3-002 detailed design

Reviewer: sw-jerry (Software Architect) | Date: 2026-10-07
Verdict: **APPROVED**
Reviewed artifact:
[r3-002-detailed-design.md](../design/r3-002-detailed-design.md) at commit
`59c97e7` on `codex/r3-002-builders` (verified via `git log` /
`git rev-parse HEAD origin/codex/r3-002-builders` after `git fetch`; HEAD ==
origin tip).
Requirements: [prd.md](../prd.md) (R3-AC-04 membership/builder portion;
compatibility/evidence portion of R3-AC-08) | Planned architecture:
[architecture-plan.md](../architecture-plan.md) (lines 19–20 import
direction, 29–34 builder convention, 90–94 namespace/snapshot contracts) and
[arch.md](../../../arch.md) | Approved test plan:
[r3-002-test-plan.md](../test/r3-002-test-plan.md) (Revision 2) |
Characterization baseline: [r3-002-baseline.md](../test/r3-002-baseline.md)
(GAP-1…GAP-3; C-1…C-6)

## Issues

No blockers. One minor editorial item, which does not change the verdict.

1. **[minor] D2 justification 1 — "run-to-run stable" understates the
   nondeterminism it cites.** Location: design §"D2 — Naming",
   justification 1: "attribute-style access would resolve to one or the
   other nondeterministically (run-to-run stable but order-dependent and
   unprincipled)". `Delegated.__getattr__` iterates
   `__delegate_attr_dicts`, a `set` of `str` (verified at
   `softlab/jin/misc/delegated.py:17, 40–49`). Under CPython string-hash
   randomization (default `PYTHONHASHSEED`), iteration order of a string
   set is **not** guaranteed stable across processes; it is stable only
   within one process (or with a fixed seed). The design's parenthetical
   therefore understates the hazard — cross-namespace key duplication
   could resolve differently *between runs*, not merely "order-dependent"
   within one. The architectural conclusion (shared namespace makes the
   situation unreachable; the `add_device` guard is required) is
   unaffected — if anything it is strengthened. Required change: when
   closing the review section, adjust the parenthetical to reflect that
   the resolution order is unspecified and may vary across interpreter
   runs. No test-plan impact: R3-TC-04f/04g pin rejection, not iteration
   order. Owner: sw-celeste (document fix); awareness sw-tom, sw-mike.

## Rulings on the three open questions (architect decisions, explicit)

### Ruling 1 — `add_device` additive cross-namespace guard: **ACCEPT**

The additive `ValueError` guard inside `add_device` (Decision D2) is
approved as designed. Justification:

1. **It is planned architecture, not new architecture.**
   `architecture-plan.md` lines 90–92: "New object/device cross-name
   collisions must be rejected without changing existing behavior where no
   object collision exists." The plan's wording is deliberately
   bidirectional ("object/device"), and the device-side guard is exactly
   the symmetric half that makes the invariant total. The design realizes
   the plan; it does not redesign it.
2. **R3-AC-08 compatibility holds by construction.** Verified against
   baseline GAP-1: before this task no object could ever be a station
   member, so `device.name in self._objects` is unreachable from any
   pre-R3-002 call sequence. The guard is an error-path addition in a
   previously unreachable state; every existing test, including
   R3-TC-04l's device-registry regression (`tests/test_tu_contracts.py`
   lines 179–194, re-verified by direct read), is unaffected. This
   satisfies the plan's "without changing existing behavior where no
   object collision exists" clause verbatim.
3. **The delegation-determinism argument is mechanically sound.**
   Verified at `delegated.py:35–63`: `__getattr__` iterates a class-level
   `set` in unspecified order and returns the first dict hit; a key
   present in both `_devices` and `_objects` would resolve unprincipledly
   (see issue 1 — across runs, not merely run-to-run). The shared
   namespace makes that state unreachable, which is the correct fix:
   enforce the invariant at the two insertion points rather than impose an
   arbitrary precedence rule with no requirement behind it (Simplicity
   Mandate).
4. **The rejected alternative is genuinely worse.** Object-side-only
   guarding without object delegation forfeits the attribute access that
   PRD lines 81–83 presume ("Explicit lookup remains available for names
   colliding with station methods" only has content if attribute access
   otherwise exists — the OBS-006 escape-hatch statement applied to a
   second member kind), and leaves a reachable nondeterministic shadow if
   delegation were ever added later without the guard. Rejected.

**Companion ruling on test coverage of the device-side guard:** the
approved test plan pins the object-side rejection (R3-TC-04f) and
namespace non-leakage (R3-TC-04b) but has no case that attempts
`add_device` with a name already held by an object. The design discloses
this honestly in its traceability table ("verified indirectly by
R3-TC-04b/04f/08c"). Ruling: the guard's behavior is already planned
architecture (line 91), so sw-mike **may** extend the R3-TC-04b fixture at
implementation time to attempt the device-side insertion and assert
rejection — this pins already-planned behavior and requires **no** test
plan revision. If the tester judges the existing cases sufficient, the
indirect verification stands; either disposition must be recorded in the
results document.

### Ruling 2 — `snapshot()`/`describe()` remain device-only: **ACCEPT**

Confirmed: no Release 3 requirement needs an object entry in the legacy
snapshot or description now. Justification:

1. **The plan mandates the legacy shape.**
   `architecture-plan.md` lines 93–94: "Preserve legacy device-only
   snapshot and description contracts; any simulation description must be
   additive and inert." Keeping `snapshot()`/`describe()` byte-shaped as
   today is the planned contract, and the additive simulation description
   node belongs to the connected-simulation design (R3-003), where the
   simulation state it would describe actually exists.
2. **R3-AC-04 is fully satisfied without it.** The membership-listing
   requirement is met by the new `objects` property (R3-TC-04a); no test
   case in the approved plan requires object nodes in
   `snapshot()`/`describe()`, and R3-TC-04l/08c pin the legacy device node
   unchanged.
3. **Compatibility is preserved, not merely unbroken.** An additive
   object node now would change the legacy snapshot shape for zero
   requirement — an entity beyond necessity, and a gratuitous R3-AC-08
   risk against `tests/test_tu_descriptions.py` and the snapshot keys
   pinned in baseline §2.

The design's explicit deferral note ("R3-003+ design work, explicitly not
decided here") is the correct seam and is approved.

### Ruling 3 — `RuntimeError` for unknown model: **ACCEPT**

Confirmed, no change. Justification:

1. **Convention fidelity.** `RuntimeError(f'Failed to get object builder
   with model {model}')` is the verbatim analog of `station.py:142–144`
   (verified by direct read: `raise RuntimeError(f'Failed to get builder
   with model {model}')`). One station construction convention should have
   one error contract; inventing a distinct exception class for the object
   path would be an entity beyond necessity and would break the mirroring
   the PRD demands ("following the **actual** device convention",
   prd.md line 90).
2. **The test plan authorizes it.** Test-plan risk note 3 explicitly
   leaves the exception class to design, pinning only "explicit and
   documented"; R3-TC-04o pins explicitness and unchanged membership, and
   CHK-04-1 pins the docstring file/line citation — which the design
   commits to in the `build_object` contract (D5 and the interface
   section). All three anchors are satisfied.
3. **Failure-safety is unaffected.** Gate 2 is a pure read; no membership
   mutation can precede the raise.

## Point-by-point findings

### 1. Architectural compliance — PASS

- **Plan realization, member for member.** Checked each clause of
  `architecture-plan.md` lines 29–34 against the design: separate registry
  (D7: distinct module-global in a distinct module) ✓; duplicate-model
  rejection (D7, `ValueError` mirroring device.py:564–566) ✓;
  discovery/lookup (`get_object_builder`, D7) ✓; construction and
  membership validation succeed before station insertion (D5 gate order)
  ✓; independent instances own independent mutable values (D6 + Release 2
  constructor defensive copies, verified at `object.py:424–425`
  `_copy_value`) ✓; a builder returning an already owned participant is
  rejected (D6 via the key == name invariant) ✓; registration does not
  construct an object (D7) ✓; no device builder refactor (scope guard:
  `device.py` byte-untouched) ✓. Nothing in the planned Release 3
  architecture is changed; the public network contract is untouched (no
  protocol exists at this layer and none is added).
- **Module placement correct.** `ObjectBuilder` and its registry live in
  the new `softlab/tu/simulation/builder.py` — the construction convention
  for simulated objects belongs beside `SimulatedObject`, mirroring how
  `DeviceBuilder` sits beside `Device`. Not touching `object.py` keeps the
  R3-001-corrected file and its 12 pinned cases at zero regression risk.
- **Import direction verified, no cycle.** `builder.py` →
  `simulation.object` (same package, type contract only); `station.py` →
  `simulation.builder`. Nothing under `tu/simulation/` imports
  `tu/station`, honoring plan lines 19–20. `tu/__init__.py` imports
  `station` before `simulation` (verified), and `station` → `simulation`
  is a one-directional edge: `simulation/__init__.py` pulls in only
  `object.py` + `builder.py`, neither of which reaches back. The planned
  implementation note (single import site via the package `__init__`) is
  sound.
- **Five-element discipline held.** Runtime changes confined to
  `softlab/tu/` (two edited files, one new); `jin`'s `Delegated` reused
  as-is with the class-level shared-set behavior correctly characterized
  (D1: verified at `delegated.py:45–48` — a `None` probe result is
  skipped, and the `key == name` guard at lines 41–43 prevents recursive
  `__getattr__` re-entry, so `'_objects'` probing on subclasses lacking
  the attribute is genuinely harmless, exactly as `'_parameters'` already
  behaves for `Station`); no `huo`/`shui`/`mu` edits; CHK-04-1's scope
  gate is enforceable against this design.

### 2. Design correctness — PASS

- **D5 ordering is correct and complete.** Verified against the device
  analog (`station.py:134–145`) and the baseline's C-5 characterization:
  gate 1 (entry-name validation, R3-TC-04h) and gate 2 (registry lookup,
  R3-TC-04o) are pure reads; `build` runs before any station state is
  touched; `add_object` performs type → duplicate-object →
  device-collision checks before the single `_objects[obj.name] = obj`
  store. Failure atomicity is **structural** (mutation is the last
  statement), not try/finally-based — strictly stronger than the device
  path's accidental atomicity, made explicit and extended by the
  entry-name guard. The gate-order contract (invalid name beats unknown
  model) is a benign, documented determinism improvement; the test plan
  does not constrain it and it makes tests more stable.
- **Failure-safety verified path by path.** Builder raises: gates are
  pure, membership bit-identical, identical exception object propagates
  (no try/except anywhere on the path — R3-TC-04n's `assertIs` holds by
  construction). Membership validation fails after a successful build:
  `add_object` rejects before insertion, pre-existing member untouched
  (R3-TC-04p). Unknown model: `RuntimeError` before any build call
  (R3-TC-04o). No partial registration is reachable on any path.
- **Instance independence correctly requires no machinery.** D6's
  reasoning is sound: the fresh-instance-per-call `build` contract plus
  the Release 2 constructor's defensive copies (verified at
  `object.py:424–425`) cover R3-TC-04q; the design never hands out
  aliasing references. Correctly resists adding identity indexes or
  per-station registries (Simplicity Mandate applied).
- **Shared-namespace delegation determinism argument verified.** The
  mechanics cited in D2 are accurate against `delegated.py` (see issue 1
  for the wording nuance, which strengthens rather than weakens the
  conclusion). Both insertion points guarded = invariant is total.
- **No inheritance of the rename-after-add defect.** D3 is airtight:
  verified at `object.py:427–438` that `SimulatedObject.name` is a
  read-only property with no setter, so the membership key equals the
  object name for its entire membership lifetime; the baseline C-3 device
  defect has no object-side reproduction path. R3-TC-04i is satisfied by
  the "rename is forbidden" branch. Zero code spent on rename handling is
  the correct minimal response to the immutability Release 2 already
  shipped. Private-attribute circumvention is correctly declared outside
  the public contract.
- **D6 already-owned rejection is complete.** Given key == name, an
  instance already inserted under its immutable name always presents a
  live duplicate key on second insertion; the `ValueError` rejects it and
  the first member stays unmodified (R3-TC-04r). Skipping the O(n)
  identity scan (`obj in self._objects.values()`) is justified: the name
  check covers every aliasing path the public contract allows. Note the
  rejection is per-station, which matches the task scope — cross-station
  and coordinated-simulation ownership is R3-003/R3-004 per the test-plan
  scope split, and the design says so.

### 3. Feasibility and simplicity — PASS

- **Implementable verbatim by sw-tom.** Signatures, error classes,
  messages, gate order, insertion points (`Station.__init__` immediately
  after the `_devices` lines; the `add_device` guard between the existing
  type check and duplicate check) and docstring requirements are all
  specified to line level against verified code citations
  (`device.py:515–572`, `station.py:118–145`, `object.py:293–296,
  427–438`, `delegated.py:35–63` — all re-verified in this review). The
  implementation order (builder.py → package exports → station.py → run
  suite after each step) is executable as written, anchored to the
  baseline's 147-test figure.
- **Minimal diff.** Two edited files, one new; the only non-additive edit
  is the one-line `add_device` guard; `build_device` stays byte-identical;
  `object.py`/`device.py` untouched. This is the smallest diff that
  realizes the planned capability.
- **Zero new dependencies.** Stdlib `typing` plus in-repository modules
  only; the Third-Party Dependencies table is honestly empty and
  R3-TC-08e passes trivially. The dependency zero-trust rule is honored.
- **Full traceability.** Every approved [PLAN]/[BASELINE-READY] case
  (R3-TC-04a–04r, 08a–08f, CHK-04-1) maps to at least one design element,
  and the single design element without a direct case (the `add_device`
  guard) is disclosed and dispositioned by Ruling 1's companion ruling.
  R3-TC-04m is automated (construction lands on `Station`), so the test
  plan's manual-fallback clause is correctly not triggered.
- **Pitfalls list is correct.** No `str()` coercion in gate 1 (would
  silently validate R3-TC-04h's `name=123`), no `objects` node in
  snapshot/describe, no omit-delegation registration — each pitfall maps
  to a pinned case and to a ruling above.

### 4. Forward-compatibility (R3-003 seams) — PASS

- The design leaves clean, explicitly-marked seams without implementing
  them: `rm_object` carries a docstring forward note that connected
  removal will be rejected by the R3-003 ownership layer (the guard has an
  obvious insertion point inside the method, before the `pop`);
  snapshot/describe deferral reserves the additive, inert simulation node
  for R3-003 per plan lines 93–94; no coordinator/edge/ownership/clock
  code appears, and CHK-04-1 enforces its absence. Membership storage
  (`_objects` keyed by immutable name) gives R3-003 a stable membership
  reference, consistent with plan line 86 ("stable membership references,
  not reparsing a live display name") — the read-only name makes this
  seam stronger than the device side could offer.
- Nothing in D1–D8 pre-commits R3-003 design choices (ownership
  representation, guard placement, description-node shape all remain
  open). The seams are clean precisely because they are absent code plus
  documented intent.

## What was inspected

- `log/release_3/design/r3-002-detailed-design.md` (full, 771 lines, at
  `59c97e7`)
- `log/release_3/architecture-plan.md` (lines 19–20, 29–34, 90–94)
- `log/release_3/prd.md` (R3-AC-04, R3-AC-08, capability statements lines
  79–97, scope guard)
- `log/release_3/tasks.md` (R3-002 row and scope split vs R3-003)
- `log/release_3/test/r3-002-test-plan.md` (Revision 2: R3-TC-04a–04r,
  08a–08f, CHK-04-1, risks 1–6)
- `log/release_3/test/r3-002-baseline.md` (GAP-1…GAP-3, C-1…C-6)
- `log/release_3/reviews/r3-001-design-review.md` (review convention)
- `softlab/tu/station/station.py` (full: membership methods 118–145,
  `build_device` 134–145, snapshot/describe 67–116)
- `softlab/tu/station/device.py` (`DeviceBuilder` + registry 515–572;
  name coercion guard 143–145; OBS-006 note 129–133)
- `softlab/tu/simulation/object.py` (name guard 293–296; read-only `name`
  property 427–438; defensive copies 424–425)
- `softlab/tu/simulation/__init__.py`, `softlab/tu/__init__.py` (import
  order and current exports)
- `softlab/jin/misc/delegated.py` (full: `__getattr__` 35–63, class-level
  shared sets 17–33, `__dir__` 65–79)
- `tests/test_tu_contracts.py` lines 178–194 (device-registry regression
  and `patch.dict` isolation pattern)
- Verifications executed: `git fetch origin codex/r3-002-builders`;
  `git rev-parse HEAD origin/codex/r3-002-builders` (both `59c97e7`);
  direct reads of every code line range the design cites.

All source reads were read-only; no production files and no design
document content were modified by this review. The design document's own
"Design Review" section (Status: PENDING, Review Date unfilled) is left
for the coordinator/designer to close against this review.

## Verdict and next steps

**APPROVED.** The design realizes the planned builder/registry/membership
capability member for member without changing the planned Release 3
architecture or any public contract; the module placement and one-way
import direction are correct; the D5 ordering delivers structural failure
atomicity with a single mutation point; instance independence and the
already-owned rejection rest on the verified key == name invariant; the
diff is minimal, dependency-free and fully traceable to the approved test
plan; and the R3-003 seams are clean and unimplemented. All three open
questions are ruled **ACCEPT** (additive `add_device` guard; device-only
snapshot/describe with deferral to R3-003; `RuntimeError` for unknown
model). The single minor issue (D2's "run-to-run stable" wording
understating cross-run nondeterminism) is a design-text fix that does not
block implementation — sw-celeste should apply it when closing the review
section. Per the serial workflow, sw-tom may proceed with implementation
once the minor is recorded; sw-mike should record the disposition of the
`add_device`-guard coverage option (Ruling 1 companion) in the results
document.

## Issue closure record

Appended by sw-celeste (design owner), 2026-10-07. The verdict above
(**APPROVED**) is unchanged.

1. **[minor] D2 justification 1 — RESOLVED.** The design's D2
   justification 1 parenthetical ("run-to-run stable but order-dependent
   and unprincipled") understated the nondeterminism: under CPython
   string-hash randomization, the set iteration order can vary across
   interpreter runs, not merely depend on order within one run. Fix
   applied in the design document
   ([r3-002-detailed-design.md](../design/r3-002-detailed-design.md),
   §"D2 — Naming", justification 1): the text now states the resolution
   order is unspecified and may vary across interpreter runs, citing
   `delegated.py:17, 40–49` and default `PYTHONHASHSEED` behavior. The
   architectural conclusion (shared namespace makes the state
   unreachable; the `add_device` guard is required) is unaffected — as
   noted in the issue, strengthened. No test-plan impact: R3-TC-04f/04g
   pin rejection, not iteration order. Closed by sw-celeste, 2026-10-07,
   in the commit `docs(log): close r3-002 design review issue` on
   `codex/r3-002-builders`. The design document's Design Review section
   is updated in the same commit (Status: APPROVED at `6973d9b`; the
   three rulings recorded; this closure referenced).
