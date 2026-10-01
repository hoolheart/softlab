# Sprint Summary — Release 1: compatible `tu` foundations

## Completion status

**ACCEPTED (PASS)** — product-owner verdict in `log/release_1/acceptance.md`
(commit 5391730). All eight tasks TU-001–TU-008 integrated to `dev`;
`main` untouched; no real hardware; no new runtime dependencies.

- Sprint start: 2026-09-27 (user authorization to continue TU-001–TU-008)
- Sprint end: acceptance PASS at `dev` tip 5391730
- Integration branch: `dev` (fast-forward per task; CI green on every candidate)

## Goals and achievements

Goal: make the existing device and numerical-model abstractions easier to
inspect and extend without breaking established callers — all eight backlog
items delivered with full serial TDD gates.

| Task | Deliverable | Integrated |
| --- | --- | --- |
| TU-001 | Compatibility matrix + characterization suite (unchanged production code) | 6f39f78 |
| TU-002 | `describe()` on Parameter/Device/Station (schema_version 1, zero I/O, strict metadata) | 8794cb7 |
| TU-003 | `VisaCommand.execute()` + `describe_operation()` (side-effect discovery) | d24da5c |
| TU-004 | `Device.prepare()/cleanup()/initialized/supports()` opt-in lifecycle | c989d4c |
| TU-005 | `Parameter.read()` + frozen `Reading` + `describe_reading()` | 18bfe64 |
| TU-006 | VISA lifecycle adoption, `timeout_seconds`, serialization, `abort()`/`confirm_stop()`, `write_raw` fix (OBS-001/002/003) | 45a995b |
| TU-007 | Theory `describe()`/`configure()` round trip/`evaluate_features(strict=)` | 54a195c |
| TU-008 | Integration suite, user guide `docs/tu_extensions.md`, DEFECT-1 fix, P1/P2 final gates | 0cc40ea |
| Release-end | `arch.md` implemented-behavior update + appended architecture record | e04e341 |

## Quality metrics

- Test pass rate: **100%** — final suite 99/99 under `-W error` (zero warnings,
  zero skips); forced compile of `softlab` + `tests` exits 0 with zero output
  (TU-008 P1; DEFECT-1 closed).
- Code review issues resolved: **100%** — 8/8 task reviews APPROVED with zero
  open issues; every CHANGES-REQUESTED round closed by its reviewer.
- Build warnings: **0** (runtime); one recorded tooling-only setuptools
  deprecation at package-build time (non-blocking, camille reservation R2).
- CI: green on Python 3.9 + 3.13 for every integration candidate (run IDs on
  the sprint board and in each task's integration evidence).
- File generation: all required `log/release_1/` artifacts exist, committed
  and pushed; user guide + architecture record complete.
- Package artifacts: sdist/wheel built (isolated PEP 517), contents inspected,
  VERSION 0.3.0 consistent (TU-008 P2).

## Issues encountered and resolutions

1. **Latent test defects** (unreachable-when-RED assertion class): 5 found
   across TU-006 (cases 4/8/9) and TU-007/008 gates; all fixed by sw-mike
   with recorded evidence; a proactive sweep institutionalized after the
   third occurrence.
2. **Design correction rounds**: TU-004 (1), TU-006 (1 + docstring fix),
   TU-007 (1), TU-008 (2) — all requested by reviewers, applied by the
   designer, confirmed by the reviewer. Review authority never bypassed.
3. **TU-001 characterization supersession**: default-construction timeout
   pin (5.0 raw → 5000 ms) superseded with architecture sanction and
   recorded in the gate + compatibility matrix.
4. **Baseline defects**: DEFECT-1 (two `__main__` SyntaxWarning sites) fixed
   in TU-008; DEFECT-2 + OBS-004/005/006 recorded as known limitations with
   workarounds in the user guide and architecture record.

## Lessons learned

- Expected-RED phases multiply CI failure noise on per-push triggers;
   accepted by the user (Option A) as reviewable evidence.
- Latent test defects of the "unreachable until production lands" class are
   the dominant review finding; proactive sweep at test gates is now
   standard practice.
- Step-split implementation (TU-006 A–D, TU-007 A–C) kept increments
   independently verifiable without weakening gates.

## Reservations (from acceptance, non-blocking)

1. OBS-004/005/006 + DEFECT-2 remain documented technical debt.
2. Setuptools license-classifier deprecation — future packaging hygiene task.
3. Acceptance reviewed recorded evidence; tests not re-executed by the PO.

## Next steps (suggested)

- **Promotion decision** (user): `main` promotion is NOT authorized by this
   release; requires explicit user decision per branch policy.
- Packaging hygiene (setuptools deprecation) as a small future task.
- `mu` services / scheduling pools / persistence remain excluded per PRD;
   a future release would need fresh requirement clarification.

## Completion record

Mode A (full release) engagement. Work scope assessment, serial execution,
formal reviews, and quality gates enforced throughout. Confirmed engagement:
original request "continue to finish remaining tasks" (TU-001–TU-008).
Scope completed vs planned: 8/8 tasks. Quality gates: all passed with actual
evidence. Unresolved items: none beyond recorded, user-visible reservations
above. This summary committed as `docs(log):` per process-file rules.
