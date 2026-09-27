# Principle inspection

## Release start / TU-001 start — 2026-09-27

PASS for starting characterization. Reviewed principles.md and AGENTS.md.
The user has now authorized the previously proposed TU backlog. dev is clean
at db256b0; task branch exists and is pushed. TU-001 changes tests/documentation
only. Subsequent implementation is additive and follows the serial gates.
No new dependency, Python support change, physical instrument access, or main
promotion is authorized. Test fixtures must restore registries/defaults and
release simulated resources. Existing suspicious behavior must be documented
separately from intended compatibility guarantees. Task-completion and final
release verdicts remain pending actual evidence.

## TU-001 completion inspection

PASS for characterization scope: requirements recorded in release_1/prd.md;
developer accepted test implementability; architect approved the contract map;
independent reviewer requested and then confirmed two assertion corrections,
closing review issues in 1c2886e. Tester reran 22 tests, compile and import with
zero skips/warnings at b5599ff. Only tests, documentation and guidance changed;
no production implementation or hardware access. OBS-001–006 remain explicitly
open for later tasks. CI on the final task candidate is required before dev
integration. Next task cannot start until that integration is verified.
