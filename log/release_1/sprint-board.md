# TU extension board

User authorization: 2026-09-27, "continue to finish remaining tasks".
Scope: TU-001 through TU-008 from release_0/tasks.md. Integration: dev.
No main promotion, real hardware, or mu/huo implementation is included.

| Task | State | Owner | Evidence |
| --- | --- | --- | --- |
| TU-001 | Done | Product owner, architect, developer, tester, reviewer | tests b5599ff; review 1c2886e APPROVED; dev integrated 6f39f78 after CI 36302785650 |
| TU-002 | Done | Developer, tester, reviewer, designer, architect | tests 86a6063 33/33 green; review b613f2a APPROVED; dev integrated 8794cb7 after CI 36306104192 |
| TU-003 | Done | Developer, tester, reviewer, designer, architect | tests ffc5f25 40/40 green; review 9002b68 APPROVED; dev integrated d24da5c after CI 36320621031 |
| TU-004 | Done | Developer, tester, reviewer, designer, architect | tests a9589eb 50/50 green; review b1d8780 APPROVED; dev integrated c989d4c after CI 36323399137 |
| TU-005 | Done | Developer, tester, reviewer, designer, architect | tests 7b7d87d 60/60 green; review 1faf8dd APPROVED; dev integrated 18bfe64 after CI 36325439753 |
| TU-006 | Done | Developer, tester, reviewer, designer, architect | tests 2ba02fd 77/77 green; review 888d7ba APPROVED; dev integrated 45a995b after CI 36725443721 |
| TU-007 | Done | Developer, tester, reviewer, designer, architect | tests f02ec2c 92/92 green; review 7920d45 APPROVED; dev integrated 54a195c after CI 36860164362 |
| TU-008 | Done | Developer, tester, reviewer, designer, architect | tests cd392de final gate green (P1/P2/DC all PASS); review 4292952 APPROVED; dev integrated 0cc40ea after CI 36866455300 |

Only one task may advance beyond Backlog before its predecessor integrates.

## TU-008 integration evidence

Candidate `0cc40ea7c1953c1431b856b6171e0677d823dbaf` passed both Python 3.9
and 3.13 jobs in [run 36866455300](https://github.com/hoolheart/softlab/actions/runs/36866455300).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched. All eight tasks are now integrated; release-end
gates (arch.md append, final acceptance) remain.

## TU-007 integration evidence

Candidate `54a195c007d1ae6ed8b0ccc648cedd725326644b` passed both Python 3.9
and 3.13 jobs in [run 36860164362](https://github.com/hoolheart/softlab/actions/runs/36860164362).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched.

## TU-006 integration evidence

Candidate `45a995bb6446c863204643e592fd1e3a33bbe2db` passed both Python 3.9
and 3.13 jobs in [run 36725443721](https://github.com/hoolheart/softlab/actions/runs/36725443721).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched.

## TU-005 integration evidence

Candidate `18bfe649e6db8c757568d3e474073d763d1f1cac` passed both Python 3.9
and 3.13 jobs in [run 36325439753](https://github.com/hoolheart/softlab/actions/runs/36325439753).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched.

## TU-004 integration evidence

Candidate `c989d4c672f76ce56b8801a120de08ed4c772c90` passed both Python 3.9
and 3.13 jobs in [run 36323399137](https://github.com/hoolheart/softlab/actions/runs/36323399137).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched.

## TU-003 integration evidence

Candidate `d24da5cc592c3c360e6c034c67af688d4f8e31f5` passed both Python 3.9
and 3.13 jobs in [run 36320621031](https://github.com/hoolheart/softlab/actions/runs/36320621031).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched.

## TU-002 integration evidence

Candidate `8794cb7e958e32dbdba54d806eec72fafe28e0ab` passed both Python 3.9
and 3.13 jobs in [run 36306104192](https://github.com/hoolheart/softlab/actions/runs/36306104192).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched.

## TU-001 integration evidence

Candidate `6f39f78066021a048da82364756bff79294da7d9` passed both Python 3.9
and 3.13 jobs in [run 36302785650](https://github.com/hoolheart/softlab/actions/runs/36302785650).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched. This completion-record commit must also pass
CI before its fast-forward into `dev`.
