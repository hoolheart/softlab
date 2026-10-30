# Independent testability and compatibility review

Verdict: APPROVED for implementation, subject to execution of the checks below.
Reviewed [adaptation design](../design/adaptation.md) against the accepted W01–W03
thermal tests and version 1 manifest constraints. The shared acquisition path,
unchanged default, and explicit boundary policy are feasible with the existing
model and scheduler. A separately validated version 2 avoids silently changing
version 1 interpretation; the independent Euler oracle makes claimed timing
checkable against stored rows. This verdict is design review, not a test pass.

The direct observation repeats the model observation callback, so its validity
is limited to the example's pure callback. It cannot support an assertion about
physical settling or general stateful sensors. Execution must check version 1
compatibility and legacy absence, malformed and cross-policy version 2 metadata,
independent per-boundary values, write/read failure ordering and cleanup, and a
fresh-process reload. The default command and previously accepted behavior must
remain compatible. The test owner will record and close any test failures after
implementation; no physical or VISA gate applies.
