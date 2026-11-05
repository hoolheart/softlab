"""M03 phase 4 walkthrough scenarios W1, W2 and W3.

Assertion-based original-versus-clean-session-rerun walkthrough of the frozen
thermal and signal oracles, per the approved plan
(log/release_12/test/walkthrough-test-plan.md, APPROVED `8ef207f`) and the
frozen oracle design (log/release_12/design/p4-oracle-design.md, FROZEN
`63ed461`). All oracle values, tolerances and exclusions below are
transcribed verbatim from that freeze, never re-derived at run time.

- W1: thermal v1 (`adapter='coordinated'`) original vs clean-session rerun.
- W2: thermal v2 (`adapter='direct'`) original vs clean-session rerun.
- W3: signal monitor (`acquire(points=3)` + DEP-1 wiring) original vs
  clean-session rerun; acquisition-time evidence (read trace,
  non-consumptive facts, caller-derived differences) is taken from each
  session driver's `acquire()` return.

Both scenarios: two sessions in separate fresh Python processes and separate
temp roots, bootstrap inspection of both artifacts on sha256-verified byte
copies (F-A guard, originals never loaded in place), per-artifact oracle
comparison and A-vs-B comparison (integers/config/identity exact ``==``;
Euler temperatures ``math.isclose(rel_tol=0.0, abs_tol=1e-10)``), §4
exclusions applied only to the named fields with every per-run retained
assertion executed, distinct execution/group identities, and the
recorded-configuration comparison (freeze §5.3).

Hygiene (carried from the Phase 1-3 discipline): no mocks at all — every
assertion is a real-seam assertion (subprocess execution, stored bytes,
bootstrap JSON). Synthetic data only, in
``tempfile.TemporaryDirectory(prefix='softlab-m03-p4-*')`` roots; nothing is
written into the repository tree. Version-portable assertions: only the
frozen §3 tolerance is ever used on temperatures; cross-version exact-repr
equality is never asserted. Any deviation from the frozen oracle fails the
test precisely as observed — deviations are reported, never edited into the
oracle.
"""

import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import h5py

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = ROOT / "tests" / "run_record_inspect_bootstrap.py"

_TEMP_PREFIX = "softlab-m03-p4-"

# ---------------------------------------------------------------------------
# Frozen literals transcribed verbatim from p4-oracle-design.md (63ed461).
# ---------------------------------------------------------------------------

# Freeze §1: the frozen ThermalConfig() defaults (all seven values).
_THERMAL_CONFIG = {
    "initial_temperature": 20.0,
    "ambient_temperature": 20.0,
    "heat_capacity": 10.0,
    "conductance": 1.0,
    "dt": 1.0,
    "power": 10.0,
    "points": 8,
}

# Freeze §2.1: thermal v1 declared column sequence (already lexicographic)
# and the frozen pre-step temperature doubles.
_THERMAL_DECLARED = (
    "commanded_power_w",
    "measurement_source_tick",
    "sim_time_s",
    "temperature_degc",
    "tick",
    "timestamp",
)
_THERMAL_V1 = {
    "adapter": "coordinated",
    "manifest_version": 1,
    "delay": 1,
    "source_tick": "tick-1",
    "tick": [1, 2, 3, 4, 5, 6, 7, 8],
    "measurement_source_tick": [0, 1, 2, 3, 4, 5, 6, 7],
    "sim_time_s": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
    "commanded_power_w": [10.0] * 8,
    "temperature_degc": [
        20.0,
        21.0,
        21.9,
        22.709999999999997,
        23.438999999999997,
        24.095099999999995,
        24.685589999999994,
        25.217030999999995,
    ],
}

# Freeze §2.2: thermal v2 (post-step temperature vector).
_THERMAL_V2 = {
    "adapter": "direct",
    "manifest_version": 2,
    "delay": 0,
    "source_tick": "tick",
    "tick": [1, 2, 3, 4, 5, 6, 7, 8],
    "measurement_source_tick": [1, 2, 3, 4, 5, 6, 7, 8],
    "sim_time_s": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
    "commanded_power_w": [10.0] * 8,
    "temperature_degc": [
        21.0,
        21.9,
        22.709999999999997,
        23.438999999999997,
        24.095099999999995,
        24.685589999999994,
        25.217030999999995,
        25.695327899999995,
    ],
}

# Freeze §3: the single non-exact tolerance, everything else exact ==.
_TEMP_ABS_TOL = 1e-10

# Freeze §2.3: signal oracle. Declared column order is NON-lexicographic
# (by-name reconstruction in envelope order is mandatory); the derived
# difference is caller-derived and never a stored column.
_SIGNAL_DECLARED = ("timestamp", "signal", "reference")
_SIGNAL_POINTS = 3
_SIGNAL_OFFSETS = {"signal": 5, "reference": 2}
_SIGNAL_COLUMNS = {
    "signal": [6, 7, 8],  # = tick + 5
    "reference": [3, 4, 5],  # = tick + 2
}
_SIGNAL_DIFFERENCE = [3, 3, 3]  # caller-derived; recomputed, never stored
_SIGNAL_READ_TRACE = [
    ["signal", 1], ["reference", 1],
    ["signal", 2], ["reference", 2],
    ["signal", 3], ["reference", 3],
]

# Freeze §5.3 recorded-configuration literals (signal).
_SIGNAL_READ_POLICY = {
    "semantics": "tick_plus_offset",
    "consumptive": False,
    "offsets": {"signal": 5, "reference": 2},
}
_SIGNAL_SCHEDULER_OWNERSHIP = "run_count_owned"
_SIGNAL_DERIVED_STORAGE = "caller_derived_not_stored"
_SIGNAL_DERIVED_FORMULA = "signal - reference"


# ---------------------------------------------------------------------------
# Session driver: a small real-seam script launched as a fresh subprocess.
# ---------------------------------------------------------------------------

_THERMAL_DRIVER = """
import json
import os
import sys

_REPO, _ADAPTER, _OUT = sys.argv[1], sys.argv[2], sys.argv[3]

# Clean-process control evidence, captured before any softlab import: a
# fresh interpreter holds no softlab-module state from a prior session.
_clean = not any(
    name == "softlab" or name.startswith("softlab.")
    for name in sys.modules
)

sys.path.insert(0, _REPO)
from pathlib import Path

from examples.thermal_experiment import ThermalConfig, execute

result = execute(ThermalConfig(), Path(_OUT), adapter=_ADAPTER)
print(json.dumps({
    "pid": os.getpid(),
    "clean_prefix": _clean,
    "env_softlab_vars": sorted(
        key for key in os.environ if key.startswith("SOFTLAB")),
    "overall_success": result.overall_success,
    "acquisition": result.acquisition,
    "requested_rows": result.requested_rows,
    "completed_rows": result.completed_rows,
    "final_tick": result.final_tick,
    "final_sim_time": result.final_sim_time,
    "execution_id": result.execution_id,
    "group_id": str(result.group.id),
    "data_path": str(result.data_path),
    "plot_path": str(result.plot_path),
    "plot_exists": Path(result.plot_path).exists(),
}))
"""


_SIGNAL_DRIVER = """
import json
import os
import sys

_REPO, _OUT = sys.argv[1], sys.argv[2]

# Clean-process control evidence, captured before any softlab import: a
# fresh interpreter holds no softlab-module state from a prior session.
_clean = not any(
    name == "softlab" or name.startswith("softlab.")
    for name in sys.modules
)

sys.path.insert(0, _REPO)
from pathlib import Path

from examples.signal_monitor import acquire, persist

# Pinned invocation form (plan "Subprocess invocation form"): acquire()
# and the DEP-1 wiring (persist) in the SAME process.
result, record, differences, reads = acquire(points=3)
out_dir = Path(_OUT)
out_dir.mkdir(parents=True, exist_ok=True)
saved = persist(result, record, out_dir / 'signal.h5')

# Non-consumptive-read facts, from the acquire() return only: every trace
# entry's tick equals its row tick (reads never consumed anything queued).
table = record.table
signal = table['signal'].tolist()
reference = table['reference'].tolist()
non_consumptive = all(
    (tick - 1) < len(signal)
    and (signal if channel == 'signal' else reference)[tick - 1]
        == tick + (5 if channel == 'signal' else 2)
    for channel, tick in reads
)
print(json.dumps({
    "pid": os.getpid(),
    "clean_prefix": _clean,
    "env_softlab_vars": sorted(
        key for key in os.environ if key.startswith("SOFTLAB")),
    "overall_success": result.overall_success,
    "acquisition": result.acquisition,
    "requested_rows": result.requested_rows,
    "completed_rows": result.completed_rows,
    "execution_id": saved.execution_id,
    "group_id": saved.group_id,
    "run_id_matches": str(result.run_id) == saved.execution_id,
    "data_path": str(saved.path),
    "report_ok": bool(saved.report.primary_ok and saved.report.cleanup_ok),
    "reads": [list(entry) for entry in reads],
    "differences": differences,
    "signal": signal,
    "reference": reference,
    "non_consumptive": non_consumptive,
    "record_columns": [column["name"] for column in record.columns],
}))
"""


# ---------------------------------------------------------------------------
# Shared helpers.
# ---------------------------------------------------------------------------

def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


_copy_counter = [0]


def _fresh_verified_copy(path, root):
    """Byte-copy ``path`` into ``root`` with a unique name; verify sha256.

    Every call produces a NEW copy (F-A guard: inspections never reuse a
    copy and never touch the original bytes).
    """
    digest = _sha256_file(str(path))
    _copy_counter[0] += 1
    copy_path = Path(root) / "inspect-copy-{:02d}.h5".format(
        _copy_counter[0]
    )
    shutil.copy2(str(path), str(copy_path))
    if _sha256_file(str(copy_path)) != digest:
        raise AssertionError("test helper copy is not byte-identical")
    return str(copy_path), digest


def _evidence(tag, **facts):
    """Print one machine-readable evidence line into the test log."""
    print(
        "[walkthrough-evidence {}] {}".format(
            tag, json.dumps(facts, sort_keys=True, default=str)
        )
    )


def _run_python(args, interpreter=None, cwd=ROOT, env=None, timeout=300):
    command = [str(interpreter or sys.executable)] + [str(a) for a in args]
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    stdout, stderr = process.communicate(timeout=timeout)
    completed = subprocess.CompletedProcess(
        command, process.returncode, stdout, stderr)
    return completed, process.pid


def _run_driver(source, root, args, interpreter=None, timeout=300):
    """Write a session driver into ``root`` and run it as a fresh process."""
    driver = Path(root) / "driver_{:02d}.py".format(_copy_counter[0] + 1)
    _copy_counter[0] += 1
    driver.write_text(source)
    completed, child_pid = _run_python([driver] + args,
                                       interpreter=interpreter,
                                       timeout=timeout)
    if completed.returncode != 0:
        raise AssertionError(
            "session driver failed (exit {})\nstdout:\n{}\nstderr:\n{}"
            .format(completed.returncode, completed.stdout,
                    completed.stderr)
        )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    return payload, completed


def _inspect(artifact, profile, root, interpreter=None):
    """Bootstrap inspection of one artifact on a fresh verified copy."""
    copy_path, digest = _fresh_verified_copy(artifact, root)
    completed, child_pid = _run_python(
        [BOOTSTRAP, copy_path, "--profile", profile],
        interpreter=interpreter,
        timeout=300,
    )
    if completed.returncode != 0:
        raise AssertionError(
            "bootstrap inspection failed (exit {})\nstdout:\n{}\nstderr:\n{}"
            .format(completed.returncode, completed.stdout,
                    completed.stderr)
        )
    payload = json.loads(completed.stdout)
    if payload["status"] != "ok":
        raise AssertionError(
            "bootstrap inspection returned non-ok payload: {}".format(payload)
        )
    return payload, child_pid, copy_path, digest


def _disk_identity_facts(copy_path, group_id):
    """Independent h5py read of identity slots on a disposable copy."""
    facts = {}
    with h5py.File(copy_path, "r") as disk:
        names = list(disk.keys())
        stored = disk[str(group_id)]
        facts["group_names"] = names
        facts["group_id_attr"] = stored.attrs["id"]
        facts["record_attrs"] = {
            name: dict(stored[name].attrs) for name in stored.keys()
        }
    return facts


def _assert_source_facts_shape(test, envelope):
    """Freeze §4: source states checked, values never compared across runs."""
    source = envelope["source"]
    test.assertEqual(source["digest_algorithm"], "sha256")
    for key in ("digest", "git_revision", "git_dirty"):
        fact = source[key]
        test.assertIn(fact["state"], ("available", "unavailable"))
        if fact["state"] == "available":
            test.assertIsNotNone(fact["value"])
            test.assertIsNone(fact["reason"])
        else:
            test.assertIsNone(fact["value"])
            test.assertIn(fact["reason"], ("source_mismatch",
                                           "capture_failed"))


def _assert_common_per_run(test, payload, session, artifact_path,
                           completed_rows):
    """Freeze §4 retained assertions, applied per run."""
    envelope = payload["envelope"]
    execution_id = envelope["execution_id"]
    group_id = envelope["group_id"]
    # Distinctness within the run.
    test.assertNotEqual(execution_id, group_id)
    test.assertEqual(payload["group_id"], group_id)
    # Outcome stage honesty: acquisition snapshot only.
    outcome = envelope["outcome"]
    test.assertEqual(outcome["acquisition"], "completed")
    for key in ("persistence", "plotting", "cleanup", "overall"):
        test.assertEqual(outcome[key], "not_yet_established")
    test.assertEqual(outcome["requested_rows"], completed_rows)
    test.assertEqual(outcome["completed_rows"], completed_rows)
    # Software facts present (values are environment exclusions, never
    # compared across runs).
    for key in ("softlab_version", "python_version"):
        test.assertIn(key, envelope["software"])
    _assert_source_facts_shape(test, envelope)
    # Report contract.
    report = payload["report"]
    test.assertTrue(report["primary_ok"])
    test.assertEqual(report["primary"], [])
    test.assertTrue(report["cleanup_ok"])
    test.assertEqual(report["cleanup"], [])
    test.assertEqual(payload["original_sha256"],
                     _sha256_file(str(artifact_path)))
    # Disk identity slots via an independent read of a fresh copy:
    # group_id == HDF5 group name == on-disk id attr; execution_id in no
    # stored UUID slot.
    copy_path, _ = _fresh_verified_copy(
        artifact_path, Path(artifact_path).parent)
    disk = _disk_identity_facts(copy_path, group_id)
    test.assertEqual(disk["group_names"], [group_id])
    test.assertEqual(disk["group_id_attr"], group_id)
    for attrs in disk["record_attrs"].values():
        for value in attrs.values():
            test.assertNotEqual(str(value), execution_id)
    test.assertNotEqual(disk["group_id_attr"], execution_id)


def _assert_columns_close(test, actual, expected, temperature=False):
    test.assertEqual(len(actual), len(expected))
    for got, want in zip(actual, expected):
        if temperature:
            test.assertTrue(
                math.isclose(got, want, rel_tol=0.0, abs_tol=_TEMP_ABS_TOL),
                "temperature deviation: actual={!r} expected={!r}".format(
                    got, want),
            )
        else:
            test.assertEqual(got, want)


def _compare_thermal_artifact(test, payload, oracle, session):
    """Freeze §5.2 step 7 oracle comparison for one thermal artifact."""
    envelope = payload["envelope"]
    record = payload["records"]["thermal_trace"]
    samples = record["samples"]

    # Declared column sequence, envelope order (p3 §3.5.1 rule 5); each
    # same-named stored dataset retrieved by name, compared by row index.
    declared = [column["name"] for column in envelope["record"]["columns"]]
    test.assertEqual(tuple(declared), _THERMAL_DECLARED)
    test.assertEqual(record["row_count"], 8)
    test.assertEqual(set(samples), set(_THERMAL_DECLARED))
    for name in _THERMAL_DECLARED:
        if name == "timestamp":
            continue  # §4 excluded: presence/shape checked below
        _assert_columns_close(
            test, samples[name], oracle[name],
            temperature=(name == "temperature_degc"),
        )

    # §2 timing relations, asserted independently of the temperature oracle.
    tick = samples["tick"]
    test.assertEqual(tick, [row + 1 for row in range(8)])
    test.assertEqual(
        samples["measurement_source_tick"],
        [value - oracle["delay"] for value in tick],
    )
    test.assertEqual(samples["sim_time_s"], [value * 1.0 for value in tick])
    test.assertEqual(samples["commanded_power_w"], [10.0] * 8)

    # §4 retained per-run assertions.
    timestamp_decl = [
        column for column in envelope["record"]["columns"]
        if column["name"] == "timestamp"
    ]
    test.assertEqual(len(timestamp_decl), 1)
    test.assertEqual(timestamp_decl[0]["unit"], "s")
    timestamps = samples["timestamp"]
    test.assertEqual(len(timestamps), 8)
    test.assertTrue(all(math.isfinite(value) for value in timestamps))

    # Outcome/boundary literals.
    outcome = envelope["outcome"]
    test.assertEqual(outcome["completed_rows"], 8)
    test.assertEqual(outcome["requested_rows"], 8)
    boundary = envelope["payload"]["boundary"]
    test.assertEqual(boundary["final_tick"], 8)
    test.assertEqual(boundary["final_sim_time"], 8.0)

    # Recorded configuration (freeze §5.3): stored configuration equals all
    # seven frozen ThermalConfig values exactly; recorded adapter matches.
    payload_section = envelope["payload"]
    test.assertEqual(payload_section["configuration"], _THERMAL_CONFIG)
    test.assertEqual(payload_section["adapter"], oracle["adapter"])
    timing = payload_section["timing"]
    test.assertEqual(timing["measurement_delay_ticks"], oracle["delay"])
    test.assertEqual(timing["source_tick"], oracle["source_tick"])

    # Legacy manifest literals (dual-written, unchanged contract).
    manifest = json.loads(payload["legacy_meta"]["thermal_manifest"])
    test.assertEqual(manifest["schema"], "softlab.thermal-reference")
    test.assertEqual(manifest["version"], oracle["manifest_version"])
    test.assertEqual(manifest["run_id"], envelope["group_id"])
    clock = manifest["clock"]
    test.assertEqual(clock["measurement_delay_ticks"], oracle["delay"])
    test.assertEqual(clock["source_tick"], oracle["source_tick"])


def _thermal_session(test, root, adapter, interpreter=None):
    """One fresh-process thermal session plus its save verification."""
    session, completed = _run_driver(
        _THERMAL_DRIVER, root,
        [str(ROOT), adapter, str(Path(root) / "out")],
        interpreter=interpreter,
    )
    test.assertTrue(session["overall_success"], session)
    test.assertEqual(session["acquisition"], "completed")
    test.assertEqual(session["requested_rows"], 8)
    test.assertEqual(session["completed_rows"], 8)
    test.assertEqual(session["final_tick"], 8)
    test.assertEqual(session["final_sim_time"], 8.0)
    artifact = Path(session["data_path"])
    test.assertTrue(artifact.exists())
    test.assertTrue(session["plot_exists"])  # §4: thermal.png existence
    sha_before = _sha256_file(str(artifact))
    _evidence("thermal-session", adapter=adapter, pid=session["pid"],
              interpreter=Path(interpreter or sys.executable).name,
              execution_id=session["execution_id"],
              group_id=session["group_id"],
              artifact=str(artifact), sha256_before=sha_before,
              temp_root=str(root), clean_prefix=session["clean_prefix"],
              env_softlab_vars=session["env_softlab_vars"],
              driver_stderr=completed.stderr.strip())
    return session, artifact, sha_before


def _inspect_twice(test, artifact, profile, root, tag, interpreter=None):
    """Two separate-process bootstrap inspections on fresh copies (F-A)."""
    payloads = []
    for index in (1, 2):
        payload, child_pid, copy_path, _ = _inspect(
            artifact, profile, root, interpreter=interpreter)
        _evidence("{}-inspection-{}".format(tag, index),
                  child_pid=child_pid, copy=copy_path,
                  profile=profile, status=payload["status"])
        payloads.append(payload)
    # The two inspections decode identical semantic content; only the
    # per-copy report.path differs (each inspection used its own fresh
    # copy, by design).
    for key in ("envelope", "records", "legacy_meta", "original_sha256",
                "group_id", "envelope_present"):
        test.assertEqual(payloads[0][key], payloads[1][key], key)
    for payload in payloads:
        test.assertTrue(payload["report"]["primary_ok"])
        test.assertEqual(payload["report"]["primary"], [])
        test.assertTrue(payload["report"]["cleanup_ok"])
        test.assertEqual(payload["report"]["cleanup"], [])
    return payloads[0]


# ---------------------------------------------------------------------------
# W3 helpers: signal session driver, oracle comparison.
# ---------------------------------------------------------------------------

def _signal_session(test, root):
    """One fresh-process signal session plus its save verification.

    Acquisition-time evidence (plan W3 step 2) is taken from the
    driver's ``acquire()`` return — the read trace, the non-consumptive
    facts, and the caller-derived differences — never from the stored
    artifact or the bootstrap JSON.
    """
    session, completed = _run_driver(
        _SIGNAL_DRIVER, root,
        [str(ROOT), str(Path(root) / "out")],
    )
    # Acquisition-success gate and storage report (freeze §5.1 inputs 1/6).
    test.assertTrue(session["overall_success"], session)
    test.assertEqual(session["acquisition"], "completed")
    test.assertEqual(session["requested_rows"], _SIGNAL_POINTS)
    test.assertEqual(session["completed_rows"], _SIGNAL_POINTS)
    test.assertTrue(session["report_ok"], session)
    test.assertTrue(session["run_id_matches"], session)
    # DEP-1 envelope declared columns in declaration order.
    test.assertEqual(tuple(session["record_columns"]), _SIGNAL_DECLARED)
    # Acquisition-time evidence from the acquire() return (freeze §2.3).
    test.assertEqual(session["signal"], _SIGNAL_COLUMNS["signal"])
    test.assertEqual(session["reference"], _SIGNAL_COLUMNS["reference"])
    test.assertEqual(session["differences"], _SIGNAL_DIFFERENCE)
    test.assertEqual(session["reads"], _SIGNAL_READ_TRACE)
    test.assertTrue(session["non_consumptive"], session["reads"])
    artifact = Path(session["data_path"])
    test.assertTrue(artifact.exists())
    sha_before = _sha256_file(str(artifact))
    _evidence("signal-session", pid=session["pid"],
              execution_id=session["execution_id"],
              group_id=session["group_id"],
              artifact=str(artifact), sha256_before=sha_before,
              temp_root=str(root), clean_prefix=session["clean_prefix"],
              env_softlab_vars=session["env_softlab_vars"],
              reads=session["reads"], differences=session["differences"],
              non_consumptive=session["non_consumptive"],
              driver_stderr=completed.stderr.strip())
    return session, artifact, sha_before


def _compare_signal_artifact(test, payload, session):
    """Freeze §5.2 step 7 oracle comparison for one signal artifact."""
    envelope = payload["envelope"]
    record = payload["records"]["signal"]
    samples = record["samples"]

    # Declared column sequence, envelope order (p3 §3.5.1 rule 5 —
    # non-lexicographic for signal); each same-named stored dataset
    # retrieved by name, compared by row index under exact == (§3).
    declared = [column["name"] for column in envelope["record"]["columns"]]
    test.assertEqual(tuple(declared), _SIGNAL_DECLARED)
    test.assertEqual(record["row_count"], _SIGNAL_POINTS)
    test.assertEqual(set(samples), set(_SIGNAL_DECLARED))
    for name in ("signal", "reference"):
        test.assertEqual(samples[name], _SIGNAL_COLUMNS[name])
    # Caller-derived difference: recomputed from stored source samples,
    # asserted ABSENT from the stored columns (never read from storage).
    test.assertNotIn("difference", samples)
    test.assertNotIn("difference", declared)
    recomputed = [s - r for s, r in zip(samples["signal"],
                                        samples["reference"])]
    test.assertEqual(recomputed, _SIGNAL_DIFFERENCE)
    # Cross-check against the acquisition-time evidence (plan W3 step 7):
    # the stored columns agree with the driver's acquire() return.
    test.assertEqual(samples["signal"], session["signal"])
    test.assertEqual(samples["reference"], session["reference"])

    # §4 retained per-run assertions.
    timestamp_decl = [
        column for column in envelope["record"]["columns"]
        if column["name"] == "timestamp"
    ]
    test.assertEqual(len(timestamp_decl), 1)
    test.assertEqual(timestamp_decl[0]["unit"], "s")
    timestamps = samples["timestamp"]
    test.assertEqual(len(timestamps), _SIGNAL_POINTS)
    test.assertTrue(all(math.isfinite(value) for value in timestamps))

    # Outcome/boundary literals: completed_rows == requested_rows == 3.
    outcome = envelope["outcome"]
    test.assertEqual(outcome["completed_rows"], _SIGNAL_POINTS)
    test.assertEqual(outcome["requested_rows"], _SIGNAL_POINTS)

    # Recorded configuration (freeze §5.3, signal): points, the full
    # read policy (non-consumptive semantics + frozen offsets), scheduler
    # ownership, and the derived-difference storage policy.
    payload_section = envelope["payload"]
    test.assertEqual(payload_section["points"], _SIGNAL_POINTS)
    test.assertEqual(payload_section["read_policy"], _SIGNAL_READ_POLICY)
    test.assertEqual(payload_section["scheduler"]["ownership"],
                     _SIGNAL_SCHEDULER_OWNERSHIP)
    difference = payload_section["derived"]["difference"]
    test.assertEqual(difference["storage"], _SIGNAL_DERIVED_STORAGE)
    test.assertEqual(difference["formula"], _SIGNAL_DERIVED_FORMULA)


class _ThermalWalkthroughBase(unittest.TestCase):
    """Shared A/B driver for W1/W2 (thermal original vs clean-session rerun)."""

    profile = "thermal"
    rows = 8

    def _run_original_vs_rerun(self, adapter, oracle):
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as root_a, \
                tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as root_b:
            # Sessions A and B: separate fresh processes, separate temp
            # roots, same declared inputs (freeze §5.2 steps 2-5).
            session_a, artifact_a, sha_a = _thermal_session(
                self, root_a, adapter)
            session_b, artifact_b, sha_b = _thermal_session(
                self, root_b, adapter)

            # Distinct identities are expected and asserted (freeze §4).
            self.assertNotEqual(session_b["execution_id"],
                                session_a["execution_id"])
            self.assertNotEqual(session_b["group_id"], session_a["group_id"])

            # Separate-process inspections: two per artifact, each on a
            # fresh verified copy; originals never loaded in place.
            payload_a = _inspect_twice(self, artifact_a, self.profile,
                                       root_a, "A")
            payload_b = _inspect_twice(self, artifact_b, self.profile,
                                       root_b, "B")

            # Oracle comparison per artifact (freeze §5.2 step 7).
            _compare_thermal_artifact(self, payload_a, oracle, session_a)
            _compare_thermal_artifact(self, payload_b, oracle, session_b)
            _assert_common_per_run(self, payload_a, session_a, artifact_a,
                                   self.rows)
            _assert_common_per_run(self, payload_b, session_b, artifact_b,
                                   self.rows)

            # Original-byte preservation (C3 discipline) per run.
            self.assertEqual(_sha256_file(str(artifact_a)), sha_a)
            self.assertEqual(_sha256_file(str(artifact_b)), sha_b)

            # A-vs-B comparison (freeze §5.2 step 8): every non-excluded
            # column matches under §3; recorded configurations agree;
            # identities stay distinct. §4 fields are never compared here.
            record_a = payload_a["records"]["thermal_trace"]
            record_b = payload_b["records"]["thermal_trace"]
            samples_a, samples_b = record_a["samples"], record_b["samples"]
            for name in samples_a:
                if name == "timestamp":
                    continue  # §4 excluded
                if name == "temperature_degc":
                    _assert_columns_close(self, samples_a[name],
                                          samples_b[name], temperature=True)
                else:
                    self.assertEqual(samples_a[name], samples_b[name])
            envelope_a = payload_a["envelope"]
            envelope_b = payload_b["envelope"]
            self.assertEqual(envelope_a["payload"], envelope_b["payload"])
            manifest_a = json.loads(
                payload_a["legacy_meta"]["thermal_manifest"])
            manifest_b = json.loads(
                payload_b["legacy_meta"]["thermal_manifest"])
            for key in ("version", "clock"):
                self.assertEqual(manifest_a[key], manifest_b[key])
            _evidence("avsb-thermal", adapter=adapter,
                      execution_id_a=session_a["execution_id"],
                      execution_id_b=session_b["execution_id"],
                      group_id_a=session_a["group_id"],
                      group_id_b=session_b["group_id"],
                      sha256_a=sha_a, sha256_b=sha_b)


class W1ThermalV1OriginalVsRerun(_ThermalWalkthroughBase):
    """W1: thermal v1 (adapter='coordinated') original vs rerun."""

    def test_w1_thermal_v1_original_vs_clean_session_rerun(self):
        self._run_original_vs_rerun("coordinated", _THERMAL_V1)


class W2ThermalV2OriginalVsRerun(_ThermalWalkthroughBase):
    """W2: thermal v2 (adapter='direct') original vs rerun."""

    def test_w2_thermal_v2_original_vs_clean_session_rerun(self):
        self._run_original_vs_rerun("direct", _THERMAL_V2)


class W3SignalOriginalVsRerun(unittest.TestCase):
    """W3: signal monitor original vs clean-session rerun (DEP-1)."""

    def test_w3_signal_original_vs_clean_session_rerun(self):
        with tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as root_a, \
                tempfile.TemporaryDirectory(prefix=_TEMP_PREFIX) as root_b:
            # Sessions A and B: separate fresh processes, separate temp
            # roots, same declared inputs (freeze §5.2 steps 2-5). Each
            # driver runs acquire(points=3) + the DEP-1 wiring in one
            # process (plan-pinned invocation form) and records its
            # acquire() return evidence for the results report.
            session_a, artifact_a, sha_a = _signal_session(self, root_a)
            session_b, artifact_b, sha_b = _signal_session(self, root_b)

            # Distinct identities are expected and asserted (freeze §4).
            self.assertNotEqual(session_b["execution_id"],
                                session_a["execution_id"])
            self.assertNotEqual(session_b["group_id"], session_a["group_id"])

            # Separate-process inspections: two per artifact on fresh
            # verified copies (F-A guard); originals never loaded in place.
            payload_a = _inspect_twice(self, artifact_a, "signal",
                                       root_a, "A")
            payload_b = _inspect_twice(self, artifact_b, "signal",
                                       root_b, "B")

            # Oracle comparison per artifact (freeze §5.2 step 7).
            _compare_signal_artifact(self, payload_a, session_a)
            _compare_signal_artifact(self, payload_b, session_b)
            _assert_common_per_run(self, payload_a, session_a, artifact_a,
                                   _SIGNAL_POINTS)
            _assert_common_per_run(self, payload_b, session_b, artifact_b,
                                   _SIGNAL_POINTS)

            # Original-byte preservation (C3 discipline) per run.
            self.assertEqual(_sha256_file(str(artifact_a)), sha_a)
            self.assertEqual(_sha256_file(str(artifact_b)), sha_b)

            # A-vs-B comparison (freeze §5.2 step 8): every non-excluded
            # column matches under §3 exact ==; the recorded configuration
            # agrees; identities stay distinct. §4 excluded fields
            # (timestamps, identity values, environment/source values,
            # cross-run byte hashes) are never compared here.
            samples_a = payload_a["records"]["signal"]["samples"]
            samples_b = payload_b["records"]["signal"]["samples"]
            for name in samples_a:
                if name == "timestamp":
                    continue  # §4 excluded
                self.assertEqual(samples_a[name], samples_b[name], name)
            envelope_a = payload_a["envelope"]
            envelope_b = payload_b["envelope"]
            self.assertEqual(envelope_a["payload"], envelope_b["payload"])
            # Acquisition-time evidence agrees across runs (freeze §2.3
            # relations): identical read trace, differences, and samples.
            self.assertEqual(session_b["reads"], session_a["reads"])
            self.assertEqual(session_b["differences"],
                             session_a["differences"])
            self.assertTrue(session_a["non_consumptive"])
            self.assertTrue(session_b["non_consumptive"])
            _evidence("avsb-signal",
                      execution_id_a=session_a["execution_id"],
                      execution_id_b=session_b["execution_id"],
                      group_id_a=session_a["group_id"],
                      group_id_b=session_b["group_id"],
                      reads_a=session_a["reads"],
                      differences_a=session_a["differences"],
                      sha256_a=sha_a, sha256_b=sha_b)


if __name__ == "__main__":
    unittest.main()
