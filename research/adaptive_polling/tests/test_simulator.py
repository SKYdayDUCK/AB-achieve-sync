"""Simulator tests: paired reproducibility, failure isolation, fixed-poll theory, checkpointing."""
import json
import os
import sys
import unittest

import numpy as np

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from aas_poll_research import (KNOBS, FixedIntervalPolicy, SimConfig, WORKLOADS, knob_kwargs,  # noqa: E402
                               make_faults, make_research_policy, make_trace, no_faults,
                               simulate_long_poll, simulate_poll)
from aas_poll_research.faults import FaultTrace  # noqa: E402

DAY = 86400.0
ALL = ["fixed_interval", "gamma_poisson", "gamma_poisson_sqrt", "renewal_window", "hazard_renewal"]


def mk(name, **kw):
    return make_research_policy(name, **knob_kwargs(name, KNOBS[name][2], **kw))


class TestTracesReproducible(unittest.TestCase):
    def test_same_seed_same_trace(self):
        for w in WORKLOADS:
            a, b = make_trace(w, 7, horizon_s=DAY), make_trace(w, 7, horizon_s=DAY)
            np.testing.assert_array_equal(a.arrivals, b.arrivals)
            np.testing.assert_array_equal(a.service_s, b.service_s)
            c = make_trace(w, 8, horizon_s=DAY)
            self.assertFalse(np.array_equal(a.arrivals, c.arrivals), w)
            self.assertTrue(np.all(np.diff(a.arrivals) >= 0))
            self.assertTrue(np.all((a.arrivals > 0) & (a.arrivals <= DAY)))

    def test_faults_reproducible_and_policy_independent(self):
        f1, f2 = make_faults(3, 2 * DAY), make_faults(3, 2 * DAY)
        np.testing.assert_array_equal(f1.outages, f2.outages)
        np.testing.assert_array_equal(f1.transient, f2.transient)

    def test_paired_runs_bitwise_reproducible(self):
        tr = make_trace("onoff", 11, horizon_s=2 * DAY)
        f = make_faults(11, 5 * DAY)
        arr_before = tr.arrivals.copy()
        for name in ALL:
            r1 = simulate_poll(mk(name), tr, f)
            r2 = simulate_poll(mk(name), tr, f)
            self.assertEqual(r1, r2, name)
        np.testing.assert_array_equal(arr_before, tr.arrivals)  # trace is never mutated by a run
        self.assertEqual(simulate_long_poll(tr, f), simulate_long_poll(tr, f))


class TestFailureIsolation(unittest.TestCase):
    def test_observe_never_called_on_failure(self):
        tr = make_trace("poisson", 2, horizon_s=DAY)
        # heavy faults: 3 long outages + 5% transient
        outages = np.array([[3600.0, 7200.0], [20000.0, 26000.0], [50000.0, 50600.0]])
        rng = np.random.default_rng(0)
        f = FaultTrace(0, outages, rng.uniform(size=4 * 86400) < 0.05, 4 * DAY)
        for name in ALL:
            seen = []
            res = simulate_poll(mk(name), tr, f, spy=lambda kind, t, o: seen.append((t, o)))
            self.assertEqual(len(seen), res["success_nonempty"] + res["success_empty"], name)
            self.assertEqual(res["attempts"], len(seen) + res["failed"])
            self.assertGreater(res["failed"], 0)
            for t, o in seen:
                self.assertFalse(f.attempt_fails(t))
                # no leak: an observation only reveals READY with ready_at <= its snapshot
                self.assertTrue(all(a <= o.snapshot_at for a in o.ready_at))

    def test_outage_delays_but_never_loses_packages(self):
        tr = make_trace("periodic", 4, horizon_s=DAY)
        f = FaultTrace(0, np.array([[0.0, 0.5 * DAY]]), np.zeros(1, bool), DAY)
        for name in ALL:
            r = simulate_poll(mk(name), tr, f)
            self.assertEqual(r["n_arrivals"], tr.n)
            self.assertGreater(r["delay_max"], 0.4 * DAY)  # packages from the outage half wait
            self.assertGreater(r["failed"], 100)

    def test_failures_do_not_change_rate_estimate(self):
        """Estimator after [success, failures..., success] == estimator after [success, success]."""
        tr = make_trace("poisson", 9, horizon_s=DAY)
        p_fail, p_clean = mk("gamma_poisson"), mk("gamma_poisson")
        obs_fail = []
        f = FaultTrace(0, np.array([[1000.0, 9000.0]]), np.zeros(1, bool), DAY)
        simulate_poll(p_fail, tr, f, spy=lambda k, t, o: obs_fail.append(o))
        for o in obs_fail:  # replay exactly the successful observations to a clean policy
            p_clean.observe(o)
        self.assertEqual(p_fail.export_state(), p_clean.export_state())


class TestFixedTheory(unittest.TestCase):
    def test_fixed_mean_delay_is_half_period(self):
        cfg = SimConfig(scan_duration_s=0.0, immediate_recheck=False)
        tr = make_trace("poisson", 21, horizon_s=8 * DAY, rate_h=60.0)  # ~11.5k arrivals
        for T in (60.0, 300.0, 600.0):
            r = simulate_poll(FixedIntervalPolicy(T), tr, no_faults(0), cfg)
            self.assertAlmostEqual(r["delay_mean"] / (T / 2), 1.0, delta=0.04, msg=T)
            self.assertAlmostEqual(r["delay_p95"] / (0.95 * T), 1.0, delta=0.04, msg=T)
            self.assertLessEqual(r["delay_max"], T + 1e-9)
            self.assertAlmostEqual(r["polls_per_hour"], 3600 / T, delta=0.02 * 3600 / T)

    def test_scan_duration_and_recheck_accounting(self):
        cfg = SimConfig(scan_duration_s=2.0, immediate_recheck=True)
        tr = make_trace("poisson", 22, horizon_s=2 * DAY)
        r = simulate_poll(FixedIntervalPolicy(120.0), tr, no_faults(0), cfg)
        self.assertGreaterEqual(r["delay_p50"], 2.0)
        self.assertEqual(r["rechecks"], r["success_nonempty"] - _nonempty_rechecks(tr, cfg))
        self.assertEqual(r["attempts"], r["success_nonempty"] + r["success_empty"] + r["failed"])

    def test_long_poll_delay_is_latency_without_faults(self):
        tr = make_trace("poisson", 5, horizon_s=DAY)
        r = simulate_long_poll(tr, no_faults(0), SimConfig(scan_duration_s=2.0))
        # an arrival during the 2s response latency is caught by the immediately re-issued request
        self.assertAlmostEqual(r["delay_p50"], 2.0, places=6)
        self.assertLessEqual(r["delay_max"], 4.0 + 1e-9)
        self.assertGreaterEqual(r["attempts"], r["success_nonempty"])


def _nonempty_rechecks(tr, cfg):
    """Re-checks that themselves found something (they do not trigger another re-check)."""
    r = simulate_poll(FixedIntervalPolicy(120.0), tr, no_faults(0), cfg, record_log=True)
    log = r["log"]
    cnt = 0
    for i in range(1, len(log)):
        prev, cur = log[i - 1], log[i]
        if prev[1] == 1 and prev[2] > 0 and prev[3] == 0.0 and cur[2] > 0:
            cnt += 1
    return cnt


class TestCheckpoint(unittest.TestCase):
    def test_export_import_mid_run_is_transparent(self):
        tr = make_trace("onoff", 13, horizon_s=2 * DAY)
        f = make_faults(13, 5 * DAY)
        for name in ALL:
            base = simulate_poll(mk(name), tr, f)
            ck = simulate_poll(mk(name), tr, f, checkpoint_at=DAY, policy_factory=lambda n=name: mk(n))
            self.assertEqual(base, ck, name)


class TestProductionCompat(unittest.TestCase):
    PROD_SRC = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "src"))

    def test_production_fixed_policy_matches_research_copy(self):
        if not os.path.isdir(self.PROD_SRC):
            self.skipTest("production source not present")
        sys.dont_write_bytecode = True        # never write __pycache__ into the production tree
        if self.PROD_SRC not in sys.path:
            sys.path.append(self.PROD_SRC)
        from aas.b.scheduling import policy as prod  # read-only import
        tr = make_trace("diurnal", 3, horizon_s=DAY)
        f = make_faults(3, 4 * DAY)
        for T in (60.0, 300.0):
            a = simulate_poll(prod.FixedIntervalPoll(T), tr, f)
            b = simulate_poll(FixedIntervalPolicy(T), tr, f)
            self.assertEqual(a, b)
        # research policies accept the production observation type
        for name in ALL[1:]:
            p = mk(name)
            p.observe(prod.DiscoveryObservation(0, 0, 1, 10.0))
            p.observe(prod.DiscoveryObservation(2, 2, 1, 400.0))
            d = p.next_delay_seconds(prod.PollContext(401.0, False, False))
            self.assertTrue(30.0 <= d <= 600.0)
        self.assertEqual(prod.FixedIntervalPoll(60).export_state(), FixedIntervalPolicy(60).export_state())


if __name__ == "__main__":
    unittest.main()
