"""Unit tests: PollPolicy contract, failure isolation, exposure updates, state round-trip, clamping."""
import json
import math
import os
import sys
import unittest

import numpy as np

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from aas_poll_research import (KNOBS, PollContext, PolicyConfigError, PolicyStateError,  # noqa: E402
                               ResearchDiscoveryObservation, DiscoveryObservation, knob_kwargs,
                               make_research_policy)
from aas_poll_research.estimators import DecayedGammaRate, GapEWMA, PoissonCUSUM  # noqa: E402

ADAPTIVE = ["gamma_poisson", "gamma_poisson_sqrt", "renewal_window", "hazard_renewal"]


def mk(name, **kw):
    return make_research_policy(name, **knob_kwargs(name, KNOBS[name][2], **kw))


def obs(t_snap, ready=(), completed=None):
    ready = tuple(float(x) for x in ready)
    return ResearchDiscoveryObservation(len(ready), len(ready), 1,
                                        t_snap + 2.0 if completed is None else completed,
                                        snapshot_at=float(t_snap), ready_at=ready)


def ctx(now):
    return PollContext(now, False, False)


def feed_periodic(p, n=30, period=600.0, jitter=None, start=0.0):
    rng = np.random.default_rng(1)
    p.observe(obs(start))
    t_last = start
    arrivals = start + period * np.arange(1, n + 1) + (rng.normal(0, jitter, n) if jitter else 0)
    for a in arrivals:
        # one empty poll mid-gap, then a poll that finds the arrival
        mid = t_last + (a - t_last) / 2
        p.observe(obs(mid))
        p.observe(obs(a + 20.0, [a]))
        t_last = a + 20.0
    return t_last + 2.0


class TestContract(unittest.TestCase):
    def test_methods_and_names(self):
        for name in ["fixed_interval"] + ADAPTIVE:
            p = mk(name)
            for m in ("validate_config", "observe", "next_delay_seconds", "export_state", "import_state"):
                self.assertTrue(callable(getattr(p, m)), (name, m))
            self.assertIsInstance(p.policy_name, str)
            p.validate_config()
            d = p.next_delay_seconds(ctx(0.0))
            self.assertTrue(math.isfinite(d) and d > 0)

    def test_adaptive_marked_research_only(self):
        for name in ADAPTIVE:
            self.assertEqual(mk(name).status, "research_candidate/not_for_production")

    def test_next_delay_is_pure(self):
        for name in ADAPTIVE:
            p = mk(name)
            now = feed_periodic(p, n=12, jitter=20.0)
            before = json.dumps(p.export_state(), sort_keys=True)
            d1 = p.next_delay_seconds(ctx(now))
            d2 = p.next_delay_seconds(ctx(now))
            self.assertEqual(d1, d2, name)
            self.assertEqual(before, json.dumps(p.export_state(), sort_keys=True), name)

    def test_invalid_configs_rejected(self):
        bad = [
            ("fixed_interval", {"interval_seconds": 0}),
            ("fixed_interval", {"interval_seconds": float("nan")}),
            ("gamma_poisson", {"poll_factor": -1}),
            ("gamma_poisson", {"min_poll_s": 100, "max_poll_s": 50}),
            ("gamma_poisson_sqrt", {"cost_ratio": 0}),
            ("renewal_window", {"kappa": 0}),
            ("renewal_window", {"alpha": 1.5}),
            ("hazard_renewal", {"cost_ratio": float("inf")}),
            ("hazard_renewal", {"grid_step_s": 60}),       # > min_poll_s
            ("hazard_renewal", {"objective": "magic"}),
            ("hazard_renewal", {"slo_cap_s": 1.0}),
            ("gamma_poisson", {"prior_rate_per_hour": 0}),
        ]
        for name, kw in bad:
            with self.assertRaises(PolicyConfigError, msg=(name, kw)):
                make_research_policy(name, **kw)

    def test_accepts_production_count_only_observation(self):
        for name in ADAPTIVE:
            p = mk(name)
            p.observe(DiscoveryObservation(0, 0, 1, 100.0))
            p.observe(DiscoveryObservation(3, 3, 1, 700.0))
            d = p.next_delay_seconds(ctx(701.0))
            self.assertTrue(p.min_poll_s <= d <= p.max_poll_s, name)
            self.assertFalse(p.has_exact)


class TestEstimatesAndFailures(unittest.TestCase):
    def test_empty_observation_adds_exposure_only(self):
        p = mk("gamma_poisson")
        p.observe(obs(0.0))
        a0, b0 = p.fast.a, p.fast.b
        r0 = p.rate_per_hour()
        p.observe(obs(3600.0))  # successful, 0 READY: one hour of exposure
        self.assertGreater(p.fast.b, b0)
        self.assertLessEqual(p.fast.a, a0 + 1e-12)
        self.assertLess(p.rate_per_hour(), r0)
        self.assertEqual(p.n_obs, 2)

    def test_empty_observation_adds_no_gap_samples(self):
        r = mk("renewal_window")
        h = mk("hazard_renewal")
        for p in (r, h):
            p.observe(obs(0.0, [-10.0]))
            p.observe(obs(600.0))
            p.observe(obs(1200.0))
        self.assertEqual(r.gaps.n, 0)
        self.assertEqual(len(h.gap_len), 0)
        h.observe(obs(1800.0, [1700.0]))
        self.assertEqual(list(h.gap_len), [1710.0])  # one completed gap, open gap never double counted

    def test_policy_state_unchanged_without_observe(self):
        """A network failure produces no observe() call; nothing else may mutate the state."""
        for name in ADAPTIVE:
            p = mk(name)
            feed_periodic(p, n=8)
            s = json.dumps(p.export_state(), sort_keys=True)
            for now in (10_000.0, 20_000.0, 90_000.0):   # long outage: only next_delay calls
                p.next_delay_seconds(ctx(now))
            self.assertEqual(s, json.dumps(p.export_state(), sort_keys=True))

    def test_regressed_snapshot_rejected(self):
        p = mk("gamma_poisson")
        p.observe(obs(1000.0))
        s = p.export_state()
        p.observe(obs(500.0, [400.0]))
        self.assertEqual(p.n_rejected, 1)
        self.assertEqual(p.export_state()["tracking"]["fast"], s["tracking"]["fast"])

    def test_events_outside_window_ignored(self):
        p = mk("gamma_poisson")
        p.observe(obs(1000.0))
        a = p.fast.a
        p.observe(obs(2000.0, [900.0, 2500.0]))  # one already-covered, one in the future: both dropped
        self.assertLessEqual(p.fast.a, a)

    def test_gap_ewma_is_not_ewma_of_inverse(self):
        rng = np.random.default_rng(3)
        g = GapEWMA(600.0, 0.02)
        inv = 1 / 600.0
        for x in rng.exponential(600.0, 5000):
            g.update(x)
            inv += 0.02 * (1 / max(x, 1e-3) - inv)
        self.assertAlmostEqual(3600 / g.mean, 6.0, delta=1.5)
        self.assertGreater(inv * 3600, 12.0)  # the forbidden estimator is badly biased

    def test_gamma_converges_without_forgetting_bias(self):
        rng = np.random.default_rng(5)
        est = DecayedGammaRate(1.0, 0.1, 12.0)
        t = 0.0
        for _ in range(2000):
            dt = 300.0
            n = rng.poisson(10.0 * dt / 3600)
            ages = rng.uniform(0, dt, n)
            est.update(dt, ages)
            t += dt
        self.assertAlmostEqual(est.rate_per_hour(), 10.0, delta=2.0)

    def test_cusum_detects_rate_jump(self):
        c = PoissonCUSUM(2.0, 5.0)
        rng = np.random.default_rng(0)
        alarms_before = sum(c.step(rng.poisson(0.5), 0.5) for _ in range(200))
        hit = None
        for i in range(200):
            if c.step(rng.poisson(4 * 0.5), 0.5):
                hit = i
                break
        self.assertLessEqual(alarms_before, 2)
        self.assertIsNotNone(hit)
        self.assertLess(hit, 20)


class TestStateRoundTrip(unittest.TestCase):
    def test_roundtrip_identical_decisions(self):
        for name in ["fixed_interval"] + ADAPTIVE:
            p = mk(name)
            now = feed_periodic(p, n=15, jitter=25.0)
            st = json.loads(json.dumps(p.export_state()))
            q = mk(name)
            q.import_state(st)
            self.assertEqual(json.dumps(p.export_state(), sort_keys=True),
                             json.dumps(q.export_state(), sort_keys=True), name)
            for dt in (0.0, 5.0, 300.0):
                self.assertEqual(p.next_delay_seconds(ctx(now + dt)), q.next_delay_seconds(ctx(now + dt)))
            # continue both with the same observations -> identical again
            for x in (p, q):
                x.observe(obs(now + 600.0, [now + 550.0]))
            self.assertEqual(p.next_delay_seconds(ctx(now + 603.0)), q.next_delay_seconds(ctx(now + 603.0)))

    def test_import_rejects_foreign_or_corrupt_state(self):
        for name in ADAPTIVE:
            p = mk(name)
            feed_periodic(p, n=5)
            good = p.export_state()
            q = mk(name)
            q_before = json.dumps(q.export_state(), sort_keys=True)
            bad_states = [
                {**good, "schema_version": 99},
                {**good, "policy_name": "other"},
                {**good, "config": {**good["config"], "max_poll_s": 1234.0}},
                {**good, "tracking": {**good["tracking"], "fast": [float("nan"), 1.0]}},
                "not a dict",
            ]
            for bs in bad_states:
                with self.assertRaises(PolicyStateError, msg=name):
                    q.import_state(bs)
                self.assertEqual(q_before, json.dumps(q.export_state(), sort_keys=True))
        f = mk("fixed_interval")
        with self.assertRaises(PolicyStateError):
            f.import_state({"schema_version": 2})

    def test_state_is_small(self):
        p = mk("hazard_renewal")
        feed_periodic(p, n=200, jitter=10.0)
        self.assertLess(len(json.dumps(p.export_state())), 1_000_000)
        self.assertLessEqual(len(p.gap_len), p.buffer_size)


class TestClamping(unittest.TestCase):
    def test_bounds_hold_under_extremes(self):
        for name in ADAPTIVE:
            # flood: thousands of events -> must not go below min_poll
            p = mk(name)
            p.observe(obs(0.0))
            p.observe(obs(60.0, list(np.linspace(1, 59, 500))))
            self.assertEqual(p.next_delay_seconds(ctx(62.0)), p.min_poll_s, name)
            # drought: days without events -> must not exceed max_poll
            q = mk(name)
            q.observe(obs(0.0, [-5.0]))
            for k in range(1, 50):
                q.observe(obs(k * 3600.0))
            self.assertLessEqual(q.next_delay_seconds(ctx(49 * 3600.0 + 2)), q.max_poll_s)
            self.assertGreaterEqual(q.next_delay_seconds(ctx(49 * 3600.0 + 2)), q.min_poll_s)

    def test_slo_cap(self):
        for name in ADAPTIVE:
            p = mk(name, slo_cap_s=120.0)
            p.observe(obs(0.0))
            for k in range(1, 30):
                p.observe(obs(k * 3600.0))
            self.assertLessEqual(p.next_delay_seconds(ctx(29 * 3600.0 + 2)), 120.0, name)

    def test_drought_lengthens_interval(self):
        for name in ("gamma_poisson", "gamma_poisson_sqrt", "hazard_renewal"):
            p = mk(name)
            p.observe(obs(0.0))
            d0 = p.next_delay_seconds(ctx(2.0))
            for k in range(1, 30):
                p.observe(obs(k * 1800.0))
            self.assertGreater(p.next_delay_seconds(ctx(29 * 1800.0 + 2)), d0, name)


class TestPolicyBehaviour(unittest.TestCase):
    def test_hazard_matches_sqrt_law_on_poisson_prior(self):
        """With no gap history the hazard policy reduces to T* = sqrt(2c/lambda)."""
        for c in (10.0, 30.0, 100.0):
            p = make_research_policy("hazard_renewal", cost_ratio=c, prior_rate_per_hour=6.0)
            g = make_research_policy("gamma_poisson_sqrt", cost_ratio=c, prior_rate_per_hour=6.0)
            dh, dg = p.next_delay_seconds(ctx(0.0)), g.next_delay_seconds(ctx(0.0))
            self.assertLess(abs(dh - dg), 15.0, (c, dh, dg))

    def test_periodic_window_sleeps_then_probes(self):
        for name in ("renewal_window", "hazard_renewal"):
            p = mk(name)
            now = feed_periodic(p, n=30, period=600.0, jitter=5.0)
            # just discovered an arrival (age ~22s): should sleep most of the period
            self.assertGreater(p.next_delay_seconds(ctx(now)), 350.0, name)


if __name__ == "__main__":
    unittest.main()
