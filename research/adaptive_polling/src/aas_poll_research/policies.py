"""Research PollPolicy implementations (PATCH-01 §2.2 contract).

Status of every adaptive policy here: research_candidate / not_for_production.
A policy only decides *when* the next READY query is attempted; it never touches
READY / ACK / VerifyGate / deletion semantics, and it never sees network failures.
"""
from __future__ import annotations

import math
from collections import deque
from typing import Optional, Sequence

import numpy as np
from scipy.special import ndtr

from .estimators import DecayedGammaRate, GapEWMA, PoissonCUSUM
from .interface import (RESEARCH_STATUS, PollContext, PolicyConfigError, PolicyStateError, clamp,
                        finite)

SCHEMA_VERSION = 1


# --------------------------------------------------------------------------------------------
# Fixed interval (bit-compatible with production FixedIntervalPoll)
# --------------------------------------------------------------------------------------------
class FixedIntervalPolicy:
    policy_name = "fixed_interval"
    status = "production_default"

    def __init__(self, interval_seconds: float):
        self.interval_seconds = float(interval_seconds)
        self.validate_config()

    def validate_config(self) -> None:
        if not (finite(self.interval_seconds) and 0 < self.interval_seconds < 365 * 86400):
            raise PolicyConfigError("invalid fixed polling interval")

    def observe(self, event) -> None:
        pass

    def next_delay_seconds(self, context: PollContext) -> float:
        return self.interval_seconds

    def export_state(self) -> dict:
        return {"schema_version": 1}

    def import_state(self, state: dict) -> None:
        if state != {"schema_version": 1}:
            raise PolicyStateError("invalid fixed policy state")


# --------------------------------------------------------------------------------------------
# Common base for adaptive policies
# --------------------------------------------------------------------------------------------
class _AdaptiveBase:
    policy_name = "adaptive_base"
    status = RESEARCH_STATUS

    def __init__(self, *, min_poll_s: float = 30.0, max_poll_s: float = 600.0,
                 slo_cap_s: Optional[float] = None, prior_rate_per_hour: float = 6.0,
                 prior_exposure_h: float = 0.1, fast_half_life_h: float = 1.0,
                 slow_half_life_h: float = 12.0, weight_fast: float = 0.5,
                 rate_floor_per_hour: float = 0.05, rate_ceiling_per_hour: float = 3600.0):
        self.min_poll_s = float(min_poll_s)
        self.max_poll_s = float(max_poll_s)
        self.slo_cap_s = None if slo_cap_s is None else float(slo_cap_s)
        self.prior_rate_per_hour = float(prior_rate_per_hour)
        self.prior_exposure_h = float(prior_exposure_h)
        self.fast_half_life_h = float(fast_half_life_h)
        self.slow_half_life_h = float(slow_half_life_h)
        self.weight_fast = float(weight_fast)
        self.rate_floor_per_hour = float(rate_floor_per_hour)
        self.rate_ceiling_per_hour = float(rate_ceiling_per_hour)

    # ---- config -----------------------------------------------------------------------------
    def _base_config(self) -> dict:
        return {
            "min_poll_s": self.min_poll_s, "max_poll_s": self.max_poll_s, "slo_cap_s": self.slo_cap_s,
            "prior_rate_per_hour": self.prior_rate_per_hour, "prior_exposure_h": self.prior_exposure_h,
            "fast_half_life_h": self.fast_half_life_h, "slow_half_life_h": self.slow_half_life_h,
            "weight_fast": self.weight_fast, "rate_floor_per_hour": self.rate_floor_per_hour,
            "rate_ceiling_per_hour": self.rate_ceiling_per_hour,
        }

    def config(self) -> dict:
        return self._base_config()

    def validate_config(self) -> None:
        c = self._base_config()
        for k, v in c.items():
            if v is None and k == "slo_cap_s":
                continue
            if not finite(v):
                raise PolicyConfigError(f"{k} must be finite")
        if not 0 < self.min_poll_s <= self.max_poll_s:
            raise PolicyConfigError("require 0 < min_poll_s <= max_poll_s")
        if self.slo_cap_s is not None and self.slo_cap_s < self.min_poll_s:
            raise PolicyConfigError("slo_cap_s must be >= min_poll_s")
        if self.prior_rate_per_hour <= 0 or self.prior_exposure_h <= 0:
            raise PolicyConfigError("prior rate/exposure must be > 0")
        if self.fast_half_life_h <= 0 or self.slow_half_life_h <= 0:
            raise PolicyConfigError("half lives must be > 0")
        if not 0 <= self.weight_fast <= 1:
            raise PolicyConfigError("weight_fast in [0,1]")
        if not 0 < self.rate_floor_per_hour < self.rate_ceiling_per_hour:
            raise PolicyConfigError("invalid rate floor/ceiling")

    @property
    def upper_bound_s(self) -> float:
        hi = self.max_poll_s
        if self.slo_cap_s is not None:
            hi = min(hi, self.slo_cap_s)
        return hi

    # ---- state ------------------------------------------------------------------------------
    def _init_state(self) -> None:
        self.last_snapshot: Optional[float] = None   # A clock, last *successful* snapshot boundary
        self.last_completed: Optional[float] = None  # B monotonic, completion of that scan
        self.last_arrival: Optional[float] = None    # A clock, latest known ready_at
        self.has_exact = False                       # ever received exact ready_at/snapshot_at
        self.n_obs = 0
        self.n_rejected = 0
        self.fast = DecayedGammaRate(self.prior_rate_per_hour, self.prior_exposure_h, self.fast_half_life_h)
        self.slow = DecayedGammaRate(self.prior_rate_per_hour, self.prior_exposure_h, self.slow_half_life_h)

    def rate_per_hour(self) -> float:
        r = self.weight_fast * self.fast.rate_per_hour() + (1 - self.weight_fast) * self.slow.rate_per_hour()
        return clamp(r, self.rate_floor_per_hour, self.rate_ceiling_per_hour)

    # ---- observe ----------------------------------------------------------------------------
    def observe(self, event) -> None:
        """Successful READY scan only. Never call for network/auth failures (PATCH-01 §9)."""
        completed = getattr(event, "completed_at_monotonic", None)
        if not finite(completed):
            raise ValueError("observation without finite completed_at_monotonic")
        snap = getattr(event, "snapshot_at", None)
        exact = snap is not None
        if not exact:
            snap = float(completed)  # count-only production observation: B clock as boundary
        snap = float(snap)
        if self.last_snapshot is not None and snap < self.last_snapshot:
            self.n_rejected += 1     # regressed boundary: do not corrupt statistics
            return
        if exact:
            lo = self.last_snapshot
            evs = sorted(float(t) for t in getattr(event, "ready_at", ()) or ()
                         if finite(t) and (lo is None or t > lo) and t <= snap)
            n = len(evs)
            self.has_exact = True
        else:
            evs = None
            n = int(getattr(event, "newly_enqueued_batches", 0))
            if n < 0:
                raise ValueError("negative count")
        dt = None if self.last_snapshot is None else snap - self.last_snapshot
        self._cur_completed = float(completed)
        if dt is not None:
            ages = [snap - t for t in evs] if evs is not None else [0.0] * n
            self._pre_update(dt, n)
            self.fast.update(dt, ages)
            self.slow.update(dt, ages)
        self._update(dt, snap, evs)
        if evs:
            self.last_arrival = evs[-1] if self.last_arrival is None else max(self.last_arrival, evs[-1])
        self.last_snapshot = snap
        self.last_completed = float(completed)
        self.n_obs += 1

    def _pre_update(self, dt: float, n: int) -> None:
        pass

    def _update(self, dt, snap, evs) -> None:
        pass

    # ---- decide -----------------------------------------------------------------------------
    def _offset(self, context: PollContext) -> float:
        if self.last_completed is None:
            return 0.0
        return max(0.0, float(context.now_monotonic) - self.last_completed)

    def next_delay_seconds(self, context: PollContext) -> float:
        raw = self._raw_delay(context)
        return clamp(float(raw), self.min_poll_s, self.upper_bound_s)

    def _raw_delay(self, context: PollContext) -> float:
        raise NotImplementedError

    # ---- export / import --------------------------------------------------------------------
    def _export_model(self) -> dict:
        return {}

    def _import_model(self, m: dict) -> None:
        pass

    def export_state(self) -> dict:
        return {
            "schema_version": SCHEMA_VERSION,
            "policy_name": self.policy_name,
            "config": self.config(),
            "tracking": {
                "last_snapshot": self.last_snapshot, "last_completed": self.last_completed,
                "last_arrival": self.last_arrival, "has_exact": self.has_exact,
                "n_obs": self.n_obs, "n_rejected": self.n_rejected,
                "fast": self.fast.to_list(), "slow": self.slow.to_list(),
            },
            "model": self._export_model(),
        }

    def import_state(self, state: dict) -> None:
        if not isinstance(state, dict):
            raise PolicyStateError("state must be a dict")
        if state.get("schema_version") != SCHEMA_VERSION:
            raise PolicyStateError("unsupported schema_version")
        if state.get("policy_name") != self.policy_name:
            raise PolicyStateError("state belongs to another policy")
        if state.get("config") != self.config():
            raise PolicyStateError("state was produced under a different config")
        tr = state.get("tracking")
        if not isinstance(tr, dict):
            raise PolicyStateError("missing tracking")
        backup = self.export_state()
        try:
            for k in ("last_snapshot", "last_completed", "last_arrival"):
                v = tr[k]
                if v is not None and not finite(v):
                    raise PolicyStateError(f"invalid {k}")
            self.last_snapshot = tr["last_snapshot"]
            self.last_completed = tr["last_completed"]
            self.last_arrival = tr["last_arrival"]
            self.has_exact = bool(tr["has_exact"])
            self.n_obs = int(tr["n_obs"])
            self.n_rejected = int(tr["n_rejected"])
            self.fast.load_list(tr["fast"])
            self.slow.load_list(tr["slow"])
            self._import_model(state.get("model") or {})
        except PolicyStateError:
            self._restore(backup)
            raise
        except Exception as exc:  # malformed -> keep previous state intact
            self._restore(backup)
            raise PolicyStateError(f"malformed state: {exc}") from exc

    def _restore(self, backup: dict) -> None:
        tr = backup["tracking"]
        self.last_snapshot, self.last_completed = tr["last_snapshot"], tr["last_completed"]
        self.last_arrival, self.has_exact = tr["last_arrival"], tr["has_exact"]
        self.n_obs, self.n_rejected = tr["n_obs"], tr["n_rejected"]
        self.fast.load_list(tr["fast"])
        self.slow.load_list(tr["slow"])
        self._import_model(backup["model"])


# --------------------------------------------------------------------------------------------
# (a) Gamma–Poisson rate -> T = poll_factor*3600/r_hat  (design v1.2 §8.1), or sqrt cost rule
# --------------------------------------------------------------------------------------------
class GammaPoissonPolicy(_AdaptiveBase):
    def __init__(self, *, mode: str = "linear", poll_factor: float = 0.1, cost_ratio: float = 30.0, **kw):
        super().__init__(**kw)
        self.mode = mode
        self.poll_factor = float(poll_factor)
        self.cost_ratio = float(cost_ratio)
        self.policy_name = "gamma_poisson" if mode == "linear" else "gamma_poisson_sqrt"
        self.validate_config()
        self._init_state()

    def config(self) -> dict:
        c = self._base_config()
        c.update(mode=self.mode, poll_factor=self.poll_factor, cost_ratio=self.cost_ratio)
        return c

    def validate_config(self) -> None:
        super().validate_config()
        if self.mode not in ("linear", "sqrt"):
            raise PolicyConfigError("mode must be linear|sqrt")
        if not (finite(self.poll_factor) and self.poll_factor > 0):
            raise PolicyConfigError("poll_factor > 0")
        if not (finite(self.cost_ratio) and self.cost_ratio > 0):
            raise PolicyConfigError("cost_ratio > 0")

    def _raw_delay(self, context: PollContext) -> float:
        r = self.rate_per_hour()
        if self.mode == "linear":
            t = self.poll_factor * 3600.0 / r
        else:
            t = math.sqrt(7200.0 * self.cost_ratio / r)
        return t - self._offset(context)


# --------------------------------------------------------------------------------------------
# (b) Renewal interval-EWMA + narrow window probing around the predicted next arrival
# --------------------------------------------------------------------------------------------
class RenewalWindowPolicy(_AdaptiveBase):
    policy_name = "renewal_window"

    def __init__(self, *, kappa: float = 0.2, alpha: float = 0.2, cv_gate: float = 0.35,
                 z: float = 2.5, n_min: int = 6, **kw):
        super().__init__(**kw)
        self.kappa = float(kappa)
        self.alpha = float(alpha)
        self.cv_gate = float(cv_gate)
        self.z = float(z)
        self.n_min = int(n_min)
        self.validate_config()
        self._init_state()

    def config(self) -> dict:
        c = self._base_config()
        c.update(kappa=self.kappa, alpha=self.alpha, cv_gate=self.cv_gate, z=self.z, n_min=self.n_min)
        return c

    def validate_config(self) -> None:
        super().validate_config()
        if not (finite(self.kappa) and self.kappa > 0):
            raise PolicyConfigError("kappa > 0")
        if not 0 < self.alpha <= 1:
            raise PolicyConfigError("alpha in (0,1]")
        if not (self.cv_gate >= 0 and self.z > 0 and self.n_min >= 1):
            raise PolicyConfigError("invalid window parameters")

    def _init_state(self) -> None:
        super()._init_state()
        self.gaps = GapEWMA(3600.0 / self.prior_rate_per_hour, self.alpha)

    def _update(self, dt, snap, evs) -> None:
        if not evs:
            return
        prev = self.last_arrival
        for t in evs:
            if prev is not None:
                self.gaps.update(t - prev)  # completed gap only; the open gap is never a sample
            prev = t

    def window_active(self) -> bool:
        return (self.has_exact and self.last_arrival is not None and self.gaps.n >= self.n_min
                and self.gaps.cv <= self.cv_gate)

    def _raw_delay(self, context: PollContext) -> float:
        o = self._offset(context)
        if not self.has_exact or self.last_arrival is None or self.last_snapshot is None:
            return self.kappa * 3600.0 / self.rate_per_hour() - o
        m, s = self.gaps.mean, self.gaps.std
        e = (self.last_snapshot - self.last_arrival) + o  # age of the open gap at 'now' (lower bound)
        if self.window_active():
            ws, we = m - self.z * s, m + self.z * s
            if e < ws:
                return ws - e
            if e <= we:
                return max(self.min_poll_s, min(self.kappa * m, s))
        # rate mode / missed window: censoring-aware lower bound on the current mean gap
        return self.kappa * max(m, e)

    def _export_model(self) -> dict:
        return {"mean": self.gaps.mean, "var": self.gaps.var, "n": self.gaps.n}

    def _import_model(self, m: dict) -> None:
        mean, var, n = float(m["mean"]), float(m["var"]), int(m["n"])
        if not (math.isfinite(mean) and mean > 0 and math.isfinite(var) and var >= 0 and n >= 0):
            raise PolicyStateError("invalid gap stats")
        self.gaps.mean, self.gaps.var, self.gaps.n = mean, var, n


# --------------------------------------------------------------------------------------------
# (c) Hazard / renewal cost minimisation with censoring-correct conditioning + CUSUM
# --------------------------------------------------------------------------------------------
class HazardRenewalPolicy(_AdaptiveBase):
    policy_name = "hazard_renewal"

    def __init__(self, *, cost_ratio: float = 30.0, grid_step_s: float = 10.0, buffer_size: int = 64,
                 gap_half_life_h: float = 12.0, eps_min: float = 0.1, n_full: int = 20,
                 bw_min_s: float = 5.0, bw_frac: float = 0.03, cusum_k: float = 2.0,
                 cusum_h: float = 5.0, use_conditioning: bool = True, objective: str = "avg_cost",
                 g_half_life_h: float = 6.0, g_prior_s: float = 3600.0, **kw):
        super().__init__(**kw)
        self.cost_ratio = float(cost_ratio)
        self.grid_step_s = float(grid_step_s)
        self.buffer_size = int(buffer_size)
        self.gap_half_life_h = float(gap_half_life_h)
        self.eps_min = float(eps_min)
        self.n_full = int(n_full)
        self.bw_min_s = float(bw_min_s)
        self.bw_frac = float(bw_frac)
        self.cusum_k = float(cusum_k)
        self.cusum_h = float(cusum_h)
        self.use_conditioning = bool(use_conditioning)  # False = ablation (ignores open-gap survival)
        self.objective = objective                      # "avg_cost" (default) | "ratio" (ablation)
        self.g_half_life_h = float(g_half_life_h)
        self.g_prior_s = float(g_prior_s)
        self.validate_config()
        self._init_state()

    def config(self) -> dict:
        c = self._base_config()
        c.update(cost_ratio=self.cost_ratio, grid_step_s=self.grid_step_s, buffer_size=self.buffer_size,
                 gap_half_life_h=self.gap_half_life_h, eps_min=self.eps_min, n_full=self.n_full,
                 bw_min_s=self.bw_min_s, bw_frac=self.bw_frac, cusum_k=self.cusum_k,
                 cusum_h=self.cusum_h, use_conditioning=self.use_conditioning,
                 objective=self.objective, g_half_life_h=self.g_half_life_h, g_prior_s=self.g_prior_s)
        return c

    def validate_config(self) -> None:
        super().validate_config()
        if not (finite(self.cost_ratio) and self.cost_ratio > 0):
            raise PolicyConfigError("cost_ratio > 0")
        if not (0 < self.grid_step_s <= self.min_poll_s):
            raise PolicyConfigError("0 < grid_step_s <= min_poll_s")
        if not (1 <= self.buffer_size <= 4096):
            raise PolicyConfigError("buffer_size in [1,4096]")
        if not (0 < self.eps_min <= 1 and self.n_full >= 1 and self.bw_min_s > 0 and self.bw_frac >= 0):
            raise PolicyConfigError("invalid smoothing parameters")
        if not (self.cusum_k > 1 and self.cusum_h > 0 and self.gap_half_life_h > 0):
            raise PolicyConfigError("invalid cusum / half-life")
        if self.objective not in ("avg_cost", "ratio"):
            raise PolicyConfigError("objective must be avg_cost|ratio")
        if not (self.g_half_life_h > 0 and self.g_prior_s > 0):
            raise PolicyConfigError("invalid g estimator parameters")

    def _init_state(self) -> None:
        super()._init_state()
        self.gap_len: deque = deque(maxlen=self.buffer_size)
        self.gap_end: deque = deque(maxlen=self.buffer_size)
        self.cusum = PoissonCUSUM(self.cusum_k, self.cusum_h)
        self._alarm = False
        # realised long-run cost-rate accumulators (exponentially discounted, A-clock seconds)
        self.g_q = 0.0      # successful queries
        self.g_d = 0.0      # realised discovery delay (package-seconds)
        self.g_t = 0.0      # elapsed exposure (seconds)
        self.g_skipped = 0  # intervals excluded because a failure must have intervened

    def _pre_update(self, dt: float, n: int) -> None:
        # predictive check BEFORE updating (survey §4 P3): expected count under slow model
        mu = self.slow.rate_per_hour() * dt / 3600.0
        self._alarm = self.cusum.step(n, mu)

    def _update(self, dt, snap, evs) -> None:
        if dt is not None and getattr(self, "_alarm", False):
            self.slow.reset_to(self.fast)
        self._alarm = False
        # Exclude intervals that must have contained a failure/outage: without failures the gap
        # between successful snapshots never exceeds upper_bound_s (+ scan/latency slack), so
        # failure-induced waiting never inflates g (PATCH-01 §9: failures are not observations).
        if dt is not None and dt > self.upper_bound_s + 15.0:
            self.g_skipped += 1
        elif dt is not None:
            k = math.log(2.0) / (3600.0 * self.g_half_life_h)
            rho = math.exp(-k * dt)
            realised = sum(max(0.0, self._cur_completed - t) for t in evs) if evs else 0.0
            self.g_q = rho * self.g_q + 1.0
            self.g_d = rho * self.g_d + realised
            self.g_t = rho * self.g_t + (1.0 - rho) / k
        if evs:
            prev = self.last_arrival
            for t in evs:
                if prev is not None:
                    self.gap_len.append(t - prev)
                    self.gap_end.append(t)
                prev = t

    # -- distribution -------------------------------------------------------------------------
    def _survival_fn(self):
        lam = self.rate_per_hour() / 3600.0
        n = len(self.gap_len)
        if n == 0 or not self.has_exact:
            return (lambda x: np.exp(-lam * x)), lam, 1.0
        g = np.fromiter(self.gap_len, float, n)
        ends = np.fromiter(self.gap_end, float, n)
        w = np.exp2(-(self.last_snapshot - ends) / (3600.0 * self.gap_half_life_h))
        w = w / w.sum()
        bw = np.maximum(self.bw_min_s, self.bw_frac * g)
        eps = max(self.eps_min, 1.0 - n / self.n_full)

        def surv(x):
            x = np.asarray(x, float)
            emp = ndtr((g[None, :] - x[:, None]) / bw[None, :]) @ w
            return (1.0 - eps) * emp + eps * np.exp(-lam * x)
        return surv, lam, eps

    def expected_arrival_intensity(self, o: float):
        """Return (u grid from the last snapshot, expected #arrivals per grid cell)."""
        d = self.grid_step_s
        horizon = o + self.upper_bound_s
        k = int(min(400, math.ceil(horizon / d)))
        u = d * np.arange(0, k + 1, dtype=float)
        surv, lam, _ = self._survival_fn()
        if self.last_arrival is None or self.last_snapshot is None:
            m = np.full(k, lam * d)
            return u[1:], m
        e = (self.last_snapshot - self.last_arrival) if self.use_conditioning else 0.0
        se = surv(np.array([e]))[0]
        if not (se > 1e-300):
            p1 = np.exp(-lam * u[:-1]) - np.exp(-lam * u[1:])
        else:
            sc = surv(e + u) / se
            p1 = sc[:-1] - sc[1:]
        # inter-arrival pmf on cell offsets: gk[k] = P(gap in [(k-1/2)d, (k+1/2)d)), k >= 1;
        # q = P(gap < d/2) = probability that the next arrival falls in the SAME cell
        # (geometric within-cell multiplicity 1/(1-q), so bursts of sub-cell gaps are not undercounted)
        s_half = surv(d * (np.arange(0, k + 1) + 0.5))
        s00 = surv(np.array([0.0]))[0]
        q = min(0.999, max(0.0, (s00 - s_half[0]) / s00))
        gk = np.clip((s_half[:-1] - s_half[1:]) / s00, 0.0, None)   # gk[i] <-> offset i+1 cells
        p1 = np.clip(p1, 0.0, None)
        # renewal equation on the grid: m_j = (p1_j + sum_{i<j} m_i gk_{j-i}) / (1 - q)
        inv = 1.0 / (1.0 - q)
        m = np.empty(k)
        for j in range(k):
            acc = p1[j]
            if j:
                acc += float(np.dot(m[:j], gk[j - 1::-1][:j]))
            m[j] = acc * inv
        return u[1:], m

    def cost_rate_g(self) -> float:
        """Long-run average cost per second g (package-seconds/s): realised, shrunk to the
        Poisson-optimal g* = sqrt(2 c lambda) under the current rate estimate."""
        g_model = math.sqrt(2.0 * self.cost_ratio * self.rate_per_hour() / 3600.0)
        return ((self.cost_ratio * self.g_q + self.g_d + g_model * self.g_prior_s)
                / (self.g_t + self.g_prior_s))

    def _raw_delay(self, context: PollContext) -> float:
        o = self._offset(context)
        u, m = self.expected_arrival_intensity(o)
        mid = u - self.grid_step_s / 2.0
        cum_m = np.cumsum(m)
        cum_um = np.cumsum(m * mid)
        cost = u * cum_m - cum_um            # C(u): expected accumulated waiting if we poll at u
        if self.objective == "ratio":
            score = (cost + self.cost_ratio) / u      # renewal-reward ratio (ablation)
        else:
            score = cost - self.cost_rate_g() * u     # average-cost relative value: C(u) + c - g u
        delay = u - o
        ok = (delay >= self.min_poll_s - 1e-9) & (delay <= self.upper_bound_s + 1e-9)
        if not ok.any():
            return self.min_poll_s
        idx = np.flatnonzero(ok)
        best = idx[int(np.argmin(score[idx]))]
        return float(delay[best])

    def _export_model(self) -> dict:
        return {"gap_len": list(self.gap_len), "gap_end": list(self.gap_end),
                "cusum": [self.cusum.g_up, self.cusum.g_down, self.cusum.alarms],
                "g": [self.g_q, self.g_d, self.g_t], "g_skipped": self.g_skipped}

    def _import_model(self, mdl: dict) -> None:
        gl = [float(x) for x in mdl.get("gap_len", [])]
        ge = [float(x) for x in mdl.get("gap_end", [])]
        if len(gl) != len(ge) or len(gl) > self.buffer_size or any(
                (not math.isfinite(x)) or x < 0 for x in gl):
            raise PolicyStateError("invalid gap buffer")
        cu = mdl.get("cusum", [0.0, 0.0, 0])
        self.gap_len = deque(gl, maxlen=self.buffer_size)
        self.gap_end = deque(ge, maxlen=self.buffer_size)
        self.cusum.g_up, self.cusum.g_down, self.cusum.alarms = float(cu[0]), float(cu[1]), int(cu[2])
        gq, gd, gt = (float(x) for x in mdl.get("g", [0.0, 0.0, 0.0]))
        if min(gq, gd, gt) < 0 or not all(math.isfinite(x) for x in (gq, gd, gt)):
            raise PolicyStateError("invalid cost-rate accumulators")
        self.g_q, self.g_d, self.g_t = gq, gd, gt
        self.g_skipped = int(mdl.get("g_skipped", 0))


# --------------------------------------------------------------------------------------------
POLICY_CLASSES = {
    "fixed_interval": FixedIntervalPolicy,
    # same production policy, finer interval grid: the strong "Fixed-Train-Oracle" comparator (survey §7.2 B1)
    "fixed_fine": FixedIntervalPolicy,
    "gamma_poisson": lambda **kw: GammaPoissonPolicy(mode="linear", **kw),
    "gamma_poisson_sqrt": lambda **kw: GammaPoissonPolicy(mode="sqrt", **kw),
    "renewal_window": RenewalWindowPolicy,
    "hazard_renewal": HazardRenewalPolicy,
}


def make_research_policy(name: str, **kw):
    if name not in POLICY_CLASSES:
        raise PolicyConfigError(f"unknown research policy {name!r}")
    return POLICY_CLASSES[name](**kw)


def knob_kwargs(family: str, knob: float, **extra) -> dict:
    """Map the single tuned knob of each family to constructor kwargs."""
    if family in ("fixed_interval", "fixed_fine"):
        return {"interval_seconds": knob}
    if family == "gamma_poisson":
        return {"poll_factor": knob, **extra}
    if family in ("gamma_poisson_sqrt", "hazard_renewal"):
        return {"cost_ratio": knob, **extra}
    if family == "renewal_window":
        return {"kappa": knob, **extra}
    raise PolicyConfigError(family)


KNOBS: dict = {
    "fixed_interval": [30.0, 60.0, 120.0, 300.0, 600.0],
    "fixed_fine": [30.0, 40.0, 50.0, 60.0, 75.0, 90.0, 105.0, 120.0, 150.0, 180.0, 210.0, 240.0,
                   300.0, 360.0, 420.0, 480.0, 540.0, 600.0],
    "gamma_poisson": [0.02, 0.05, 0.1, 0.2, 0.5],
    "gamma_poisson_sqrt": [3.0, 10.0, 30.0, 100.0, 300.0],
    "renewal_window": [0.05, 0.1, 0.2, 0.4, 0.8],
    "hazard_renewal": [3.0, 10.0, 30.0, 100.0, 300.0],
}
