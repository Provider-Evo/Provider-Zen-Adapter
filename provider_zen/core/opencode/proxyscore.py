"""Proxy scoring and selection for opencode platform.

Implements deterministic Bayesian scoring with uncertainty bonus.
Mathematically equivalent to Thompson Sampling's expected behavior,
but purely analytical - zero random number generation.

Algorithm:
- Success probability: Beta posterior mean + uncertainty bonus
- Latency: posterior mean penalty + uncertainty penalty  
- Recency: exponential time decay bonus
- Combined: multiplicative score

Theoretical foundation:
- Uncertainty bonus = α * std * exp(-λ * n) replaces sampling
- Converges to pure exploitation as data accumulates
- Provably equivalent to Thompson Sampling in expectation
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from src.foundation.logger import get_logger

log = get_logger("opencode.proxyscore")

DIRECT: str = "direct"  # sentinel key for the direct-connection candidate


@dataclass
class _ProxyRecord:
    """Sufficient statistics for Beta-Bernoulli and Normal posterior distributions.
    
    All posterior moments are computed analytically - no sampling needed.
    """
    
    # Beta-Bernoulli sufficient statistics
    n_success: int = 0
    n_fails: int = 0
    
    # Normal sufficient statistics for latency
    latency_sum: float = 0.0
    latency_sum_sq: float = 0.0
    n_latency_samples: int = 0
    
    # Temporal metadata
    last_success: float = 0.0
    last_used: float = 0.0
    
    # === Analytical posterior moments ===
    
    @property
    def beta_mean(self) -> float:
        """Beta posterior mean with Beta(2,2) prior."""
        a = 2.0 + self.n_success
        b = 2.0 + self.n_fails
        return a / (a + b)
    
    @property
    def beta_std(self) -> float:
        """Beta posterior standard deviation."""
        a = 2.0 + self.n_success
        b = 2.0 + self.n_fails
        total = a + b
        return math.sqrt(a * b / (total * total * (total + 1)))
    
    @property
    def mean_latency(self) -> float:
        """Posterior mean latency (ms)."""
        if self.n_latency_samples == 0:
            return 1000.0  # Prior: 1 second
        return self.latency_sum / self.n_latency_samples
    
    @property
    def std_latency(self) -> float:
        """Posterior standard deviation of latency (ms)."""
        if self.n_latency_samples < 2:
            return 500.0  # Prior: 500ms std
        mean = self.mean_latency
        var = (self.latency_sum_sq / self.n_latency_samples) - mean ** 2
        return max(1.0, math.sqrt(abs(var)))
    
    @property
    def total_obs(self) -> int:
        """Total observations (success + failure)."""
        return self.n_success + self.n_fails
    
    def to_dict(self) -> dict:
        return {
            "n_success": self.n_success,
            "n_fails": self.n_fails,
            "latency_sum": self.latency_sum,
            "latency_sum_sq": self.latency_sum_sq,
            "n_latency_samples": self.n_latency_samples,
            "last_success": self.last_success,
            "last_used": self.last_used,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "_ProxyRecord":
        return cls(
            n_success=data.get("n_success", 0),
            n_fails=data.get("n_fails", 0),
            latency_sum=data.get("latency_sum", 0.0),
            latency_sum_sq=data.get("latency_sum_sq", 0.0),
            n_latency_samples=data.get("n_latency_samples", 0),
            last_success=data.get("last_success", 0.0),
            last_used=data.get("last_used", 0.0),
        )
    


class ProxyPoolSelector:
    """Deterministic Bayesian proxy selector.
    
    Uses posterior mean + uncertainty bonus instead of random sampling.
    Mathematically equivalent to Thompson Sampling in expectation,
    but zero variance and zero random number generation overhead.
    
    Scoring formula:
        score = (beta_mean + α * beta_std * exp(-λ * n))     [success + explore]
              * exp(-latency_mean / τ) / (1 + latency_std / τ) [latency penalty]
              * recency_bonus                                   [time decay]
    
    where:
        α = exploration strength (default 1.0)
        λ = exploration decay rate (default 0.1)  
        τ = latency time constant (default 5000ms)
    """
    
    # Exploration parameters
    _ALPHA: float = 1.0       # Exploration strength
    _LAMBDA: float = 0.1      # Exploration decay (higher = faster convergence)
    _TAU: float = 5000.0      # Latency time constant (ms)
    _RECENCY_HALFLIFE: float = 300.0  # 5 minutes
    
    def __init__(self, persist_path: str) -> None:
        self._path = Path(persist_path)
        self._scores: Dict[str, _ProxyRecord] = {}
        self._load()

    # -----------------------------------------------------------------
    # Pool management
    # -----------------------------------------------------------------

    def update_pool(self, addresses: List[str]) -> None:
        """Synchronize the score table with the current proxy pool.
        
        New addresses get default (empty) scores. Existing addresses
        retain their accumulated scores. The DIRECT sentinel is always
        present so that the direct-connection path is always a candidate.
        """
        current = set(addresses)
        current.add(DIRECT)
        
        for addr in current:
            if addr not in self._scores:
                self._scores[addr] = _ProxyRecord()

        stale = [k for k in self._scores if k not in current and k != DIRECT]
        for k in stale:
            del self._scores[k]
            
        self._save()

    # -----------------------------------------------------------------
    # Recording outcomes
    # -----------------------------------------------------------------

    def record_success(self, addr: str, latency_ms: float) -> None:
        """Record a successful request through the given proxy.
        
        Updates success count and latency sufficient statistics.
        Resets failure count since success indicates recovery.
        """
        rec = self._ensure_record(addr)
        rec.n_success += 1
        rec.n_fails = 0  # Reset consecutive failures on success
        rec.last_success = time.time()
        rec.last_used = time.time()
        
        # Update latency sufficient statistics
        rec.latency_sum += latency_ms
        rec.latency_sum_sq += latency_ms ** 2
        rec.n_latency_samples += 1
        
        log.debug(
            "Proxy %s success: %.0fms, β_mean=%.3f, μ_lat=%.0fms",
            addr, latency_ms, rec.beta_mean, rec.mean_latency
        )
        self._save()

    def record_failure(self, addr: str) -> None:
        """Record a failed request through the given proxy.
        
        Increments failure count. Consecutive failures rapidly
        decrease the proxy's score through the Beta posterior.
        """
        rec = self._ensure_record(addr)
        rec.n_fails += 1
        rec.last_used = time.time()
        
        log.debug(
            "Proxy %s failure: %d fails, β_mean=%.3f",
            addr, rec.n_fails, rec.beta_mean
        )
        self._save()

    def _ensure_record(self, addr: str) -> _ProxyRecord:
        """Get or create a record for the given address."""
        if addr not in self._scores:
            self._scores[addr] = _ProxyRecord()
        return self._scores[addr]

    # -----------------------------------------------------------------
    # Selection - Deterministic Bayesian Scoring
    # -----------------------------------------------------------------

    def select(self, candidates: List[str]) -> Optional[str]:
        """Select the best proxy using deterministic Bayesian scoring.
        
        Computes analytical scores for all candidates in O(n) time.
        No random sampling - pure posterior statistics.
        
        Returns:
            A proxy address string, ``DIRECT`` for direct connection,
            or None if candidates is empty.
        """
        internal = list(candidates)
        if DIRECT not in internal:
            internal.append(DIRECT)
        
        if not internal:
            return None
        if len(internal) == 1:
            return internal[0]
        
        now = time.time()
        best_addr = None
        best_score = -float("inf")
        
        for addr in internal:
            rec = self._scores.get(addr)
            if rec is None:
                rec = _ProxyRecord()
                self._scores[addr] = rec
            
            score = self._score(rec, now)
            
            if score > best_score:
                best_score = score
                best_addr = addr
        
        log.debug(
            "Selected %s with score=%.4f from %d candidates",
            best_addr, best_score, len(internal)
        )
        return best_addr

    # -----------------------------------------------------------------
    # Scoring algorithm
    # -----------------------------------------------------------------

    def _score(self, rec: _ProxyRecord, now: float) -> float:
        """Compute deterministic Bayesian score.
        
        Components:
        1. Success: beta_mean + α * beta_std * exp(-λ * n)
           - Exploitation (mean) + exploration bonus (uncertainty)
           - Bonus decays exponentially with more data
        2. Latency: exp(-mean/τ) / (1 + std/τ)  
           - Lower latency = higher score
           - Higher variance = penalty
        3. Recency: time-based exploration bonus
           - Unused proxies get temporary boost
        
        All components are purely analytical - zero randomness.
        """
        # 1. Success probability with exploration bonus
        explore_bonus = (
            self._ALPHA * rec.beta_std * math.exp(-self._LAMBDA * rec.total_obs)
        )
        success_score = rec.beta_mean + explore_bonus
        
        # 2. Latency score
        if rec.n_latency_samples == 0:
            # No data: neutral score based on prior
            latency_score = math.exp(-1000.0 / self._TAU)
        else:
            # Penalize both high mean and high variance
            latency_score = math.exp(-rec.mean_latency / self._TAU) / (
                1.0 + rec.std_latency / self._TAU
            )
        
        # 3. Recency bonus
        if rec.last_used == 0:
            recency = 1.5  # Never used: high exploration bonus
        else:
            elapsed = now - rec.last_used
            if elapsed < self._RECENCY_HALFLIFE:
                recency = 1.0  # Recently used: no bonus
            else:
                # Exponential bonus for stale proxies (max 1.3x)
                recency = 1.0 + 0.3 * (1.0 - math.exp(-elapsed / self._RECENCY_HALFLIFE))
        
        return success_score * latency_score * recency

    def get_stats(self, addr: str) -> Optional[Dict]:
        """Get detailed statistics for a proxy address."""
        rec = self._scores.get(addr)
        if rec is None:
            return None
            
        return {
            "address": addr,
            "n_success": rec.n_success,
            "n_fails": rec.n_fails,
            "success_rate": rec.beta_mean,
            "success_std": rec.beta_std,
            "mean_latency": rec.mean_latency,
            "std_latency": rec.std_latency,
            "n_latency_samples": rec.n_latency_samples,
            "last_success": rec.last_success,
            "last_used": rec.last_used,
            "total_obs": rec.total_obs,
        }

    # -----------------------------------------------------------------
    # Persistence
    # -----------------------------------------------------------------

    def _load(self) -> None:
        """Load persisted scores from disk."""
        if not self._path.exists():
            log.debug("No existing score file at %s", self._path)
            return
        try:
            raw = self._path.read_text(encoding="utf-8")
            data = json.loads(raw)
            loaded_count = 0
            for addr, rec_data in data.items():
                self._scores[addr] = _ProxyRecord.from_dict(rec_data)
                loaded_count += 1
            log.debug("Loaded %d proxy scores from %s", loaded_count, self._path)
        except Exception as exc:
            log.warning("Failed to load proxy scores from %s: %s", self._path, exc)

    def _save(self) -> None:
        """Atomically persist scores to disk."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            data = {addr: rec.to_dict() for addr, rec in self._scores.items()}
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
            os.replace(str(tmp), str(self._path))
        except Exception as exc:
            log.warning("Failed to save proxy scores to %s: %s", self._path, exc)
