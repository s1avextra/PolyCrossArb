"""Bregman / KL divergence projection onto the marginal polytope.

Given observed prices p (which may violate constraints), find the nearest
feasible point q that satisfies all polytope constraints, minimising
KL(q || p) = sum(q_i * log(q_i / p_i)).

The projection distance is the guaranteed arbitrage profit under the
Logarithmic Market Scoring Rule used by Polymarket.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
from scipy.optimize import minimize

from polycrossarb.arb.polytope import PolytopeConstraints

log = logging.getLogger(__name__)

_EPS = 1e-10  # avoid log(0)


@dataclass
class ProjectionResult:
    """Result of projecting observed prices onto the polytope."""
    observed: np.ndarray       # original prices p
    projected: np.ndarray      # feasible prices q (nearest point)
    kl_divergence: float       # KL(q || p) — the "distance"
    profit_per_set: float      # guaranteed profit per unit
    price_adjustments: np.ndarray  # q - p (what needs to change)
    converged: bool
    n_iterations: int = 0

    @property
    def max_adjustment(self) -> float:
        return float(np.max(np.abs(self.price_adjustments)))

    @property
    def trade_directions(self) -> list[str]:
        """For each variable: 'buy' if projected < observed, 'sell' if projected > observed."""
        dirs = []
        for adj in self.price_adjustments:
            if adj < -1e-6:
                dirs.append("buy")   # price should be lower → market overpriced → sell
            elif adj > 1e-6:
                dirs.append("sell")  # price should be higher → market underpriced → buy
            else:
                dirs.append("hold")
        return dirs


def kl_divergence(q: np.ndarray, p: np.ndarray) -> float:
    """KL(q || p) = sum(q_i * log(q_i / p_i) - q_i + p_i).

    Uses the generalised KL (I-divergence) which is non-negative
    and doesn't require q, p to be normalised distributions.
    """
    q_safe = np.clip(q, _EPS, 1 - _EPS)
    p_safe = np.clip(p, _EPS, 1 - _EPS)
    return float(np.sum(q_safe * np.log(q_safe / p_safe) - q_safe + p_safe))


def project_onto_polytope(
    polytope: PolytopeConstraints,
    max_iter: int = 500,
    tol: float = 1e-10,
) -> ProjectionResult:
    """Project observed prices onto the feasible polytope using KL minimisation.

    Uses scipy SLSQP to minimise KL(q || p) subject to linear constraints.
    """
    p = polytope.observed_prices.copy()
    n = len(p)

    if n == 0:
        return ProjectionResult(
            observed=p, projected=p, kl_divergence=0.0,
            profit_per_set=0.0, price_adjustments=np.zeros(0), converged=True,
        )

    # Clamp observed prices to valid range
    p_safe = np.clip(p, _EPS, 1 - _EPS)

    # Objective: minimise KL(q || p)
    def objective(q: np.ndarray) -> float:
        return kl_divergence(q, p_safe)

    def gradient(q: np.ndarray) -> np.ndarray:
        q_safe = np.clip(q, _EPS, 1 - _EPS)
        return np.log(q_safe / p_safe) + 1.0

    # Build scipy constraints
    scipy_constraints = []

    # Equality constraints (partition sums = 1)
    A_eq, b_eq = polytope.get_equality_constraints()
    for i in range(len(b_eq)):
        scipy_constraints.append({
            "type": "eq",
            "fun": lambda q, row=A_eq[i], val=b_eq[i]: row @ q - val,
            "jac": lambda q, row=A_eq[i]: row,
        })

    # Inequality constraints (A_ub @ x <= b_ub → b_ub - A_ub @ x >= 0)
    A_ub, b_ub = polytope.get_inequality_constraints()
    for i in range(len(b_ub)):
        scipy_constraints.append({
            "type": "ineq",
            "fun": lambda q, row=A_ub[i], val=b_ub[i]: val - row @ q,
            "jac": lambda q, row=A_ub[i]: -row,
        })

    # Variable bounds: (eps, 1-eps) for numerical stability
    bounds = [(_EPS, 1 - _EPS)] * n

    # Initial guess: project p onto bounds
    q0 = np.clip(p_safe, _EPS, 1 - _EPS)

    result = minimize(
        objective,
        q0,
        jac=gradient,
        method="SLSQP",
        bounds=bounds,
        constraints=scipy_constraints,
        options={"maxiter": max_iter, "ftol": tol, "disp": False},
    )

    q_star = np.clip(result.x, _EPS, 1 - _EPS)
    kl = kl_divergence(q_star, p_safe)
    adjustments = q_star - p

    # Profit = sum of absolute price moves weighted by direction
    # For overpriced partition: profit = sum(p_i) - 1.0 ≈ sum of adjustments
    profit = float(np.sum(np.abs(adjustments))) / 2  # approximate

    return ProjectionResult(
        observed=p,
        projected=q_star,
        kl_divergence=kl,
        profit_per_set=profit,
        price_adjustments=adjustments,
        converged=result.success,
        n_iterations=result.nit,
    )
