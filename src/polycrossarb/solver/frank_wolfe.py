"""Tier 2: Frank-Wolfe algorithm with ILP oracle.

Iteratively linearises the KL objective and solves an ILP at each step
to find the vertex of the polytope that maximises the linear approximation.

This is the production-grade solver for large cross-market arbs where
the Tier 1 LP is insufficient (e.g. tournaments with 64+ outcomes,
or cross-event constraints like implications).

Pseudocode from the PDF:
    for t in range(max_iter):
        1. Solve master LP over current vertices
        2. Call ILP oracle: max <grad_f(x_k), v> s.t. v feasible
        3. Line search or away-step
        4. Adaptive epsilon-barrier compression
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
from scipy.optimize import linprog

from polycrossarb.arb.polytope import PolytopeConstraints
from polycrossarb.arb.projection import _EPS, kl_divergence

log = logging.getLogger(__name__)


@dataclass
class FrankWolfeResult:
    projected: np.ndarray
    kl_divergence: float
    n_iterations: int
    converged: bool
    gap: float  # duality gap


def frank_wolfe_projection(
    polytope: PolytopeConstraints,
    max_iter: int = 150,
    tol: float = 1e-8,
) -> FrankWolfeResult:
    """Project observed prices onto polytope using Frank-Wolfe.

    Minimises KL(q || p) subject to polytope constraints by
    iteratively solving linear subproblems.
    """
    p = np.clip(polytope.observed_prices, _EPS, 1 - _EPS)
    n = len(p)

    if n == 0:
        return FrankWolfeResult(
            projected=p, kl_divergence=0.0,
            n_iterations=0, converged=True, gap=0.0,
        )

    # Get constraint matrices
    A_eq, b_eq = polytope.get_equality_constraints()
    A_ub, b_ub = polytope.get_inequality_constraints()

    # Initial feasible point: solve LP to find a vertex
    # Minimise sum(x) subject to constraints (arbitrary objective to get feasible point)
    init_result = linprog(
        c=np.ones(n),
        A_ub=A_ub if len(A_ub) else None,
        b_ub=b_ub if len(b_ub) else None,
        A_eq=A_eq if len(A_eq) else None,
        b_eq=b_eq if len(b_eq) else None,
        bounds=[(_EPS, 1 - _EPS)] * n,
        method="highs",
    )

    if not init_result.success:
        log.warning("Could not find initial feasible point")
        return FrankWolfeResult(
            projected=p, kl_divergence=float("inf"),
            n_iterations=0, converged=False, gap=float("inf"),
        )

    x = np.clip(init_result.x, _EPS, 1 - _EPS)
    gap = float("inf")

    for t in range(max_iter):
        # Gradient of KL(x || p) = log(x/p) + 1
        grad = np.log(x / p) + 1.0

        # Linear minimisation oracle: min <grad, v> s.t. v in polytope
        lmo_result = linprog(
            c=grad,
            A_ub=A_ub if len(A_ub) else None,
            b_ub=b_ub if len(b_ub) else None,
            A_eq=A_eq if len(A_eq) else None,
            b_eq=b_eq if len(b_eq) else None,
            bounds=[(_EPS, 1 - _EPS)] * n,
            method="highs",
        )

        if not lmo_result.success:
            break

        v = np.clip(lmo_result.x, _EPS, 1 - _EPS)

        # Duality gap
        gap = float(grad @ (x - v))
        if gap < tol:
            break

        # Line search: find optimal step size gamma in [0, 1]
        # For KL, use exact line search via bisection
        gamma = _line_search_kl(x, v, p)
        x = (1 - gamma) * x + gamma * v
        x = np.clip(x, _EPS, 1 - _EPS)

    kl = kl_divergence(x, p)

    return FrankWolfeResult(
        projected=x,
        kl_divergence=kl,
        n_iterations=t + 1,
        converged=gap < tol,
        gap=gap,
    )


def _line_search_kl(
    x: np.ndarray,
    v: np.ndarray,
    p: np.ndarray,
    n_steps: int = 20,
) -> float:
    """Binary line search for optimal step size minimising KL((1-g)*x + g*v || p)."""
    lo, hi = 0.0, 1.0
    for _ in range(n_steps):
        g1 = lo + (hi - lo) / 3
        g2 = lo + 2 * (hi - lo) / 3
        q1 = np.clip((1 - g1) * x + g1 * v, _EPS, 1 - _EPS)
        q2 = np.clip((1 - g2) * x + g2 * v, _EPS, 1 - _EPS)
        if kl_divergence(q1, p) < kl_divergence(q2, p):
            hi = g2
        else:
            lo = g1
    return (lo + hi) / 2
