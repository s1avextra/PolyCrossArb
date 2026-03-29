"""Marginal polytope constraint generation.

Converts logical market dependencies into linear constraints
that define the feasible price space (the marginal polytope).

Constraint types:
  - PARTITION: sum of YES prices = 1.0 (mutually exclusive & exhaustive)
  - IMPLICATION: p_A <= p_B (A implies B)
  - EXCLUSION: p_A + p_B <= 1.0 (at most one can be true)
  - BOUNDS: 0 <= p_i <= 1 for all outcomes
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np

from polycrossarb.data.models import Market
from polycrossarb.graph.screener import Dependency, EventGroup, RelationType

log = logging.getLogger(__name__)


@dataclass
class LinearConstraint:
    """A single linear constraint: lb <= A @ x <= ub.

    Represented as coefficients over the price vector x.
    """
    coefficients: np.ndarray  # length = number of price variables
    lb: float = -np.inf       # lower bound
    ub: float = np.inf        # upper bound
    name: str = ""

    @property
    def is_equality(self) -> bool:
        return abs(self.ub - self.lb) < 1e-12


@dataclass
class PolytopeConstraints:
    """The full set of linear constraints defining the feasible price space."""
    variables: list[str] = field(default_factory=list)  # variable names (market_id:outcome_idx)
    var_to_idx: dict[str, int] = field(default_factory=dict)
    observed_prices: np.ndarray = field(default_factory=lambda: np.array([]))
    constraints: list[LinearConstraint] = field(default_factory=list)

    @property
    def n_vars(self) -> int:
        return len(self.variables)

    @property
    def n_constraints(self) -> int:
        return len(self.constraints)

    def get_A_bounds(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Return constraint matrix A, lower bounds lb, upper bounds ub.

        Such that lb <= A @ x <= ub for all constraints.
        """
        if not self.constraints:
            return np.empty((0, self.n_vars)), np.array([]), np.array([])

        A = np.zeros((self.n_constraints, self.n_vars))
        lb = np.full(self.n_constraints, -np.inf)
        ub = np.full(self.n_constraints, np.inf)

        for i, c in enumerate(self.constraints):
            A[i] = c.coefficients
            lb[i] = c.lb
            ub[i] = c.ub

        return A, lb, ub

    def get_equality_constraints(self) -> tuple[np.ndarray, np.ndarray]:
        """Return A_eq, b_eq for equality constraints (partition sums = 1)."""
        eq = [c for c in self.constraints if c.is_equality]
        if not eq:
            return np.empty((0, self.n_vars)), np.array([])
        A = np.array([c.coefficients for c in eq])
        b = np.array([c.ub for c in eq])
        return A, b

    def get_inequality_constraints(self) -> tuple[np.ndarray, np.ndarray]:
        """Return A_ub, b_ub for inequality constraints (A_ub @ x <= b_ub)."""
        ineq = [c for c in self.constraints if not c.is_equality]
        if not ineq:
            return np.empty((0, self.n_vars)), np.array([])
        A = np.array([c.coefficients for c in ineq])
        b = np.array([c.ub for c in ineq])
        return A, b

    def check_feasibility(self, prices: np.ndarray | None = None, tol: float = 1e-6) -> list[str]:
        """Check which constraints are violated by given prices."""
        if prices is None:
            prices = self.observed_prices
        violations = []
        for c in self.constraints:
            val = c.coefficients @ prices
            if val < c.lb - tol:
                violations.append(f"{c.name}: {val:.6f} < lb {c.lb:.6f}")
            elif val > c.ub + tol:
                violations.append(f"{c.name}: {val:.6f} > ub {c.ub:.6f}")
        return violations


def build_polytope(
    partitions: list[EventGroup],
    implications: list[Dependency] | None = None,
) -> PolytopeConstraints:
    """Build the marginal polytope from event partitions and dependencies.

    Args:
        partitions: Event groups where outcomes are mutually exclusive.
        implications: Cross-event logical implications.

    Returns:
        PolytopeConstraints with all linear constraints and observed prices.
    """
    # Collect all unique market outcomes as variables
    var_map: dict[str, int] = {}
    variables: list[str] = []
    markets_seen: dict[str, Market] = {}

    def _add_var(market: Market, outcome_idx: int) -> int:
        key = f"{market.condition_id}:{outcome_idx}"
        if key not in var_map:
            idx = len(variables)
            var_map[key] = idx
            variables.append(key)
            markets_seen[market.condition_id] = market
        return var_map[key]

    # Register all variables from partitions
    for group in partitions:
        for market in group.markets:
            _add_var(market, 0)  # YES outcome

    # Register variables from implications
    if implications:
        for dep in implications:
            _add_var(dep.market_a, dep.outcome_a_idx)
            _add_var(dep.market_b, dep.outcome_b_idx)

    n = len(variables)
    constraints: list[LinearConstraint] = []

    # ── Partition constraints: sum of YES prices = 1.0 ────────────
    for group in partitions:
        coeff = np.zeros(n)
        for market in group.markets:
            idx = var_map[f"{market.condition_id}:0"]
            coeff[idx] = 1.0

        constraints.append(LinearConstraint(
            coefficients=coeff,
            lb=1.0,
            ub=1.0,
            name=f"partition:{group.event_id}({group.event_title[:30]})",
        ))

    # ── Implication constraints: p_A <= p_B ───────────────────────
    if implications:
        for dep in implications:
            if dep.relation == RelationType.IMPLIES:
                coeff = np.zeros(n)
                idx_a = var_map[f"{dep.market_a.condition_id}:{dep.outcome_a_idx}"]
                idx_b = var_map[f"{dep.market_b.condition_id}:{dep.outcome_b_idx}"]
                coeff[idx_a] = 1.0
                coeff[idx_b] = -1.0
                constraints.append(LinearConstraint(
                    coefficients=coeff,
                    lb=-np.inf,
                    ub=0.0,
                    name=f"implies:{dep.market_a.condition_id[:8]}->{dep.market_b.condition_id[:8]}",
                ))

    # ── Bounds: 0 <= p_i <= 1 ─────────────────────────────────────
    for i in range(n):
        # Lower bound
        coeff_lb = np.zeros(n)
        coeff_lb[i] = -1.0
        constraints.append(LinearConstraint(
            coefficients=coeff_lb, lb=-np.inf, ub=0.0,
            name=f"lb:{variables[i]}",
        ))
        # Upper bound
        coeff_ub = np.zeros(n)
        coeff_ub[i] = 1.0
        constraints.append(LinearConstraint(
            coefficients=coeff_ub, lb=-np.inf, ub=1.0,
            name=f"ub:{variables[i]}",
        ))

    # ── Observed prices ───────────────────────────────────────────
    observed = np.zeros(n)
    for key, idx in var_map.items():
        cid, oidx = key.rsplit(":", 1)
        market = markets_seen.get(cid)
        if market and int(oidx) < len(market.outcomes):
            observed[idx] = market.outcomes[int(oidx)].price

    polytope = PolytopeConstraints(
        variables=variables,
        var_to_idx=var_map,
        observed_prices=observed,
        constraints=constraints,
    )

    violations = polytope.check_feasibility()
    log.info(
        "Polytope: %d vars, %d constraints, %d violations in observed prices",
        n, len(constraints), len(violations),
    )

    return polytope
