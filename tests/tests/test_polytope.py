"""Tests for marginal polytope constraint generation."""
import numpy as np

from polycrossarb.arb.polytope import build_polytope
from polycrossarb.data.models import Market, Outcome
from polycrossarb.graph.screener import EventGroup


def _make_market(cid: str, yes_price: float, event_id: str = "e1") -> Market:
    return Market(
        condition_id=cid,
        question=f"Q {cid}?",
        slug=cid,
        outcomes=[
            Outcome(token_id=f"{cid}_yes", name="Yes", price=yes_price),
            Outcome(token_id=f"{cid}_no", name="No", price=1 - yes_price),
        ],
        event_id=event_id,
        event_title="Test Event",
        neg_risk=True,
    )


def _make_partition(n_outcomes: int, prices: list[float], event_id: str = "e1") -> EventGroup:
    markets = [_make_market(f"m{i}_{event_id}", prices[i], event_id) for i in range(n_outcomes)]
    return EventGroup(
        event_id=event_id,
        event_title="Test Event",
        markets=markets,
        is_neg_risk=True,
    )


class TestPolytopeConstruction:
    def test_single_partition_creates_sum_constraint(self):
        partition = _make_partition(3, [0.5, 0.3, 0.2])
        polytope = build_polytope([partition])

        assert polytope.n_vars == 3

        # Should have 1 equality (sum=1) + 6 bounds (lb/ub per var)
        eq_count = sum(1 for c in polytope.constraints if c.is_equality)
        assert eq_count == 1

    def test_observed_prices_populated(self):
        partition = _make_partition(3, [0.5, 0.3, 0.2])
        polytope = build_polytope([partition])

        np.testing.assert_allclose(polytope.observed_prices, [0.5, 0.3, 0.2])

    def test_feasible_prices_no_violations(self):
        # prices sum to 1.0 = feasible
        partition = _make_partition(3, [0.5, 0.3, 0.2])
        polytope = build_polytope([partition])

        violations = polytope.check_feasibility()
        assert len(violations) == 0

    def test_overpriced_partition_has_violation(self):
        # prices sum to 1.3 > 1.0 = violates partition constraint
        partition = _make_partition(3, [0.5, 0.4, 0.4])
        polytope = build_polytope([partition])

        violations = polytope.check_feasibility()
        assert len(violations) > 0

    def test_underpriced_partition_has_violation(self):
        partition = _make_partition(3, [0.3, 0.2, 0.1])
        polytope = build_polytope([partition])

        violations = polytope.check_feasibility()
        assert len(violations) > 0

    def test_multiple_partitions(self):
        p1 = _make_partition(3, [0.5, 0.3, 0.2], event_id="e1")
        p2 = _make_partition(2, [0.6, 0.4], event_id="e2")
        polytope = build_polytope([p1, p2])

        assert polytope.n_vars == 5
        eq_count = sum(1 for c in polytope.constraints if c.is_equality)
        assert eq_count == 2  # one per partition

    def test_constraint_matrices(self):
        partition = _make_partition(2, [0.6, 0.4])
        polytope = build_polytope([partition])

        A_eq, b_eq = polytope.get_equality_constraints()
        assert A_eq.shape == (1, 2)
        assert b_eq[0] == 1.0
        np.testing.assert_allclose(A_eq[0], [1.0, 1.0])
