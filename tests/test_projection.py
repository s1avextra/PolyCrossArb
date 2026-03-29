"""Tests for KL divergence projection."""
import numpy as np

from polycrossarb.arb.polytope import build_polytope
from polycrossarb.arb.projection import kl_divergence, project_onto_polytope
from polycrossarb.data.models import Market, Outcome
from polycrossarb.graph.screener import EventGroup


def _make_partition(prices: list[float], event_id: str = "e1") -> EventGroup:
    markets = []
    for i, p in enumerate(prices):
        markets.append(Market(
            condition_id=f"m{i}_{event_id}",
            question=f"Outcome {i}?",
            slug=f"m{i}",
            outcomes=[
                Outcome(token_id=f"t{i}_yes", name="Yes", price=p),
                Outcome(token_id=f"t{i}_no", name="No", price=1 - p),
            ],
            event_id=event_id,
            event_title="Test",
            neg_risk=True,
        ))
    return EventGroup(
        event_id=event_id,
        event_title="Test",
        markets=markets,
        is_neg_risk=True,
    )


class TestKLDivergence:
    def test_kl_zero_when_equal(self):
        p = np.array([0.3, 0.7])
        assert abs(kl_divergence(p, p)) < 1e-10

    def test_kl_positive(self):
        p = np.array([0.3, 0.7])
        q = np.array([0.5, 0.5])
        assert kl_divergence(q, p) > 0

    def test_kl_asymmetric(self):
        p = np.array([0.3, 0.7])
        q = np.array([0.5, 0.5])
        assert abs(kl_divergence(q, p) - kl_divergence(p, q)) > 1e-6


class TestProjection:
    def test_feasible_prices_project_to_self(self):
        # Prices already sum to 1.0
        partition = _make_partition([0.5, 0.3, 0.2])
        polytope = build_polytope([partition])

        result = project_onto_polytope(polytope)

        assert result.converged
        assert result.kl_divergence < 1e-6
        np.testing.assert_allclose(result.projected, [0.5, 0.3, 0.2], atol=1e-4)

    def test_overpriced_projects_down(self):
        # Sum = 1.3, should project to sum = 1.0
        partition = _make_partition([0.5, 0.4, 0.4])
        polytope = build_polytope([partition])

        result = project_onto_polytope(polytope)

        assert result.converged
        assert abs(sum(result.projected) - 1.0) < 1e-4
        # All projected prices should be lower than observed
        for i in range(3):
            assert result.projected[i] <= 0.5 + 1e-4

    def test_underpriced_projects_up(self):
        # Sum = 0.6, should project to sum = 1.0
        partition = _make_partition([0.3, 0.2, 0.1])
        polytope = build_polytope([partition])

        result = project_onto_polytope(polytope)

        assert result.converged
        assert abs(sum(result.projected) - 1.0) < 1e-4

    def test_projection_distance_proportional_to_deviation(self):
        # Larger deviation should have larger KL divergence
        p1 = _make_partition([0.4, 0.3, 0.4])  # sum=1.1
        p2 = _make_partition([0.5, 0.4, 0.5])  # sum=1.4

        poly1 = build_polytope([p1])
        poly2 = build_polytope([p2])

        r1 = project_onto_polytope(poly1)
        r2 = project_onto_polytope(poly2)

        assert r2.kl_divergence > r1.kl_divergence

    def test_two_outcome_projection(self):
        partition = _make_partition([0.6, 0.5])  # sum=1.1
        polytope = build_polytope([partition])

        result = project_onto_polytope(polytope)

        assert result.converged
        assert abs(sum(result.projected) - 1.0) < 1e-4
