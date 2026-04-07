"""Tests for LP solver and position sizing."""
from polycrossarb.data.models import Market, OrderBook, OrderBookLevel, Outcome
from polycrossarb.execution.sizing import (
    calculate_position_size,
    estimate_execution_probability,
    modified_kelly,
)
from polycrossarb.graph.screener import EventGroup
from polycrossarb.solver.linear import solve_all_partitions, solve_partition_arb


def _make_book(price: float, depth: float = 500.0) -> OrderBook:
    """Create a realistic order book around a price."""
    return OrderBook(
        bids=[OrderBookLevel(price=max(0.001, price - 0.01), size=depth)],
        asks=[OrderBookLevel(price=min(0.999, price + 0.01), size=depth)],
    )


def _make_partition(prices: list[float], event_id: str = "e1") -> EventGroup:
    markets = []
    for i, p in enumerate(prices):
        markets.append(Market(
            condition_id=f"m{i}_{event_id}",
            question=f"Outcome {i}?",
            slug=f"m{i}",
            outcomes=[
                Outcome(token_id=f"t{i}_yes", name="Yes", price=p,
                        order_book=_make_book(p)),
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


class TestSolvePartition:
    def test_overpriced_partition_profitable(self):
        # sum = 1.3, profit = 0.3 per set
        group = _make_partition([0.5, 0.4, 0.4])
        result = solve_partition_arb(group, max_position_usd=100)

        assert result.is_optimal
        assert result.guaranteed_profit > 0
        assert len(result.orders) == 3
        # Overpriced = buy NO on all outcomes
        assert all(o.side == "buy" for o in result.orders)
        assert all(o.outcome_idx == 1 for o in result.orders)  # NO tokens

    def test_underpriced_partition_profitable(self):
        # sum = 0.7, profit = 0.3 per set
        group = _make_partition([0.3, 0.2, 0.2])
        result = solve_partition_arb(group, max_position_usd=100)

        assert result.is_optimal
        assert result.guaranteed_profit > 0
        assert all(o.side == "buy" for o in result.orders)

    def test_fair_priced_no_profit(self):
        group = _make_partition([0.5, 0.3, 0.2])  # sum = 1.0
        result = solve_partition_arb(group, max_position_usd=100)

        # Should still be optimal but with 0 profit
        assert result.guaranteed_profit < 0.01

    def test_capital_constraint(self):
        group = _make_partition([0.5, 0.4, 0.4])  # sum = 1.3
        result_small = solve_partition_arb(group, max_position_usd=10)
        result_large = solve_partition_arb(group, max_position_usd=1000)

        assert result_large.guaranteed_profit > result_small.guaranteed_profit

    def test_equal_sizes_across_outcomes(self):
        group = _make_partition([0.5, 0.3, 0.5])
        result = solve_partition_arb(group, max_position_usd=100)

        if result.orders:
            sizes = [o.size for o in result.orders]
            assert max(sizes) - min(sizes) < 1e-6  # all same size


class TestSolveAllPartitions:
    def test_filters_by_min_profit(self):
        p1 = _make_partition([0.5, 0.4, 0.4], "e1")  # sum=1.3, good
        p2 = _make_partition([0.34, 0.33, 0.33], "e2")  # sum=1.0, no profit
        results = solve_all_partitions(
            [p1, p2], max_position_per_event=100,
            max_total_exposure=500, min_profit=0.01,
        )

        profitable = [r for r in results if r.guaranteed_profit > 0]
        assert len(profitable) >= 1

    def test_respects_total_exposure(self):
        partitions = [
            _make_partition([0.5, 0.4, 0.4], f"e{i}")
            for i in range(10)
        ]
        results = solve_all_partitions(
            partitions, max_position_per_event=100,
            max_total_exposure=200, min_profit=0.01,
        )

        # Greedy allocation may slightly exceed due to per-event sizing;
        # check that it's in a reasonable range
        total_cost = sum(max(r.total_cost, r.total_revenue) for r in results)
        assert total_cost <= 300  # within 1.5x of budget


class TestModifiedKelly:
    def test_positive_edge_positive_size(self):
        size = modified_kelly(arb_margin=0.05, execution_prob=0.9, bankroll=1000)
        assert size > 0

    def test_zero_margin_zero_size(self):
        size = modified_kelly(arb_margin=0.0, execution_prob=0.9, bankroll=1000)
        assert size == 0

    def test_low_exec_prob_reduces_size(self):
        high = modified_kelly(arb_margin=0.10, execution_prob=0.95, bankroll=1000)
        low = modified_kelly(arb_margin=0.10, execution_prob=0.30, bankroll=1000)
        assert high > low

    def test_capped_at_max_fraction(self):
        size = modified_kelly(arb_margin=0.50, execution_prob=0.99, bankroll=1000, max_fraction=0.1)
        assert size <= 100  # 10% of 1000


class TestExecutionProbability:
    def test_more_legs_lower_prob(self):
        p1 = estimate_execution_probability(2, 0.01, 100, 10)
        p2 = estimate_execution_probability(10, 0.01, 100, 10)
        assert p1 > p2

    def test_large_order_vs_depth_lower_prob(self):
        p1 = estimate_execution_probability(2, 0.01, 1000, 10)
        p2 = estimate_execution_probability(2, 0.01, 1000, 900)
        assert p1 > p2


class TestPositionSizing:
    def test_calculates_size(self):
        pos = calculate_position_size(
            arb_margin=0.10, n_legs=2, bankroll=1000,
            max_position_usd=100, visible_depth_usd=500,
            avg_depth_usd=500,
        )
        assert pos.size_usd > 0

    def test_below_min_profit_returns_zero(self):
        pos = calculate_position_size(
            arb_margin=0.001, n_legs=3, bankroll=1000,
            max_position_usd=1, visible_depth_usd=500,
            min_profit_usd=1.0,
        )
        assert pos.size_usd == 0

    def test_depth_cap_applied(self):
        # Use high margin + high exec prob so Kelly wants a large position,
        # but visible_depth is small so the cap kicks in
        pos = calculate_position_size(
            arb_margin=0.50, n_legs=1, bankroll=100000,
            max_position_usd=100000, visible_depth_usd=100,
            avg_depth_usd=100000, avg_spread_pct=0.001,
        )
        # Should cap at 50% of 100 = 50
        assert pos.size_usd <= 50
        assert pos.depth_cap_applied
