"""Tests for single-market arbitrage detection."""
from polycrossarb.arb.detector import (
    detect_single_market_arbs,
    detect_single_market_orderbook_arbs,
)
from polycrossarb.data.models import Market, OrderBook, OrderBookLevel, Outcome


def _make_market(prices: list[float], **kwargs) -> Market:
    names = [f"Outcome_{i}" for i in range(len(prices))]
    outcomes = [
        Outcome(token_id=f"tok_{i}", name=names[i], price=p)
        for i, p in enumerate(prices)
    ]
    defaults = dict(
        condition_id="cond_test",
        question="Test market?",
        slug="test-market",
        outcomes=outcomes,
        active=True,
        closed=False,
    )
    defaults.update(kwargs)
    return Market(**defaults)


class TestSingleMarketDetection:
    def test_no_arb_when_prices_sum_to_one(self):
        market = _make_market([0.60, 0.40])
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 0

    def test_overpriced_arb(self):
        # YES=0.55, NO=0.50 → sum=1.05, margin=0.05
        market = _make_market([0.55, 0.50])
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 1
        assert opps[0].arb_type == "single_over"
        assert abs(opps[0].margin - 0.05) < 1e-9

    def test_underpriced_arb(self):
        # YES=0.45, NO=0.50 → sum=0.95, margin=0.05
        market = _make_market([0.45, 0.50])
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 1
        assert opps[0].arb_type == "single_under"
        assert abs(opps[0].margin - 0.05) < 1e-9

    def test_margin_below_threshold_ignored(self):
        market = _make_market([0.505, 0.50])  # sum=1.005
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 0

    def test_multi_outcome_overpriced(self):
        # 4 outcomes summing to 1.10
        market = _make_market([0.30, 0.30, 0.25, 0.25])
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 1
        assert opps[0].arb_type == "single_over"
        assert abs(opps[0].margin - 0.10) < 1e-9

    def test_closed_market_ignored(self):
        market = _make_market([0.55, 0.50], closed=True)
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 0

    def test_inactive_market_ignored(self):
        market = _make_market([0.55, 0.50], active=False)
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 0

    def test_single_outcome_ignored(self):
        market = _make_market([0.50])
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 0

    def test_zero_prices_ignored(self):
        market = _make_market([0.0, 0.0])
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 0

    def test_profit_per_dollar_overpriced(self):
        # sum=1.10 → profit_per_dollar = 0.10/1.10 ≈ 0.0909
        market = _make_market([0.60, 0.50])
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 1
        expected = 0.10 / 1.10
        assert abs(opps[0].profit_per_dollar - expected) < 1e-6

    def test_profit_per_dollar_underpriced(self):
        # sum=0.90 → profit_per_dollar = 0.10/0.90 ≈ 0.1111
        market = _make_market([0.40, 0.50])
        opps = detect_single_market_arbs([market], min_margin=0.01)
        assert len(opps) == 1
        expected = 0.10 / 0.90
        assert abs(opps[0].profit_per_dollar - expected) < 1e-6

    def test_sorted_by_margin_descending(self):
        m1 = _make_market([0.55, 0.50], condition_id="c1", question="Q1")
        m2 = _make_market([0.60, 0.60], condition_id="c2", question="Q2")
        opps = detect_single_market_arbs([m1, m2], min_margin=0.01)
        assert len(opps) == 2
        assert opps[0].margin >= opps[1].margin


class TestOrderBookDetection:
    def _book(self, bid: float, ask: float, size: float = 100.0) -> OrderBook:
        return OrderBook(
            bids=[OrderBookLevel(price=bid, size=size)],
            asks=[OrderBookLevel(price=ask, size=size)],
        )

    def test_overpriced_book_arb(self):
        outcomes = [
            Outcome(token_id="t0", name="Yes", price=0.55,
                    order_book=self._book(bid=0.54, ask=0.56)),
            Outcome(token_id="t1", name="No", price=0.50,
                    order_book=self._book(bid=0.49, ask=0.51)),
        ]
        market = Market(
            condition_id="c1", question="Q?", slug="q",
            outcomes=outcomes, active=True, closed=False,
        )
        # bid sum = 0.54+0.49 = 1.03 > 1.0
        opps = detect_single_market_orderbook_arbs([market], min_margin=0.01)
        assert len(opps) == 1
        assert opps[0].arb_type == "single_over_book"
        assert abs(opps[0].margin - 0.03) < 1e-9

    def test_no_book_arb_when_spread_kills_margin(self):
        outcomes = [
            Outcome(token_id="t0", name="Yes", price=0.55,
                    order_book=self._book(bid=0.50, ask=0.55)),
            Outcome(token_id="t1", name="No", price=0.50,
                    order_book=self._book(bid=0.45, ask=0.50)),
        ]
        market = Market(
            condition_id="c1", question="Q?", slug="q",
            outcomes=outcomes, active=True, closed=False,
        )
        # bid sum = 0.50+0.45 = 0.95 < 1.0, ask sum = 0.55+0.50 = 1.05 > 1.0
        # Neither side has arb
        opps = detect_single_market_orderbook_arbs([market], min_margin=0.01)
        assert len(opps) == 0
