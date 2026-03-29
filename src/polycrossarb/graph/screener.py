"""Rule-based market dependency detection.

Identifies logical relationships between markets without an LLM:
  - PARTITION: mutually exclusive + exhaustive (neg_risk event groups)
  - IMPLICATION: "A wins league" implies "A makes playoffs"
  - EXCLUSION: "A wins" and "B wins" same single-winner event
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum

from polycrossarb.data.models import Market

log = logging.getLogger(__name__)


class RelationType(str, Enum):
    PARTITION = "partition"    # outcomes are mutually exclusive & exhaustive (sum = 1)
    IMPLIES = "implies"        # p(A) <= p(B)
    EXCLUDES = "excludes"      # p(A) + p(B) <= 1
    INDEPENDENT = "independent"


@dataclass
class Dependency:
    """A logical dependency between two market outcomes."""
    relation: RelationType
    market_a: Market
    outcome_a_idx: int  # index into market_a.outcomes (0 = YES)
    market_b: Market
    outcome_b_idx: int
    confidence: float = 1.0  # 1.0 = certain, <1.0 = heuristic
    reason: str = ""


@dataclass
class EventGroup:
    """A group of markets forming a partition (mutually exclusive outcomes)."""
    event_id: str
    event_title: str
    markets: list[Market] = field(default_factory=list)
    is_neg_risk: bool = False

    @property
    def yes_prices(self) -> list[float]:
        return [m.outcomes[0].price for m in self.markets if m.outcomes]

    @property
    def price_sum(self) -> float:
        return sum(self.yes_prices)


def find_event_partitions(markets: list[Market]) -> list[EventGroup]:
    """Group markets into partition sets based on shared event_id.

    Polymarket's neg_risk flag confirms mutual exclusivity.
    Non-neg_risk groups are also returned but flagged as unconfirmed.
    """
    by_event: dict[str, list[Market]] = defaultdict(list)
    for m in markets:
        if m.event_id and m.active and not m.closed:
            by_event[m.event_id].append(m)

    groups: list[EventGroup] = []
    for event_id, event_markets in by_event.items():
        if len(event_markets) < 2:
            continue

        is_neg = any(m.neg_risk for m in event_markets)
        title = event_markets[0].event_title or event_markets[0].event_slug or event_id

        groups.append(EventGroup(
            event_id=event_id,
            event_title=title,
            markets=event_markets,
            is_neg_risk=is_neg,
        ))

    return groups


# ── Heuristic cross-event dependency detection ────────────────────────

# Patterns that suggest implication relationships
_IMPLICATION_PATTERNS: list[tuple[re.Pattern, re.Pattern]] = [
    # "X wins championship" implies "X makes playoffs"
    (re.compile(r"will (.+?) win (?:the )?(.+?)(?:\?|$)", re.I),
     re.compile(r"will (.+?) (?:make|qualify|advance|reach) (?:the )?(.+?)(?:\?|$)", re.I)),
]


def _extract_entity(question: str) -> str | None:
    """Extract the main entity (team/person) from a market question."""
    m = re.match(r"will (.+?) (?:win|make|be|become|fight|attend|visit)", question, re.I)
    if m:
        return m.group(1).strip().lower()
    return None


def find_cross_event_implications(
    groups: list[EventGroup],
) -> list[Dependency]:
    """Find implication relationships across different event groups.

    Example: If "Lakers win NBA Championship" is in one group and
    "Lakers make NBA Playoffs" is in another, then championship implies playoffs.
    """
    dependencies: list[Dependency] = []

    # Index markets by entity for faster matching
    entity_markets: dict[str, list[tuple[Market, EventGroup]]] = defaultdict(list)
    for group in groups:
        for market in group.markets:
            entity = _extract_entity(market.question)
            if entity:
                entity_markets[entity].append((market, group))

    # Look for implication pairs: same entity, one is "stronger" than the other
    for entity, market_list in entity_markets.items():
        if len(market_list) < 2:
            continue

        for i, (m_a, g_a) in enumerate(market_list):
            for m_b, g_b in market_list[i + 1:]:
                if g_a.event_id == g_b.event_id:
                    continue  # same event, already handled as partition

                dep = _check_implication(m_a, m_b)
                if dep:
                    dependencies.append(dep)

    log.info("Found %d cross-event implications", len(dependencies))
    return dependencies


def _check_implication(m_a: Market, m_b: Market) -> Dependency | None:
    """Check if m_a YES implies m_b YES (or vice versa).

    Uses keyword heuristics:
      - "win" implies "make/qualify/advance"
      - "win championship" implies "win division"
    """
    q_a = m_a.question.lower()
    q_b = m_b.question.lower()

    win_keywords = ["win", "champion"]
    qualify_keywords = ["make", "qualify", "advance", "reach", "playoff"]

    a_is_win = any(k in q_a for k in win_keywords)
    a_is_qualify = any(k in q_a for k in qualify_keywords)
    b_is_win = any(k in q_b for k in win_keywords)
    b_is_qualify = any(k in q_b for k in qualify_keywords)

    if a_is_win and b_is_qualify and not a_is_qualify:
        # "A wins" implies "A qualifies" → p(A_yes) <= p(B_yes)
        return Dependency(
            relation=RelationType.IMPLIES,
            market_a=m_a, outcome_a_idx=0,
            market_b=m_b, outcome_b_idx=0,
            confidence=0.8,
            reason=f"'{m_a.question[:50]}' winning implies '{m_b.question[:50]}' qualifying",
        )
    elif b_is_win and a_is_qualify and not b_is_qualify:
        return Dependency(
            relation=RelationType.IMPLIES,
            market_a=m_b, outcome_a_idx=0,
            market_b=m_a, outcome_b_idx=0,
            confidence=0.8,
            reason=f"'{m_b.question[:50]}' winning implies '{m_a.question[:50]}' qualifying",
        )
    return None
