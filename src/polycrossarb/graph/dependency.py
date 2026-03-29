"""NetworkX-based dependency graph for market relationships.

Nodes represent market outcomes (market_id, outcome_idx).
Edges represent logical constraints between outcomes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import networkx as nx

from polycrossarb.data.models import Market
from polycrossarb.graph.screener import (
    Dependency,
    EventGroup,
    RelationType,
    find_cross_event_implications,
    find_event_partitions,
)

log = logging.getLogger(__name__)


@dataclass
class OutcomeNode:
    market: Market
    outcome_idx: int  # 0 = YES, 1 = NO for binary markets

    @property
    def node_id(self) -> str:
        return f"{self.market.condition_id}:{self.outcome_idx}"

    @property
    def price(self) -> float:
        if self.outcome_idx < len(self.market.outcomes):
            return self.market.outcomes[self.outcome_idx].price
        return 0.0

    @property
    def label(self) -> str:
        name = self.market.outcomes[self.outcome_idx].name if self.outcome_idx < len(self.market.outcomes) else "?"
        return f"{self.market.question[:40]}:{name}"


@dataclass
class ConstraintSubgraph:
    """A connected subgraph of constraints that can be solved together."""
    nodes: list[OutcomeNode] = field(default_factory=list)
    partitions: list[EventGroup] = field(default_factory=list)
    implications: list[Dependency] = field(default_factory=list)

    @property
    def prices(self) -> list[float]:
        return [n.price for n in self.nodes]

    @property
    def num_constraints(self) -> int:
        return len(self.partitions) + len(self.implications)


class DependencyGraph:
    """Builds and queries a graph of market dependencies."""

    def __init__(self):
        self._graph = nx.DiGraph()
        self._partitions: list[EventGroup] = []
        self._implications: list[Dependency] = []
        self._node_lookup: dict[str, OutcomeNode] = {}

    def build(self, markets: list[Market]) -> None:
        """Build the dependency graph from a list of markets."""
        self._graph.clear()
        self._node_lookup.clear()

        # Step 1: Find partition groups (event-grouped mutually exclusive markets)
        self._partitions = find_event_partitions(markets)
        neg_risk_partitions = [p for p in self._partitions if p.is_neg_risk]

        # Step 2: Add partition nodes and edges
        for group in neg_risk_partitions:
            partition_nodes = []
            for market in group.markets:
                node = OutcomeNode(market=market, outcome_idx=0)  # YES outcome
                self._add_node(node)
                partition_nodes.append(node)

            # Add exclusion edges between all pairs in the partition
            for i, n_a in enumerate(partition_nodes):
                for n_b in partition_nodes[i + 1:]:
                    self._graph.add_edge(
                        n_a.node_id, n_b.node_id,
                        relation=RelationType.EXCLUDES,
                        partition_id=group.event_id,
                    )
                    self._graph.add_edge(
                        n_b.node_id, n_a.node_id,
                        relation=RelationType.EXCLUDES,
                        partition_id=group.event_id,
                    )

        # Step 3: Find cross-event implications
        self._implications = find_cross_event_implications(neg_risk_partitions)
        for dep in self._implications:
            node_a = OutcomeNode(market=dep.market_a, outcome_idx=dep.outcome_a_idx)
            node_b = OutcomeNode(market=dep.market_b, outcome_idx=dep.outcome_b_idx)
            self._add_node(node_a)
            self._add_node(node_b)
            self._graph.add_edge(
                node_a.node_id, node_b.node_id,
                relation=RelationType.IMPLIES,
                confidence=dep.confidence,
            )

        log.info(
            "Dependency graph: %d nodes, %d edges, %d partitions, %d implications",
            self._graph.number_of_nodes(),
            self._graph.number_of_edges(),
            len(neg_risk_partitions),
            len(self._implications),
        )

    def _add_node(self, node: OutcomeNode) -> None:
        if node.node_id not in self._node_lookup:
            self._node_lookup[node.node_id] = node
            self._graph.add_node(
                node.node_id,
                market_id=node.market.condition_id,
                outcome_idx=node.outcome_idx,
                price=node.price,
            )

    @property
    def partitions(self) -> list[EventGroup]:
        return self._partitions

    @property
    def neg_risk_partitions(self) -> list[EventGroup]:
        return [p for p in self._partitions if p.is_neg_risk]

    @property
    def implications(self) -> list[Dependency]:
        return self._implications

    def get_node(self, node_id: str) -> OutcomeNode | None:
        return self._node_lookup.get(node_id)

    def get_related_markets(self, market: Market) -> list[Market]:
        """Get all markets connected to the given market."""
        node_id = f"{market.condition_id}:0"
        if node_id not in self._graph:
            return []

        related_ids = set()
        for neighbor in nx.descendants(self._graph, node_id) | nx.ancestors(self._graph, node_id):
            node = self._node_lookup.get(neighbor)
            if node and node.market.condition_id != market.condition_id:
                related_ids.add(node.market.condition_id)

        return [
            self._node_lookup[f"{cid}:0"].market
            for cid in related_ids
            if f"{cid}:0" in self._node_lookup
        ]

    def get_constraint_subgraphs(self) -> list[ConstraintSubgraph]:
        """Extract connected components as constraint subgraphs for solving."""
        undirected = self._graph.to_undirected()
        subgraphs: list[ConstraintSubgraph] = []

        for component in nx.connected_components(undirected):
            nodes = [self._node_lookup[nid] for nid in component if nid in self._node_lookup]
            if len(nodes) < 2:
                continue

            # Find which partitions are in this component
            component_set = set(component)
            partitions = []
            for p in self._partitions:
                p_nodes = {f"{m.condition_id}:0" for m in p.markets}
                if p_nodes & component_set:
                    partitions.append(p)

            implications = [
                d for d in self._implications
                if f"{d.market_a.condition_id}:{d.outcome_a_idx}" in component_set
            ]

            subgraphs.append(ConstraintSubgraph(
                nodes=nodes,
                partitions=partitions,
                implications=implications,
            ))

        subgraphs.sort(key=lambda s: s.num_constraints, reverse=True)
        return subgraphs
