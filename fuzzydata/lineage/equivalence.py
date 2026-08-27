# -*- coding: utf-8 -*-

"""
fuzzydata.lineage.equivalence
~~~~~~~~~~~~

Equivalence classes over a workflow's artifacts.

Lineage is identifiable only up to mutual derivability: if A can be recovered from B and B
from A, no content-based method can distinguish "A came from B" from "B came from A". The
quotient-aware metrics are therefore computed over these classes rather than over raw
artifacts, and they cannot be computed at all without them.

Cheaper to compute here than anywhere downstream, because at generation time the operator is
known symbolically -- no need to infer invertibility from the data.
"""

import json
import logging
from typing import Dict

import networkx as nx

logger = logging.getLogger(__name__)


def build_equivalence_graph(workflow) -> nx.Graph:
    """Undirected graph whose edges are the invertible derivations in `workflow`.

    Direction is dropped deliberately: an invertible edge means the two endpoints are
    mutually derivable, so for equivalence purposes it is symmetric.
    """
    graph = nx.Graph()
    graph.add_nodes_from(workflow.artifact_dict)

    for operation in workflow.operation_list:
        annotation = operation.get('annotation') or {}
        if not annotation.get('invertible_on_input'):
            continue
        destination = operation.get('new_label')
        sources = operation.get('sources') or []
        # A multi-source operation (merge) is never invertible, so in practice this is a
        # single source; guard anyway rather than assume.
        if destination is None or len(sources) != 1:
            continue
        graph.add_edge(sources[0], destination)

    return graph


def compute_equivalence_classes(workflow) -> Dict[str, int]:
    """Map every artifact label to a class id.

    Two artifacts share a class iff connected by a path of invertible edges, in either
    direction. Class ids are assigned in sorted order of each class's smallest member, so
    they are stable across runs -- otherwise the output would not be comparable between
    corpus regenerations.

    :param workflow: a Workflow that has been generated (operation_list populated).
    :return: {artifact_label: class_id}
    """
    graph = build_equivalence_graph(workflow)
    components = [sorted(component) for component in nx.connected_components(graph)]
    components.sort(key=lambda members: members[0])

    classes = {}
    for class_id, members in enumerate(components):
        for label in members:
            classes[label] = class_id

    non_singletons = sum(1 for c in components if len(c) > 1)
    logger.info(f'{len(components)} equivalence classes over {len(classes)} artifacts '
                f'({non_singletons} non-trivial)')
    return classes


def serialize_equivalence_classes(workflow, output_dir: str = None) -> str:
    """Write `{name}_equivalence_classes.json` next to the other workflow sidecars.

    :return: the path written.
    """
    if not output_dir:
        output_dir = workflow.out_dir
    classes = compute_equivalence_classes(workflow)
    path = f'{output_dir}/{workflow.name}_equivalence_classes.json'
    with open(path, 'w') as outfile:
        json.dump(classes, outfile, indent=2, sort_keys=True)
    return path
