# -*- coding: utf-8 -*-

"""
fuzzydata.lineage.annotations
~~~~~~~~~~~~

Per-operation provenance metadata, recorded at generation time.

These fields are free to emit while generating and effectively impossible to recover
afterwards: once an artifact is on disk, whether the edge that produced it was stochastic,
augmenting or invertible is no longer visible. They exist so downstream analysis can
stratify by edge character.
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

#: Coarse operator taxonomy.
#:
#:   map        -- per-row value transformation, shape preserved
#:   projection -- column selection or renaming
#:   selection  -- row filtering, cardinality reduced
#:   sampling   -- stochastic row subsetting
#:   aggregation-- many rows to one, cardinality collapsed
#:   reshaping  -- layout change (rows <-> columns)
#:   augmenting -- introduces data absent from the input
#:
#: Note `augmenting` is a category here and NOT also a separate boolean. The original spec
#: had it both ways, which allowed contradictory records (category=map, augmenting=true).
#: One representation, derived one way.
CATEGORIES = ('map', 'projection', 'selection', 'sampling', 'aggregation', 'reshaping',
              'augmenting')

#: op -> category.
OP_CATEGORY: Dict[str, str] = {
    'apply': 'map',
    'astype': 'map',
    'fill': 'map',
    'label_encode': 'map',
    'normalize': 'map',
    'standardize': 'map',
    'project': 'projection',
    'rename': 'projection',
    'select': 'selection',
    'dropna': 'selection',
    'dedupe': 'selection',
    'sample': 'sampling',
    'train_test_split': 'sampling',
    'groupby': 'aggregation',
    'pivot': 'reshaping',
    'one_hot_encode': 'reshaping',
    'merge': 'augmenting',
}

#: Operations whose output depends on a drawn random value. Each must record a concrete
#: random_state in its args, or replay cannot reproduce it.
STOCHASTIC_OPS = frozenset({'sample', 'train_test_split'})

#: Operations that introduce data not present in the input. In 0.1.0 only merge qualifies:
#: it synthesises its right-hand table via generate_pkfk_join_table() rather than joining an
#: existing artifact, so the "new" data is synthetic. concat and impute would also qualify
#: but are Track B priority-2 and deliberately not referenced here.
AUGMENTING_OPS = frozenset({'merge'})


def op_category(op: str) -> str:
    """Category for an operation name, or 'map' for an unknown one."""
    if op not in OP_CATEGORY:
        logger.warning(f'No category registered for operation {op!r}; defaulting to "map". '
                       f'Add it to OP_CATEGORY.')
    return OP_CATEGORY.get(op, 'map')


def is_stochastic(op: str) -> bool:
    return op in STOCHASTIC_OPS


def is_augmenting(op: str) -> bool:
    return op in AUGMENTING_OPS


def annotate_op_list(op_list: List[Dict]) -> List[Dict]:
    """Per-operation annotations for one materialized edge, in chain order.

    :param op_list: the operation's op_list, i.e. [{'op':..., 'args':...}, ...]
    :return: list of annotation dicts, one per chained operation.
    """
    annotations = []
    for position, entry in enumerate(op_list):
        op = entry['op']
        annotations.append({
            'op': op,
            'category': op_category(op),
            'stochastic': is_stochastic(op),
            'augmenting': is_augmenting(op),
            'chain_position': position,
        })
    return annotations


def annotate_edge(operation) -> Dict:
    """Edge-level annotation record for a materialized operation.

    :param operation: the Operation that produced the destination artifact.
    :return: annotation dict for the operation record.
    """
    op_list = operation.op_list or []
    names = [e['op'] for e in op_list]
    per_op = annotate_op_list(op_list)

    return {
        # Realized matfreq: how many chained transformations sit behind this one edge. The
        # configured matfreq is an upper bound -- a chain can end early (a merge forces
        # materialization, and pivot is chain-final) -- so record what actually happened.
        'composition_depth': len(op_list),
        'categories': [a['category'] for a in per_op],
        'stochastic': any(a['stochastic'] for a in per_op),
        'augmenting': any(a['augmenting'] for a in per_op),
        'invertible_on_input': operation.is_invertible_on_input(),
        'sibling_group': operation.sibling_group,
        'ops': per_op,
        'unrecognized_ops': sorted(set(names) - set(OP_CATEGORY)),
    }
