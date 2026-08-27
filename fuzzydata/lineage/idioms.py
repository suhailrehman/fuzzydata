# -*- coding: utf-8 -*-

"""
fuzzydata.lineage.idioms
~~~~~~~~~~~~

Named workflow idioms: ordered category preferences that bias operator selection.

Why this exists. The default policy picks uniformly among whatever the schema permits, which
makes every edge conditionally independent of the ones before it given the schema. Real
analyst workflows are not like that -- cleaning precedes encoding, encoding precedes
splitting -- so the default policy is an empirical test of the state-independence assumption
rather than a neutral choice. Sampling a latent idiom per workflow and biasing toward its
next stage produces within-workflow correlation between edges, which is the condition the
assumption is supposed to tolerate.

Idioms bias, they do not force: schema legality always wins, so an idiom stage that cannot
be satisfied is skipped rather than blocking generation.
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

#: idiom name -> ordered stages, each a list of operator names acceptable at that stage.
IDIOMS: Dict[str, List[List[str]]] = {
    # Prepare data for model training: clean, encode, scale, split.
    'ml_prep': [
        ['dropna', 'dedupe', 'fill'],
        ['label_encode', 'one_hot_encode'],
        ['normalize', 'standardize', 'astype'],
        ['train_test_split', 'sample'],
    ],
    # Business rollup: narrow, filter, aggregate, reshape.
    'bi_rollup': [
        ['project', 'rename'],
        ['select', 'dropna'],
        ['groupby'],
        ['pivot'],
    ],
    # Exploratory poking around: look at columns, filter, subsample, summarise.
    'eda': [
        ['project'],
        ['select'],
        ['sample'],
        ['groupby', 'apply'],
    ],
    # Assemble a wider table, then derive from it.
    'enrich': [
        ['merge'],
        ['apply', 'fill'],
        ['project', 'rename'],
    ],
}

IDIOM_NAMES = tuple(IDIOMS)

#: Fraction of the time an idiom-guided workflow follows its idiom when the stage is legal.
#: Below 1.0 so idiom workflows still contain off-idiom edges -- a perfectly obedient idiom
#: would be trivially identifiable and would not test anything interesting.
IDIOM_ADHERENCE = 0.75


class IdiomState:
    """Tracks how far through an idiom one workflow has progressed."""

    def __init__(self, name: str, adherence: float = IDIOM_ADHERENCE):
        if name not in IDIOMS:
            raise ValueError(f'Unknown idiom {name!r}; expected one of {IDIOM_NAMES}')
        self.name = name
        self.stages = IDIOMS[name]
        self.adherence = adherence
        self.stage = 0

    def preferred_ops(self) -> List[str]:
        """Operators acceptable at the current stage, or [] once the idiom is exhausted."""
        if self.stage >= len(self.stages):
            return []
        return list(self.stages[self.stage])

    def advance(self, chosen_op: str) -> None:
        """Move to the next stage once the current one has been satisfied."""
        if self.stage < len(self.stages) and chosen_op in self.stages[self.stage]:
            self.stage += 1

    def select(self, ops_choices: List[Dict], rng) -> Dict:
        """Choose an operation, biased toward the idiom's current stage.

        Falls through to a uniform choice when the stage cannot be satisfied by anything the
        schema currently permits, so an idiom never blocks generation.
        """
        preferred = set(self.preferred_ops())
        candidates = [c for c in ops_choices if c['op'] in preferred]

        if candidates and rng.random() < self.adherence:
            chosen = candidates[int(rng.integers(0, len(candidates)))]
        else:
            chosen = ops_choices[int(rng.integers(0, len(ops_choices)))]

        self.advance(chosen['op'])
        return chosen


def sample_idiom(rng) -> str:
    """Draw a latent idiom for one workflow."""
    return IDIOM_NAMES[int(rng.integers(0, len(IDIOM_NAMES)))]
