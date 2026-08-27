# -*- coding: utf-8 -*-

"""
fuzzydata.lineage.validity
~~~~~~~~~~~~

Non-degeneracy checks for generated artifacts.

Chained selection can drive an artifact to a handful of rows; project can reduce it to one
column; groupby collapses cardinality; one_hot_encode can leave columns that are constant
zero. A content-only lineage encoder learns nothing from such artifacts, and worse, they are
trivially confusable with each other -- an empty table derived from A is indistinguishable
from an empty table derived from B, which puts a floor on achievable accuracy that has
nothing to do with the method under test.

This is the check the release plan did not have. It is cheap here and protects the corpus's
central claim more directly than any distributional tuning.

The generator also guards against the main causes up front -- it will not offer a groupby
whose group count is below the row floor, a pivot that would be too wide or too short, a
project that would leave a single column, or a sample that would shrink past the floor.
Those guards are necessarily approximate when matfreq > 1: the generator only sees the
artifact at the START of an operation chain, so by the second or third step in the chain its
view is stale. Measured residual after the guards: about 2% of artifacts, down from 11%.
That is why this check exists as a post-hoc gate as well as a set of pre-conditions.
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

#: Fewest rows an artifact may have and still carry usable signal.
MIN_ROWS = 5

#: Fewest columns an artifact may have.
MIN_COLUMNS = 2

#: An artifact whose columns are this fraction constant (single distinct value) or more is
#: considered degenerate: there is almost nothing left to distinguish it by.
MAX_CONSTANT_COLUMN_FRACTION = 0.9


class DegenerateArtifactError(ValueError):
    """Raised when an artifact carries too little signal to belong in a corpus."""


def describe_artifact(df) -> Dict:
    """Shape and degeneracy summary for one artifact."""
    rows, columns = len(df.index), len(df.columns)
    constant = [str(c) for c in df.columns if df[c].nunique(dropna=False) <= 1]
    all_null = [str(c) for c in df.columns if df[c].isna().all()]
    return {
        'rows': rows,
        'columns': columns,
        'constant_columns': constant,
        'all_null_columns': all_null,
        'constant_fraction': (len(constant) / columns) if columns else 1.0,
    }


def check_artifact(df, label: str = '<artifact>') -> List[str]:
    """Reasons `df` is degenerate. Empty list means it is fine.

    :param df: the materialized artifact contents.
    :param label: artifact label, for the messages.
    :return: list of human-readable problems.
    """
    summary = describe_artifact(df)
    problems = []

    if summary['rows'] < MIN_ROWS:
        problems.append(f'{label}: {summary["rows"]} rows (minimum {MIN_ROWS})')
    if summary['columns'] < MIN_COLUMNS:
        problems.append(f'{label}: {summary["columns"]} columns (minimum {MIN_COLUMNS})')
    if summary['columns'] and summary['constant_fraction'] >= MAX_CONSTANT_COLUMN_FRACTION:
        problems.append(
            f'{label}: {len(summary["constant_columns"])}/{summary["columns"]} columns are '
            f'constant (limit {MAX_CONSTANT_COLUMN_FRACTION:.0%})')
    if summary['all_null_columns']:
        problems.append(f'{label}: all-null columns {summary["all_null_columns"]}')

    return problems


def validate_workflow(workflow, strict: bool = False) -> Dict[str, List[str]]:
    """Check every artifact in a generated workflow.

    :param workflow: a generated Workflow.
    :param strict: raise DegenerateArtifactError instead of returning the problems.
    :return: {artifact_label: [problem, ...]} for offending artifacts only.
    """
    problems = {}
    for label, artifact in workflow.artifact_dict.items():
        try:
            found = check_artifact(artifact.to_df(), label)
        except Exception as e:
            found = [f'{label}: could not be read back ({e})']
        if found:
            problems[label] = found

    if problems:
        flattened = [p for reasons in problems.values() for p in reasons]
        message = (f'{len(problems)}/{len(workflow.artifact_dict)} artifacts in '
                   f'{workflow.name} are degenerate:\n  ' + '\n  '.join(flattened))
        if strict:
            raise DegenerateArtifactError(message)
        logger.warning(message)
    else:
        logger.info(f'All {len(workflow.artifact_dict)} artifacts in {workflow.name} '
                    f'passed the validity check')

    return problems


def workflow_validity_summary(workflow) -> Dict:
    """Aggregate shape statistics for a workflow, for the corpus manifest."""
    summaries = {}
    for label, artifact in workflow.artifact_dict.items():
        try:
            summaries[label] = describe_artifact(artifact.to_df())
        except Exception as e:
            logger.warning(f'Could not summarise {label}: {e}')
    if not summaries:
        return {'artifacts': 0}
    rows = [s['rows'] for s in summaries.values()]
    columns = [s['columns'] for s in summaries.values()]
    return {
        'artifacts': len(summaries),
        'min_rows': min(rows),
        'max_rows': max(rows),
        'min_columns': min(columns),
        'max_columns': max(columns),
        'degenerate_artifacts': sum(1 for label, s in summaries.items()
                                    if check_artifact_summary(s)),
    }


def check_artifact_summary(summary: Dict) -> bool:
    """Whether a describe_artifact() summary is degenerate. Kept separate so the manifest
    can reuse the judgement without re-reading the data."""
    return (summary['rows'] < MIN_ROWS
            or summary['columns'] < MIN_COLUMNS
            or summary['constant_fraction'] >= MAX_CONSTANT_COLUMN_FRACTION
            or bool(summary['all_null_columns']))
