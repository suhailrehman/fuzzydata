# Workflow topology

`fuzzydata` chooses which existing artifact each new artifact derives from. That choice is
what determines the shape of the lineage graph, and the shape matters: sibling confusion in
content-based lineage inference is expected to peak on star-shaped workflows, where many
artifacts share one parent and differ only slightly.

Set the strategy with `topology=` on `generate_workflow()`, or `--topology` on the CLI.

## Modes

| mode | parent chosen | shape |
|---|---|---|
| `chain` | the newest artifact | one long path |
| `star` | the root artifact | every artifact hangs off the base |
| `balanced` | oldest artifact with spare capacity | balanced k-ary tree (k = `Workflow.BALANCED_BRANCHING_FACTOR`, default 2) |
| `random_recursive` | uniformly at random | random recursive tree |
| `bfactor` | weighted `exp(bfactor * i)` over generation order | tunable, see below |

`chain`, `star` and `balanced` select deterministically. That is the point of them: **no
value of `bfactor` produces a true star.** The exponential weighting favours the newest
artifact as `bfactor` grows (a chain) and approaches uniform as it shrinks (a random
recursive tree). Neither extreme is a star, so before these modes existed the star stratum
was simply ungeneratable.

## The `bfactor` direction was documented backwards

Through 0.0.11 the README and `--bfactor` help both said *"0.1 is linear, 100 is star-like"*.
The implementation does the opposite: weight increases with generation index, so a **high**
`bfactor` concentrates selection on the newest artifact and produces a **chain**, while a
**low** value spreads selection across all existing artifacts and is branchy. The tables
below are measured, not asserted.

Two related defects were fixed at the same time:

- The leading constant `bfactor / (exp(bfactor * size) - 1)` overflowed to `0` for large
  `bfactor * size`, after which normalising gave `nan` probabilities and selection raised.
  `--bfactor 100` — the value the documentation recommended — was a hard crash. The constant
  is cancelled by the normalisation that follows it, so it was removed and `exp()` is now
  shifted by its maximum.
- The CLI default was `5.0` while the function default was `0.5`, a 10x mismatch. `5.0` is
  deep in the always-pick-newest regime, so every corpus generated with CLI defaults up to
  0.0.11 was close to a chain.

## Measured shapes

`num_versions=20`, `base_shape=(8, 300)`, `matfreq=1`, `exclude_ops=['merge']`, averaged over
seeds 1-5. `merge` is excluded because it introduces a second parent and a synthetic
root, which perturbs both depth and out-degree.

| mode | mean depth | mean branching factor | max out-degree | leaf fraction |
|---|---|---|---|---|
| `chain` | 17.20 | 1.11 | 2.00 | 0.14 |
| `star` | 1.00 | 19.00 | 19.00 | 0.95 |
| `balanced` | 3.80 | 2.03 | 2.60 | 0.53 |
| `random_recursive` | 5.00 | 2.20 | 5.00 | 0.56 |

| `bfactor` b | mean depth | mean branching factor | max out-degree | leaf fraction |
|---|---|---|---|---|
| 0.01 | 4.40 | 2.32 | 5.00 | 0.57 |
| 0.1 | 5.40 | 1.75 | 3.60 | 0.45 |
| 1.0 | 11.60 | 1.35 | 2.40 | 0.29 |
| 10.0 | 18.60 | 1.02 | 1.40 | 0.07 |
| 100.0 | 18.60 | 1.02 | 1.40 | 0.07 |

Reading the tables:

- `star` is exact: max out-degree `n-1 = 19`, depth `1`.
- `chain` reaches depth 17.2 rather than the full 19 because `train_test_split` produces two
  children from one parent, which branches the path. Excluding it as well gives depth
  exactly `n-1`; the test suite asserts that case.
- `bfactor` is monotonic in the direction described above, and saturates by about `b = 10`:
  beyond that the newest artifact is selected essentially always, so `100` and `10` are
  indistinguishable.
- `random_recursive` and `bfactor` at `b = 0.01` agree closely, which is the expected
  limiting behaviour and a useful sanity check on the weighting.

Regenerate these tables with `tests/test_topology.py` as a starting point; the shape
assertions there are exact rather than approximate, because the deterministic modes have
nothing to be approximate about.
