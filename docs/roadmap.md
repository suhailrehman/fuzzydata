# Roadmap / TODO Documents

## Core Enhancements
- [x] Core interface cleanup, minimize direct attribute access in objects
- [x] Correctness checking for desterministic operations
- [ ] Customizable operation materialization rates
  - [ ] Requires retooling our model; a transformation grammar keeps information about schema evolution as operations are chained together
  - [ ] Dataframe operations chained by dot notation
  - [ ] SQL using nested subqueries
  - [ ] Execute requires special exception handling for problems occuring in between materializations

## Generator Enhancements
- [X] Better branching factor expression - simple exponential distribution with scale factor
- [ ] Weighted probabilities for next operation selection
- [ ] Operational ancestor histories and at-most two `pivot` or `groupby` operations
- [ ] NaN Checking
- [ ] Generate tables with tunable cardinality for specific columns - use faker columns with parameters for this


## Client Enhancements
### Modin
- [x] Allow for programmatic selection of `modin` engine when instantiating the class

### SQL
- [x] `CREATE VIEW` instead of `CREATE TABLE` to allow for SQL query optimizations
- [ ] Implement `pivot`


## Performance Evaluation Enhancements
- [ ] Add generation or artifact serialize/deserialize times to performance counter, not just operations
- [ ] Automatic Gantt Chart Generator using matplotlib or scrollable HTML