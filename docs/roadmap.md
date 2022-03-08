# Roadmap / TODO Documents

## Core Enhancements
- [x] Core interface cleanup, minimize direct attribute access in objects
- [x] Correctness checking for desterministic operations
- [x] Customizable operation materialization rates
  - [x] Requires retooling our model; a transformation grammar keeps information about schema evolution as operations are chained together
  - [x] Dataframe operations chained by dot notation
  - [x] SQL using nested subqueries
  - [ ] Execute requires special exception handling for problems occuring in between materializations

## Generator Enhancements
- [x] Better branching factor expression - simple exponential distribution with scale factor
- [x] Weighted probabilities for next operation selection
- [ ] Operational ancestor histories and at-most two `pivot` or `groupby` operations
- [ ] NaN Checking
- [ ] Generate tables with tunable cardinality for specific columns - use faker columns with parameters for this
- [ ] Allow FD specifications for value pairs with cardinality as mentioned above. E.g. Company "Microsoft" w/ HQ: "Redmond" should be consistent in the table.
- [ ] Generate normalized source tables (Product, Company, Order) e.g. and then join them for consistent FDs


## Client Enhancements
### Modin
- [x] Allow for programmatic selection of `modin` engine when instantiating the class

### SQL
- [x] `CREATE VIEW` instead of `CREATE TABLE` to allow for SQL query optimizations
- [ ] Implement `pivot`


## Performance Evaluation Enhancements
- [ ] Add generation or artifact serialize/deserialize times to performance counter, not just operations
- [ ] Automatic Gantt Chart Generator using matplotlib or scrollable HTML
