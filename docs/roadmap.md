# Roadmap / TODO Documents

## Core Enhancements
* Core interface cleanup, minimize direct attribute access in objects
* Customizable operation materialization/serialization rates
* Correctness checking for desterministic operations

## Generator Enhancements
* Better branching factor expression - simple exponential distribution with scale factor
* Weighted probabilities for next operation selection
* Operational ancestor histories and at-most two `pivot` or `groupby` operations
* NaN Checking


## Client Enhancements
### Modin
* Allow for programmatic selection of `modin` engine when instantiating the class

### SQL
* `CREATE VIEW` instead of `CREATE TABLE` to allow for SQL query optimizations
* Implement `pivot`


## Performance Evaluation Enhancements
* Add generation or artifact serialize/deserialize times to performance counter, not just operations
* Automatic Gantt Chart Generator using matplotlib or scrollable HTML