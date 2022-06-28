# mindexer

An experimental tool to recommend indexes for MongoDB based on a query workload and sample of the data.

## Description

`mindexer` is a command line tool written in Python to recommend indexes for MongoDB. It uses queries logged to the `system.profile` collection (find out more about [profiling in MongoDB](https://www.mongodb.com/docs/manual/tutorial/manage-the-database-profiler/)) and a small random sample of the original collection to determine the indexes best suited for a workload and dataset.

## Disclaimer

Please note: This tool is not officially supported or endorsed by MongoDB Inc. The code is released for use "AS IS" without any warranties of any kind, including, but not limited to its installation, use, or performance. Do not run this tool against critical production systems. It is recommended to use `mindexer` in a test / QA environment that closely resembles your production system.

## Installation

This tool requires python 3.x and pip on your system. To install `mindexer`, run the following command:

```bash
pip install mindexer
```

## Usage

`mindexer` proposes indexes for a collection by evaluating index candidates based on the query workload and the data distribution.

### Step 1. Collect Queries

In order to provide queries to `mindexer`, you need to record a typical workload to the `system.profile` collection using the [MongoDB Profiler](https://www.mongodb.com/docs/v5.0/tutorial/manage-the-database-profiler/).

Ideally, you should first turn off the profiler, delete the `system.profile` collection for the database in which the collection resides, then re-enable the profiler and run the query workload against the collection.

Assuming you want to determine indexes for the `mydatabase.mycollection` collection, you can run the following commands in the [mongosh shell](https://www.mongodb.com/docs/mongodb-shell/):

```js
// switch to the correct database
use mydatabase

// disable the profiler
db.setProfilingLevel(0)

// delete the system.profile collection
db.system.profile.drop()

// re-enable the profiler at level 2, logging all operations
db.setProfilingLevel(2)
```

For more fine-grained control over which operations are logged to the profiler, you can specify filters and sample rate, as discussed in the [MongoDB Profiler](https://www.mongodb.com/docs/v5.0/tutorial/manage-the-database-profiler/#set-a-filter-to-determine-profiled-operations) documentation. For example, you could limit profiling to only _queries_ on the target collection like so:

```js
// re-enable the profiler to log only queries on the mydatabase.mycollection namespace
db.setProfilingLevel(2, {
  filter: { op: "query", ns: "mydatabase.mycollection" },
});
```

Once the workload has been collected, you can disable the profiler again as shown above.

### Step 2. Execute mindexer

With the workload recorded in `system.profile` you can run `mindexer`, providing the MongoDB URI connection string, database and collection name for which to recommend indexes.

**Example**

```
mindexer --uri mongodb://my.mongodb.url:27017 --db mydatabase --collection mycollection
```

`mindexer` queries the `system.profile` collection to find all queries related to the provided collection. Currently, only a subset of the query language is supported, see below for limitations. Unsupported queries will be skipped.

`mindexer` then extracts a sample of the original collection and stores it in a temporary collection in the `mindexer_samples` database. By default, `mindexer` extracts 0.1% of the collection size (sample ratio = 0.001) using an aggregation with a `$sample` stage, but this is configurable via the `--sample-ratio` command line argument.

Based on the found queries, `mindexer` produces a list of potential index candidates, which are then evaluated by running a number of queries against the sample collection extracted in the previous step. These queries help determine cardinalities of sub-queries to score the index candidates.

The final output is a list of indexes that `mindexer` determined to be beneficial. The indexes are sorted in order of their scores, with the top indexes having the highest scores (and thus most benefit given the workload).

## Limitations

`mindexer` is an early prototype and does not support the full MongoDB query language yet.

`mindexer` supports _find_ queries over one or more predicates (fields) with the following query operators: equality (via simple key/value pairs, e.g. `{foo: "bar"}`), ranges (`$gt`, `$gte`, `$lt`, `$lte`), `$in`, `$exists`, `$regex`, `$size` and negations `$ne` and `$nin`. Also supported are projections, sorts and limits.

The following features are not yet supported:

- Disjunctions in queries (`$or`, `$nor`) or multiply nested `$and` conjunctions (top-level `$and` is supported)
- Aggregation pipelines via the `aggregate` command
- More advanced query operators like `$text`
- Compound index mixed sort order is currently not considered, all fields are sorted in ascending (1) order
- Anything not explicitly mentioned as supported above

## Tests

To execute the unit tests, run from the top-level directory:

```
python -m unittest discover ./tests
```
