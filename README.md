# widerow

This is an open source project!

[![Build Status](https://magnum.travis-ci.com/PagerDuty/???)](https://magnum.travis-ci.com/PagerDuty/widerow)

## Description

WideRow is a high level API for dealing with Cassandra wide-rows as if they were lazy collections. While primarily meant for Cassandra, the WideRow API is database and driver agnostic.

Key features:
 * Chaining of multiple rows into one logical sequence
 * Supports collection methods like `map()`, `flatMap()`, `filter()`, `collect()`, and `group()`
 * Concervative querying to minimize data read
 * Fast querying to fetch data concurrently

## Installation

Make sure your project has a resolver for the PagerDuty artifactory repository, you can then add the dependency to your SBT build file:

```scala
libraryDependencies += "com.pagerduty" %% "widerow" % "0.4.4"
```

## Contact

This library is primarily maintained by the Core Team at PagerDuty.

## Contributing

Contributions are welcome in the form of pull-requests based on the master branch.

We ask that your changes are consistently formatted as the rest of the code in this repository, and also that any changes are covered by unit tests.

## Changelog

See [CHANGELOG.md](./CHANGELOG.md)
