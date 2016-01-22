# widerow [![Build Status](https://travis-ci.org/PagerDuty/widerow.svg?branch=master)](https://travis-ci.org/PagerDuty/widerow/builds)

This is an open source project!

## Description

WideRow is a high level API for dealing with Cassandra wide-rows as if they were lazy collections. While primarily meant for Cassandra, the WideRow API is database and driver agnostic.

Key features:
 * Chaining of multiple rows into one logical sequence
 * Supports collection methods like `map()`, `flatMap()`, `filter()`, `collect()`, and `group()`
 * Conservative querying to minimize data reads
 * Fast querying to fetch data concurrently

## Installation

This library is published to PagerDuty Bintray OSS Maven repository:
```scala
resolvers += "bintray-pagerduty-oss-maven" at "https://dl.bintray.com/pagerduty/oss-maven"
```

Adding the dependency to your SBT build file:
```scala
libraryDependencies += "com.pagerduty" %% "widerow" % "0.5.1"
```

## Contact

This library is primarily maintained by the Core Team at PagerDuty.

## Contributing

Contributions are welcome in the form of pull-requests based on the master branch.

We ask that your changes are consistently formatted as the rest of the code in this repository, and also that any changes are covered by unit tests.

## Release

Follow these steps to release a new version:
 - Update version.sbt in your PR
 - Update CHANGELOG.md in your PR
 - When the PR is approved, merge it to master, and delete the branch
 - Travis will run all tests, publish artifacts to Bintray, and create a new version tag in Github

## Changelog

See [CHANGELOG.md](./CHANGELOG.md)
