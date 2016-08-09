# Contributing Guide

## Dev and Testing Setup

Before running integration tests, you will need to start Kafka, Zookeeper, and Cassandra. Unit tests can be run without any dependencies.

### Setting up Services

This library, and its integration tests, rely on Cassandra and Kafka to be running on localhost on default ports. You
can get it from the sources, or use our quick development setup repository:

- Clone [dev-setup](https://github.com/PagerDuty/dev-setup/)
- `cd` into cloned directory
- Run `make kafka.run cassandra.run`

#### Configuring Kafka

- Create a `scheduler-integration-test-queue` Kafka topic with 16 partitions each:

```
~/travis-deps/kafka*/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 16 --topic scheduler-integration-test-queue
```

If the topic already exists you can alter it:

```
~/travis-deps/kafka*/bin/kafka-topics.sh --alter --zookeeper localhost:2181 --partitions 16 --topic scheduler-integration-test-queue
```

#### Configuring Cassandra

No extra cassandra setup is needed. The integration tests will automatically create the test schema.

### Running Unit Tests

```
sbt "test"
```


### Running Integration Tests


```
sbt "it:test"
```

To run all unit tests and then integration tests: `sbt ";test;it:test"`

## Contributing Changes

If you have a change, please make sure that you open a pull request that makes everybody happy:
- Format the code to use the same style as the rest of the code (see [the Akka style guide](akka-style-guide.md) when working in the Akka code);
- Have test coverage;
- Give a good description, rationale, etcetera in your pull request.

We will do our best to respond to any incoming pull requests as soon as possible. If you are
not sure whether we will be inclined to merge your pull request, feel free to open an issue
on GH and ask us beforehand.

## Release

Follow these steps to release a new version:
 - Update version.sbt in your PR
 - When the PR is approved, merge it to master, and delete the branch
 - Travis will run all tests, publish to Bintray, and create a new version tag in Github
