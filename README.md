# Scheduler [![Build Status](https://travis-ci.org/PagerDuty/scheduler.svg?branch=master)](https://travis-ci.org/PagerDuty/scheduler/builds)

## Introduction

This library schedules arbitrary Scala code (tasks) to run at an arbitrary time. It tries to do this despite changing infrastructure, and it tries to maintain some sense of task ordering.

Scheduler is in production use at PagerDuty. It's meant to be included in Scala services deployed on many servers for redundancy and scaling. It's designed from the ground up to be running in a multi-datacenter environment. We take fault tolerance and reliability very seriously, so using this library is a little more complicated then just including it via SBT.

Scheduler relies on Kafka for task buffering and Cassandra for task persistence. Most documentation is
in the specific guides in doc/.

## Additional Documentation
- [Akka Style Guide](doc/akka-style-guide.md)
- [Design](doc/design.md)
- [User Guide](doc/user.md)
- [Operations Guide](doc/operations.md)

### Scheduler Dev and Testing Setup

Before running integration tests, you will need to start Kafka, Zookeeper, and Cassandra. Unit tests can be run without any dependencies.

#### Setting up Services

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


### Running Integration Tests

- To run integration tests:

```
sbt "it:test"
```

To run all unit tests and then integration tests: `sbt ";test;it:test"`

## Contributing

If you have a change, please make sure that you open a pull request that makes everybody happy:
- Format the code to use the same style as the rest of the code;
- Have test coverage;
- Give a good description, rationale, etcetera in your pull request.

We will do our best to respond to any incoming pull requests as soon as possible. If you are
not sure whether we will be inclined to merge your pull request, feel free to open an issue
on GH and ask us beforehand.

## Release

Follow these steps to release a new version:
 - Update version.sbt in your PR
 - When the PR is approved, merge it to master, and delete the branch
 - Travis will run all tests, publish to Artifactory, and create a new version tag in Github
