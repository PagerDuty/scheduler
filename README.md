# Scheduler [![Build Status](https://travis-ci.com/PagerDuty/scheduler.svg?token=UxiJumPCnAq598SFB4MA&branch=master)](https://travis-ci.com/PagerDuty/scheduler/builds)

## Introduction

This library schedules work using Kafka for buffering and elasticity and Cassandra for persistence. Most documentation is
in the specific guides in doc/

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

## Release
 
Follow these steps to release a new version:
 - Update version.sbt in your PR
 - Update CHANGELOG.md in your PR
 - When the PR is approved, merge it to master, and delete the branch
 - Travis will run all tests, publish to Artifactory, and create a new version tag in Github
