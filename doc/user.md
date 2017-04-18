# Scheduler User Guide

## Introduction

Scheduler is a task scheduling library made for building distributed services at PagerDuty.

It provides the following functionality:

- Lets you schedule a task for execution at a specified future time.

- Executes the tasks at the scheduled time. (if possible - see "Scheduling & Order" below for details)

- Lets you specify a serial execution scope for each task and ensure that tasks are executed one at a time within each scope. (see "orderingId" below for details)

- Distributes tasks in your service cluster. This includes automatic rebalancing as individual hosts in your cluster are added or removed (or crash or recover).


Scheduler is intended as a replacement for our current WorkQueues. The new library provides scheduling but does NOT provide "fairness". It is also not intended for simple FIFO usage. (you could use it for that, but you should use just Kafka - see the pd-kafka-consumer library)


## High-level Design

Scheduler is a library that you include in your service code, not a separate service. The main API is specified
in [`SchedulerClient`](https://github.com/PagerDuty/scheduler/blob/master/scala-api/src/main/scala/com/pagerduty/scheduler/SchedulerClient.scala).

It's operation is shown in the following diagram:

```
         +------------------------------------------+
         |             SCHEDULER                    |
         |                                          |
 --------> scheduleTask()                           |-----> taskRunner
         |                                          |
         +------------------------------------------+
                      |     ^              | ^
                      |     |              | |
                      v     |              v |
                     [ Kafka ]        [ Cassandra ]
```

To schedule a [`Task`](https://github.com/PagerDuty/scheduler/blob/master/common/src/main/scala/com/pagerduty/scheduler/model/Task.scala), you call [`scheduleTask()`](https://github.com/PagerDuty/scheduler/blob/master/scala-api/src/main/scala/com/pagerduty/scheduler/SchedulerClient.scala#L27) which will write this task to persistent storage.

The [scheduler](https://github.com/PagerDuty/scheduler/blob/master/scheduler/src/main/scala/com/pagerduty/scheduler/Scheduler.scala#L71) will then attempt to execute the task at the scheduled time on one of the hosts in the service cluster. Note that this host is unrelated to the host on which `scheduleTask()` was called for that task.

Task execution will be a call to the `execute` method of the [`TaskExecutorService`](https://github.com/PagerDuty/scheduler/blob/master/scheduler/src/main/scala/com/pagerduty/scheduler/TaskExecutorService.scala) you configured the scheduler with. Once the handler finishes, the task is marked completed. If the handler throws an exception, the task is retried up to a certain limit and then marked as failed.

The scheduler aims for "at least once" execution of tasks. This means that your tasks may be run more than once & you should design your task logic to handle this. This can happen due to a number of errors: network errors, persistence errors, crashing servers, etc.

As shown in the diagram, the scheduler uses both Kafka and Cassandra for persistence. Note that we have a shared "bitpipe" Kafka cluster which you can use, so you can probably avoid having to spin up new infrastructure for this purpose.


## Scheduling & Ordering

During normal operation, the scheduler aims to run your tasks at the scheduled time and in the order determined by the scheduled time. But when the scheduler is falling behind, recently submitted tasks that are overdue will be executed immediately*.

There is a way to have deterministic order of execution for a given orderingId: ensure that task enqueue order matches scheduling time order.

In other words, let's say you have two tasks A & B with the same orderingId and task A is scheduled before task B.  If you want to guarantee that task A is executed before task B, you should ensure task A is submitted before task B.

_* e.g. if the scheduler is behind by about 2 minutes - tasks due now that were submitted in 1 minute ago may run before tasks due now that were submitted 10 minutes ago. See "Links" below for more reading._


## Task data structure

A task to be scheduled is represented using a Task data structue which consists of the following fields:

- `orderingId` - a string of your choice used for multiple scoping purposes - see details below
- `scheduledTime` - the time you would like your task to execute
- `uniquenessKey` - a string of your choice to handle duplicates - see details below
- `taskData` - a map of the data that your task will need - the contents must be serializable to JSON

### "orderingId" & serial execution

The scheduler guarantees that, for a given orderingId, only one task is executed at a time. Tasks with different orderingIds can and will run in parallel.

Note that schedule time order is _NOT_ guaranteed even within a given orderingId - see "Scheduling & Ordering" above.


### "uniquenessKey" & deduplication

The scheduler uses the tuple `(orderingId, scheduledTime, uniquenessKey)` as a task identifier, and ignores multiple entries that have the identifier value. This lets clients ensure that duplicates are not created if they retry a `scheduleTask()` call that threw an error.

Note that using this identifier as a mechanism to update a previously scheduled task with different task data is NOT supported at this time. (the actual behavior is undefined and may change)


## Configuring & Using

### Kafka & Cassandra

As shown in the diagram above, the scheduler uses both Kafka and Cassandra for persistence. Accordingly, the scheduler has a Kafka topic and a Cassandra keyspace.

Note that PagerDuty infrastructure has a "bitpipe" Kafka cluster which is meant to be a shared Kafka cluster, especially for the new scheduler. You can choose to use this and avoid creating up a new Kafka cluster.


### Library Setup

- This set of libraries is published to PagerDuty Bintray OSS Maven repository. Add it to your resolvers:

```scala
resolvers += "bintray-pagerduty-oss-maven" at "https://dl.bintray.com/pagerduty/oss-maven"
```

- Then add the dependency to your SBT build file:

```scala
libraryDependencies ++= Seq(
  "com.pagerduty" %% "scheduler" % "<latest version>",
  "com.pagerduty" %% "scheduler-scala-api" % "<latest version>" // only if scheduling tasks from Scala
)
```

- Copy the schema migrations from `cassandra-migrations/` to your project, and change the keyspace name in those files from `<YourServiceName>Scheduler` to a suitable name (e.g. `MyAwesomeServiceScheduler`)
- Add configuration with appropriate Kafka & Cassandra details
- Instantiate a Scheduler instance in your service
- Wire up scheduleTask
- Wire up taskRunner

### Scheduling Tasks

You can schedule tasks in two ways:

#### Schedule through Scala library

Call `Scheduler.scheduleTask()`. The Task object is used to validate
your data, compose a Kafka record, and send it off. This is probably
the simplest if you only use Scala. See above for the meaning of
the fields in a Task object.

#### Schedule by writing into the Kafka topic

Write a JSON record to the Kafka topic a Scheduler is listening on.
The JSON record contains the Task record (again, see above for more
details) and looks like this:

```
{
  "version":        1,
  "orderingId":     "this is my ordering id",
  "scheduledTime":  "2016-06-07T21:11:26.000Z",
  "uniquenessKey":  "42 always unique",
  "taskData": {
    "foo": "bar",
    "bar": "quux"
  }
}

```

There are some important rules to keep in mind here:

- You _MUST_ write this to a partition indicated by `orderingId`
and using that field as the key (see the Partitioner class for the
modulo operation used in the library to assign partitions). The
library works by assuming that all tasks for the same orderingId
are processed on the same node.
- `scheduledTime` should not be too far in the past. By default,
"too far in the past" is 10 seconds ago. Unpredictable things may
happen. I'd advice you to visit your local Gaudi museum to find out
what.
- `version` is to make room for future changes. Keep it at `1` for now.


## Links

- [Scheduler Operations Guide](operations.md)
- [Scheduler Design](design.md)
- [Kafka Operations Guide](https://pagerduty.atlassian.net/wiki/display/ENG/Kafka+Operations+Guide)
- [Cassandra Operations](https://pagerduty.atlassian.net/wiki/display/ENG/Cassandra+-+Operations)
