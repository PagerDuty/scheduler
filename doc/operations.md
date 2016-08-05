# Scheduler Operations Guide

You should have read the User Guide and also have a basic knowledge of Kafka producers, consumers and partitioning. Also note that the operations here are heavily slanted towards PagerDuty's setup, which, among others, features:

- Lots of Datadog graphs (created through calls to the [metrics](https://github.com/PagerDuty/scala-metrics) library;
- Lots of datacenters.

Please adapt to your own setup as necessary.

### Dashboards
We have created a single Dashboard in Datadog, where some tags can be used to select the instance of
the scheduler (for us, which application and whether it is in production or staging). [This document](dashboard.md)
contains an overview of the graphs, mentioned below, we use on that dashboard.

### Procedures
A single datacenter operating in a degraded state may result in frequent Scheduler and Application errors. If these errors are causing unacceptable delays for customer facing services, then it is recommended to shut down all the Scheduler-related services in the degraded datacenter:
- Shut down all the Scheduler application nodes in the affected DC
- Shut down all the Kafka brokers in the affected DC
- Shut down all the Cassandra nodes in the affected DC

Once none of the services from the degraded datacenter are interacting with any of the healthy services, the overall system performance will fully recover.

### Task Flow, Stages & Monitoring

This diagram shows the flow of tasks in the scheduler:

```
--> [ scheduleTask() ] --> [ Kafka ] --> [ BULK OF SCHEDULER ] --> taskRunner
                                                 | ^
                                                 v |
                                            [ Cassandra ]
```

Basically, `scheduleTask()` is a thin wrapper around Kafka send. It uses Kafka to partition tasks - using `orderingId` as the Kafka message key - and durably stores newly scheduled tasks in dedicated topic-partition streams. This step is monitored with `Tasks Enqueued to Kafka` DataDog graphs.

The bulk of Scheduler sits downstream of Kafka. It uses Kafka consumers to take care of auto-rebalancing and ensuring mutual exclusivity of topic-partition assignment as we distribute task processing across several appication nodes. On each app node, the Kafka consumer loads tasks in batches, and immediately forwards them to Cassandra, which stores tasks in dedicated rows for each partition. This step is monitored with `Persist Polled Task Batch to Cass` DataDog graphs.

We then use Cassandra's native row-sorting capabilities to load tasks sorted by scheduled time and execute them. This step is monitored with `Task Execution` DataDog graphs.

Internally, we use Akka for task scheduling and execution. We monitor the number of tasks loaded from Cassandra and awaiting their scheduled time with the `Tasks in SchedulerActor` graph. We monitor tasks in progress with the `Tasks in ExecutorActor` graph.

Additionally, we use the `Stale Tasks` dashboard, backed by raw Cassandra queries, to monitor how well the system is keeping up.

If there is a stuck task, it will be retried the number of times configured with the `scheduler.max-task-retries` setting (found in `src/main/resources/application.conf` with `src/main/resources/reference.conf` as default). Once the retry limit is reached, the task will be completed with a "failed" status. Once the task has been failed it will not be retried again. The task retry count is not stored durably, so any partition rebalancing will reset the `retryCount` back to zero.
