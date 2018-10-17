# Scheduler Design

_(Note: this document was written when we started this project at PagerDuty; it's quite terse, but contains
some important ideas that we thought were worth sharing when open sourcing the library. We've done some
mild edits but left the original mostly in tact)_

## Goals
A scalable, understandable library for scheduling work that needs to be done at some point in 
the future.

## High Level Definitions
Each **Scheduler** queue consists of many smaller ordering queues, as identified by **orderingId**. The order of execution is defined at the ordering queue level.

## Assumption
 - Tasks are idempotent (this is required for any distributed work queue);
 - We do not need fairness (tasks for one logical queue may delay tasks for another logical queue);
 - We do not need to enforce the order of execution between different ordering queues;
 - We do not need to enforce the order of execution for tasks in ordering queues when task enqueueing order differs from task scheduling order. For example: `{ enqueue(task1, time2); enqueue(task2, time1) }` may be executed as either `(task2, task1)` or `(task1, task2)`;
 - Tasks will be scheduled to run in the present or in the future, but not in the past;
 - Scheduling does not have to be precise (deltas in the low tens of seconds are acceptable);
 - Task execution may be delayed;
 - Task failure is rare.

## Functional Requirements
 - Must have API to enqueue tasks;
 - Must run tasks at the time scheduled;
 - For ordering queue, when tasks are enqueued in the same order they are scheduled, those tasks must be executed in that order. For example: `{ enqueue(task1, time1); enqueue(task2, time2) }` must be executed as `(task1, task2)`.

## Non-Functional Requirements
 - Must have multi-datacenter replication;
 - Must be resilient to datacenter outages;
 - Must allow adding or removing processing nodes dynamically;
 - Rebalancing caused by adding/removing nodes must be quick enough to allow the whole pipeline to stay within SLA, even with multiple queues in the pipeline;
 - Single stuck ordering queue must not prevent other ordering queues from making progress;

## Not Doing
 - Fairness;
 - We will not optimize for persistent task failures. We explicitly require other ordering queues to make progress while a single OrderingQueue is stuck, however this may cause overall performance degradation, proportional to the number of tasks stuck, and the duration of the problem.

## Design
In order to satisfy dynamic rebalancing part of non-functional requirements, we will take advantage of Kafka's partition rebalancing algorithm. For scheduling and task completion tracking, we will use Cassandra's wide rows. This will allow us to satisfy the quick rebalancing requirements, as well as allow fine-grained execution where failure in one ordering queue has limited effect on others. Finally, we will use Akka as an execution engine that ties everything together.

### Design Understanding Prerequisites
 - Basic knowledge of Kafka producer and consumer;
 - Understanding of the general Kafka partitioning strategy;
 - Basic knowledge of Cassandra wide rows;
 - Understanding of actor model;
 - Basic knowledge of Akka.

Configurations for cross datacenter replication for Cassandra and Kafka are generic and should be covered elsewhere.

### Design Definitions
 - **partitionId**, derived by Kafka producer from **orderingId** using a hash function;
 - **orderingId**, user specified ordering id, used as Kafka message key, as well as when persisting tasks to Cassandra;
 - **uniquenessKey**, user specified key, used to allow multiple tasks for the same **orderingId** at the same time;
 - **taskData**, serialized task data, interpreted by user provided executor (we recommend JSON format);
 - **scheduledTime**, target execution time;
 - **enqueueingTime**, time stamp of when the task was enqueued, as seen by the Kafka producer.

### Fixed Partitioning
Each **Scheduler** queue will be backed by a dedicated Kafka topic. Kafka supports native topic partitioning by hashing the message key and assigning the message to a partition based on the hashed value. We take full advantage of this partitioning scheme to distribute task processing between multiple nodes, where each node may be responsible for one or more partitions. For this configuration to work, all the **Scheduler** Kafka consumers must belong to the same consumer group.

The design heavily relies on fixed mapping between message keys and Kafka partitions. With fixed mapping, we know that all messages for the same **orderingId** will always end up in the same partition, and get processed by at most one physical node at any one time. This means that we cannot change the number of partitions in the running system. So we must choose a sufficiently large number of partitions to start with. The general consensus so far is to have the number of partitions in triple digits (low hundreds).

### Enqueueing
We will provide a **Scheduler** class that will encapsulate connection configuration, and provide a scheduleTask method with the following signature:
```
  def scheduleTask(Task): Future[Unit]
```
The task contains the taskKey, which is used to identify each task. `TaskKey` is a case class consisiting of the **orderingId**, **scheduledTime** and **uniquenessKey**. When two tasks are enqueued with the same task key, the second task will overwrite the first one. If the original task was already run, and the task key was marked as complete, then the task will not re-run.

The method will check **scheduledTime** against current system time, returning failed future, if we attempt to enqueue too far in the past. To simplify the usage, we will allow a grace window of a few seconds in the past from the current time. The primary purpose of our queue is to schedule task
execution, so enqueueing far in the past does not serve any meaningful purpose. By disallowing this behaviour we can make significant simplifications in our design.

**uniquenessKey** can be used to enqueue multiple tasks for the same **orderingId**, at the same time. Alternatively, when **uniquenessKey** is derived from invariable task data, it can be used to enforce idempotency.

### Processing
Most of the design revolves around dequeuing and executing tasks. This process is driven by the Kafka consumer's **poll** method.

#### Partition Rebalancing
Kafka consumer interaction is done using a single threaded loop that calls a **poll** method. All the partition reassignment callbacks happen in that thread as part of **poll** method invocation. This allows us to block the thread, waiting for asynchronous operations to complete.

Partition rebalancing callbacks are invoked as part of the call to the **poll** method. The two callbacks are [**onPartitionsRevoked**](http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html#onPartitionsRevoked(java.util.Collection)) and [**onPartitionsAssigned**](http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html#onPartitionsAssigned(java.util.Collection)). Kafka ensures that **onPartitionsRevoked** has completed on all the nodes, before proceeding to **onPartitionsAssigned**. This behaviour allows us to ensure data integrity during partition rebalancing as follows:

##### Upon receiving onPartitionsRevoked 
  1. Shutdown the actor system to prevent further scheduling
  2. Shut down task ExecutorService to prevent enqueued tasks from starting; since we cannot stop already running tasks, we will allow them to run to completion
  3. Wait a few seconds to allow tasks to complete normally, reducing error rate in logs
  4. Shut down Cassandra connection pool to prevent further writes to Cassandra; this should cause any tasks that are still running to fail when accessing Cassandra
  5. Wait a few seconds to account for clock drift between nodes

##### Upon receiving onPartitionsAssigned
When we receive **onPartitionsAssigned**, we spin up a new actor system configured with new partitions. It may be tempting to keep existing partitions after **onPartitionsRevoked**, in hopes of getting them back with **onPartitionsAssigned**. However, this add non-trivial complexity to the
system. Instead, we will go with the simples approach, terminating all the running partitions when revoked, and loading a fresh copy when assigned.

#### Cassandra Persistence
For each partition, we use time-bucketing to further break down task scheduling data into multiple rows. Within the row, we use Cassandra's native sorting capabilities to build the schedule. Let's call this **ScheduledColumnFamily**:
```
RowKey = (partitionId, hourKey)
ColumnName = (scheduledTime, orderingId, uniquenessKey)
ColumnValue = taskData
(partitionId, hourKey) => SortedMap(... (scheduledTime, orderingId, uniquenessKey) -> taskData, ...)
```
We use separate column family to store task completion. Let's call this **CompletionColumnFamily**:
```
RowKey = (partitionId, hourKey)
ColumnName = (scheduledTime, orderingId, uniquenessKeasy)
ColumnValue = null
(partitionId, hourKey) => SortedMap(... (scheduledTime, orderingId, uniquenessKey) -> null, ...)
```
One of the non-functional requirements is to be able to rebalance partitions quickly. In order to do this, we can try to eliminate already completed tasks when loading them from Cassandra. A simple and cheap solution is to delete tasks from the **ScheduledColumnFamily** after they were inserted
into **CompletionColumnFamily**. This deletion operation is an optimization and is not required for consistency, so we will not retry the task if deletion fails. When reading **ScheduledColumnFamily**, completed tasks that were deleted will not be loaded, significantly speeding up partition startup time.

#### Scheduling and Execution
Since we are dealing with an event driven concurrent system, we can leverage Akka to deal with concurrency. The entry point into the actor system will be **TopicSupervisor**. After some tasks have been polled from Kafka, we use the Akka's ask pattern to send them to the **TopicSupervisor**, and wait on the resulting Future. This will block the **poll** thread until
all the tasks have been persisted. Once the **poll** thread resumes, we can commit the consumer offset.

See the attached diagram for further details, and the details actor description below the diagram. Here, actors are represented by circles and messages are represented by arrows, there are no state changes shown on this diagram: [Scheduled Queue Design](scheduler-actor-diagram.png)

We need to limit task execution to a configurable number of concurrent tasks in order to prevent Cassandra cluster from collapsing due to high number of concurrent reads. The are many strategies to do achieve this, but the simplest one is an ExecutorService with an unbounded waiting queue backed by a fixed-size thread pool (aka **TaskExecutorService**).

### Details On Each Actor:

##### Supervision Strategy
Because our messages between actors contain tasks, losing messages will cause tasks to be dropped, so the supervision strategies of the actors must be able to reflect that. The default supervision strategy will cause actors to restart upon exception, thereby causing them to drop all their messages. Our supervision strategy must reload the entire system to a clean state to prevent the droppping of tasks. Thus our supervision strategy is to escalate all exceptions up until it reaches the root guardian, at which point the guardian will shutdown the system. We can then reload the tasks from cassandra and return back to a clean state.

##### TopicSupervisor
* Is the top-level actor and handles supervision of other actors
* When receiving `ProcessTaskBatch(Map[PartitionId, Seq[Task]])` message, it will spawn a **PartitionSupervisor** for each partition and send a `PersistTasks(Seq[Task])` to each **PartitionSupervisor**. 
* Once each **TaskPersistence** actor, which is a child of **PartitionSupervisor**, has replied with a `TasksPersisted(PartitionId)`, it will then reply to the system with a `TaskBatchProcessed`
 
##### PartitionSupervisor 
* is the actor which manages all of the actors in a given partition. 
* On creation, it will spawn the **TaskCompletionTracker**, **ThroughputController**, **PartitionExecutor**, **PartitionScheduler** and **TaskPersistence** actors.
* Upon receiving a `PersistTasks(Seq[Task])` from the **PersistRequestActor**, it will forward the message to the **TaskPersistence** actor.

##### TaskPersistence 
* handles reads and writes to **ScheduledColumnFamily** in Cassandra for a given partition
* Maintains `readCheckpoint`, which is the last task key it has read.
* When receiving `PersistTasks(Seq[Task])`, the tasks will be written to the **ScheduledColumnFamily** in Cassandra.
* After tasks have been successfully written to the datastore, the actor replies to the TopicSupervisor with `TasksPersisted(PartitionId)`. Any tasks that have a scheduled time before the `readCheckpoint` are immediately sent to the **PartitionScheduler** through the message `ScheduleTasks(Seq[Task])`
* When receiving `LoadTasks(upperBound: Time, limit: Int)` message, it will load the tasks from Cassandra over the time period with a maximum amount being the limit that was provided
* When tasks are loaded from Cassandra, the actor will send `TasksLoaded(readCheckpointTime)` to the **ThroughputController**, and finally advance the `readCheckpoint`. These tasks are then forwarded to the **PartitionScheduler** through the message `ScheduleTasks(Seq[Task])`.

##### PartitionScheduler
* Upon receiving `ScheduledTasks(Seq[Tasks])`, the actor will merge this list with the list of tasks already being held in memory and hold them.
* Tasks which are not yet due are held in memory until they are due. 
* Due tasks are dispatched to the **PartitionExecutor** via `ExecutePartitionTask(Task)`
* Upon receiving a `FetchInProgressTaskCount`  message from **ThroughputController**, it replies back with the number of pending tasks in its memory via `InProgressTaskCountFetched(Int)`

##### ThroughputController
* Controls loading of the schedule from the database. If there are not a sufficient number of tasks in flight, the **ThroughputController** will send `LoadTasks` to the **TaskPersistence** actor to load more tasks from Cassandra
* Sends `FetchInProgressTaskCount` to each actor which can have in-progress tasks and receives an `InProgressTaskCountFetched(Int)` which contains the number of tasks in progress by that actor.

##### PartitionExecutor
* Manages the execution of tasks for a partition. It spawns an **OrderingExecutor** for each unique **orderingId**
* It forwards tasks received from the **PartitionScheduler** through the message `ExecutePartitionTask(Task)` to the **OrderingExecutor** with the relevant **orderingId**
* Upon receiving a `FetchInProgressTaskCount` from the **ThroughputController**, it replies back with the number of in-flight tasks via`InProgressTaskCountFetched(Int)`

##### OrderingExecutor
* Manages a task queue for a given **orderingId**
* Spawns a new **TaskExecutor** for each task, and waits for it to complete before attempting to create a **TaskExecutor** for the next task in queue
* Receives a task to execute from the **PartitionExecutor** through `ExecuteEntityTask(Task)`.
* Receivies a `TaskExecuted(Taskkey)` from a **TaskExecutor** when the executor has finished running the task.

##### TaskExecutor
* It is a short-lived actor that manages the execution of a task and reports it as either complete or incomplete
* Upon initialization, it checks with the **TaskCompletionTracker** to see if the task is already marked as complete by sending the message `FetchTaskCompletion(TaskKey)`
* It receives a `TaskCompletionFetched(TaskKey, Boolean)` from the **TaskCompletionTracker**. If the task has already been marked as completed, it will simply remove the task from the **ScheduledColumnFamily** by sending the message `RemoveTask(TaskKey)`. The **TaskExecutor** will also reply back to the **OrderingExecutor** with a `TaskExecuted(TaskKey)` and will stop itself.
* If the task is not already marked as completed, the **TaskExecutor** will send the task to the **TaskExecutorService** to execute and wait for its completion.
* Upon successful completion of a task, it will send a `MarkTaskComplete(TaskKey)` to the **TaskCompletionTracker**, remove the task from the **ScheduledColumnFamily**, report back to the **OrderingExecutor** with a `TaskExecuted(TaskKey)` and stop itself.
* Upon failure, the task will be retried after a user-specified retry period and will continue to be retried indefinitely until success.

##### TaskCompletionTracker
* Manages and tracks whether or not the tasks for a given partition have been completed or not.
* When it receives a `FetchTaskCompletion(TaskKey)`, the actor checks Cassandra to see whether or not the task is complete. If the query is successful, it replies back to the sender with a `TaskCompletionFetched(TaskKey, Boolean)`. If an error occurs while querying Cassandra, it replies back to the sender with a `TaskCompletionNotFetched(TaskKey, Throwable)`.
* Upon receiving a `MarkTaskComplete(TaskKey)`, it will store the task's completion status in Cassandra and reply back to the sender with either a `TaskMarkedComplete(TaskKey)` if successful or a `TaskNotMarkedComplete(TaskKey, Throwable)` upon error or failure.

## Design Principles
 - Single responsibility principle (do one thing, do it well)
 - Simplicity (keep it simple)

## Design Challenges
 - We need push for tasks in the past, and pull for tasks in the future
 - System has to handle large backlog gracefully (example: resuming after being down for a long
 time)
 - System should not lock up because of a stuck old task (but we wont optimize for this behaviour)
