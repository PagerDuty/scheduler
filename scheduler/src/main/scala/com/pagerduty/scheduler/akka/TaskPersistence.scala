package com.pagerduty.scheduler.akka

import akka.actor._
import akka.pattern._
import com.pagerduty.scheduler._
import com.pagerduty.scheduler.dao.TaskScheduleDao
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{Task, TaskKey}
import java.time.Instant
import scala.concurrent.duration._

object TaskPersistence {

  /**
    * Durably store provided tasks. All the tasks passed here must belong to the same partition
    * as the TaskPersistence actor. The reply is either `TopicSupervisor.TasksPersisted` or
    * `TopicSupervisor.TasksNotPersisted`.
    * @param tasks
    */
  case class PersistTasks(tasks: Seq[Task]) extends RetryableRequest

  /**
    * A response sent after all the incoming tasks have been durably stored.
    *
    * @param partitionId
    */
  case class TasksPersisted(partitionId: PartitionId) extends SuccessResponse

  /**
    * A response sent if the incoming tasks have not been durably stored.
    *
    * @param throwable
    */
  case class TasksNotPersisted(throwable: Throwable) extends FailureResponse

  /**
    * Remove tasks with provided task keys. No reply will be sent to these messages.
    * @param taskKey
    */
  case class RemoveTask(taskKey: TaskKey)

  /**
    * A request to load more tasks, up to the limit or the upperBound, whichever hit first.
    * Successfully loaded tasks will be forwarded to PartitionScheduler, and the new reachCheckpoint
    * time-stamp will be sent to ThroughputController. A failure will be propagated to the
    * ThroughputController in a form of `ThroughputController.TasksNotLoaded` message.
    * @param upperBound
    * @param limit
    */
  case class LoadTasks(upperBound: Instant, limit: Int) extends RetryableRequest

  /**
    * A notification from TaskPersistence sent after successful fetch from the database.
    * @param readCheckpointTime
    */
  case class TasksLoaded(readCheckpointTime: Instant) extends SuccessResponse

  /**
    * A notification from TaskPersistence about a failed fetch attempt.
    * @param throwable
    */
  case class TasksNotLoaded(throwable: Throwable) extends FailureResponse

  private case class DaoReadSucceeded(result: Seq[Task])
  private case class DaoWriteSucceeded(tasks: Seq[Task])

  sealed trait State
  case object Ready extends State
  case object AwaitingDaoRead extends State
  case object AwaitingDaoWrite extends State

  sealed trait Data
  case class ReadCheckpoint(readCheckpoint: TaskKey) extends Data
  case class InFlightDaoRead(readCheckpoint: TaskKey, queryUpperBound: Instant, queryLimit: Int) extends Data
  case class InFlightDaoWrite(readCheckpoint: TaskKey, replyTo: ActorRef) extends Data

  def props(args: TaskPersistenceArgs): Props = Props(new TaskPersistence(args))
}

/**
  * Manages task persistence. The idea behind this actor is to load schedule data from persistent
  * store only once. This is achieved by maintaining a `readCheckpoint` for the last successful
  * read operation. All the incoming tasks are durable stored, then tasks with `taskKey` less than
  * the `readCheckpoint` are forwarded for scheduling immediately. This way, there is no need
  * to re-load the state from the database.
  * @param args
  */
class TaskPersistence(args: TaskPersistenceArgs)
    extends ExtendedLoggingFSM[TaskPersistence.State, TaskPersistence.Data]
    with Stash {
  import TaskPersistence._
  import PartitionScheduler._
  import context.dispatcher
  import args._
  override val supervisorStrategy = Supervision.AlwaysEscalateStrategy
  val lookBackOnRestart = settings.lookBackOnRestart

  startWith(Ready, ReadCheckpoint(getReadCheckpointOnRestart))

  when(Ready) {
    case Event(LoadTasks(upperBound, limit), data: ReadCheckpoint)
        if upperBound.compareTo(data.readCheckpoint.scheduledTime) > 0 => {
      taskScheduleDao
        .load(partitionId, data.readCheckpoint, upperBound, limit)
        .map(result => DaoReadSucceeded(result))
        .pipeTo(self)
      goto(AwaitingDaoRead) using InFlightDaoRead(data.readCheckpoint, upperBound, limit)
    }
    case Event(PersistTasks(tasks), data: ReadCheckpoint) => {
      taskScheduleDao
        .insert(partitionId, tasks)
        .map(_ => DaoWriteSucceeded(tasks))
        .pipeTo(self)
      goto(AwaitingDaoWrite) using InFlightDaoWrite(data.readCheckpoint, sender)
    }
  }

  when(AwaitingDaoRead) {
    case Event(DaoReadSucceeded(tasks), data: InFlightDaoRead) => {
      val readCheckpoint = {
        if (tasks.size >= data.queryLimit) tasks.last.taskKey
        else TaskKey.lowerBound(data.queryUpperBound)
      }
      throughputController ! TasksLoaded(readCheckpoint.scheduledTime)
      partitionScheduler ! ScheduleTasks(tasks)
      goto(Ready) using ReadCheckpoint(readCheckpoint)
    }
    case Event(Status.Failure(exception), data: InFlightDaoRead) => {
      throughputController ! TasksNotLoaded(exception)
      goto(Ready) using ReadCheckpoint(data.readCheckpoint)
    }
  }

  when(AwaitingDaoWrite) {
    case Event(DaoWriteSucceeded(tasks), data: InFlightDaoWrite) => {
      val passThroughTasks = tasks.filter(_.taskKey <= data.readCheckpoint)
      val sortedTasks = passThroughTasks.toSeq.sortBy(_.taskKey)
      if (sortedTasks.nonEmpty) partitionScheduler ! ScheduleTasks(sortedTasks)
      data.replyTo ! TasksPersisted(partitionId)
      goto(Ready) using ReadCheckpoint(data.readCheckpoint)
    }
    case Event(failure: Status.Failure, data: InFlightDaoWrite) => {
      data.replyTo ! TasksNotPersisted(failure.cause)
      goto(Ready) using ReadCheckpoint(data.readCheckpoint)
    }
  }

  val handleTaskRemoval: StateFunction = {
    case Event(RemoveTask(taskKey), _) => {
      taskScheduleDao.remove(partitionId, taskKey)
      stay()
    }
  }
  val stashReadWriteRequests: StateFunction = {
    case Event(_: LoadTasks | _: PersistTasks, _) => {
      stash()
      stay()
    }
  }

  when(Ready)(handleTaskRemoval)
  when(AwaitingDaoRead)(handleTaskRemoval orElse stashReadWriteRequests)
  when(AwaitingDaoWrite)(handleTaskRemoval orElse stashReadWriteRequests)

  onTransition {
    case AwaitingDaoRead -> Ready => unstashAll()
    case AwaitingDaoWrite -> Ready => unstashAll()
  }

  private def getReadCheckpointOnRestart: TaskKey = {
    val checkpointTime = Instant.now() - lookBackOnRestart
    TaskKey.lowerBound(checkpointTime)
  }
}

case class TaskPersistenceArgs(
    settings: Settings,
    partitionId: PartitionId,
    taskScheduleDao: TaskScheduleDao,
    partitionScheduler: ActorRef,
    throughputController: ActorRef)
