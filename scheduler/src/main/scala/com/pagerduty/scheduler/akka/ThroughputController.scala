package com.pagerduty.scheduler.akka

import akka.actor._
import com.pagerduty.scheduler._
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task.PartitionId
import java.time.Instant
import scala.concurrent.duration._

object ThroughputController {

  /**
    * Delayed initialization. All the other messages are stashed until this message is received.
    * @param taskPersistence
    * @param inProgressTaskOwners
    */
  case class Initialize(taskPersistence: ActorRef, inProgressTaskOwners: Set[ActorRef])

  /**
    * Request sent to actors with in-progress tasks.
    */
  case object FetchInProgressTaskCount

  /**
    * Reply with the in-progress task count.
    * @param taskCount
    */
  case class InProgressTaskCountFetched(taskCount: Int)

  sealed trait State
  case object Uninitialized extends State
  case object AwaitingInProgressCounts extends State
  case object AwaitingTasksLoaded extends State
  case object AwaitingNextTick extends State

  sealed trait Data
  case class StatusResults(awaitingReplies: Set[ActorRef], inProgressTaskCount: Int) extends Data
  case object NoData extends Data

  def props(partitionId: PartitionId, settings: Settings, logging: Scheduler.Logging): Props = {
    Props(new ThroughputController(partitionId, settings, logging))
  }
}

/**
  * ThroughputController drives loading of the schedule from the database. It will do the
  * book-keeping for when to issue the next `TaskPersistence.LoadTasks`. Before issuing a next
  * load request, it will first query the rest of the system to get a count of in-progress
  * tasks. If there are too many tasks in progress, the ThroughputController will wait for the
  * system to catch up, checking the state periodically.
  *
  * All Actors that may have in-progress tasks for the given TaskPersistence should be passed
  * in inProgressTaskOwners, and should reply to the FetchInProgressTaskCount message.
  */
class ThroughputController(partitionId: PartitionId, settings: Settings, logging: Scheduler.Logging)
    extends ExtendedLoggingFSM[ThroughputController.State, ThroughputController.Data]
    with Stash {
  import ThroughputController._

  val maxInProgressTasks = settings.maxInFlightTasks
  val batchSize = settings.taskFetchBatchSize
  val minTickDelay = settings.minTickDelay
  val maxLookAhead = settings.maxLookAhead
  val prefetchWindow = settings.prefetchWindow

  val timerName = "ThroughputControllerTimer"
  var taskPersistence: ActorRef = _
  var inProgressTaskOwners: Set[ActorRef] = _
  override val supervisorStrategy = Supervision.AlwaysEscalateStrategy

  startWith(Uninitialized, NoData)

  when(Uninitialized) {
    case Event(init: Initialize, _) => {
      taskPersistence = init.taskPersistence
      inProgressTaskOwners = init.inProgressTaskOwners
      requestInProgressTaskCount()
    }
    case Event(_, _) => {
      stash()
      stay()
    }
  }

  when(AwaitingInProgressCounts) {
    case Event(
        InProgressTaskCountFetched(taskCount),
        data: StatusResults
        ) if data.awaitingReplies.contains(sender) => {
      val remainingReplies = data.awaitingReplies - sender
      val totalCount = data.inProgressTaskCount + taskCount

      if (remainingReplies.isEmpty) {
        loadMoreTasksOrWait(totalCount)
      } else {
        stay() using StatusResults(remainingReplies, totalCount)
      }
    }
  }

  when(AwaitingTasksLoaded) {
    case Event(results: TaskPersistence.TasksLoaded, _) => {
      val nextFetchTime = results.readCheckpointTime - prefetchWindow
      val nextFetchDelay = java.time.Duration.between(Instant.now(), nextFetchTime).toScalaDuration
      val tickDelay = minTickDelay.max(nextFetchDelay)
      waitForNextTick(tickDelay)
    }
    case Event(TaskPersistence.TasksNotLoaded(throwable), _) => {
      throw throwable
    }
  }

  when(AwaitingNextTick) {
    case Event(StateTimeout, _) => {
      requestInProgressTaskCount()
    }
  }

  onTransition {
    case Uninitialized -> _ => unstashAll()
  }

  def requestInProgressTaskCount() = {
    inProgressTaskOwners.foreach(_ ! FetchInProgressTaskCount)
    val data = StatusResults(
      awaitingReplies = inProgressTaskOwners,
      inProgressTaskCount = 0
    )
    goto(AwaitingInProgressCounts) using data
  }

  def loadMoreTasksOrWait(inProgressTaskCount: Int) = {
    if (inProgressTaskCount >= maxInProgressTasks) {
      waitForNextTick(minTickDelay)
    } else {
      loadMoreTasks(batchSize)
    }
  }

  def waitForNextTick(tickDelay: FiniteDuration) = {
    setTimer(timerName, StateTimeout, tickDelay)
    goto(AwaitingNextTick) using NoData
  }

  def loadMoreTasks(batchSize: Int) = {
    val upperBound = Instant.now() + maxLookAhead
    taskPersistence ! TaskPersistence.LoadTasks(upperBound, batchSize)
    goto(AwaitingTasksLoaded) using NoData
  }
}
