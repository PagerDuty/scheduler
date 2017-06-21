package com.pagerduty.scheduler.akka

import akka.actor._
import com.pagerduty.scheduler._
import com.pagerduty.scheduler.akka.PartitionExecutor.ExecutePartitionTask
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{Task, TaskKey}
import java.time.Instant
import scala.collection.immutable.SortedMap
import scala.concurrent.duration._

object PartitionScheduler {
  case class ScheduleTasks(tasks: Seq[Task])

  private case object TaskIsDue

  def props(partitionId: PartitionId, partitionExecutor: ActorRef, logging: Scheduler.Logging): Props = {
    Props(new PartitionScheduler(partitionId, partitionExecutor, logging))
  }
}

/**
  * Holds scheduled tasks in memory till they are due and then sends them
  * to the given ParitionExecutor.
  *
  * Tasks should be sent in a ScheduleTasks message. The tasks in this
  * message must be in scheduledTime order i.e. the one due the earliest
  * should be the first in the tasks Seq.
  *
  * Due and overdue tasks will be dispatched immediately.
  */
class PartitionScheduler(partitionId: PartitionId, partitionExecutor: ActorRef, logging: Scheduler.Logging)
    extends Actor
    with ActorLogging {
  import PartitionScheduler._

  var pendingTasks: SortedMap[TaskKey, Task] = SortedMap.empty
  override val supervisorStrategy = Supervision.AlwaysEscalateStrategy
  var nextPendingTaskTimer: Option[Cancellable] = None

  override def postStop(): Unit = {
    cancelPendingTimer()
  }

  def receive = {
    case ScheduleTasks(tasks) => {
      val newTasks = tasks.map(task => task.taskKey -> task)
      pendingTasks = pendingTasks ++ newTasks
      sendEligiblePendingTasks()
    }
    case TaskIsDue => {
      sendEligiblePendingTasks()
    }
    case ThroughputController.FetchInProgressTaskCount => {
      sender ! ThroughputController.InProgressTaskCountFetched(pendingTasks.size)
    }
  }

  def sendEligiblePendingTasks(): Unit = {
    val now = Instant.now()
    val (readyTasks, remainingPendingTasks) = pendingTasks.partition {
      case (taskKey, task) => taskKey.scheduledTime.compareTo(now) <= 0
    }
    pendingTasks = remainingPendingTasks
    logging.reportInMemoryTaskCount(partitionId, "partitionScheduler", pendingTasks.size)
    val nextTaskOption = pendingTasks.headOption.map { case (taskKey, task) => task }
    updatePendingTaskTimer(nextTaskOption)
    sendTasks(readyTasks.values)
  }

  def sendTasks(readyTasks: Iterable[Task]): Unit = {
    for (task <- readyTasks) {
      partitionExecutor ! ExecutePartitionTask(task)
    }
  }

  def updatePendingTaskTimer(nextTaskToRun: Option[Task]): Unit = {
    cancelPendingTimer()
    nextTaskToRun.foreach { task =>
      val delayToNextTask = java.time.Duration.between(Instant.now(), task.scheduledTime).toScalaDuration
      val effectiveTimerDelay = delayToNextTask.max(Duration.Zero)
      setPendingTaskTimer(effectiveTimerDelay)
    }
  }

  def cancelPendingTimer(): Unit = {
    nextPendingTaskTimer.foreach(_.cancel)
    nextPendingTaskTimer = None
  }

  def setPendingTaskTimer(delay: FiniteDuration) {
    cancelPendingTimer()
    import context.dispatcher
    val timer = context.system.scheduler.scheduleOnce(delay, self, TaskIsDue)
    nextPendingTaskTimer = Some(timer)
  }
}
