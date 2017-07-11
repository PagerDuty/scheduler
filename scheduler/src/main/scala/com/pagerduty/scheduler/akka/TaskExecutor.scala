package com.pagerduty.scheduler.akka

import akka.actor.{ActorRef, Props}
import com.pagerduty.scheduler.akka.TaskStatusTracker._
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model._
import java.time.Instant
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * The TaskExecutor is a short-lived actor that handles task execution by delegating tasks to a
  * TaskExecutorService.
  *
  * It is initialized with a task to execute, and a reference to the requesting actor. Then, it:
  *  - Checks with the TaskStatusTracker if the task is already completed
  *  - If the task is already completed, it deletes the task from TaskPersistence and reports
  *    success
  *  - If the task is not yet completed, it is submitted to the TaskExecutorService
  *  - If the task succeeds, the TaskExecutor marks it as complete using the TaskStatusTracker
  *  - If marking as complete succeeds, it deletes the Task from TaskPersistence and reports success
  *    to the requester
  *  - If the task fails, it is retried until the max number of retries is reached then is marked as
  *    failed by the TaskStatusTracker
  *  - If the marking as failed succeeds, it deletes the Task from TaskPersistence and reports
  *    success to the requester
  *  - If marking as complete or as failed fails, it crashes
  *
  *  Task attempt count is persisted after every attempt.
  */
object TaskExecutor {
  private case object ExecutionThreadSuccess
  private case class ExecutionThreadFailed(throwable: Throwable)
  private case class AttemptTask(attemptNumber: Int, attemptAt: Instant)

  sealed trait State
  case object CheckingTaskStatus extends State
  case object ExecutingTask extends State
  case object AwaitingTaskAttempt extends State
  case object UpdatingTaskStatus extends State
  case object RewritingTaskStatus extends State

  sealed trait Data
  case object NoData extends Data
  case class ExecutingData(attemptNumber: Int, startedAt: Instant) extends Data
  case class AttemptData(startedAt: Instant, finishedAt: Instant, exception: Option[Throwable]) extends Data

  def props(settings: Settings, partitionContext: PartitionContext, task: Task, requester: ActorRef): Props = {
    Props(
      new TaskExecutor(
        settings,
        partitionContext,
        task,
        requester
      )
    )
  }

  private[akka] def jitteredWaitForRetry(
      nextAttemptNumber: Int,
      settings: Settings,
      randomDouble: () => Double
    ): FiniteDuration = {
    val backoffFactorMs = settings.exponentialBackoffFactor.toMillis
    val maxBackoffTimeMs = settings.maxTaskBackoffPeriod.toMillis.toDouble
    val tempWaitDurationMs = maxBackoffTimeMs.min(backoffFactorMs * math.pow(2, nextAttemptNumber - 2))
    (tempWaitDurationMs / 2 + tempWaitDurationMs * randomDouble() / 2).millis
  }
}

class TaskExecutor(settings: Settings, partitionContext: PartitionContext, task: Task, requester: ActorRef)
    extends ExtendedLoggingFSM[TaskExecutor.State, TaskExecutor.Data] {
  import TaskExecutor._
  import context.dispatcher
  override val supervisorStrategy = Supervision.AlwaysEscalateStrategy

  val maxAttempts = settings.maxTaskRetries + 1
  val partitionId = partitionContext.partitionId
  val taskStatusTracker = partitionContext.taskStatusTracker
  val taskPersistence = partitionContext.taskPersistence
  val taskExecutorService = partitionContext.taskExecutorService
  val logging = partitionContext.logging

  startWith(CheckingTaskStatus, NoData)
  taskStatusTracker ! FetchTaskStatus(task.taskKey)

  when(CheckingTaskStatus) {
    case Event(TaskStatusFetched(taskKey, taskStatus), _) if taskKey == task.taskKey => {
      if (taskStatus.isComplete) {
        rewriteTaskStatus(taskStatus)
      } else {
        val attemptNumber = taskStatus.numberOfAttempts + 1
        nextAttempt(attemptNumber, taskStatus.nextAttemptAt)
      }
    }

    case Event(TaskStatusNotFetched(taskKey, throwable), _) if taskKey == task.taskKey => {
      logging.reportTaskStatusNotFetched(taskKey, throwable)
      throw throwable
    }
  }

  when(ExecutingTask) {
    case Event(ExecutionThreadSuccess, data: ExecutingData) => {
      updateTaskAsSuccessful(data)
    }

    case Event(ExecutionThreadFailed(throwable), data: ExecutingData) => {
      if (data.attemptNumber < maxAttempts) {
        updateTaskStatusThenRetry(data, throwable)
      } else updateTaskAsFailed(data, throwable)
    }
  }

  when(UpdatingTaskStatus) {
    case Event(TaskStatusUpdated(taskKey, taskStatus), data: AttemptData) if taskKey == task.taskKey => {
      val taskAttempt = makeTaskAttempt(data, taskStatus)
      logging.reportTaskAttemptFinished(partitionId, task, taskAttempt)

      if (taskStatus.isComplete) removeTaskAndReportResultsThenStop()
      else nextAttempt(attemptNumber = taskStatus.numberOfAttempts + 1, taskStatus.nextAttemptAt)
    }

    case Event(TaskStatusNotUpdated(taskKey, taskStatus, throwable), _) if taskKey == task.taskKey => {
      handleTaskStatusNotUpdated(taskKey, taskStatus, throwable)
    }
  }

  when(RewritingTaskStatus) {
    case Event(TaskStatusUpdated(taskKey, taskStatus), _) if taskKey == task.taskKey && taskStatus.isComplete => {
      removeTaskAndReportResultsThenStop()
    }

    case Event(TaskStatusNotUpdated(taskKey, taskStatus, throwable), _) if taskKey == task.taskKey => {
      handleTaskStatusNotUpdated(taskKey, taskStatus, throwable)
    }
  }

  when(AwaitingTaskAttempt) {
    case Event(AttemptTask(attemptNumber, attemptAt), _) => {
      executeTask(attemptNumber, attemptAt)
    }
  }

  private def removeTaskAndReportResultsThenStop(): State = {
    taskPersistence ! TaskPersistence.RemoveTask(task.taskKey)
    requester ! OrderingExecutor.TaskExecuted(task.taskKey)
    stop()
  }

  private def nextAttempt(attemptNumber: Int, nextAttemptAt: Option[Instant]): State = {
    val attemptAt = nextAttemptAt.getOrElse(task.scheduledTime)
    val attemptDelay = java.time.Duration.between(Instant.now(), attemptAt).toScalaDuration.max(0.seconds)
    context.system.scheduler.scheduleOnce(attemptDelay, self, AttemptTask(attemptNumber, attemptAt))
    goto(AwaitingTaskAttempt) using NoData
  }

  private def updateTaskAsSuccessful(data: ExecutingData): State = {
    val taskStatus = TaskStatus(data.attemptNumber, CompletionResult.Success, nextAttemptAt = None)
    val attemptData = AttemptData(data.startedAt, finishedAt = Instant.now(), exception = None)
    updateTaskStatus(taskStatus, attemptData)
  }

  private def updateTaskStatusThenRetry(data: ExecutingData, exception: Throwable): State = {
    val finishedAt = Instant.now()
    val nextAttemptNumber = data.attemptNumber + 1
    val retryAt = finishedAt + jitteredWaitForRetry(nextAttemptNumber, settings, util.Random.nextDouble)
    val taskStatus = TaskStatus(data.attemptNumber, CompletionResult.Incomplete, Some(retryAt))
    val attemptData = AttemptData(data.startedAt, finishedAt, Some(exception))
    updateTaskStatus(taskStatus, attemptData)
  }

  private def updateTaskAsFailed(data: ExecutingData, exception: Throwable): State = {
    logging.reportMaxTaskRetriesReached(task, partitionId)
    val taskStatus = TaskStatus(data.attemptNumber, CompletionResult.Failure, nextAttemptAt = None)
    val attemptData = AttemptData(data.startedAt, finishedAt = Instant.now(), Some(exception))
    updateTaskStatus(taskStatus, attemptData)
  }

  private def updateTaskStatus(taskStatus: TaskStatus, attemptData: AttemptData): State = {
    taskStatusTracker ! UpdateTaskStatus(task.taskKey, taskStatus)
    goto(UpdatingTaskStatus) using attemptData
  }

  private def rewriteTaskStatus(taskStatus: TaskStatus): State = {
    taskStatusTracker ! UpdateTaskStatus(task.taskKey, taskStatus)
    goto(RewritingTaskStatus) using NoData
  }

  private def executeTask(attemptNumber: Int, attemptAt: Instant): State = {
    val startedAt = Instant.now()
    logging.reportTaskExecutionDelay(
      partitionId,
      task,
      java.time.Duration.between(attemptAt, startedAt).toScalaDuration
    )

    val taskExecutionFuture = taskExecutorService.execute(partitionId, task)
    taskExecutionFuture onComplete {
      case Success(_) => self ! ExecutionThreadSuccess
      case Failure(t) => self ! ExecutionThreadFailed(t)
    }
    logging.monitorTaskExecution(taskExecutionFuture, task)

    goto(ExecutingTask) using ExecutingData(attemptNumber, startedAt)
  }

  private def handleTaskStatusNotUpdated(taskKey: TaskKey, taskStatus: TaskStatus, t: Throwable): State = {
    logging.reportTaskStatusNotUpdated(taskKey, taskStatus, t)
    throw t
  }

  private def makeTaskAttempt(data: AttemptData, taskStatus: TaskStatus) = {
    TaskAttempt(
      attemptNumber = taskStatus.numberOfAttempts,
      data.startedAt,
      data.finishedAt,
      taskResult = taskStatus.completionResult,
      taskResultUpdatedAt = Instant.now(),
      data.exception
    )
  }
}
