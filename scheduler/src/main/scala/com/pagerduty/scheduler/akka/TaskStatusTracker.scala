package com.pagerduty.scheduler.akka

import scala.util.{Failure, Success}
import akka.actor.{Actor, ActorLogging, Props}
import com.pagerduty.scheduler.dao.TaskStatusDao
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{TaskKey, TaskStatus}

/**
  * The TaskStatusTracker actor tracks tasks status for a given partition.
  *
  * When it receives FetchTaskStatus, it queries the task status from the
  * appropriate Cassandra DAO, and replies with TaskStatus(Not)Fetched.
  *
  * When it receives UpdateTaskStatus, it uses the appropriate Cassandra DAO to store the task
  * status, and replies with TaskStatus(Not)Updated.
  */
object TaskStatusTracker {

  /**
    * A request to get task status from Cassandra.
    *
    * @param taskKey The key of the task for which status is needed.
    */
  case class FetchTaskStatus(taskKey: TaskKey) extends RetryableRequest

  /**
    * A response from TaskStatusTracker denoting a task's completion status.
    *
    * @param taskKey the key of the task which completed refers to
    * @param taskStatus task status
    */
  case class TaskStatusFetched(taskKey: TaskKey, taskStatus: TaskStatus) extends SuccessResponse

  /**
    * A response from TaskStatusTracker denoting a failure to get a task's completion status
    *
    * @param taskKey The key of the task which this response refers to
    * @param throwable The failure encountered when getting the task's completion status
    */
  case class TaskStatusNotFetched(taskKey: TaskKey, throwable: Throwable) extends FailureResponse

  /**
    * A request to update task status.
    *
    * @param taskKey key for task whose status should be updated
    * @param taskStatus the new task status
    */
  case class UpdateTaskStatus(taskKey: TaskKey, taskStatus: TaskStatus) extends RetryableRequest

  /**
    * A response from TaskStatusTracker denoting that a task has been marked as complete
    *
    * @param taskKey The key of the task which was marked as complete
    * @param taskStatus task status
    */
  case class TaskStatusUpdated(taskKey: TaskKey, taskStatus: TaskStatus) extends SuccessResponse

  /**
    * A response from TaskStatusTracker denoting a failure when marking a task as complete
    *
    * @param taskKey The key of the task which was not marked as complete
    * @param throwable The failure encountered when marking the task as complete
    */
  case class TaskStatusNotUpdated(taskKey: TaskKey, status: TaskStatus, throwable: Throwable) extends FailureResponse

  def props(partitionId: PartitionId, taskStatusDao: TaskStatusDao): Props = {
    Props(new TaskStatusTracker(partitionId, taskStatusDao))
  }
}

class TaskStatusTracker(partitionId: PartitionId, taskStatusDao: TaskStatusDao) extends Actor with ActorLogging {
  import TaskStatusTracker._
  import TaskExecutor._
  import context.dispatcher
  override val supervisorStrategy = Supervision.AlwaysEscalateStrategy

  def receive = {
    case FetchTaskStatus(taskKey) =>
      val requester = sender
      taskStatusDao.getStatus(partitionId, taskKey) onComplete {
        case Success(taskStatus) => requester ! TaskStatusFetched(taskKey, taskStatus)
        case Failure(e) => requester ! TaskStatusNotFetched(taskKey, e)
      }

    case UpdateTaskStatus(taskKey, taskStatus) =>
      val requester = sender
      taskStatusDao.insert(partitionId, taskKey, taskStatus) onComplete {
        case Success(_) => requester ! TaskStatusUpdated(taskKey, taskStatus)
        case Failure(e) => requester ! TaskStatusNotUpdated(taskKey, taskStatus, e)
      }
  }
}
