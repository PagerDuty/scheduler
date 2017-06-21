package com.pagerduty.scheduler.admin

import com.pagerduty.scheduler.admin.model.{AdminTask, TaskDetails}
import com.pagerduty.scheduler.dao.{AttemptHistoryDao, TaskScheduleDao, TaskStatusDao}
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{Task, TaskKey}
import java.time.Instant
import scala.collection.immutable.SortedMap
import scala.concurrent.Future

trait AdminService {
  def dropTask(key: TaskKey): Future[Unit]

  def fetchTaskWithDetails(key: TaskKey, attemptHistoryLimit: Int = 20): Future[AdminTask]

  def fetchIncompleteTasks(
      from: Instant,
      fromOrderingId: Option[Task.OrderingId],
      fromUniquenessKey: Option[Task.UniquenessKey],
      to: Instant,
      limit: Int
    ): Future[Seq[AdminTask]]
}

class AdminServiceImpl(
    taskScheduleDao: TaskScheduleDao,
    taskStatusDao: TaskStatusDao,
    attemptHistoryDao: AttemptHistoryDao,
    triggerRebalancing: () => Unit,
    numPartitions: () => Option[Int])
    extends AdminService {

  import scala.concurrent.ExecutionContext.Implicits.global

  def dropTask(key: TaskKey): Future[Unit] = {
    dropTasks(Set(key))
  }

  /**
    * The primary purpose of this method is to allow dropping selected task keys from an admin API
    * in an emergency.
    * The Scheduler is not design to allow dropping tasks, so this operation is very inefficient and
    * should never be used as part of normal system workflow. This method works by marking all the
    * given keys as dropped on all partitions and then trigger cluster-wide rebalancing.
    *
    * @param taskKeys
    */
  def dropTasks(taskKeys: Set[TaskKey]): Future[Unit] = {
    val dropFutures = taskKeys.map { taskKey =>
      val partitionIds = Set(taskKey.partitionId(numPartitions().get))
      taskStatusDao.dropTaskOnPartitions(partitionIds, taskKey)
    }
    val dropFuture = Future.sequence(dropFutures)
    dropFuture.map { _ =>
      triggerRebalancing()
    }
  }

  def fetchTaskWithDetails(key: TaskKey, attemptLimit: Int): Future[AdminTask] = {
    val partitionId = key.partitionId(numPartitions().get)

    // execute the DAO calls in parallel
    val futTask = taskScheduleDao.find(partitionId, key)
    val futTaskStatus = taskStatusDao.getStatus(partitionId, key)
    val futAttempts = attemptHistoryDao.load(partitionId, key, attemptLimit)

    for {
      optTask <- futTask
      taskStatus <- futTaskStatus
      attempts <- futAttempts
    } yield {
      AdminTask(
        Some(key),
        Some(partitionId),
        optTask map (_.taskData),
        Some(taskStatus.numberOfAttempts),
        Some(taskStatus.completionResult),
        Some(TaskDetails(attempts.toList))
      )
    }
  }

  def fetchIncompleteTasks(
      from: Instant,
      fromOrderingId: Option[Task.OrderingId],
      fromUniquenessKey: Option[Task.UniquenessKey],
      to: Instant,
      limit: Int
    ): Future[Seq[AdminTask]] = {

    assertTimeWindowReasonable(from, to)

    val partitions = (0 until numPartitions().get).toSet

    // this could give numPartitions * limit tasks, very inefficient for now
    val futTasks = taskScheduleDao.loadTasksFromPartitions(
      partitions,
      from,
      fromOrderingId,
      fromUniquenessKey,
      to,
      limit
    )

    // first, reduce the tasks to a Seq of AdminTasks of size `limit`
    val futAdminTasks = tasksToLimitedAdminTasks(futTasks, limit)

    // then, get the status for each of the AdminTasks that made it in under the limit
    adminTasksWithStatus(futAdminTasks)
  }

  private def tasksToLimitedAdminTasks(
      futTasks: Future[Map[PartitionId, Seq[Task]]],
      limit: Int
    ): Future[Seq[AdminTask]] = {
    futTasks map { taskMap =>
      val adminTasksMap = taskMap.foldLeft(SortedMap[TaskKey, AdminTask]()) {
        case (result, (partitionId, tasks)) =>
          val adminTasks = tasks map { t =>
            val adminTask = AdminTask(t.taskKey, partitionId, t.taskData)

            (t.taskKey, adminTask)
          }

          result ++ adminTasks
      }

      adminTasksMap.take(limit).values.toSeq
    }
  }

  private def adminTasksWithStatus(futAdminTasks: Future[Seq[AdminTask]]): Future[Seq[AdminTask]] = {
    futAdminTasks flatMap { adminTasks =>
      // get the status for each admin task _IN SERIES_ so we don't overwhelm Cassandra
      adminTasks.foldLeft(Future.successful[List[AdminTask]](Nil)) { (futAdminTasks, adminTask) =>
        futAdminTasks flatMap { adminTasks =>
          val futStatus = taskStatusDao.getStatus(adminTask.partitionId.get, adminTask.key.get)

          futStatus map { status =>
            adminTask.copy(
              numberOfAttempts = Some(status.numberOfAttempts),
              status = Some(status.completionResult)
            )
          } map (_ :: adminTasks)
        }
      } map (_.reverse)
    }
  }

  private def assertTimeWindowReasonable(from: Instant, to: Instant) = {
    if (java.time.Duration.between(from, to).toScalaDuration.toHours > 8) {
      throw new IllegalArgumentException(
        "You can only query over 8 hours in order to protect Cassandra from too many reads"
      )
    }
  }
}
