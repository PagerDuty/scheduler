package com.pagerduty.scheduler.admin

import com.pagerduty.scheduler.admin.model.{AdminTask, TaskDetails}
import com.pagerduty.scheduler.dao.{AttemptHistoryDao, TaskScheduleDao, TaskStatusDao}
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task._
import com.pagerduty.scheduler.model.{CompletionResult, Task, TaskKey, TaskStatus}
import com.pagerduty.scheduler.specutil.{FreeUnitSpec, TaskFactory}
import java.time.Instant
import java.util.NoSuchElementException
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.duration._
import scala.concurrent.Future

class AdminServiceSpec extends FreeUnitSpec with ScalaFutures with MockFactory {

  import scala.concurrent.ExecutionContext.Implicits.global

  trait AdminTestContext {
    val mockRebalanceFunction = stubFunction[Unit]
    val mockTaskScheduleDao = stub[TaskScheduleDao]
    val mockAttemptHistoryDao = stub[AttemptHistoryDao]
    val mockTaskStatusDao = stub[TaskStatusDao]
    val kafkaNumPartitions = 10
    val kafkaPartitions = (0 until kafkaNumPartitions).toSet

    val adminService =
      new AdminServiceImpl(
        mockTaskScheduleDao,
        mockTaskStatusDao,
        mockAttemptHistoryDao,
        mockRebalanceFunction,
        () => { Some(kafkaNumPartitions) }
      )
  }

  "AdminService" - {
    "should drop tasks and trigger rebalancing" in new AdminTestContext {
      val taskKeys = TaskFactory.makeTasks(2).map(_.taskKey).toSet

      val dropSuccess: Future[Unit] = Future.successful(Unit)
      for (taskKey <- taskKeys) {
        val partitionIds = Set(taskKey.partitionId(kafkaNumPartitions))
        (mockTaskStatusDao
          .dropTaskOnPartitions(_, _))
          .when(partitionIds, taskKey)
          .returns(dropSuccess)
      }

      adminService.dropTasks(taskKeys).futureValue
      for (taskKey <- taskKeys) {
        val partitionIds = Set(taskKey.partitionId(kafkaNumPartitions))
        (mockTaskStatusDao
          .dropTaskOnPartitions(_, _))
          .verify(partitionIds, taskKey)
      }
      mockRebalanceFunction.verify()
    }

    "should fetch a task with details" - {

      trait FetchTaskTestContext extends AdminTestContext {
        val task = TaskFactory.makeTask()
        val partitionId = task.partitionId(kafkaNumPartitions)
        val limit = 10
      }

      "when all of the DAO calls succeed" - {
        trait DaoCallsSucceedTestContext extends FetchTaskTestContext {
          val taskStatus = TaskStatus(1, CompletionResult.Success, None)
          val attempts = Seq()
          (mockTaskStatusDao
            .getStatus(_, _))
            .when(partitionId, task.taskKey)
            .returns(Future.successful(taskStatus))
          (mockAttemptHistoryDao
            .load(_, _, _))
            .when(partitionId, task.taskKey, limit)
            .returns(Future.successful(attempts))
        }

        "and the task exists in the schedule" in new DaoCallsSucceedTestContext {
          (mockTaskScheduleDao
            .find(_, _))
            .when(partitionId, task.taskKey)
            .returns(Future.successful(Some(task)))

          val result = adminService.fetchTaskWithDetails(task.taskKey, limit).futureValue

          val expectedResult = AdminTask(
            Some(task.taskKey),
            Some(partitionId),
            Some(task.taskData),
            Some(taskStatus.numberOfAttempts),
            Some(taskStatus.completionResult),
            Some(TaskDetails(attempts.toList))
          )

          result should equal(expectedResult)
        }

        "and the task doesn't exist in the schedule" in new DaoCallsSucceedTestContext {
          (mockTaskScheduleDao
            .find(_, _))
            .when(partitionId, task.taskKey)
            .returns(Future.successful(None))

          val result = adminService.fetchTaskWithDetails(task.taskKey, limit).futureValue

          val expectedResult = AdminTask(
            Some(task.taskKey),
            Some(partitionId),
            None,
            Some(taskStatus.numberOfAttempts),
            Some(taskStatus.completionResult),
            Some(TaskDetails(attempts.toList))
          )

          result should equal(expectedResult)
        }
      }

      "when one of the DAO calls fails" - {

        trait OneDaoCallFailsTestContext extends FetchTaskTestContext {
          val taskStatus = TaskStatus(1, CompletionResult.Success, None)
          val attempts = Seq()
          def getStatusSucceeds() {
            (mockTaskStatusDao
              .getStatus(_, _))
              .when(partitionId, task.taskKey)
              .returns(Future.successful(taskStatus))
          }
          def loadSucceeds() {
            (mockAttemptHistoryDao
              .load(_, _, _))
              .when(partitionId, task.taskKey, limit)
              .returns(Future.successful(attempts))
          }
          def findSucceeds() {
            (mockTaskScheduleDao
              .find(_, _))
              .when(partitionId, task.taskKey)
              .returns(Future.successful(Some(task)))
          }
        }

        "and it's the TaskScheduleDao call" in new OneDaoCallFailsTestContext {
          getStatusSucceeds()
          loadSucceeds()
          (mockTaskScheduleDao
            .find(_, _))
            .when(partitionId, task.taskKey)
            .returns(Future.failed(new IllegalArgumentException("test exception")))

          val f = adminService.fetchTaskWithDetails(task.taskKey, limit)

          whenReady(f.failed) { e =>
            e shouldBe a[IllegalArgumentException]
          }
        }

        "and it's the TaskStatusDao call" in new OneDaoCallFailsTestContext {
          (mockTaskStatusDao
            .getStatus(_, _))
            .when(partitionId, task.taskKey)
            .returns(Future.failed(new IllegalArgumentException("test exception")))
          loadSucceeds()
          findSucceeds()

          val f = adminService.fetchTaskWithDetails(task.taskKey, limit)

          whenReady(f.failed) { e =>
            e shouldBe a[IllegalArgumentException]
          }
        }

        "and it's the AttemptHistoryDao call" in new OneDaoCallFailsTestContext {
          getStatusSucceeds()
          (mockAttemptHistoryDao
            .load(_, _, _))
            .when(partitionId, task.taskKey, limit)
            .returns(Future.failed(new IllegalArgumentException("test exception")))
          findSucceeds()

          val f = adminService.fetchTaskWithDetails(task.taskKey, limit)

          whenReady(f.failed) { e =>
            e shouldBe a[IllegalArgumentException]
          }
        }

      }

    }

    "should fetch incomplete tasks" - {

      "when the load tasks DAO call fails" in new AdminTestContext {
        val fromTask = TaskFactory.makeTask()
        val from = fromTask.taskKey.scheduledTime
        val fromOrderingId = fromTask.taskKey.orderingId
        val fromUniquenessKey = fromTask.taskKey.uniquenessKey
        val to = from + 1.minute

        val limit = 12345
        (mockTaskScheduleDao
          .loadTasksFromPartitions(
            _: Set[PartitionId],
            _: Instant,
            _: Option[Task.OrderingId],
            _: Option[Task.UniquenessKey],
            _: Instant,
            _: Int
          ))
          .when(
            kafkaPartitions,
            from,
            Some(fromOrderingId),
            Some(fromUniquenessKey),
            to,
            limit
          )
          .returns(
            Future.failed(new IllegalArgumentException("test exception: load tasks failed"))
          )

        val f = adminService.fetchIncompleteTasks(
          from,
          Some(fromOrderingId),
          Some(fromUniquenessKey),
          to,
          limit
        )

        whenReady(f.failed) { e =>
          e shouldBe an[IllegalArgumentException]
        }
      }

      "when the load tasks DAO call succeeds" - {

        trait FetchIncompleteTasksWhenDaoSucceedsTestContext extends AdminTestContext {
          val task1 = TaskFactory.makeTask()
          val task1PartitionId = task1.partitionId(kafkaNumPartitions)

          val task2 = task1.copy(scheduledTime = task1.scheduledTime + 1.second)
          val task2PartitionId = task2.partitionId(kafkaNumPartitions)

          val from = task1.scheduledTime
          val fromOrderingId = task1.orderingId
          val fromUniquenessKey = task1.uniquenessKey
          val to = from + 1.minute
          val limit = 2

          task1PartitionId should equal(task2PartitionId)

          val task3 = TaskFactory.makeTask(scheduledTime = task1.scheduledTime - 1.second)
          val task3PartitionId = task3.partitionId(kafkaNumPartitions)

          val taskMap = Map(
            task1PartitionId -> IndexedSeq(task1, task2),
            task3PartitionId -> IndexedSeq(task3)
          )

          (mockTaskScheduleDao
            .loadTasksFromPartitions(
              _: Set[PartitionId],
              _: Instant,
              _: Option[Task.OrderingId],
              _: Option[Task.UniquenessKey],
              _: Instant,
              _: Int
            ))
            .when(
              kafkaPartitions,
              from,
              Some(fromOrderingId),
              Some(fromUniquenessKey),
              to,
              limit
            )
            .returns(Future.successful(taskMap))
        }

        "and all of the TaskStatus DAO calls succeed" in new FetchIncompleteTasksWhenDaoSucceedsTestContext {
          val taskStatus1 = TaskStatus(1, CompletionResult.Success, None)
          val taskStatus2 = TaskStatus(2, CompletionResult.Failure, Some(task2.scheduledTime + 1.minute))
          val taskStatus3 = TaskStatus(3, CompletionResult.Dropped, None)

          (mockTaskStatusDao
            .getStatus(_, _))
            .when(task1PartitionId, task1.taskKey)
            .returns(Future.successful(taskStatus1))
          (mockTaskStatusDao
            .getStatus(_, _))
            .when(task2PartitionId, task2.taskKey)
            .returns(Future.successful(taskStatus2))
          (mockTaskStatusDao
            .getStatus(_, _))
            .when(task3PartitionId, task3.taskKey)
            .returns(Future.successful(taskStatus3))

          val result = adminService
            .fetchIncompleteTasks(
              from,
              Some(fromOrderingId),
              Some(fromUniquenessKey),
              to,
              limit
            )
            .futureValue

          result should have size (limit)
          val result0 = result(0)
          val result1 = result(1)

          val expectedResult0 = AdminTask(
            Some(task3.taskKey),
            Some(task3PartitionId),
            Some(task3.taskData),
            Some(taskStatus3.numberOfAttempts),
            Some(taskStatus3.completionResult),
            None
          )

          val expectedResult1 = AdminTask(
            Some(task1.taskKey),
            Some(task1PartitionId),
            Some(task1.taskData),
            Some(taskStatus1.numberOfAttempts),
            Some(taskStatus1.completionResult),
            None
          )

          result0 should equal(expectedResult0)
          result1 should equal(expectedResult1)
        }

        "and one of the TaskStatusDao calls fails" in
          new FetchIncompleteTasksWhenDaoSucceedsTestContext {
            val taskStatus1 = TaskStatus(1, CompletionResult.Success, None)
            val taskStatus2 = TaskStatus(2, CompletionResult.Failure, Some(task2.scheduledTime + 1.minute))

            (mockTaskStatusDao
              .getStatus(_, _))
              .when(task1PartitionId, task1.taskKey)
              .returns(Future.successful(taskStatus1))
            (mockTaskStatusDao
              .getStatus(_, _))
              .when(task2PartitionId, task2.taskKey)
              .returns(Future.successful(taskStatus2))
            (mockTaskStatusDao
              .getStatus(_, _))
              .when(task3PartitionId, task3.taskKey)
              .returns(Future.failed(new NumberFormatException("test exception")))

            val f = adminService.fetchIncompleteTasks(
              from,
              Some(fromOrderingId),
              Some(fromUniquenessKey),
              to,
              limit
            )

            whenReady(f.failed) { e =>
              e shouldBe an[NumberFormatException]
            }
          }
      }

    }
  }

}
