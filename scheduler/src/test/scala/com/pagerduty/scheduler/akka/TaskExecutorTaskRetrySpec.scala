package com.pagerduty.scheduler.akka

import akka.testkit.{TestFSMRef, TestProbe}
import com.pagerduty.metrics.Metrics
import com.pagerduty.scheduler.Scheduler.LoggingImpl
import com.pagerduty.scheduler._
import com.pagerduty.scheduler.model.{CompletionResult, Task, TaskStatus}
import com.pagerduty.scheduler.specutil.{ActorPathFreeSpec, TaskFactory}
import com.typesafe.config.ConfigFactory
import java.time.Instant
import org.scalamock.scalatest.PathMockFactory
import scala.concurrent.duration._
import scala.concurrent.Future

class TaskExecutorTaskRetrySpecSpec extends ActorPathFreeSpec("TaskExecutorSpec") with PathMockFactory {
  import TaskPersistence._
  import TaskStatusTracker._
  import TaskExecutor._
  val maxMessageDelay = 1000.millis

  "A TaskExecutor" - {
    val failingRunner = (task: Task) => throw new RuntimeException("Simulated test exception.")
    val mockTaskExecutorService = new TestExecutorService(1, failingRunner)
    val taskStatusTracker = TestProbe()
    val taskPersistence = TestProbe()
    val partitionContext = PartitionContext(
      partitionId = 1,
      taskStatusTracker = taskStatusTracker.ref,
      taskPersistence = taskPersistence.ref,
      mockTaskExecutorService,
      logging = stub[Scheduler.Logging]
    )
    val orderingExecutor = TestProbe()
    val task = TaskFactory.makeTask()
    val taskKey = task.taskKey

    "when constructed with a task that will perpetually fail" - {
      val maxRetries = 2
      val maxBackoffPeriod = 100.milliseconds
      val failureStatus = TaskStatus.failed(numberOfAttempts = maxRetries + 1)
      val settings = Settings().copy(maxTaskBackoffPeriod = maxBackoffPeriod, maxTaskRetries = maxRetries)
      val taskExecutor = TestFSMRef(
        new TaskExecutor(
          settings,
          partitionContext,
          task,
          orderingExecutor.ref
        )
      )

      "when it executes the task" - {
        taskStatusTracker.expectMsg(FetchTaskStatus(task.taskKey))
        taskExecutor ! TaskStatusFetched(task.taskKey, TaskStatus.NeverAttempted)

        "it retries the task until the max number of retries is reached" in {
          for (i <- 1 to maxRetries) {
            taskStatusTracker.expectMsgPF(maxMessageDelay) {
              case UpdateTaskStatus(`taskKey`, TaskStatus(`i`, CompletionResult.Incomplete, _)) =>
              // accept
            }
            val attemptUpdate = TaskStatus(i, CompletionResult.Incomplete, Some(Instant.now()))
            taskExecutor ! TaskStatusUpdated(task.taskKey, attemptUpdate)
          }

          // Marks the task as failed after it has failed that many times
          taskStatusTracker.expectMsg(UpdateTaskStatus(task.taskKey, failureStatus))

          // Reports the task back as executed
          taskExecutor ! TaskStatusUpdated(task.taskKey, failureStatus)
          taskPersistence.expectMsg(RemoveTask(task.taskKey))
          orderingExecutor.expectMsg(OrderingExecutor.TaskExecuted(task.taskKey))
        }

        "when marking the task as failed fails" - {
          val exception = new Exception("Simulated exception")
          taskExecutor ! TaskStatusNotUpdated(task.taskKey, failureStatus, exception)
          "it crashes" - {
            orderingExecutor.expectNoMsg()
            taskPersistence.expectNoMsg()
          }
        }

      }
    }
  }

  verifyExpectations()
}
