package com.pagerduty.scheduler.akka

import akka.testkit.{TestFSMRef, TestProbe}
import com.pagerduty.scheduler._
import com.pagerduty.scheduler.model._
import com.pagerduty.scheduler.specutil.{ActorPathFreeSpec, TaskFactory}
import org.scalamock.scalatest.PathMockFactory
import java.time.Instant
import scala.concurrent.duration._
import scala.concurrent.Future

class TaskExecutorSpec extends ActorPathFreeSpec("TaskExecutorSpec") with PathMockFactory {
  import TaskPersistence._
  import TaskStatusTracker._
  import TaskExecutor._
  val maxMessageDelay = 1000.millis

  "A TaskExecutor" - {
    class NoArgsTaskExecutorService extends TestExecutorService(1, null)
    val mockTaskExecutorService = stub[NoArgsTaskExecutorService]
    val taskStatusTracker = TestProbe()
    val taskPersistence = TestProbe()
    val logging = stub[Scheduler.Logging]
    val partitionContext = PartitionContext(
      partitionId = 1,
      taskStatusTracker = taskStatusTracker.ref,
      taskPersistence = taskPersistence.ref,
      mockTaskExecutorService,
      logging
    )
    val orderingExecutor = TestProbe()

    val task = TaskFactory.makeTask()
    val taskKey = task.taskKey
    val successStatus = TaskStatus.successful(1)

    "when using exponential backoff" - {
      val returnsOne = () => 1.0
      val maxBackoff = 20.seconds
      val backoffSettings = Settings().copy(maxTaskBackoffPeriod = maxBackoff)

      "base case works" in {
        val backoffFactor = 2.second
        val settings = backoffSettings.copy(exponentialBackoffFactor = backoffFactor)
        val nextAttemptNumber = 2 // The fist wait time will be calculate for 2nd attempt.
        TaskExecutor.jitteredWaitForRetry(nextAttemptNumber, settings, returnsOne) should be(backoffFactor)
      }

      "it uses the backoff factor" in {
        val settings = backoffSettings.copy(exponentialBackoffFactor = 100.milliseconds)
        val nextAttemptNumber = 5
        TaskExecutor.jitteredWaitForRetry(nextAttemptNumber, settings, returnsOne) should be(800.milliseconds)
      }

      "it respects the max backoff time" in {
        TaskExecutor.jitteredWaitForRetry(Int.MaxValue, backoffSettings, returnsOne) should be(maxBackoff)
      }

      "it uses the random function for jitter" in {
        val settings = backoffSettings.copy(exponentialBackoffFactor = 2.seconds)
        val nextAttemptNumber = 5
        TaskExecutor.jitteredWaitForRetry(nextAttemptNumber, settings, () => 0.5) should be(12.seconds)
      }
    }

    "when constructed with a task that will fail once" - {
      var failCount = 0
      (partitionContext.taskExecutorService
        .execute(_: Int, _: Task))
        .when(*, task)
        .onCall(
          { (_, task: Task) =>
            if (failCount == 0) {
              failCount += 1
              Future.failed(new Exception("task failed"))
            } else {
              Future.successful()
            }
          }
        )

      val maxBackoffPeriod = 100.milliseconds
      val settings = Settings().copy(maxTaskBackoffPeriod = maxBackoffPeriod)
      val taskExecutor = TestFSMRef(
        new TaskExecutor(
          settings,
          partitionContext,
          task,
          orderingExecutor.testActor
        )
      )

      "when it executes the task" - {
        taskStatusTracker.expectMsg(FetchTaskStatus(task.taskKey))
        taskExecutor ! TaskStatusFetched(task.taskKey, TaskStatus.NeverAttempted)

        "it retries the task until it succeeds and reports intermediate attempts" in {
          taskStatusTracker.expectMsgPF(maxMessageDelay) {
            case UpdateTaskStatus(`taskKey`, TaskStatus(1, CompletionResult.Incomplete, _)) =>
            // accept
          }
          val attemptUpdate = TaskStatus(1, CompletionResult.Incomplete, Some(Instant.now()))
          taskExecutor ! TaskStatusUpdated(task.taskKey, attemptUpdate)

          // task automatically is retried, and will succeed the second time
          val successAfter2Attempts = TaskStatus.successful(2)
          taskStatusTracker.expectMsg(UpdateTaskStatus(task.taskKey, successAfter2Attempts))

          (logging.reportTaskAttemptFinished _).verify(where { (_, task: Task, taskAttempt: TaskAttempt) =>
            task.taskKey == taskKey &&
            taskAttempt.attemptNumber == 1 &&
            taskAttempt.taskResult == CompletionResult.Incomplete &&
            taskAttempt.exceptionClass.isDefined
          })
        }
      }
    }

    "when constructed with a task that will succeed" - {
      (partitionContext.taskExecutorService
        .execute(_: Int, _: Task))
        .when(*, task)
        .returns(Future.successful())

      val taskExecutor = TestFSMRef(
        new TaskExecutor(
          Settings(),
          partitionContext,
          task,
          orderingExecutor.testActor
        )
      )

      "it checks if the task is already completed" in {
        taskStatusTracker.expectMsg(FetchTaskStatus(task.taskKey))
      }

      "when checking task completion" - {
        taskStatusTracker.expectMsg(FetchTaskStatus(task.taskKey))

        "when task completion cannot be determined" - {
          taskExecutor ! TaskStatusNotFetched(task.taskKey, new Exception)

          "it crashes" in {
            orderingExecutor.expectNoMsg
            taskPersistence.expectNoMsg
          }
        }

        "when the task is already completed" - {
          taskExecutor ! TaskStatusFetched(task.taskKey, successStatus)

          "it rewrites the task status" in {
            taskStatusTracker.expectMsg(UpdateTaskStatus(task.taskKey, successStatus))
          }

          "when the task is successfully marked as completed" - {
            taskExecutor ! TaskStatusUpdated(task.taskKey, successStatus)

            "it removes the task and reports success" in {
              taskPersistence.expectMsg(RemoveTask(task.taskKey))
              orderingExecutor.expectMsg(OrderingExecutor.TaskExecuted(task.taskKey))
            }
          }

          "when the task is not successfully marked as completed" - {
            val exception = new Exception("Simulated exception")
            taskExecutor ! TaskStatusNotUpdated(task.taskKey, successStatus, exception)

            "it crashes" in {
              taskPersistence.expectNoMsg
              orderingExecutor.expectNoMsg
            }
          }
        }

        "when the task is not already completed" - {
          taskExecutor ! TaskStatusFetched(task.taskKey, TaskStatus.NeverAttempted)

          "it executes the task and marks it as complete when finished" in {
            (mockTaskExecutorService.execute(_: Int, _: Task)).verify(*, task)
            taskStatusTracker.expectMsg(
              UpdateTaskStatus(task.taskKey, successStatus)
            )
          }

          "when the task is successfully marked as completed" - {
            taskExecutor ! TaskStatusUpdated(task.taskKey, successStatus)

            "it removes the task and reports success" in {
              taskPersistence.expectMsg(RemoveTask(task.taskKey))
              orderingExecutor.expectMsg(OrderingExecutor.TaskExecuted(task.taskKey))
            }

            "it reports task attempt" in {
              (logging.reportTaskAttemptFinished _).verify(where { (_, task: Task, taskAttempt: TaskAttempt) =>
                task.taskKey == taskKey &&
                taskAttempt.attemptNumber == 1 &&
                taskAttempt.taskResult == CompletionResult.Success &&
                taskAttempt.exceptionClass == None
              })
            }
          }

          "when the task is not successfully marked as completed" - {
            val exception = new Exception("Simulated exception")
            taskExecutor ! TaskStatusNotUpdated(task.taskKey, successStatus, exception)

            "it crashes" in {
              taskPersistence.expectNoMsg
              orderingExecutor.expectNoMsg
            }
          }
        }
      }
    }
  }

  verifyExpectations()
}
