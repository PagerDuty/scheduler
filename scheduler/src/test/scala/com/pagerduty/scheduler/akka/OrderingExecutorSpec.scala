package com.pagerduty.scheduler.akka

import akka.actor._
import akka.testkit.TestProbe
import com.pagerduty.eris.TimeUuid
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.specutil.{ActorPathFreeSpec, TaskFactory}
import org.scalamock.scalatest.PathMockFactory

class OrderingExecutorSpec extends ActorPathFreeSpec("OrderingExecutorSpec") with PathMockFactory {
  import OrderingExecutor._

  val orderingId = TimeUuid().toString
  val tasks = TaskFactory.makeTasks(3).map(_.copy(orderingId = orderingId))
  val task = tasks(0)

  "OrderingExecutor" - {
    case class ExecuteTaskRequest(task: Task)
    val partitionExecutor = TestProbe()
    val taskExecutorStub = TestProbe()
    val taskExecutorFactory = null

    val orderingExecutorProps = Props(
      new OrderingExecutor(
        orderingId,
        partitionExecutor.testActor,
        taskExecutorFactory
      ) {
        override def execute(task: Task): Unit = {
          taskExecutorStub.ref ! ExecuteTaskRequest(task)
        }
      }
    )
    val orderingExecutor = system.actorOf(orderingExecutorProps)

    "run tasks immediately when idle" in {
      orderingExecutor ! ExecuteOrderingTask(task)
      taskExecutorStub expectMsg ExecuteTaskRequest(task)
    }

    "wait for running task to complete before starting next one" in {
      orderingExecutor ! ExecuteOrderingTask(tasks(0))
      orderingExecutor ! ExecuteOrderingTask(tasks(1))
      taskExecutorStub expectMsg ExecuteTaskRequest(tasks(0))
      orderingExecutor ! TaskExecuted(tasks(0).taskKey)
      taskExecutorStub expectMsg ExecuteTaskRequest(tasks(1))
    }

    "overwrite tasks with the same key" in {
      val blockingTask = tasks(0)
      orderingExecutor ! ExecuteOrderingTask(blockingTask)
      taskExecutorStub expectMsg ExecuteTaskRequest(blockingTask)

      val originalTask = tasks(1)
      val modifiedTask = originalTask.copy(taskData = Map("id" -> TimeUuid().toString))
      orderingExecutor ! ExecuteOrderingTask(originalTask)
      orderingExecutor ! ExecuteOrderingTask(modifiedTask)

      orderingExecutor ! TaskExecuted(blockingTask.taskKey)
      taskExecutorStub expectMsg ExecuteTaskRequest(modifiedTask)
    }

    "run all tasks in order" in {
      for (task <- tasks) orderingExecutor ! ExecuteOrderingTask(task)
      for (task <- tasks) {
        taskExecutorStub expectMsg ExecuteTaskRequest(task)
        orderingExecutor ! TaskExecuted(task.taskKey)
      }
    }

    "notify partition executor on task completion" in {
      orderingExecutor ! ExecuteOrderingTask(task)
      orderingExecutor ! TaskExecuted(task.taskKey)
      partitionExecutor.expectMsg(OrderingTaskExecuted(task.taskKey))
    }
  }
}
