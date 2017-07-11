package com.pagerduty.scheduler.akka

import akka.actor._
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.pagerduty.eris.TimeUuid
import com.pagerduty.scheduler.akka.OrderingExecutor.ExecuteOrderingTask
import com.pagerduty.scheduler.specutil.{ActorPathFreeSpec, TaskFactory}
import com.pagerduty.scheduler.Scheduler
import com.pagerduty.scheduler.model.Task
import org.scalamock.scalatest.PathMockFactory

class PartitionExecutorSpec extends ActorPathFreeSpec("PartitionExecutorSpec") with PathMockFactory {
  import PartitionExecutor._

  "PartitionExecutor" - {
    val orderingExecutor = TestProbe()
    val orderingExecutorFactory = { (_: ActorRefFactory, _: PartitionContext, _: Task.OrderingId, _: ActorRef) =>
      orderingExecutor.ref
    }
    val partitionExecutorProps = Props(new PartitionExecutor(orderingExecutorFactory))
    val partitionExecutor = TestActorRef[PartitionExecutor](partitionExecutorProps)
    val partitionContext = PartitionContext(1, null, null, null, logging = stub[Scheduler.Logging])
    partitionExecutor ! PartitionExecutor.Initialize(partitionContext)

    def sendTaskToOrderingExecutor(task: Task): Unit = {
      partitionExecutor ! ExecutePartitionTask(task)
      orderingExecutor.expectMsg(ExecuteOrderingTask(task))
    }

    "should spawn up an ordering executor and send task" in {
      val task = TaskFactory.makeTask()
      sendTaskToOrderingExecutor(task)
    }

    "should delete an ordering executor if there are no more tasks left for it" in {
      val deathWatcher = TestProbe()
      deathWatcher.watch(orderingExecutor.ref)
      val task = TaskFactory.makeTask()
      sendTaskToOrderingExecutor(task)
      orderingExecutor.send(partitionExecutor, OrderingExecutor.OrderingTaskExecuted(task.taskKey))
      deathWatcher.expectMsgType[Terminated]
    }

    "spawn a unique ordering executor for each unique orderingId" in {
      val tasks = TaskFactory.makeTasks(2)
      sendTaskToOrderingExecutor(tasks(0))
      sendTaskToOrderingExecutor(tasks(1))
      partitionExecutor.underlyingActor.orderingExecutors.size shouldEqual 2
    }

    "handle tasks with the same key" in {
      val originalTask = TaskFactory.makeTask()
      val modifiedTask = originalTask.copy(taskData = Map("id" -> TimeUuid().toString))
      sendTaskToOrderingExecutor(originalTask)
      sendTaskToOrderingExecutor(modifiedTask)
      val taskKey = originalTask.taskKey
      val orderingId = taskKey.orderingId
      partitionExecutor.underlyingActor.orderingExecutors(orderingId).tasks shouldEqual Set(taskKey)
    }

    "not spawn a unique ordering executor if the ordering ids are the same" in {
      val orderingId = TimeUuid().toString
      val tasks = TaskFactory.makeTasks(2).map(_.copy(orderingId = orderingId))
      sendTaskToOrderingExecutor(tasks(0))
      sendTaskToOrderingExecutor(tasks(1))
      partitionExecutor.underlyingActor.orderingExecutors.size shouldEqual 1
    }

    "reply to in-flight task status queries" in {
      partitionExecutor ! ThroughputController.FetchInProgressTaskCount
      expectMsg(ThroughputController.InProgressTaskCountFetched(0))

      val taskCount = 2
      val tasks = TaskFactory.makeTasks(taskCount)
      tasks.foreach(sendTaskToOrderingExecutor)
      partitionExecutor ! ThroughputController.FetchInProgressTaskCount
      expectMsg(ThroughputController.InProgressTaskCountFetched(taskCount))
    }
  }
}
