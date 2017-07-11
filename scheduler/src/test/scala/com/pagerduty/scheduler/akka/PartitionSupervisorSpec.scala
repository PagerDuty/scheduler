package com.pagerduty.scheduler.akka

import akka.actor.{ActorRefFactory, Props}
import akka.testkit.TestProbe
import com.pagerduty.scheduler.specutil.{ActorPathFreeSpec, TaskFactory}
import com.pagerduty.scheduler.Scheduler
import org.scalamock.scalatest.PathMockFactory

class PartitionSupervisorSpec extends ActorPathFreeSpec("PartitionSupervisorSpec") with PathMockFactory {
  val settings = Settings()
  val partitionId = 1
  val tasks = TaskFactory.makeTasks(3)
  val queueContext = QueueContext(
    taskScheduleDao = null,
    taskStatusDao = null,
    taskExecutorService = null,
    logging = stub[Scheduler.Logging]
  )

  "PartitionSupervisor should" - {
    val taskPersistence = TestProbe()
    val taskPersistenceFactory = (_: ActorRefFactory, args: TaskPersistenceArgs) => {
      args.partitionId shouldEqual partitionId
      taskPersistence.testActor
    }
    val partitionSupervisorProps = Props(
      new PartitionSupervisor(
        settings,
        queueContext,
        partitionId,
        taskPersistenceFactory
      )
    )
    val paritionsSupervisor = system.actorOf(partitionSupervisorProps)
    taskPersistence.expectMsgType[TaskPersistence.LoadTasks]

    "forward PersistTask messages" in {
      val msg = TaskPersistence.PersistTasks(tasks)
      paritionsSupervisor ! msg
      taskPersistence expectMsg msg
    }
  }
}
