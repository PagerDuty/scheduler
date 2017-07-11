package com.pagerduty.scheduler.akka

import akka.actor._
import akka.pattern._
import akka.testkit.TestProbe
import akka.util.Timeout
import com.pagerduty.scheduler.akka.TaskPersistence.PersistTasks
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.specutil.{ActorPathFreeSpec, TaskFactory}
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.{Await, TimeoutException}
import scala.concurrent.duration._

class TopicSupervisorSpec extends ActorPathFreeSpec("TopicSupervisorSpec") with PathMockFactory with ScalaFutures {
  import TopicSupervisor._

  val partitions = (1 to 3).toSet
  val uninitializedPartition = 0
  val tasksPerPartition = 2
  val tasks = TaskFactory.makeTasks(partitions.size * tasksPerPartition)
  val tasksByPartition = {
    val groupedTasks = tasks.grouped(tasksPerPartition).toSeq
    partitions.zip(groupedTasks).toMap
  }
  val queueContext = QueueContext(
    taskScheduleDao = null,
    taskStatusDao = null,
    taskExecutorService = null,
    logging = null
  )
  val persistRequestTimeout = 500.millis
  val settings = Settings().copy(persistRequestTimeout = persistRequestTimeout)

  "TopicSupervisor should" - {
    val partitionSupervisors = partitions.map(_ -> TestProbe()).toMap
    val partitionSupervisorFactory = { (_: ActorRefFactory, partitionId: PartitionId, _: QueueContext) =>
      {
        partitionSupervisors(partitionId).testActor
      }
    }
    val queueSupervisorProps = Props(
      new TopicSupervisor(
        settings,
        queueContext,
        partitions,
        partitionSupervisorFactory
      )
    )
    val queueSupervisor = system.actorOf(queueSupervisorProps)

    def replyForSomePartitions(): Unit = {
      val notAllSupervisors = partitionSupervisors.drop(1)
      for ((partitionId, partitionSupervisor) <- notAllSupervisors) {
        val partitionTasks = tasksByPartition(partitionId)
        partitionSupervisor expectMsg PersistTasks(partitionTasks)
        partitionSupervisor reply TaskPersistence.TasksPersisted(partitionId)
      }
    }

    "return success when there are no tasks to persist" in {
      queueSupervisor ! ProcessTaskBatch(Map.empty)
      expectMsg(TaskBatchProcessed)
    }

    "forward tasks to partition supervisors" in {
      queueSupervisor ! ProcessTaskBatch(tasksByPartition)
      for ((partitionId, partitionSupervisor) <- partitionSupervisors) {
        val partitionTasks = tasksByPartition(partitionId)
        partitionSupervisor expectMsg PersistTasks(partitionTasks)
      }
    }

    "await replies from all affected partitions" in {
      val tasksForSomePartitions = tasksByPartition - partitions.head
      queueSupervisor ! ProcessTaskBatch(tasksForSomePartitions)

      val affectedPartitions = tasksForSomePartitions.keySet
      val affectedSupervisors = partitionSupervisors.filterKeys(affectedPartitions.contains)

      for ((partitionId, partitionSupervisor) <- affectedSupervisors) {
        val partitionTasks = tasksForSomePartitions(partitionId)
        partitionSupervisor expectMsg PersistTasks(partitionTasks)
        partitionSupervisor reply TaskPersistence.TasksPersisted(partitionId)
      }

      expectMsg(TaskBatchProcessed)
    }

    "time out when some partitions have not replied" in {
      queueSupervisor ! ProcessTaskBatch(tasksByPartition)
      replyForSomePartitions()
      val m = expectMsgType[TaskBatchNotProcessed]
      m.throwable shouldBe a[TimeoutException]
    }

    "forward the exception from a failed persistence" in {
      queueSupervisor ! ProcessTaskBatch(tasksByPartition)
      val exception = new Exception("Test Exception: error persisting tasks")
      val (partitionId, partitionSupervisor) = partitionSupervisors.head

      partitionSupervisor.expectMsg(PersistTasks(tasksByPartition(partitionId)))
      partitionSupervisor.reply(TaskPersistence.TasksNotPersisted(exception))

      val m = expectMsgType[TaskBatchNotProcessed]
      m.throwable should equal(exception)
    }
  }
}
