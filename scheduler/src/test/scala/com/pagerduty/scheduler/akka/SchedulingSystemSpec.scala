package com.pagerduty.scheduler.akka

import akka.actor.ActorSystem
import akka.pattern.AskTimeoutException
import akka.testkit.TestProbe
import com.netflix.astyanax.Cluster
import com.pagerduty.metrics.NullMetrics
import com.pagerduty.scheduler.akka.TopicSupervisor.{ProcessTaskBatch, TaskBatchNotProcessed, TaskBatchProcessed}
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.{Scheduler, TestExecutorService}
import com.pagerduty.scheduler.specutil.ActorPathFreeSpec
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class SchedulingSystemSpec extends ActorPathFreeSpec("SchedulingSystemSpec") with PathMockFactory with ScalaFutures {

  val mockSystem = stub[ActorSystem]
  val queueSupervisor = TestProbe()
  val mockTopicSupervisor = queueSupervisor.ref
  val config = ConfigFactory.load()
  val cluster = stub[Cluster]
  val keyspace = null
  val partitions = Set[PartitionId]()
  val taskRunner = (_: Task) => {}
  val executorFactory = (_: Set[PartitionId]) => new TestExecutorService(threadPoolSize = 2, taskRunner)
  val logging = stub[Scheduler.Logging]

  class TestSchedulingSystem
      extends SchedulingSystem(config, cluster, keyspace, partitions, executorFactory, logging, NullMetrics) {
    override protected val system = mockSystem
    override protected lazy val queueSupervisor = mockTopicSupervisor
  }

  "Scheduling System" - {
    val schedulingSystem = new TestSchedulingSystem
    "should detect an actor system shutdown during task persistence" in {
      (mockSystem.isTerminated _).when().returns(true)

      val thrown = the[RuntimeException] thrownBy (schedulingSystem.persistAndSchedule(Map()))
      thrown.getMessage should include("Actor System has terminated")
    }

    "should throw an AskTimeoutException if the task persistence ask times out" in {
      (mockSystem.isTerminated _).when().returns(false)
      an[AskTimeoutException] should be thrownBy (schedulingSystem.persistAndSchedule(Map()))
    }

    "should persist tasks" - {
      (mockSystem.isTerminated _).when().returns(false)

      import scala.concurrent.ExecutionContext.Implicits.global
      val futResult = Future { schedulingSystem.persistAndSchedule(Map()) }

      val tasks = Map[PartitionId, Seq[Task]]()
      queueSupervisor.expectMsg(ProcessTaskBatch(tasks))

      "and return unit if successful" in {
        queueSupervisor.reply(TaskBatchProcessed)

        futResult.futureValue should equal(())
      }

      "and wrap the thrown exception if task persistence failed" in {
        val exception = new Exception("Test Exception: Failed persisting tasks")
        queueSupervisor.reply(TaskBatchNotProcessed(exception))

        whenReady(futResult.failed) { e =>
          val e = the[RuntimeException] thrownBy (futResult.futureValue)
          e.getCause.getCause should equal(exception)

        }
      }

      "and throw an exception if an unexpected message is received" in {
        queueSupervisor.reply("UnexpectedMessage")

        whenReady(futResult.failed) { e =>
          val e = the[RuntimeException] thrownBy (futResult.futureValue)
          e.getMessage should include("Unexpected response")
        }
      }
    }
  }
}
