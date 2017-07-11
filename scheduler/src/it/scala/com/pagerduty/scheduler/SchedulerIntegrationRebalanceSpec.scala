package com.pagerduty.scheduler

import com.pagerduty.eris.schema.SchemaLoader
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task
import com.typesafe.config.ConfigFactory
import java.time.Instant
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FreeSpec, Matchers}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

// IMPORTANT:
//
// - See the README for manual Setup Steps before running this test!
//
// - The timing in these tests requires specific values in the config file
//   `src/it/resources/application.conf`. If you change the timing of tests
//   here, fix up the config file as needed.
//

class SchedulerIntegrationRebalanceSpec extends SchedulerIntegrationSpecBase with BeforeAndAfter {

  var scheduler1: TestScheduler = null
  var scheduler2: TestScheduler = null

  before {
    startScheduler1()
    startScheduler2()
  }

  after {
    shutdownScheduler1()
    shutdownScheduler2()
  }

  override def getSchemaLoader() = {
    val originalLoader = super.getSchemaLoader()
    val cluster = originalLoader.cluster
    val sideLoader = CassandraTaskExecutorTestService.schemaLoader(cluster)
    new SchemaLoader(cluster, originalLoader.columnFamilyDefs ++ sideLoader.columnFamilyDefs)
  }

  def makeTestScheduler(): TestScheduler = {
    new TestScheduler() {
      override def taskExecutorServiceFactory = {
        val clusterBuilder = makeClusterCtx _
        CassandraTaskExecutorTestService.cassTestExecutorFactory(clusterBuilder, taskRunner)
      }
    }
  }

  def startScheduler1() {
    scheduler1 shouldBe null
    scheduler1 = makeTestScheduler()
  }

  def startScheduler2() {
    scheduler2 shouldBe null
    scheduler2 = makeTestScheduler()
  }

  def shutdownScheduler1() {
    if (scheduler1 != null) {
      scheduler1.shutdown()
      scheduler1 = null
    }
  }

  def shutdownScheduler2() {
    if (scheduler2 != null) {
      scheduler2.shutdown()
      scheduler2 = null
    }
  }

  "Scheduler should" - {

    "handle Kafka rebalancing" in {
      val tzero = Instant.now()
      val tplusFoo = tzero + 20.seconds
      val tplusBar = tzero + 60.seconds

      // A mix of tasks.
      val taskA = new Task("oid1", tzero, "uniq-key-1", Map("a" -> "1"))
      val taskB = new Task("oid1", tzero, "uniq-key-2", Map("b" -> "2"))
      val taskC = new Task("OID2", tzero, "uniq-key-5", Map("c" -> "3"))
      val taskD = new Task("oid1", tplusFoo, "uniq-key-1", Map("d" -> "4"))
      val taskE = new Task("oid1", tplusFoo, "uniq-key-2", Map("e" -> "5"))
      val taskF = new Task("OID2", tplusFoo, "uniq-key-6", Map("f" -> "6"))
      val taskG = new Task("oid1", tplusBar, "uniq-key-1", Map("g" -> "7"))
      val taskH = new Task("OID2", tplusBar, "uniq-key-5", Map("h" -> "8"))
      val taskI = new Task("OID2", tplusBar, "uniq-key-6", Map("i" -> "9"))

      // Schedule tasks across either scheduler
      scheduler1.scheduleTask(taskA)
      scheduler2.scheduleTask(taskB)
      scheduler2.scheduleTask(taskC)
      scheduler1.scheduleTask(taskD)
      scheduler2.scheduleTask(taskE)
      scheduler2.scheduleTask(taskF)
      scheduler2.scheduleTask(taskG)
      scheduler1.scheduleTask(taskH)
      scheduler1.scheduleTask(taskI)

      // check that tasks spread acceptably across the schedulers
      val possiblePairs = Map(
        sortedTasks(List(taskA, taskB, taskC)) -> Nil, // both orderingIds in scheduler1
        sortedTasks(List(taskA, taskB)) -> sortedTasks(List(taskC)), // oid1 in scheduler1; OID2 in scheduler2
        sortedTasks(List(taskC)) -> sortedTasks(List(taskA, taskB)), // oid1 in scheduler2; OID2 in scheduler1
        Nil -> sortedTasks(List(taskA, taskB, taskC)) // both orderingIds in scheduler2
      )
      scheduler1.tasksShouldConditionBy(tzero, true) {
        possiblePairs should contain(
          sortedTasks(scheduler1.executedTasks) -> sortedTasks(scheduler2.executedTasks)
        )
      }
      // capture which tasks ran on scheduler2
      val scheduler2tasks = scheduler2.executedTasks
      // and then ensure it's test history is cleared
      scheduler2.tasksShouldConditionBy(tzero, true) {}

      // wait for scheduler2 tasks to update Cassandra to avoid race with shutdown
      eventually {
        val adminService2 = scheduler2.actualScheduler.adminService
        scheduler2tasks.foreach { task =>
          val adminTask = Await.result(adminService2.fetchTaskWithDetails(task.taskKey), Duration.Inf)
          adminTask.numberOfAttempts.getOrElse(0) should equal(1)
        }
      }

      // shutdown scheduler2 to trigger kafka partition reassignment
      shutdownScheduler2()

      // check that all tplusFoo tasks ran on scheduler1
      scheduler1.tasksShouldRunAt(List(taskD, taskE, taskF), tplusFoo)

      // restart scheduler2 to trigger kafka partition assignment
      startScheduler2()

      // check that all tplusBar tasks ran on appropriate schedulers
      val possiblePairs2 = Map(
        sortedTasks(List(taskG, taskH, taskI)) -> Nil, // both orderingIds in scheduler1
        sortedTasks(List(taskH, taskI)) -> sortedTasks(List(taskG)), // oid1 in scheduler2; OID2 in scheduler1
        sortedTasks(List(taskG)) -> sortedTasks(List(taskH, taskI)), // oid1 in scheduler1; OID2 in scheduler2
        Nil -> sortedTasks(List(taskG, taskH, taskI)) // both orderingIds in scheduler2
      )
      scheduler1.tasksShouldConditionBy(tplusBar, true) {
        possiblePairs2 should contain(
          sortedTasks(scheduler1.executedTasks) -> sortedTasks(scheduler2.executedTasks)
        )
      }
      scheduler2.tasksShouldConditionBy(tplusBar, true) {}

    }
  }

}
