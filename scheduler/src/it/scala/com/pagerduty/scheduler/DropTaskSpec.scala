package com.pagerduty.scheduler

import com.pagerduty.scheduler.model.Task
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * IMPORTANT:
  *
  * - See the README for manual Setup Steps before running this test.
  */
class DropTaskSpec extends SchedulerIntegrationSpecBase with BeforeAndAfter with Eventually {
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(40, Seconds)))

  var scheduler: TestScheduler = null

  before {
    scheduler = new TestScheduler()
  }

  after {
    scheduler.shutdown()
  }

  "Scheduler should" - {
    "be able to drop stuck tasks" in {
      val orderingId = "orderingId"
      val scheduledTime = Instant.now().truncatedTo(ChronoUnit.MILLIS)
      val stuckTask = new Task(orderingId, scheduledTime, "01", Map("throw!" -> "stuck task"))
      val goodTask = new Task(orderingId, scheduledTime, "02", Map("key" -> "good task"))
      scheduler.scheduleTask(stuckTask)
      scheduler.scheduleTask(goodTask)

      val adminService = scheduler.actualScheduler.adminService

      // Wait for stuckTask to run & fail
      eventually { scheduler.oopsedTasks.size should be > 0 }
      // Wait for Cassandra update to finish to avoid a write race with dropTask.
      eventually {
        val adminTask = Await.result(adminService.fetchTaskWithDetails(stuckTask.taskKey), Duration.Inf)
        adminTask.numberOfAttempts.getOrElse(0) should equal(1)
      }
      // Make sure we haven't taken too long!
      scheduler.executedTasks should not contain (goodTask)
      // Drop the task
      Await.result(adminService.dropTask(stuckTask.taskKey), Duration.Inf)
      // Once stuckTask is dropped, goodTask should be able to run
      eventually { scheduler.executedTasks should contain(goodTask) }
    }
  }
}
