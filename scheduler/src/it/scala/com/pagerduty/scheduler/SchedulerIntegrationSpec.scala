package com.pagerduty.scheduler

import com.pagerduty.eris.schema.SchemaLoader
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task
import com.typesafe.config.ConfigFactory
import java.time.Instant
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FreeSpec, Matchers}
import org.scalatest.time.{Seconds, Span}
import scala.concurrent.duration._

// IMPORTANT:
//
// - See the README for manual Setup Steps before running this test!
//
// - The timing in these tests requires specific values in the config file
//   `src/it/resources/application.conf`. If you change the timing of tests
//   here, fix up the config file as needed.
//

class SchedulerIntegrationSpec extends SchedulerIntegrationSpecBase with BeforeAndAfter {
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(20, Seconds)))

  var scheduler: TestScheduler = _

  before {
    startScheduler()
  }

  after {
    shutdownScheduler()
  }

  "Scheduler should" - {
    "run tasks as scheduled" in {
      val tzero = Instant.now()
      val tplusFoo = tzero + 10.seconds

      // Schedule multiple tasks covering a mix of partially matching
      // orderingId, scheduledTime & uniquenessKey.
      val taskA = new Task("oid1", tzero, "uniq-key-1", Map("a" -> "b"))
      val taskB = new Task("oid1", tplusFoo, "uniq-key-1", Map("c" -> "d"))
      val taskC = new Task("oid1", tplusFoo, "uniq-key-2", Map("e" -> "f"))
      val taskD = new Task("oid2", tplusFoo, "uniq-key-2", Map("g" -> "h"))
      scheduler.scheduleTask(taskA)
      scheduler.scheduleTask(taskB)
      scheduler.scheduleTask(taskC)
      scheduler.scheduleTask(taskD)

      // check that 1st task is done immediately
      scheduler.tasksShouldRunNow(taskA :: Nil)

      // check that the remaining tasks run at the scheduled time
      scheduler.tasksShouldRunAt(List(taskB, taskC, taskD), tplusFoo)
    }

    "run tasks across scheduler restarts (in the same jvm)" in {
      val tzero = Instant.now()
      val tplusFoo = tzero + 10.seconds

      // Send due task & another task due in some time
      val taskA = new Task("oid5", tzero, "uniq-key-5", Map("i" -> "j"))
      val taskB = new Task("oid6", tplusFoo, "uniq-key-6", Map("k" -> "l"))
      scheduler.scheduleTask(taskA)
      scheduler.scheduleTask(taskB)

      // check that 1st task is done immediately
      scheduler.tasksShouldRunNow(taskA :: Nil)

      // shutdown the current scheduler
      val origScheduler: TestScheduler = scheduler
      shutdownScheduler()

      // check that 2nd task is not done at the scheduled time
      origScheduler.noTasksRunBy(tplusFoo)

      // start a new scheduler
      startScheduler()

      // check that the now overdue 2nd task is done immediately
      scheduler.tasksShouldRunNow(taskB :: Nil)
      origScheduler.tasksShouldRunNow(Nil)
    }

    "run tasks beyond max-look-ahead" in {
      val tzero = Instant.now()
      val tplusFoo = tzero + 20.seconds

      // Task due now and task due in after max-look-ahead seconds
      // Assumes "max-look-ahead" is 15 seconds
      val taskA = new Task("oid5", tzero, "uniq-key-5", Map("i" -> "j"))
      val taskB = new Task("oid6", tplusFoo, "uniq-key-6", Map("k" -> "l"))
      scheduler.scheduleTask(taskA)
      scheduler.scheduleTask(taskB)

      // check that 1st task is done immediately
      scheduler.tasksShouldRunNow(taskA :: Nil)

      // check that 2nd task is done at the scheduled time
      scheduler.tasksShouldRunAt(taskB :: Nil, tplusFoo)
    }

    "run tasks correctly regardless of scheduling order" in {
      val tzero = Instant.now()
      val tplusFoo = tzero + 5.seconds

      // Send task due in some time & another due now
      val taskA = new Task("oid7", tplusFoo, "uniq-key-7", Map("m" -> "n"))
      val taskB = new Task("oid8", tzero, "uniq-key-8", Map("o" -> "p"))
      scheduler.scheduleTask(taskA)
      scheduler.scheduleTask(taskB)

      // check that 2nd task is done immediately
      scheduler.tasksShouldRunNow(taskB :: Nil)

      // check that 1st task is done at the scheduled time
      scheduler.tasksShouldRunAt(taskA :: Nil, tplusFoo)
    }

    "run tasks correctly regardless of scheduling order - same orderingId" in {
      val tzero = Instant.now()
      val tplusFoo = tzero + 5.seconds

      // Send task due in some time & another due now
      val taskA = new Task("oid9", tplusFoo, "uniq-key-7", Map("m" -> "n"))
      val taskB = new Task("oid9", tzero, "uniq-key-8", Map("o" -> "p"))
      scheduler.scheduleTask(taskA)
      scheduler.scheduleTask(taskB)

      // check that 2nd task is done immediately
      scheduler.tasksShouldRunNow(taskB :: Nil)

      // check that 1st task is done at the scheduled time
      scheduler.tasksShouldRunAt(taskA :: Nil, tplusFoo)
    }

    "run duplicate task just once" in {
      val tzero = Instant.now()
      val tplusFoo = tzero + 5.seconds

      val taskA = new Task("oid1", tplusFoo, "uniq-key-1", Map("a" -> "b"))
      scheduler.scheduleTask(taskA)
      scheduler.scheduleTask(taskA)

      // check that dup task is run only once at the right time
      scheduler.tasksShouldRunAt(taskA :: Nil, tplusFoo)
    }

    "do not schedule tasks older than scheduling-grace-window" in {
      // Task due 10 seconds ago and task due 30 seconds ago
      // Assumes "scheduling-grace-window" is 20 seconds
      val taskA = new Task("oid7", Instant.now() - 10.seconds, "uniq-key-7", Map("m" -> "n"))
      val taskB = new Task("oid8", Instant.now() - 30.seconds, "uniq-key-8", Map("o" -> "p"))
      scheduler.scheduleTask(taskA)
      intercept[IllegalArgumentException] {
        scheduler.scheduleTask(taskB)
      }

      // check that only 1st task is done
      scheduler.tasksShouldRunNow(taskA :: Nil)
    }

    "task scheduled before the current execution time " in {
      // Assumes "scheduling-grace-window" is more than the schedule delta + misc running time
      val scheduleDelta = 5.seconds

      val tzero = Instant.now()
      val tplusFoo = tzero + scheduleDelta

      // Schedule task due in some time
      val taskA = new Task("oid101", tplusFoo, "uniq-key-1", Map("a" -> "b"))
      scheduler.scheduleTask(taskA)

      // Check that the task is run at the scheduled time
      scheduler.tasksShouldRunAt(taskA :: Nil, tplusFoo)

      // Schedule older task for the same orderingId
      // Uses actual now Time in case previous task ran slow
      val taskB = new Task("oid101", Instant.now() - scheduleDelta, "uniq-key-1", Map("c" -> "d"))
      scheduler.scheduleTask(taskB)

      // Check that the task runs
      scheduler.tasksShouldRunNow(taskB :: Nil)
    }

    "retry tasks that throw exceptions" in {
      val tzero = Instant.now()
      val tplus5secs = tzero + 5.seconds
      val tplus10secs = tzero + 10.seconds
      val tplus20secs = tzero + 20.seconds

      // Send task due in 5 seconds & another due now
      // Assumes task-retry-period is 20 seconds
      val taskA = new Task("oid9", tzero, "uniq-key-9", Map("throw!" -> "m and n!"))
      val taskB = new Task("oid10", tplus5secs, "uniq-key-10", Map("o" -> "p"))
      scheduler.scheduleTask(taskA)
      scheduler.scheduleTask(taskB)

      // check that 1st task is run immediately
      scheduler.tasksShouldRunNow(taskA :: Nil, taskA :: Nil)

      // check that 2nd task is done at the 5th second
      scheduler.tasksShouldRunAt(taskB :: Nil, tplus5secs)

      // check that 1st task is retried between the 10th and 20th second
      // we use 10th second because jitter chooses actual retry delay of 0.5 * 20 secs to 20 secs
      // assumes "task-retry-period" is set to 20 seconds
      // note: we're actually only checking that the task runs after the 10th second
      scheduler.tasksShouldRunAt(taskA :: Nil, tplus10secs, taskA :: Nil)
    }
  }

  def startScheduler() {
    scheduler shouldBe null
    scheduler = new TestScheduler()
  }

  def shutdownScheduler() {
    if (scheduler != null) {
      scheduler.shutdown()
      scheduler = null
    }
  }
}
