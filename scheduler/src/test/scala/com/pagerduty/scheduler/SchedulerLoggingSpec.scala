package com.pagerduty.scheduler

import com.pagerduty.scheduler.dao.AttemptHistoryDao
import com.pagerduty.scheduler.gauge.StaleTasksGauge
import com.pagerduty.metrics.NullMetrics
import com.pagerduty.scheduler.model.CompletionResult
import com.pagerduty.scheduler.specutil.{TaskAttemptFactory, TaskFactory, UnitSpec}
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.slf4j.Logger

import scala.concurrent.Future

class SchedulerLoggingSpec extends UnitSpec with MockFactory with BeforeAndAfter {

  // Because of buggy mocking.
  case class AddMetricsInvocation(name: String, tags: Seq[(String, String)])
  class MockMetrics extends NullMetrics {
    private var _invocations = Seq.empty[AddMetricsInvocation]
    def invocations = this.synchronized {
      _invocations
    }
    override def histogram(name: String, value: Int, tags: (String, String)*): Unit = this.synchronized {
      _invocations :+= AddMetricsInvocation(name, tags)
    }
  }
  val successTag = "result" -> "success"
  val failureTag = "result" -> "failure"
  val additionalTag = "additional" -> "tag"
  val additionalTags = Seq(additionalTag)

  "Scheduler.LoggingImpl" should {
    val mockLogger = mock[Logger]
    val mockMetrics = new MockMetrics
    val mockAttemptHistoryDao = mock[AttemptHistoryDao]
    val logging = new Scheduler.LoggingImpl(
      SchedulerSettings(ConfigFactory.load()),
      mockMetrics,
      Some(mockAttemptHistoryDao),
      mockLogger
    )
    val name = "test_name"

    "persist task attempts" in {
      val partitionId = 2
      val task = TaskFactory.makeTask()
      val taskAttempt = TaskAttemptFactory.makeTaskAttempt(1, CompletionResult.Incomplete)
      (mockAttemptHistoryDao.insert _)
        .expects(partitionId, task.taskKey, taskAttempt)
        .returning(Future.successful(Unit))
      logging.reportTaskAttemptFinished(partitionId, task, taskAttempt)
    }

    "build a stale tasks gauge sample consumer" in {
      val consumer = logging.staleTasksGaugeSampleConsumer

      (mockLogger.info(_: String)).expects(*)

      consumer(1)

      eventually {
        mockMetrics.invocations should contain(AddMetricsInvocation("stale_task_count", Seq()))
      }
    }
  }
}
