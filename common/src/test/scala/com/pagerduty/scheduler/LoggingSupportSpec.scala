package com.pagerduty.scheduler

import com.pagerduty.metrics.{Event, Metrics, NullMetrics}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpecLike}
import org.slf4j.Logger
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.util.Try

class LoggingSupportSpec extends WordSpecLike with Matchers with Eventually with MockFactory {
  import scala.concurrent.ExecutionContext.Implicits.global

  // Because of buggy mocking (said lex).
  case class AddMetricsInvocation(name: String, tags: Seq[(String, String)])
  class MockStats extends NullMetrics {
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

  "LoggingSupport" should {

    val mockLogger = mock[Logger]
    val name = "test_name"

    "reportFutureResults on success" in {
      val stats = new MockStats
      val promise = Promise[Unit]()
      val future = promise.future
      val logString = "Faux log"

      inSequence {
        (mockLogger
          .info(_: String))
          .expects(where { logStr: String =>
            logStr.contains("Attempting") && logStr.contains(logString)
          })
        (mockLogger
          .info(_: String))
          .expects(where { logStr: String =>
            logStr.contains("Succeeded") && logStr.contains(logString)
          })
      }
      LoggingSupport.reportFutureResults(stats, mockLogger, name, Some(logString), future, additionalTags)

      promise.success()

      val tags = Seq(additionalTag, successTag)
      eventually {
        stats.invocations shouldEqual Seq(AddMetricsInvocation(name, tags))
      }
    }

    "reportFutureResults on failure" in {
      val stats = new MockStats
      val promise = Promise[Unit]()
      val future = promise.future
      val logString = "Fail log"

      inSequence {
        (mockLogger
          .info(_: String))
          .expects(where { logStr: String =>
            logStr.contains("Attempting") && logStr.contains(logString)
          })
        (mockLogger
          .error(_: String, _: Throwable))
          .expects(where { (logStr: String, _) =>
            logStr.contains("Failed") && logStr.contains(logString)
          })
      }

      LoggingSupport.reportFutureResults(stats, mockLogger, name, Some(logString), future, additionalTags)
      promise.failure(new IllegalStateException("Simulated exception."))

      val tags = Seq(additionalTag, failureTag, "exception" -> "IllegalStateException")
      eventually {
        stats.invocations shouldEqual Seq(AddMetricsInvocation(name, tags))
      }
    }

    "reportFutureResults logs nothing if given no log string" in {
      val stats = new MockStats
      val promise = Promise[Unit]()
      val future = promise.future
      val logString = "Fail log"

      (mockLogger.info(_: String)).expects(*).never
      (mockLogger.error(_: String)).expects(*).never

      LoggingSupport.reportFutureResults(stats, mockLogger, name, None, future)
      promise.success()
      Try(Await.result(future, Duration.Inf))
    }
  }

}
