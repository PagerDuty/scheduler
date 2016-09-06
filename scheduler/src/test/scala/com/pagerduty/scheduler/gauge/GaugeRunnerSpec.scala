package com.pagerduty.scheduler.gauge

import com.pagerduty.scheduler.Scheduler
import com.pagerduty.scheduler.specutil.UnitSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

class GaugeRunnerSpec extends UnitSpec with MockFactory with Eventually {

  val sampleException = new Exception("test exception")

  class TestGauge extends Gauge[Int] {
    var numSamples = 0

    def doSample: Int = {
      numSamples += 1
      if (numSamples == 2) {
        throw sampleException
      } else {
        1
      }
    }
  }

  "A GaugeRunner" should {
    val mockLogging = stub[Scheduler.Logging]
    val gaugeRunner = new GaugeRunner(mockLogging)

    "periodically tell a gauge to sample, reporting any failures" in {
      val gauge = new TestGauge

      gaugeRunner.runGauge(gauge, 200.milliseconds)

      eventually(timeout(990.milliseconds)) {
        // Travis really runs slow - allow it to miss 1 call
        gauge.numSamples should (be > (4) and be <= (5))
      }

      gaugeRunner.stopGauges

      (mockLogging.reportGaugeFailure _).verify(gauge, sampleException)
    }
  }

}
