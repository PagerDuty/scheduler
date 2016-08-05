package com.pagerduty.scheduler.gauge

import com.pagerduty.scheduler.specutil.UnitSpec
import org.scalamock.scalatest.MockFactory

class GaugeSpec extends UnitSpec with MockFactory {

  val testSample = 1234

  class TestGauge extends Gauge[Int] {
    def doSample: Int = testSample
  }

  "A Gauge" should {
    val gauge = new TestGauge

    "sample" in {
      gauge.sample should equal(testSample)
    }

    "run callbacks after a sample" in {
      val callback = stub[(Int) => Unit]
      gauge.registerOnSampleCallback(callback)

      gauge.sample

      (callback.apply _).verify(testSample)
    }
  }

}
