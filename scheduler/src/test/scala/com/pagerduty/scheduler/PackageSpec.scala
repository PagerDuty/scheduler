package com.pagerduty.scheduler

import com.pagerduty.scheduler.specutil.UnitSpec
import scala.concurrent.duration._

class PackageSpec extends UnitSpec {
  "scheduler package" should {
    "have correct blockingWait" in {
      val waitDuration = 800.millis
      val maxError = 400.millis
      val start = System.nanoTime().nanos
      blockingWait(waitDuration)
      val elapsed = System.nanoTime().nanos - start
      elapsed should be >= waitDuration
      elapsed should be < waitDuration + maxError
    }
  }
}
