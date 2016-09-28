package com.pagerduty.scheduler

import java.time.Instant
import java.time.temporal.ChronoField
import scala.concurrent.duration._

package object datetimehelpers {

  implicit class InstantExt(time: Instant) {
    def inHours: Int = (time.getEpochSecond() / 3600).toInt

    def +(duration: Duration): Instant = time.plusNanos(duration.toNanos)
    def -(duration: Duration): Instant = time.minusNanos(duration.toNanos)
  }

  implicit class JavaDurationExt(d: java.time.Duration) {
    def toScalaDuration: FiniteDuration = Duration.fromNanos(d.toNanos)
  }
}
