package com.pagerduty

import java.util.concurrent.TimeUnit
import com.typesafe.config.Config
import scala.concurrent.duration._
import scala.language.{ implicitConversions, postfixOps }

package object scheduler {

  implicit def scalaDurationToTwitter(duration: Duration): com.twitter.util.Duration = {
    com.twitter.util.Duration.fromNanoseconds(duration.toNanos)
  }

  implicit class TwitterDurationExt(duration: com.twitter.util.Duration) {
    def toScalaDuration: FiniteDuration = Duration.fromNanos(duration.inNanoseconds)
  }

  private[scheduler] def getDuration(config: Config, key: String): FiniteDuration = {
    Duration.fromNanos(config.getDuration(key, TimeUnit.NANOSECONDS))
  }

  private[scheduler] def blockingWait(duration: Duration): Unit = {
    val destTime = System.nanoTime().nanos + duration
    while (destTime > System.nanoTime().nanos) {
      val remainingMs = (destTime - System.nanoTime().nanos).toMillis.max(0)
      try {
        Thread.sleep(remainingMs)
      } catch {
        case e: InterruptedException => // Ignore.
      }
    }
  }
}
