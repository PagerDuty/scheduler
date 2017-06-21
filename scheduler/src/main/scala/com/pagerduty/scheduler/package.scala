package com.pagerduty

import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

package object scheduler {

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
