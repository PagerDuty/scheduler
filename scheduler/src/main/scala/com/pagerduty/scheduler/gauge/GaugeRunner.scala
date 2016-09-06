package com.pagerduty.scheduler.gauge

import java.util.concurrent.{ Executors, ScheduledExecutorService, ThreadFactory, TimeUnit }

import com.pagerduty.scheduler.Scheduler

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class GaugeRunner(logging: Scheduler.Logging) {
  private object ExecutorsLock
  private var executors: Set[ScheduledExecutorService] = Set()

  def runGauge(gauge: Gauge[_], samplePeriod: FiniteDuration): Unit = {
    val gaugeRunnable = new Runnable {
      override def run(): Unit = {
        try {
          gauge.sample
        } catch {
          case NonFatal(e) => logging.reportGaugeFailure(gauge, e)
        }
      }
    }

    // Spawns a daemon thread to sample the gauge at a fixed interval
    val executor = getGaugeExecutorService
    ExecutorsLock.synchronized(executors += executor)
    executor.scheduleAtFixedRate(gaugeRunnable, 0, samplePeriod.toMillis, TimeUnit.MILLISECONDS)
  }

  def stopGauges: Unit = {
    ExecutorsLock.synchronized {
      executors foreach (_.shutdownNow())
      executors = Set()
    }
  }

  private def getGaugeExecutorService: ScheduledExecutorService = {
    val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactory() {
        override def newThread(runnable: Runnable): Thread = {
          val thread = new Thread(runnable)
          thread.setDaemon(true)
          thread
        }
      }
    )
    executor
  }
}
