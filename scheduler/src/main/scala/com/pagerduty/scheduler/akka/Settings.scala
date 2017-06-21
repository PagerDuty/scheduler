package com.pagerduty.scheduler.akka

import scala.concurrent.duration._
import com.typesafe.config.{Config, ConfigFactory}
import com.pagerduty.scheduler.getDuration

/**
  * Dedicated settings wrapper for the SchedulingSystem.
  */
case class Settings(
    exponentialBackoffFactor: FiniteDuration,
    maxTaskBackoffPeriod: FiniteDuration,
    maxTaskRetries: Int,
    persistRequestTimeout: FiniteDuration,
    lookBackOnRestart: FiniteDuration,
    maxInFlightTasks: Int,
    taskFetchBatchSize: Int,
    minTickDelay: FiniteDuration,
    maxLookAhead: FiniteDuration,
    prefetchWindow: FiniteDuration,
    timeUntilStaleTask: FiniteDuration,
    maxDataAccessAttempts: Int) {
  val askPersistRequestTimeout = persistRequestTimeout + 100.millis
}

object Settings {
  private val configPrefix = "scheduler"
  def apply(config: Config = ConfigFactory.load()): Settings = {
    config.checkValid(ConfigFactory.defaultReference(), configPrefix)
    val libConfig = config.getConfig(configPrefix)
    Settings(
      exponentialBackoffFactor = getDuration(libConfig, "exponential-backoff-factor"),
      maxTaskBackoffPeriod = getDuration(libConfig, "max-task-backoff-period"),
      maxTaskRetries = libConfig.getInt("max-task-retries"),
      persistRequestTimeout = getDuration(libConfig, "persist-request-timeout"),
      lookBackOnRestart = getDuration(libConfig, "look-back-on-restart"),
      maxInFlightTasks = libConfig.getInt("max-in-flight-tasks"),
      taskFetchBatchSize = libConfig.getInt("task-fetch-batch-size"),
      minTickDelay = getDuration(libConfig, "min-tick-delay"),
      maxLookAhead = getDuration(libConfig, "max-look-ahead"),
      prefetchWindow = getDuration(libConfig, "prefetch-window"),
      timeUntilStaleTask = getDuration(libConfig, "time-until-tasks-stale"),
      maxDataAccessAttempts = libConfig.getInt("max-data-access-attempts")
    )
  }
}
