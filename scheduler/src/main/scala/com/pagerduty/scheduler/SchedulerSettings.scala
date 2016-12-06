package com.pagerduty.scheduler

import com.typesafe.config._

import scala.concurrent.duration.FiniteDuration

case class SchedulerSettings(
  kafkaBootstrapBroker: String,
  kafkaTopic: String,
  kafkaConsumerGroup: String,
  kafkaPdConsumerRestartOnExceptionDelay: FiniteDuration,
  schedulingGraceWindow: FiniteDuration,
  maxTasksFetchedPerPartition: Int,
  taskDataTagNames: Set[String],
  maxPollRecords: Option[Int]
)

object SchedulerSettings {
  import collection.JavaConversions._
  private val configPrefix = "scheduler"
  def apply(config: Config): SchedulerSettings = {
    config.checkValid(ConfigFactory.defaultReference(), configPrefix)
    val libConfig = config.getConfig(configPrefix)
    SchedulerSettings(
      kafkaBootstrapBroker = "localhost:9092",
      kafkaTopic = libConfig.getString("kafka.topic"),
      kafkaConsumerGroup = libConfig.getString("kafka.consumer-group"),
      kafkaPdConsumerRestartOnExceptionDelay = getDuration(libConfig, "kafka.pd-simple-consumer.restart-on-exception-delay"),
      schedulingGraceWindow = getDuration(libConfig, "scheduling-grace-window"),
      maxTasksFetchedPerPartition = libConfig.getInt("stats.max-task-fetch-per-partition"),
      taskDataTagNames = libConfig.getStringList("stats.task-data-tag-names").toSet,
      maxPollRecords = {
        if (libConfig.hasPath("kafka.max-poll-records")) Some(libConfig.getInt("kafka.max-poll-records"))
        else None
      }
    )
  }
}
