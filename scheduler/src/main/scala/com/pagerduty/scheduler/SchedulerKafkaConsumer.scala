package com.pagerduty.scheduler

import java.util.{Collection, Properties}

import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.scheduler.akka.SchedulingSystem
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.model.Task.PartitionId
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import com.pagerduty.kafkaconsumer.{ConsumerMetrics, SimpleKafkaConsumer}
import com.pagerduty.metrics.Metrics

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * The kafka consumer used by the scheduler. It consumes the messages created when a task is
  * scheduled and spawns the scheduling actor system to schedule and execute tasks.
  * @param schedulerSettings
  * @param config
  * @param cluster
  * @param keyspace
  * @param kafkaConsumerProperties
  * @param taskExecutorServiceFactory
  */
class SchedulerKafkaConsumer(
    schedulerSettings: SchedulerSettings,
    config: Config,
    cluster: Cluster,
    keyspace: Keyspace,
    kafkaConsumerProperties: Properties,
    taskExecutorServiceFactory: Set[PartitionId] => TaskExecutorService,
    logging: Scheduler.Logging,
    metrics: Metrics)
    extends SimpleKafkaConsumer[String, String](
      schedulerSettings.kafkaTopic,
      kafkaConsumerProperties,
      pollTimeout = 400.millis,
      restartOnExceptionDelay = schedulerSettings.kafkaPdConsumerRestartOnExceptionDelay,
      commitOffsetTimeout = 10.seconds,
      metrics = new SchedulerConsumerMetrics(metrics)
    ) {

  @volatile private var initializedSchedulingSystem: Try[SchedulingSystem] = {
    Failure(new RuntimeException("Scheduling system was not initialized."))
  }

  protected val maxMessageProcessingDuration = akka.Settings(config).askPersistRequestTimeout

  def arePartitionsAssigned: Boolean = initializedSchedulingSystem.isSuccess

  def countStaleTasks: Int = {
    initializedSchedulingSystem match {
      case Success(scheduler) => {
        partitionCount match {
          case Some(numPartitions) => {
            // Calculates stale tasks across all partitions, regardless of if they are assigned or not.
            val allPartitions = (0 until numPartitions).toSet
            scheduler.calculateStaleTasks(allPartitions, schedulerSettings.maxTasksFetchedPerPartition)
          }
          case None => {
            0
          }
        }
      }
      case _ => 0
    }
  }

  protected def deserializeTaskBatch(records: ConsumerRecords[String, String]): Map[PartitionId, Seq[Task]] = {
    val recordsByPartition = records.groupBy(_.partition)
    recordsByPartition.map {
      case (partitionId, consumerRecords) =>
        val taskSequence = consumerRecords.toSeq.map(deserializeTask)
        partitionId -> taskSequence
    }
  }

  override protected def shutdownResources(): Unit = {
    initializedSchedulingSystem.foreach(_.shutdownAndWait())
  }

  private def deserializeTask(record: ConsumerRecord[String, String]): Task = {
    Task.fromJson(record.value)
  }

  /**
    * Listens for changes in partitions and shuts down the entire system upon a change in the
    * number of partitions
    *
    */
  override protected def makeRebalanceListener() = new ConsumerRebalanceListener {
    def partitionIdSet(partitions: Collection[TopicPartition]): Set[PartitionId] = {
      val filteredPartitions = partitions.filter(_.topic == topic)
      filteredPartitions.map(_.partition).toSet
    }
    override def onPartitionsRevoked(partitions: Collection[TopicPartition]): Unit = {
      shutdownResources()
      val partitionIds = partitionIdSet(partitions)
      val isolationDetectionDuration = minIsolationDetectionDuration(maxMessageProcessingDuration)
      logging.reportIsolationDetectionWait(partitionIds, isolationDetectionDuration)
      blockingWait(isolationDetectionDuration)
      logging.reportPartitionsRevoked(partitionIds)
    }
    override def onPartitionsAssigned(partitionAssignment: Collection[TopicPartition]): Unit = {
      initializedSchedulingSystem = Try {
        val schedulerPartitions = partitionIdSet(partitionAssignment)
        logging.reportPartitionsAssigned(schedulerPartitions)
        new SchedulingSystem(
          config,
          cluster,
          keyspace,
          schedulerPartitions,
          taskExecutorServiceFactory,
          logging,
          metrics
        )
      }
    }
  }

  /**
    * Processes the records sent by the kafka producer, deserializes the tasks and proceeds to
    * schedule them. Upon failure, it will shut down the entire scheduling system.
    *
    */
  protected def processRecords(records: ConsumerRecords[String, String]): Unit = {
    initializedSchedulingSystem match {
      case Success(system) => sendDeserializedRecordsToSchedulingSystem(system, records)
      case Failure(e) => throw e
    }
  }

  protected def sendDeserializedRecordsToSchedulingSystem(
      schedulingSystem: SchedulingSystem,
      records: ConsumerRecords[String, String]
    ): Unit = {
    val deserializedBatch = deserializeTaskBatch(records)
    schedulingSystem.persistAndSchedule(deserializedBatch)
  }

  private[scheduler] def triggerPartitionRebalancing(): Unit = {
    shutdownResources()
  }
}

class SchedulerConsumerMetrics(metrics: Metrics) extends ConsumerMetrics {
  def timeCommitSync(f: => Unit): Unit = {
    metrics.time("kafka_consumer.commit_sync")(f)
  }
}
