package com.pagerduty.scheduler

import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.eris.dao.ErisSettings
import com.pagerduty.kafkaconsumer.SimpleKafkaConsumer
import com.pagerduty.metrics.Event.{AlertType, Priority}
import com.pagerduty.metrics.{Event, Metrics}
import com.pagerduty.scheduler.admin.AdminServiceImpl
import com.pagerduty.scheduler.akka.FailureResponse
import com.pagerduty.scheduler.dao._
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.gauge.StaleTasksGauge
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{Task, TaskAttempt, TaskKey, TaskStatus}
import com.typesafe.config.Config
import java.time.Instant

import com.pagerduty.metrics.gauge.GaugeReporter
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

/**
  * Abstract class which contains the base functions required for the scheduler
  *
  * @param schedulerSettings The settings object which contains the necessary properties for the scheduler
  */
abstract class Scheduler(
    schedulerSettings: SchedulerSettings,
    metrics: Metrics
  )(logging: Scheduler.Logging = new Scheduler.LoggingImpl(schedulerSettings, metrics)) {
  protected val schedulingGraceWindow = schedulerSettings.schedulingGraceWindow
  protected val kafkaTopic = schedulerSettings.kafkaTopic
  protected val kafkaConsumerGroup = schedulerSettings.kafkaConsumerGroup

  protected val kafkaConsumer: SchedulerKafkaConsumer

  /**
    * Starts up the scheduler service
    */
  def start(): Unit = {
    kafkaConsumer.start()
  }

  /**
    * Stops the scheduler service and releases the partitions,
    * blocks until the scheduler has shutdown.
    */
  def shutdown(): Unit = {
    logging.trackResourceShutdown("Scheduler") {
      Await.result(kafkaConsumer.shutdown(), Duration.Inf)
    }
  }

  def arePartitionsAssigned: Boolean = kafkaConsumer.arePartitionsAssigned
}

/**
  * Implementation of the Scheduler and the primary interface for users of this library.
  *
  * @param schedulerSettings The settings object which contains the necessary properties for the scheduler
  * @param config The raw Typesafe config for this library
  * @param cluster The Cassandra cluster used by this library
  * @param keyspace The Cassandra keyspace used by this library for storing tasks
  * @param taskExecutorServiceFactory A factory for creating disposable task executor services
  */
class SchedulerImpl(
    schedulerSettings: SchedulerSettings,
    config: Config,
    metrics: Metrics,
    cluster: Cluster,
    keyspace: Keyspace,
    taskExecutorServiceFactory: Set[PartitionId] => TaskExecutorService
  )(logging: Scheduler.Logging = new Scheduler.LoggingImpl(schedulerSettings, metrics))
    extends Scheduler(schedulerSettings, metrics)(logging) {

  private val gaugeReporter = new GaugeReporter

  val kafkaConsumer = {
    val kafkaConsumerProps = {
      // Start with the optional extra properties, so that
      // makeProps has a chance to have the final say.
      val props = schedulerSettings.kafkaProperties
      props.putAll(
        SimpleKafkaConsumer.makeProps(
          schedulerSettings.kafkaBootstrapBroker,
          kafkaConsumerGroup,
          schedulerSettings.maxPollRecords
        )
      )
      props
    }
    new SchedulerKafkaConsumer(
      schedulerSettings,
      config,
      cluster,
      keyspace,
      kafkaConsumerProps,
      taskExecutorServiceFactory,
      logging,
      metrics
    )
  }

  // Adds gauge to constantly check for the number of stale tasks in Cassandra
  val staleTasksGauge = new StaleTasksGauge(kafkaConsumer)
  val staleTaskSampleConsumer = logging.staleTasksGaugeSampleConsumer
  gaugeReporter.addGauge(staleTasksGauge, Set(staleTaskSampleConsumer))

  override def shutdown(): Unit = {
    gaugeReporter.stop()
    super.shutdown()
  }

  private lazy val erisSettings = new ErisSettings(metrics)

  private def getTaskScheduleDao(): TaskScheduleDao = {
    new TaskScheduleDaoImpl(cluster, keyspace, erisSettings)
  }

  private def getAttemptHistoryDao(): AttemptHistoryDao = {
    new AttemptHistoryDaoImpl(cluster, keyspace, erisSettings)
  }

  private def getTaskStatusDao(): TaskStatusDao = {
    new TaskStatusDaoImpl(cluster, keyspace, erisSettings)
  }

  lazy val adminService = new AdminServiceImpl(
    getTaskScheduleDao(),
    getTaskStatusDao(),
    getAttemptHistoryDao(),
    kafkaConsumer.triggerPartitionRebalancing _,
    kafkaConsumer.partitionCount _
  )
}

object Scheduler {
  import com.pagerduty.scheduler.LoggingSupport._
  import scala.concurrent.ExecutionContext.Implicits.global

  trait Logging {
    def staleTasksGaugeSampleConsumer: (Int) => Unit
    def monitorTasksSentToScheduler(persistTasksFuture: Future[Any], taskBatch: Map[PartitionId, Seq[Task]]): Unit
    def monitorTaskExecution(taskExecutionFuture: Future[Unit], task: Task): Unit

    def reportKafkaClusterLookupError(clusterTag: String): Unit
    def reportInMemoryTaskCount(partitionId: PartitionId, bufferName: String, count: Int)

    def reportTaskExecutionDelay(partitionId: PartitionId, task: Task, delay: Duration): Unit
    def reportTaskStatusNotFetched(taskKey: TaskKey, exception: Throwable): Unit
    def reportTaskAttemptFinished(partitionId: PartitionId, task: Task, taskAttempt: TaskAttempt): Unit

    def reportTaskStatusNotUpdated(taskKey: TaskKey, status: TaskStatus, exception: Throwable): Unit

    def reportRetryableRequestFailed(message: Any, response: FailureResponse, willRetry: Boolean): Unit
    def reportMaxTaskRetriesReached(task: Task, partitionId: PartitionId): Unit

    def reportPartitionsRevoked(partitions: Set[PartitionId]): Unit
    def reportIsolationDetectionWait(partitions: Set[PartitionId], duration: Duration): Unit
    def reportPartitionsAssigned(partitions: Set[PartitionId]): Unit

    def reportConsistencyCheckException(message: String, t: Throwable): Unit
    def reportConsistencyCheckResults(
        from: Instant,
        to: Instant,
        totalEnqueued: Int,
        incompleteTasks: Seq[TaskKey]
      ): Unit

    def reportActorSystemRestart(throwable: Throwable): Unit

    def trackResourceShutdown(resourceName: String)(block: => Unit): Unit
    def trackResourceAsyncShutdown(resourceName: String)(block: => Future[Unit]): Future[Unit]
  }

  class LoggingImpl(
      settings: SchedulerSettings,
      metrics: Metrics,
      attemptHistoryDao: Option[AttemptHistoryDao] = None,
      private val log: Logger = LoggerFactory.getLogger(getClass))
      extends Logging {
    private val consumerGroup = settings.kafkaConsumerGroup
    private val topic = settings.kafkaTopic
    private val taskDataTagNames = settings.taskDataTagNames

    def staleTasksGaugeSampleConsumer: (Int) => Unit = { (staleCount: Int) =>
      {
        log.info(s"Scheduler stale tasks result: $staleCount stale tasks.")
        metrics.histogram("stale_task_count", staleCount)
      }
    }

    def monitorTasksSentToScheduler(persistTasksFuture: Future[Any], taskBatch: Map[PartitionId, Seq[Task]]): Unit = {
      if (taskBatch.nonEmpty) {
        val logString = s"sending tasks to scheduler: ${taskBatch.values.flatten.map(_.taskKey)}"

        persistTasksFuture.onSuccess {
          case _ =>
            val taskCountByPartition = taskBatch.map {
              case (partitionId, tasks) => partitionId -> tasks.size
            }
            for ((parititonId, taskCount) <- taskCountByPartition) {
              metrics.count("tasks_persist_to_cass", taskCount, partitionTag(parititonId))
            }
        }
        reportFutureResults(metrics, log, "tasks_sent_to_akka", Some(logString), persistTasksFuture)
      }
    }

    def monitorTaskExecution(taskExecutionFuture: Future[Unit], task: Task): Unit = {
      val logString = Some(s"executing task: ${task.taskKey}.")
      reportFutureResults(
        metrics,
        log,
        "task_execution",
        logString,
        taskExecutionFuture,
        additionalTags(task, taskDataTagNames)
      )
    }

    def reportKafkaClusterLookupError(clusterTag: String): Unit = {
      log.error(s"Error looking up Kafka Cluster with tag: ${clusterTag}.")
    }

    def reportTaskExecutionDelay(partitionId: PartitionId, task: Task, delay: Duration): Unit = {
      val tags = additionalTags(task, taskDataTagNames) :+ partitionTag(partitionId)
      metrics.histogram("task_execution_delay", delay.toMillis.toInt, tags: _*)
    }

    def reportTaskStatusNotFetched(taskKey: TaskKey, exception: Throwable): Unit = {
      log.error(s"Failure determining if task with key $taskKey has been completed.", exception)
    }
    def reportTaskAttemptFinished(partitionId: PartitionId, task: Task, taskAttempt: TaskAttempt): Unit = {
      val tags = additionalTags(task, taskDataTagNames)
      val executionDuration = java.time.Duration.between(taskAttempt.startedAt, taskAttempt.finishedAt).toScalaDuration
      metrics.histogram("task_execution_duration", executionDuration.toMillis.toInt, tags: _*)
      attemptHistoryDao.foreach { dao =>
        dao
          .insert(partitionId, task.taskKey, taskAttempt)
          .onFailure {
            case NonFatal(e) => log.error(s"Exception when saving $taskAttempt for $task.", e)
          }
      }
    }
    def reportTaskStatusNotUpdated(taskKey: TaskKey, status: TaskStatus, exception: Throwable): Unit = {
      log.error(s"Couldn't mark task with key $taskKey as $status.", exception)
    }

    def reportTaskRetry(taskKey: TaskKey, delay: Duration): Unit = {
      val approximateTargetTime = Instant.now() + delay
      log.warn(s"Retrying $taskKey in $delay, at approximately $approximateTargetTime.")
    }

    def reportRetryableRequestFailed(message: Any, response: FailureResponse, willRetry: Boolean): Unit = {
      log.warn(s"Retryable request: $message failed. Response was $response.")
      if (willRetry) {
        log.warn(s"Will retry request: $message...")
      } else {
        log.warn(s"Reached max attempts for request: $message. Giving up!")
      }
    }

    def reportMaxTaskRetriesReached(task: Task, partitionId: PartitionId): Unit = {
      log.error(s"Max retries reached for task $task. Marking task as failed.")
      val tags = additionalTags(task, taskDataTagNames) :+ partitionTag(partitionId)
      metrics.increment("task_retries_exhausted", tags: _*)
    }

    def reportInMemoryTaskCount(partitionId: PartitionId, bufferName: String, count: Int): Unit = {
      val tag = partitionTag(partitionId)
      metrics.histogram(s"${bufferName}_task_count", count, tag)
    }

    def reportConsistencyCheckResults(
        from: Instant,
        to: Instant,
        totalEnqueued: Int,
        incompleteTasks: Seq[TaskKey]
      ): Unit = {
      metrics.count("checker_total_enqueued", totalEnqueued)
      metrics.count("checker_incomplete", incompleteTasks.size)

      if (incompleteTasks.nonEmpty) {
        val incompleteTasksString = incompleteTasks.mkString(", ")
        reportConsistencyCheckException(
          s"After verifying $totalEnqueued tasks, found enqueued but incomplete tasks " +
            s"in range $from to $to: $incompleteTasksString.",
          new RuntimeException("Consistency check failed.")
        )
      } else {
        log.info(
          s"Consistency check successfully verified $totalEnqueued tasks " +
            s"in range $from to $to."
        )
      }
    }

    def reportPartitionsRevoked(partitions: Set[PartitionId]): Unit = {
      recordRebalancingEvent()
      val partitionsString = partitions.toSeq.sorted.mkString(", ")
      log.info(
        s"Rebalancing group=$consumerGroup for topic=$topic, " +
          s"partitions revoked [$partitionsString]."
      )
    }

    def reportIsolationDetectionWait(partitions: Set[PartitionId], duration: Duration): Unit = {
      val partitionsString = partitions.toSeq.sorted.mkString(", ")
      log.info(
        s"Holding group=$consumerGroup for topic=$topic, " +
          s"partitions [$partitionsString] for $duration for proper isolation detection."
      )
    }

    def reportPartitionsAssigned(partitions: Set[PartitionId]): Unit = {
      val partitionsString = partitions.toSeq.sorted.mkString(", ")
      log.info(
        s"Rebalancing group=$consumerGroup for topic=$topic, " +
          s"partitions assigned [$partitionsString]."
      )
    }

    def reportConsistencyCheckException(message: String, t: Throwable): Unit = {
      log.error(s"SchedulerConsistencyCheckError: $message", t)
    }

    def reportActorSystemRestart(throwable: Throwable): Unit = {
      val tag = ("throwable", throwable.getClass.getSimpleName)
      metrics.increment("sys_restart_on_error", tag)
    }

    def trackResourceShutdown(resourceName: String)(codeBlock: => Unit): Unit = {
      log.info(s"Shutting down $resourceName...")
      codeBlock
      log.info(s"$resourceName was shut down.")
    }

    def trackResourceAsyncShutdown(resourceName: String)(codeBlock: => Future[Unit]): Future[Unit] = {
      log.info(s"Shutting down $resourceName...")
      val future = codeBlock
      future.onSuccess {
        case _ => log.info(s"$resourceName was shut down.")
      }
      future
    }

    private def recordRebalancingEvent(): Unit = {
      val event = Event(
        "Rebalancing Kafka Consumers",
        s"Rebalancing group=$consumerGroup for topic=$topic.",
        System.currentTimeMillis,
        AlertType.INFO,
        Priority.LOW
      )
      metrics.recordEvent(event)
    }

    private def partitionTag(partitionId: PartitionId): (String, String) = {
      "partition" -> f"$partitionId%02d"
    }
  }
}
