package com.pagerduty.scheduler

import com.pagerduty.metrics.Metrics
import com.pagerduty.scheduler.model.{Task, TaskKey}
import java.time.Instant
import java.net.InetAddress
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NonFatal

/**
  * Application interface to the scheduler.
  */
class SchedulerClient(
    kafkaProducer: Producer[String, String],
    kafkaTopic: String,
    schedulingGraceWindow: Duration,
    metrics: Metrics
  )(logging: SchedulerClient.Logging = new SchedulerClient.LoggingImpl(metrics)) {

  val numPartitions = kafkaProducer.partitionsFor(kafkaTopic).size()

  /**
    * Schedules a task to be run at a given time.
    *
    * @param task The task to be scheduled. Note that if the task is scheduled too far in the past,
    *             as defined by scheduler.scheduling-grace-window, this method will immediately
    *             return a failed future.
    * @return A future that will eventually be successful if the task has been scheduled. Failed
    *         scheduling results in an eventually failed future.
    */
  def scheduleTask(task: Task): Future[Unit] = {
    val timeDiff = Duration.fromNanos(java.time.Duration.between(task.scheduledTime, Instant.now()).toNanos)
    if (timeDiff > schedulingGraceWindow) {
      Future.failed(new IllegalArgumentException("Attempting to enqueue too far in the past."))
    } else {
      val future = doScheduleTask(task)
      logging.monitorTaskEnqueueToKafka(future, task)
      future
    }
  }

  private def doScheduleTask(task: Task): Future[Unit] = {
    val message = task.toJson
    val record = new ProducerRecord[String, String](
      kafkaTopic,
      task.partitionId(numPartitions),
      task.orderingId,
      message
    )
    val promise = Promise[Unit]()
    kafkaProducer.send(
      record,
      new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (Option(metadata).isDefined) promise.success(Unit)
          else promise.failure(exception)
        }
      }
    )
    promise.future
  }

}

object SchedulerClient {

  /**
    * Helper to make a Kafka configuration appropriate for a Scheduler Kafka client.
    *
    * @param kafkaHostPort The host:port of a Kafka bootstrap server
    * @return
    */
  def makeKafkaConfig(kafkaHostPort: String = "localhost:9092"): java.util.Map[String, Object] = {
    import scala.collection.JavaConverters._

    Map(
      "bootstrap.servers" -> s"$kafkaHostPort",
      "acks" -> "all",
      "compression.type" -> "snappy",
      "client.id" -> s"scheduler-${InetAddress.getLocalHost}-${Thread.currentThread().getId}",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "max.block.ms" -> 5 * 1000
    ).asJava.asInstanceOf[java.util.Map[String, Object]]
  }

  trait Logging {
    def monitorTaskEnqueueToKafka(future: Future[Unit], task: Task): Unit
  }

  class LoggingImpl(metrics: Metrics, private val log: Logger = LoggerFactory.getLogger(classOf[SchedulerClient]))
      extends Logging {
    import scala.concurrent.ExecutionContext.Implicits.global
    import com.pagerduty.scheduler.LoggingSupport._

    def monitorTaskEnqueueToKafka(taskEnqueueFuture: Future[Unit], task: Task): Unit = {
      val logString = Some(s"enqueuing task to Kafka: ${task.taskKey}.")
      reportFutureResults(
        metrics,
        log,
        "task_enqueue_to_kafka",
        logString,
        taskEnqueueFuture
      )
    }
  }
}
