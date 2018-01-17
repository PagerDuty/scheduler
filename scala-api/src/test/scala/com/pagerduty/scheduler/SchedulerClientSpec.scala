package com.pagerduty.scheduler

import com.pagerduty.metrics.Metrics
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task
import java.time.Instant
import java.util
import java.util.concurrent.{ExecutionException, TimeUnit, Future => JFuture}

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class SchedulerClientSpec extends WordSpecLike with Matchers with ScalaFutures with MockFactory {
  val schedulingGraceWindow = 42.seconds
  val kafkaTopic = "Test_topic_please_ignore"
  val kafkaNumPartitions = 10

  class StubKafkaProducer extends Producer[String, String] {
    private val partitionInfos = {
      import scala.collection.JavaConverters._
      (0 until kafkaNumPartitions).map(_ => null.asInstanceOf[PartitionInfo]).asJava
    }
    override def partitionsFor(topic: String): util.List[PartitionInfo] = {
      partitionInfos
    }

    @volatile private var metadata: RecordMetadata = _
    @volatile private var exception: Exception = _
    def completeCallbackWith(metadata: RecordMetadata, exception: Exception): Unit = {
      this.metadata = metadata
      this.exception = exception
    }

    override def send(record: ProducerRecord[String, String], callback: Callback): JFuture[RecordMetadata] = {
      callback.onCompletion(metadata, exception)
      null.asInstanceOf[JFuture[RecordMetadata]]
    }

    override def flush(): Unit = ???
    override def metrics(): util.Map[MetricName, _ <: Metric] = ???
    override def send(record: ProducerRecord[String, String]): JFuture[RecordMetadata] = ???
    override def close(): Unit = ???
    override def close(timeout: Long, unit: TimeUnit): Unit = ???
    override def sendOffsetsToTransaction(
        offsets: util.Map[TopicPartition, OffsetAndMetadata],
        consumerGroupId: String
      ): Unit = ???
    override def abortTransaction(): Unit = ???
    override def initTransactions(): Unit = ???
    override def beginTransaction(): Unit = ???
    override def commitTransaction(): Unit = ???
  }

  trait LocalVals {
    val stubKafkaProducer = new StubKafkaProducer
    val schedulerClient = new SchedulerClient(
      stubKafkaProducer,
      kafkaTopic,
      schedulingGraceWindow,
      stub[Metrics]
    )(
      stub[SchedulerClient.Logging]
    )
  }

  "A Scheduler" when {
    "scheduling a task with an allowed scheduledTime" when {
      val task = Task.example
      val orderingId = task.orderingId
      val partitionId = task.partitionId(kafkaNumPartitions)

      "successful" should {
        val result = new RecordMetadata(new TopicPartition(kafkaTopic, 0), 0L, 0L, 0, java.lang.Long.valueOf(0L), 0, 0)

        "return a successful future" in new LocalVals {
          stubKafkaProducer.completeCallbackWith(result, null)
          val future = schedulerClient.scheduleTask(task)
          future.futureValue should equal(())
        }
      }

      "unsuccessful" should {
        val exception = new ExecutionException("Test Exception", new Exception)

        "return a failed future with the thrown exception" in new LocalVals {
          stubKafkaProducer.completeCallbackWith(null, exception)
          val future = schedulerClient.scheduleTask(task)
          future.failed.futureValue should equal(exception)
        }
      }
    }

    "scheduling a task that is too old to be scheduled" should {
      val task = Task.example.copy(scheduledTime = Instant.now() - (schedulingGraceWindow + 1.second))

      "return a failed future" in new LocalVals {
        schedulerClient.scheduleTask(task).failed.futureValue shouldBe an[Exception]
      }
    }
  }
}
