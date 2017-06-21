package com.pagerduty.scheduler

import com.pagerduty.metrics.{Event, Metrics, NullMetrics}
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.specutil.CassandraIntegrationSpec
import com.typesafe.config.ConfigFactory
import java.time.Instant
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, Matchers}
import com.pagerduty.kafkaconsumer.testsupport.CleanupConsumer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

// IMPORTANT:
//
// - See the README for manual Setup Steps before running this test!
//
// - The timing in these tests requires specific values in the config file
//   `src/it/resources/application.conf`. If you change the timing of tests
//   here, fix up the config file as needed.
//

abstract class SchedulerIntegrationSpecBase
    extends CassandraIntegrationSpec
    with Matchers
    with BeforeAndAfterAll
    with Eventually {

  println(s"Started ${getClass.getSimpleName}.")

  val schedulerWarmUpOrSlowTestTimeout = 180.seconds
  val taskEarlyByMax = 200.milliseconds
  val taskLateByMax = 20.seconds

  // FIXME: load this from src/it/resources/application.conf for DRYness
  val topic = "scheduler-integration-test-queue"

  val config = ConfigFactory.load()

  override def beforeAll() {
    drainOldMessages()
    super.beforeAll()
  }

  def drainOldMessages(): Unit = {
    val consumer = new CleanupConsumer(topic)
    consumer.start()
    consumer.awaitTermination()
  }

  def sortedTasks(tasks: List[Task]): List[Task] = {
    tasks.sortBy(_.taskKey)
  }

  class TestScheduler {
    var actualScheduler: SchedulerImpl = _
    var actualSchedulerClient: SchedulerClient = _

    // TODO Any operations on the mutable concurrent state represented by these vars must be in synchronized block.
    var executedTasks: List[Task] = Nil
    var executedTaskTimes: List[Instant] = Nil
    var oopsedTasks: List[Task] = Nil

    def taskExecutorServiceFactory = SchedulerIntegrationSpecBase.simpleTestExecutorFactory(taskRunner)

    // init & warmup
    {
      val schedulerSettings = SchedulerSettings(ConfigFactory.load())
      actualScheduler = new SchedulerImpl(
        schedulerSettings,
        config,
        NullMetrics,
        cluster,
        keyspace,
        taskExecutorServiceFactory
      )(
        logging = new Scheduler.LoggingImpl(schedulerSettings, NullMetrics, Some(attemptHistoryDaoImpl))
      )
      actualScheduler.start()
      actualSchedulerClient = {
        val kafkaProducer = new KafkaProducer[String, String](SchedulerClient.makeKafkaConfig())
        new SchedulerClient(
          kafkaProducer,
          schedulerSettings.kafkaTopic,
          schedulerSettings.schedulingGraceWindow,
          NullMetrics
        )()
      }
      eventually(timeout(schedulerWarmUpOrSlowTestTimeout)) {
        actualScheduler.arePartitionsAssigned shouldBe true
      }
    }

    def shutdown(): Unit = {
      actualScheduler.shutdown()
      actualScheduler = null
    }

    def scheduleTask(task: Task): Unit = {
      Await.result(actualSchedulerClient.scheduleTask(task), Duration.Inf)
    }

    def tasksShouldRunNow(expectedTasks: List[Task], expectedOopsedTasks: List[Task] = Nil): Unit = {
      tasksShouldRunAt(expectedTasks, Instant.now(), expectedOopsedTasks, false)
    }

    def noTasksRunBy(by: Instant): Unit = {
      executedTasks shouldBe Nil
      val sleepForMillis = java.time.Duration.between(Instant.now() + taskLateByMax, by).toMillis
      if (sleepForMillis > 0) {
        Thread.sleep(sleepForMillis)
      }
      tasksShouldRunAt(Nil, by)
    }

    def tasksShouldRunAt(
        expectedTasks: List[Task],
        expectedAt: Instant,
        expectedOopsedTasks: List[Task] = Nil,
        checkTaskTimes: Boolean = true
      ): Unit = {
      tasksShouldConditionBy(expectedAt, checkTaskTimes) {
        sortedTasks(executedTasks) shouldBe sortedTasks(expectedTasks)
        sortedTasks(oopsedTasks) shouldBe sortedTasks(expectedOopsedTasks)
      }
    }

    def tasksShouldConditionBy(shouldBy: Instant, checkTaskTimes: Boolean)(shouldCondition: => Unit): Unit = {
      val timeoutAt = shouldBy + schedulerWarmUpOrSlowTestTimeout
      val timeoutIn = java.time.Duration.between(Instant.now(), timeoutAt).toScalaDuration.max(0.seconds)
      try {
        eventually(timeout(timeoutIn)) {
          shouldCondition
        }
      } catch {
        case NonFatal(e) =>
          println(s"###### $e")
          throw new Exception(e)
      }
      if (checkTaskTimes) {
        executedTaskTimes.foreach { taskTime =>
          val taskLateBy = java.time.Duration.between(shouldBy, taskTime).toScalaDuration.toMillis
          taskLateBy should be >= -taskEarlyByMax.toMillis
          taskLateBy should be <= taskLateByMax.toMillis
        }
      }
      executedTasks = Nil
      executedTaskTimes = Nil
      oopsedTasks = Nil
    }

    def taskRunner(task: Task): Unit = {
      executedTasks = task :: executedTasks
      executedTaskTimes = Instant.now() :: executedTaskTimes
      task.taskData
        .get("throw!")
        .foreach(_ => {
          oopsedTasks = task :: oopsedTasks
          throw new Exception("Task throws exception for testing purposes.")
        })
    }
  }
}

object SchedulerIntegrationSpecBase {
  def simpleTestExecutorFactory(taskRunner: Task => Unit) =
    (_: Set[PartitionId]) =>
      new TaskExecutorService {
        private val executionContext = FixedSizeExecutionContext(2)

        def execute(partitionId: PartitionId, task: Task): Future[Unit] = {
          Future(
            taskRunner(task)
          )(executionContext)
        }

        def shutdown(): Unit = {
          executionContext.shutdown()
        }
    }
}
