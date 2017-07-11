package com.pagerduty.scheduler.akka

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.pattern.AskTimeoutException
import akka.util.Timeout
import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.eris.dao.ErisSettings
import com.pagerduty.metrics.Metrics
import com.pagerduty.scheduler.dao.{TaskScheduleDaoImpl, TaskStatusDaoImpl}
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.{Scheduler, TaskExecutorService, _}
import com.typesafe.config.Config
import java.time.Instant
import scala.annotation.tailrec
import scala.concurrent.{Await, TimeoutException}
import scala.concurrent.duration._

object SchedulingSystem {
  val PersistPollingPeriod = 250.milliseconds
}

/**
  * SchedulingSystem manages lifecycle of the underlying Akka system and allows to interact with
  * actors using a simple method-based interface.
  * @param partitions the ids of the partitions
  */
class SchedulingSystem(
    config: Config,
    cluster: Cluster,
    keyspace: Keyspace,
    partitions: Set[PartitionId],
    taskExecutorServiceFactory: Set[PartitionId] => TaskExecutorService,
    logging: Scheduler.Logging,
    metrics: Metrics) {
  import SchedulingSystem._

  private val settings = Settings(config)
  private val askPersistRequestTimeout = settings.askPersistRequestTimeout
  private val erisSettings = new ErisSettings(metrics)
  private lazy val taskScheduleDao = new TaskScheduleDaoImpl(cluster, keyspace, erisSettings)

  /**
    * Calculates the number of stale tasks across all partitions, assigned and unasssigned
    *
    * @return the number of stale tasks in ScheduledColumnFamily
    */
  def calculateStaleTasks(allPartitionIds: Set[PartitionId], limit: Int): Int = {
    val from = Instant.now() - settings.lookBackOnRestart
    val to = Instant.now() - settings.timeUntilStaleTask
    val totalTaskCount = taskScheduleDao.getTotalTaskCount(
      allPartitionIds,
      from,
      to,
      limit
    )
    Await.result(totalTaskCount, Duration.Inf)
  }

  protected val system = {
    val akkaConfig = config.getConfig("scheduler")
    val actorSystem = ActorSystem("Scheduler", akkaConfig)
    /*
     * All the resources external to the actor system that it uses will shutdown upon
     * the actor system successfully shutting down
     */
    actorSystem.registerOnTermination(shutdownResources())
    actorSystem
  }
  protected val taskExecutorService = taskExecutorServiceFactory(partitions)
  protected lazy val queueSupervisor = {
    val taskStatusDao = new TaskStatusDaoImpl(cluster, keyspace, erisSettings)

    val queueContext = {
      QueueContext(taskScheduleDao, taskStatusDao, taskExecutorService, logging)
    }
    val props = TopicSupervisor.props(settings, queueContext, partitions)
    system.actorOf(props, "queueSupervisor")
  }

  /**
    * Forwards tasks for scheduling and execution, blocking until all the tasks have been
    * durably stored in Cassandra. This method throws an exception if there was a problem
    * persisting some tasks.
    *
    * @param taskBatch a batch of tasks
    */
  def persistAndSchedule(taskBatch: Map[PartitionId, Seq[Task]]): Unit = {
    implicit val timeout = Timeout(askPersistRequestTimeout)
    val future = queueSupervisor ? TopicSupervisor.ProcessTaskBatch(taskBatch)
    logging.monitorTasksSentToScheduler(future, taskBatch)

    @tailrec
    def waitForPersistence: Unit = {
      try {
        Await.result(future, PersistPollingPeriod) match {
          case TopicSupervisor.TaskBatchProcessed => // nothing to do
          case TopicSupervisor.TaskBatchNotProcessed(t) =>
            throw new RuntimeException("Exception processing task batch!", t)
          case m =>
            throw new RuntimeException(s"Unexpected response from TopicSupervisor: $m")
        }
      } catch {
        case e: AskTimeoutException =>
          throw e
        case _: TimeoutException =>
          if (system.isTerminated) {
            throw new RuntimeException("Actor System has terminated during task persistence")
          } else {
            waitForPersistence
          }
      }
    }

    waitForPersistence
  }

  private def shutdownResources(): Unit = {
    logging.trackResourceShutdown("TaskExecutorService") {
      taskExecutorService.shutdown()
    }
  }

  private[scheduler] def getActorSystem(): ActorSystem = system

  /**
    * Shuts down the actor system. Will block, waiting for underlying systems to shutdown.
    */
  def shutdownAndWait(): Unit = {
    logging.trackResourceShutdown("SchedulingSystem") {
      system.shutdown()
      system.awaitTermination()
    }
  }
}
