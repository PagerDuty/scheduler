package com.pagerduty.scheduler

import com.pagerduty.metrics.NullMetrics
import com.pagerduty.scheduler.akka.SchedulingSystem
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.model.Task.PartitionId
import java.time.temporal.ChronoUnit
import java.time.Instant
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.BeforeAndAfter
import scala.concurrent.duration._

class SchedulerIntegrationWhiteboxSpec extends SchedulerIntegrationSpecBase with BeforeAndAfter with PathMockFactory {
  after {
    dropAndLoadCassSchema()
  }

  "Scheduler Whitebox Integration Tests" - {
    val taskRunner = (task: Task) => {}
    val executorFactory = SchedulerIntegrationSpecBase.simpleTestExecutorFactory(taskRunner)
    val logging = stub[Scheduler.Logging]

    "should shut down the entire actor system upon encountering an exception" in {
      val partitionIds = Set[PartitionId](0)
      val schedulingSystem = {
        new SchedulingSystem(config, cluster, keyspace, partitionIds, executorFactory, logging, NullMetrics)
      }
      val scheduledTime = (Instant.now() + 5.seconds).truncatedTo(ChronoUnit.MILLIS)
      val task = new Task("oid1", scheduledTime, "uniq-key-1", Map("k" -> "v"))
      val persistAndScheduleTask = Map[PartitionId, Seq[Task]](partitionIds.head -> Seq(task))
      schedulingSystem.persistAndSchedule(persistAndScheduleTask)

      getSchemaLoader().dropSchema() // This will cause Cassandra error when the task runs.

      // The cassandra error should cause an error to propagate up the actor system.
      // This will activate the supervision strategy and cause the scheduling system to shut down.
      schedulingSystem.getActorSystem().awaitTermination(1.minute)
    }
  }
}
