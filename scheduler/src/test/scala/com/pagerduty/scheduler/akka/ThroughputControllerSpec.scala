package com.pagerduty.scheduler.akka

import akka.actor._
import akka.testkit._
import com.pagerduty.scheduler._
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.specutil.ActorPathFreeSpec
import java.time.Instant

import org.scalamock.scalatest.PathMockFactory
import org.scalatest.Matchers

import scala.concurrent.duration._

class ThroughputControllerSpec
    extends ActorPathFreeSpec("ThroughputControllerSpec")
    with PathMockFactory
    with Matchers {
  import ThroughputController._

  val partitionId = 1
  val maxInProgressTasks = 10
  val batchSize = 3
  val minTickDelay = 200.millis
  val maxLookAhead = 1.second
  val prefetchWindow = 5.seconds
  val settings = Settings().copy(
    maxInFlightTasks = maxInProgressTasks,
    taskFetchBatchSize = batchSize,
    minTickDelay = minTickDelay,
    maxLookAhead = maxLookAhead,
    prefetchWindow = prefetchWindow
  )

  "ThroughputController should" - {
    val throughputControllerProps = ThroughputController.props(
      partitionId,
      settings,
      stub[Scheduler.Logging]
    )
    val taskPersistence = TestProbe()
    val scheduler = TestProbe()
    val partitionExecutor = TestProbe()
    val initMessage = Initialize(
      taskPersistence = taskPersistence.testActor,
      inProgressTaskOwners = Set(scheduler.testActor, partitionExecutor.testActor)
    )

    def expectInProgressTaskCountRequest(): Unit = {
      scheduler expectMsg FetchInProgressTaskCount
      partitionExecutor expectMsg FetchInProgressTaskCount
    }
    def expectNoInProgressTaskCountRequests(duration: FiniteDuration): Unit = {
      scheduler.expectNoMsg(duration)
      taskPersistence.expectNoMsg(duration)
    }
    def reportInProgressTaskCounts(schedulerCount: Int, partitionExecutorCount: Int): Unit = {
      scheduler reply InProgressTaskCountFetched(schedulerCount)
      partitionExecutor reply InProgressTaskCountFetched(partitionExecutorCount)
    }

    val throughputController = system.actorOf(throughputControllerProps)
    throughputController ! initMessage
    expectInProgressTaskCountRequest()

    "accumulate in-progress task count before loading more tasks" in {
      scheduler reply InProgressTaskCountFetched(1)
      scheduler reply InProgressTaskCountFetched(maxInProgressTasks) // Should be ignored.
      partitionExecutor reply InProgressTaskCountFetched(1)
      taskPersistence.expectMsgType[TaskPersistence.LoadTasks]
    }

    "keep checking in-progress task count when task limit is reached" in {
      reportInProgressTaskCounts(schedulerCount = maxInProgressTasks, partitionExecutorCount = 0)
      taskPersistence.expectNoMsg(minTickDelay + 100.millis)
      expectInProgressTaskCountRequest()
    }

    "wait for tasks to load before asking for more" in {
      reportInProgressTaskCounts(schedulerCount = 1, partitionExecutorCount = 1)
      taskPersistence.expectMsgType[TaskPersistence.LoadTasks]
      expectNoInProgressTaskCountRequests(minTickDelay + 100.millis)
      throughputController.tell(TaskPersistence.TasksLoaded(Instant.now()), taskPersistence.testActor)
      expectInProgressTaskCountRequest()
    }

    "set correct timer when fetched ahead" in {
      reportInProgressTaskCounts(schedulerCount = 1, partitionExecutorCount = 1)
      taskPersistence.expectMsgType[TaskPersistence.LoadTasks]
      val now = Instant.now()
      val nextFetchLowerBound = now + maxLookAhead
      val readCheckpoint = nextFetchLowerBound + prefetchWindow
      taskPersistence reply TaskPersistence.TasksLoaded(readCheckpoint)
      expectInProgressTaskCountRequest()
      Instant.now().compareTo(nextFetchLowerBound) should be >= 0
    }

    "not lose tick timer when there are unhandled messages" in {
      reportInProgressTaskCounts(schedulerCount = maxInProgressTasks, partitionExecutorCount = 0)
      throughputController ! "SimulatedUnhandledMessage"
      expectInProgressTaskCountRequest()
    }

    "request more tasks correctly" in {
      val ExpectedBatchSize = batchSize
      val now = Instant.now()
      val expectedUpperBound = now + maxLookAhead
      val maxMessageDelay = 1000.millis
      def equalWithinMessageDelay(a: Instant, b: Instant): Boolean = {
        java.time.Duration.between(a, b).toScalaDuration.toNanos.abs < maxMessageDelay.toNanos
      }
      reportInProgressTaskCounts(schedulerCount = 1, partitionExecutorCount = 1)
      taskPersistence.expectMsgPF(maxMessageDelay) {
        case TaskPersistence.LoadTasks(upperBound, ExpectedBatchSize)
            if equalWithinMessageDelay(upperBound, expectedUpperBound) => // Match only.
      }
    }
  }
}
