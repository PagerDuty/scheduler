package com.pagerduty.scheduler.akka

import akka.testkit._
import com.pagerduty.eris.TimeUuid
import com.pagerduty.scheduler.akka.PartitionExecutor.ExecutePartitionTask
import com.pagerduty.scheduler.{ Scheduler, scalaDurationToTwitter }
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import com.twitter.util.Time
import com.pagerduty.scheduler.specutil.{ ActorPathFreeSpec, TaskFactory }

import scala.language.postfixOps

class PartitionSchedulerSpec extends ActorPathFreeSpec("PartitionSchedulerSpec")
    with PathMockFactory with Eventually {
  import PartitionScheduler._
  val partitionId = 3

  "PartitionScheduler should" - {
    val partitionExecutorProbe = TestProbe()
    val scheduler = TestActorRef[PartitionScheduler](
      PartitionScheduler.props(
        partitionId, partitionExecutorProbe.testActor, stub[Scheduler.Logging]
      )
    )

    "start with no pending tasks or timer" in {
      scheduler.underlyingActor.nextPendingTaskTimer shouldBe empty
      scheduler.underlyingActor.pendingTasks shouldBe empty
    }

    "dispatch overdue tasks right away" in {
      val tasks = TaskFactory.makeTasks(2)
      scheduler ! ScheduleTasks(tasks)
      partitionExecutorProbe.expectMsgAllOf(tasks.map(ExecutePartitionTask): _*)
      partitionExecutorProbe.expectNoMsg(100 millis)
      scheduler.underlyingActor.nextPendingTaskTimer shouldBe empty
      scheduler.underlyingActor.pendingTasks shouldBe empty
    }

    "dispatch tasks due in the future later" in {
      val tasks = TaskFactory.makeTasks(2, Time.now + 1.second)
      scheduler ! ScheduleTasks(tasks)

      partitionExecutorProbe.expectNoMsg(900 millis)
      eventually {
        scheduler.underlyingActor.pendingTasks.values.toSeq shouldEqual tasks
        scheduler.underlyingActor.nextPendingTaskTimer shouldBe 'defined
      }

      partitionExecutorProbe.expectMsgAllOf(3 seconds, tasks.map(ExecutePartitionTask): _*)
      eventually {
        scheduler.underlyingActor.nextPendingTaskTimer shouldBe empty
        scheduler.underlyingActor.pendingTasks shouldBe empty
      }
    }

    "correctly merge incoming tasks" in {
      val tasks2 = TaskFactory.makeTasks(2, Time.now + 2.second)
      scheduler ! ScheduleTasks(tasks2)

      val tasks1 = TaskFactory.makeTasks(2, Time.now + 1.second)
      scheduler ! ScheduleTasks(tasks1)

      scheduler.underlyingActor.pendingTasks.values.toSeq shouldEqual (tasks1 ++ tasks2)
    }

    "overwrite tasks with the same key" in {
      val originalTask = TaskFactory.makeTask(Time.now + 2.second)
      val modifiedTask = originalTask.copy(taskData = Map("id" -> TimeUuid().toString))
      scheduler ! ScheduleTasks(Seq(originalTask))
      scheduler ! ScheduleTasks(Seq(modifiedTask))
      scheduler.underlyingActor.pendingTasks.values.toSeq shouldEqual Seq(modifiedTask)
    }

    "reply to in-flight task status queries" in {
      scheduler ! ThroughputController.FetchInProgressTaskCount
      expectMsg(ThroughputController.InProgressTaskCountFetched(0))

      val taskCount = 2
      val tasks = TaskFactory.makeTasks(taskCount, Time.now + 1.second)
      scheduler ! ScheduleTasks(tasks)
      scheduler ! ThroughputController.FetchInProgressTaskCount
      expectMsg(ThroughputController.InProgressTaskCountFetched(taskCount))
    }
  }
}
