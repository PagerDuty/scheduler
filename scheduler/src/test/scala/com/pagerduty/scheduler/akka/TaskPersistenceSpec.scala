package com.pagerduty.scheduler.akka

import akka.testkit.{TestFSMRef, TestProbe}
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.dao.TaskScheduleDao
import com.pagerduty.scheduler.model.{Task, TaskKey}
import com.pagerduty.scheduler.specutil.{ActorPathFreeSpec, TaskFactory}
import java.time.Instant
import org.scalamock.scalatest.PathMockFactory
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

class TaskPersistenceSpec extends ActorPathFreeSpec("TaskPersistenceSpec") with PathMockFactory {
  import TaskPersistence._
  import PartitionScheduler._

  val partitionId = 1
  val settings = Settings()

  "TaskPersistence" - {
    val taskScheduleDao = mock[TaskScheduleDao]
    val partitionScheduler = TestProbe()
    val throughputController = TestProbe()
    val taskPersistenceArgs = TaskPersistenceArgs(
      settings,
      partitionId,
      taskScheduleDao,
      partitionScheduler.testActor,
      throughputController.testActor
    )
    val taskPersistence = TestFSMRef(new TaskPersistence(taskPersistenceArgs))
    val taskCount = 2
    val tasks = TaskFactory.makeTasks(taskCount)

    "persist tasks in" in {
      (taskScheduleDao.insert _).stubs(partitionId, tasks).returns(Future.successful(Unit))
      taskPersistence ! PersistTasks(tasks)
      expectMsg(TasksPersisted(partitionId))
    }

    "forward persist failures" in {
      val exception = new RuntimeException("Simulated exception")
      (taskScheduleDao.insert _).stubs(*, *).returns(Future.failed(exception))
      taskPersistence ! PersistTasks(tasks)
      expectMsg(TasksNotPersisted(exception))
    }

    "pass through tasks scheduled before read checkpoint" in {
      val ReadCheckpoint(readCheckpoint) = taskPersistence.underlyingActor.stateData
      val readCheckpointTime = readCheckpoint.scheduledTime
      val beforeReadCheckpoint = readCheckpointTime - 1.millisecond

      val passThroughTasks = TaskFactory.makeTasks(taskCount, scheduledTime = beforeReadCheckpoint)
      val persistAndDiscardTasks = TaskFactory.makeTasks(taskCount, readCheckpointTime)
      val allTasks = passThroughTasks ++ persistAndDiscardTasks
      (taskScheduleDao.insert _).stubs(*, *).returns(Future.successful(Unit))

      taskPersistence ! PersistTasks(allTasks)
      expectMsg(TasksPersisted(partitionId))
      partitionScheduler.expectMsg(ScheduleTasks(passThroughTasks))
    }

    "remove task" in {
      val taskKey = tasks.head.taskKey
      (taskScheduleDao.remove _).expects(partitionId, taskKey)
      taskPersistence ! RemoveTask(taskKey)
    }

    "load tasks" in {
      val ReadCheckpoint(readCheckpoint) = taskPersistence.underlyingActor.stateData
      val upperBound = readCheckpoint.scheduledTime + 1.hour
      val limit = 200
      (taskScheduleDao.load _)
        .expects(partitionId, readCheckpoint, upperBound, limit)
        .returns(Future.successful(tasks))

      taskPersistence ! LoadTasks(upperBound, limit)
      partitionScheduler.expectMsg(ScheduleTasks(tasks))
      throughputController.expectMsg(TaskPersistence.TasksLoaded(upperBound))
    }

    "notify about load failures" in {
      val exception = new RuntimeException("Simulated exception")
      (taskScheduleDao.load _).stubs(*, *, *, *).returns(Future.failed(exception))
      taskPersistence ! LoadTasks(Instant.now(), limit = 1000)
      throughputController.expectMsg(TaskPersistence.TasksNotLoaded(exception))
    }

    "advance read checkpoint after successful load" - {
      "requested range is fully loaded" in {
        val ReadCheckpoint(readCheckpoint) = taskPersistence.underlyingActor.stateData
        val upperBound = readCheckpoint.scheduledTime + 1.hour
        val expectedReadCheckpoint = TaskKey.lowerBound(upperBound)
        val afterCheckpointBeforeUpperBound = readCheckpoint.scheduledTime + 1.minute
        val resultTasks = TaskFactory.makeTasks(taskCount, afterCheckpointBeforeUpperBound)

        // Query results under limit indicate range is full loaded.
        val limit = resultTasks.size + 1
        (taskScheduleDao.load _)
          .expects(partitionId, readCheckpoint, upperBound, limit)
          .returns(Future.successful(resultTasks))

        taskPersistence ! LoadTasks(upperBound, limit)
        partitionScheduler.expectMsg(ScheduleTasks(resultTasks))
        throughputController.expectMsg(TaskPersistence.TasksLoaded(upperBound))
        taskPersistence.underlyingActor.stateData shouldEqual ReadCheckpoint(expectedReadCheckpoint)
      }
      "requested range is partially loaded" in {
        val ReadCheckpoint(readCheckpoint) = taskPersistence.underlyingActor.stateData
        val upperBound = readCheckpoint.scheduledTime + 1.hour
        val afterCheckpointBeforeUpperBound = readCheckpoint.scheduledTime + 1.minute
        val resultTasks = TaskFactory.makeTasks(taskCount, afterCheckpointBeforeUpperBound)

        // Query results at limit indicate partial progress.
        val limit = resultTasks.size
        val latestTaskKey = resultTasks.last.taskKey
        val latestTaskTime = latestTaskKey.scheduledTime
        (taskScheduleDao.load _)
          .expects(partitionId, readCheckpoint, upperBound, limit)
          .returns(Future.successful(resultTasks))

        taskPersistence ! LoadTasks(upperBound, limit)
        partitionScheduler.expectMsg(ScheduleTasks(resultTasks))
        throughputController.expectMsg(TaskPersistence.TasksLoaded(latestTaskTime))
        taskPersistence.underlyingActor.stateData shouldEqual ReadCheckpoint(latestTaskKey)
      }
    }

    "keep read checkpoint on failed load" in {
      val ReadCheckpoint(readCheckpoint) = taskPersistence.underlyingActor.stateData
      val upperBound = readCheckpoint.scheduledTime + 1.hour
      (taskScheduleDao.load _).stubs(*, *, *, *).returns(Future.failed(new RuntimeException()))
      taskPersistence ! LoadTasks(upperBound, limit = 1000)
      throughputController.expectMsgType[TasksNotLoaded]
      taskPersistence.underlyingActor.stateData shouldEqual ReadCheckpoint(readCheckpoint)
    }

    "serialize persist and load operations" in {
      val ReadCheckpoint(readCheckpoint) = taskPersistence.underlyingActor.stateData
      val upperBound = readCheckpoint.scheduledTime + 1.hour

      val promise1 = Promise[Unit]()
      val promise2 = Promise[IndexedSeq[Task]]()

      (taskScheduleDao.insert _).expects(*, *).returns(promise1.future).once()
      taskPersistence ! PersistTasks(tasks)
      taskPersistence ! LoadTasks(upperBound, limit = 1000)
      taskPersistence ! PersistTasks(tasks)

      (taskScheduleDao.load _).expects(*, *, *, *).returns(promise2.future).once()
      promise1.success(Unit) // Blocks execution until expectations the line above are set.
      (taskScheduleDao.insert _).expects(*, *).returns(Future.successful(Unit)).once()
      promise2.success(IndexedSeq.empty) // Blocks execution until expectations are set.
    }
  }

  verifyExpectations()
}
