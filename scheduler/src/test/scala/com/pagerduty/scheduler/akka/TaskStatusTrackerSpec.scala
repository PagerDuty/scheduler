package com.pagerduty.scheduler.akka

import org.scalamock.scalatest.PathMockFactory

import scala.concurrent.Future
import com.pagerduty.scheduler.dao.TaskStatusDao
import com.pagerduty.scheduler.model.TaskStatus
import com.pagerduty.scheduler.specutil.{ActorPathFreeSpec, TaskFactory}

class TaskStatusTrackerSpec extends ActorPathFreeSpec("TaskStatusTrackerSpec") with PathMockFactory {
  import TaskStatusTracker._
  import TaskExecutor._

  val partitionId = 1
  val mockTaskStatusDao = mock[TaskStatusDao]
  val taskStatus = TaskStatus.successful(2)

  "A TaskStatusTracker" - {
    val actor = system.actorOf(TaskStatusTracker.props(partitionId, mockTaskStatusDao))
    val taskKey = TaskFactory.makeTask().taskKey

    "it receives a UpdateTaskStatus message" - {
      "status is persisted successfully" - {
        (mockTaskStatusDao
          .insert(_, _, _))
          .expects(partitionId, taskKey, taskStatus)
          .returning(Future.successful(()))

        "send a TaskStatusUpdated to the requester" in {
          actor ! UpdateTaskStatus(taskKey, taskStatus)
          expectMsg(TaskStatusUpdated(taskKey, taskStatus))
        }
      }

      "status is not persisted successfully" - {
        val failureTaskKey = TaskFactory.makeTask().taskKey
        val exception = new Exception
        (mockTaskStatusDao
          .insert(_, _, _))
          .expects(partitionId, failureTaskKey, taskStatus)
          .returning(Future.failed(exception))

        "send a TaskStatusUpdated to the requester" in {
          actor ! UpdateTaskStatus(failureTaskKey, taskStatus)
          expectMsg(TaskStatusNotUpdated(failureTaskKey, taskStatus, exception))
        }
      }

      "it receives a FetchTaskStatus message" - {
        "status is found successfully" - {
          (mockTaskStatusDao
            .getStatus(_, _))
            .expects(partitionId, taskKey)
            .returning(Future.successful(taskStatus))

          "send a TaskStatusFetched to the requester" in {
            actor ! FetchTaskStatus(taskKey)
            expectMsg(TaskStatusFetched(taskKey, taskStatus))
          }
        }

        "status is not found successfully" - {
          val failureTaskKey = TaskFactory.makeTask().taskKey
          val exception = new Exception("Failure getting task completion")
          (mockTaskStatusDao
            .getStatus(_, _))
            .expects(partitionId, failureTaskKey)
            .returning(Future.failed(exception))

          "send a TaskStatusNotFetched to the requester" in {
            actor ! FetchTaskStatus(failureTaskKey)
            expectMsg(TaskStatusNotFetched(failureTaskKey, exception))
          }
        }
      }
    }
  }
}
