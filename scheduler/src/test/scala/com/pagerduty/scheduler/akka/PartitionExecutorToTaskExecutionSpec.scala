package com.pagerduty.scheduler.akka

import akka.testkit.{TestActorRef, TestProbe}
import com.pagerduty.scheduler.akka.PartitionExecutor.ExecutePartitionTask
import com.pagerduty.scheduler.akka.TaskExecutor._
import com.pagerduty.scheduler.akka.TaskStatusTracker._
import com.pagerduty.scheduler.specutil.{ActorPathFreeSpec, TaskFactory}
import com.pagerduty.scheduler._
import com.pagerduty.scheduler.model.{Task, TaskStatus}
import org.scalamock.scalatest.PathMockFactory

class PartitionExecutorToTaskExecutionSpec
    extends ActorPathFreeSpec("PartitionExecutorToTaskExecutionSpec")
    with PathMockFactory {
  val settings = Settings()

  "PartitionExecutorToTaskExecutionSpec" - {
    val taskPersistence = TestProbe()
    val taskStatusTracker = TestProbe()
    val taskRunner = (task: Task) => {
      println(s"Running task $task")
    }
    val threadPoolSize = 10
    val partitionContext = PartitionContext(
      partitionId = 1,
      taskPersistence = taskPersistence.ref,
      taskStatusTracker = taskStatusTracker.ref,
      taskExecutorService = new TestExecutorService(threadPoolSize, taskRunner),
      logging = stub[Scheduler.Logging]
    )

    "should send a task to partition executor and is successfully executed" in {
      val partitionExecutorProps = PartitionExecutor.props(settings)
      val partitionExecutor = TestActorRef[PartitionExecutor](partitionExecutorProps)
      partitionExecutor ! PartitionExecutor.Initialize(partitionContext)
      val task = TaskFactory.makeTask()
      partitionExecutor ! ExecutePartitionTask(task)

      taskStatusTracker.expectMsg(FetchTaskStatus(task.taskKey))
      taskStatusTracker.reply(TaskStatusFetched(task.taskKey, TaskStatus.NeverAttempted))
      val successStatus = TaskStatus.successful(1)
      taskStatusTracker.expectMsg(UpdateTaskStatus(task.taskKey, successStatus))
      taskStatusTracker.reply(TaskStatusUpdated(task.taskKey, successStatus))

      taskPersistence.expectMsg(TaskPersistence.RemoveTask(task.taskKey))
    }
  }
}
