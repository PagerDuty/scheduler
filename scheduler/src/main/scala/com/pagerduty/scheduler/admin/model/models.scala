package com.pagerduty.scheduler.admin.model

import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{CompletionResult, Task, TaskAttempt, TaskKey}

case class GetStatusResponse(status: String)

case class GetTaskResponse(task: Option[AdminTask], errors: Seq[String])

case class GetTasksResponse(tasks: Option[Seq[AdminTask]], errors: Seq[String])

case class AdminTask(
    key: Option[TaskKey],
    partitionId: Option[Int],
    data: Option[Task.TaskData],
    numberOfAttempts: Option[Int],
    status: Option[CompletionResult],
    details: Option[TaskDetails])

object AdminTask {
  def apply(key: TaskKey, partitionId: PartitionId, data: Task.TaskData): AdminTask = {
    AdminTask(Some(key), Some(partitionId), Some(data), None, None, None)
  }
}

case class TaskDetails(attempts: Seq[TaskAttempt])
