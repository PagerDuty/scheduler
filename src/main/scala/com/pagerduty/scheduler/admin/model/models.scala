package com.pagerduty.scheduler.admin.model

import com.pagerduty.scheduler.admin.AdminServlet
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{ CompletionResult, Task, TaskAttempt, TaskKey }
import com.twitter.util.Time
import org.json4s.JsonAST.JString
import org.json4s.{ CustomSerializer, MappingException }

case class GetStatusResponse(status: String)

case class GetTaskResponse(task: Option[AdminTask], errors: Seq[String])

case class GetTasksResponse(tasks: Option[Seq[AdminTask]], errors: Seq[String])

case class AdminTask(
  key: Option[TaskKey],
  partitionId: Option[Int],
  data: Option[Task.TaskData],
  numberOfAttempts: Option[Int],
  status: Option[CompletionResult],
  details: Option[TaskDetails]
)

object AdminTask {
  def apply(key: TaskKey, partitionId: PartitionId, data: Task.TaskData): AdminTask = {
    AdminTask(Some(key), Some(partitionId), Some(data), None, None, None)
  }
}

case class TaskDetails(attempts: Seq[TaskAttempt])

case class PutTaskRequest(task: AdminTask)

case class PutTaskResponse(task: Option[AdminTask], errors: Seq[String])

class CompletionStatusSerializer extends CustomSerializer[CompletionResult](format => (
  {
    case JString(status) =>
      lazy val error = s"Can't convert '$status' to CompletionStatus"
      CompletionResult.fromString(status).getOrElse(throw new MappingException(error))
  },
  {
    case cs: CompletionResult => JString(cs.toString)
  }
))

class TimeSerializer extends CustomSerializer[Time](format => (
  {
    case JString(timeString) =>
      AdminServlet.TimeFormat.parse(timeString)
  },
  {
    case t: Time => JString(AdminServlet.TimeFormat.format(t))
  }
))

class TaskKeySerializer extends CustomSerializer[TaskKey](format => (
  {
    case JString(keyString) =>
      TaskKey.fromString(keyString)
  },
  {
    case key: TaskKey => JString(key.toString)
  }
))
