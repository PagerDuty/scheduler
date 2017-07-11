package com.pagerduty.scheduler.admin.http

import com.pagerduty.scheduler.admin.model.AdminTask
import com.pagerduty.scheduler.model.{CompletionResult, TaskKey}
import java.time.{Instant, ZoneOffset}
import org.json4s.{CustomSerializer, MappingException}
import org.json4s.JsonAST.JString

case class PutTaskRequest(task: AdminTask)

case class PutTaskResponse(task: Option[AdminTask], errors: Seq[String])

class CompletionStatusSerializer
    extends CustomSerializer[CompletionResult](
      format =>
        (
          {
            case JString(status) =>
              lazy val error = s"Can't convert '$status' to CompletionStatus"
              CompletionResult.fromString(status).getOrElse(throw new MappingException(error))
          }, {
            case cs: CompletionResult => JString(cs.toString)
          }
      )
    )

class TimeSerializer
    extends CustomSerializer[Instant](
      format =>
        (
          {
            case JString(timeString) => Instant.parse(timeString)
          }, {
            case t: Instant => JString(AdminServlet.TimeFormat.format(t))
          }
      )
    )

class TaskKeySerializer
    extends CustomSerializer[TaskKey](
      format =>
        (
          {
            case JString(keyString) =>
              TaskKey.fromString(keyString)
          }, {
            case key: TaskKey => JString(key.toString)
          }
      )
    )
