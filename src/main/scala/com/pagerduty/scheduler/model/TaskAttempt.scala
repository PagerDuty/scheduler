package com.pagerduty.scheduler.model

import com.twitter.util.Time
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{ read, write }

/**
 * Captures results of a task attempt for troubleshooting.
 */
case class TaskAttempt(
    attemptNumber: Int,
    startedAt: Time,
    finishedAt: Time,
    taskResult: CompletionResult,
    taskResultUpdatedAt: Time,
    exceptionClass: Option[String],
    exceptionMessage: Option[String],
    exceptionStackTrace: Option[String]
) {
  implicit val formats = DefaultFormats + new TaskKeyTimeSerializer + new CompletionResultSerializer

  def toJson: String = write(this)
}

object TaskAttempt {
  def apply(
    attemptNumber: Int,
    startedAt: Time,
    finishedAt: Time,
    taskResult: CompletionResult,
    taskResultUpdatedAt: Time,
    exception: Option[Throwable]
  ): TaskAttempt = {
    TaskAttempt(
      attemptNumber,
      startedAt,
      finishedAt,
      taskResult,
      taskResultUpdatedAt,
      exceptionClass = exception.map(_.getClass.getName),
      exceptionMessage = exception.map(_.getMessage),
      exceptionStackTrace = exception.map(_.getStackTraceString)
    )
  }

  implicit val formats = DefaultFormats + new TaskKeyTimeSerializer + new CompletionResultSerializer

  def fromJson(taskAttempt: String): TaskAttempt = read[TaskAttempt](taskAttempt)
}
