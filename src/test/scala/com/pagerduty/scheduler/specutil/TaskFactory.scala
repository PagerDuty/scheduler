package com.pagerduty.scheduler.specutil

import com.pagerduty.eris.TimeUuid
import com.pagerduty.scheduler.model.{ CompletionResult, Task, TaskAttempt, TaskKey }
import com.twitter.conversions.time._
import com.twitter.util.{ Duration, Time }

object TaskFactory {
  private def timeNowFlooredToMs(): Time = Time.now.floor(1.millisecond)

  def makeTask(scheduledTime: Time = timeNowFlooredToMs): Task = {
    Task(
      orderingId = TimeUuid().toString,
      scheduledTime = scheduledTime,
      uniquenessKey = TimeUuid().toString,
      taskData = Map("taskId" -> TimeUuid().toString)
    )
  }

  def makeTaskKey(scheduledTime: Time = timeNowFlooredToMs): TaskKey = {
    makeTask(scheduledTime).taskKey
  }

  def makeTasks(
    count: Int,
    scheduledTime: Time = timeNowFlooredToMs,
    spacing: Duration = 0.seconds
  ): IndexedSeq[Task] = {
    for (i <- 0 until count) yield TaskFactory.makeTask(scheduledTime + spacing * i)
  }

  def makeTasksInConsecutiveBuckets(
    rowTimeBucketDuration: Duration
  ): (Seq[Task], Seq[Task]) = {
    val currentTimeBucket = Time.now
    val nextTimeBucket = currentTimeBucket + rowTimeBucketDuration
    val tasks = for (i <- 0 to 5) yield TaskFactory.makeTask()
    val currentBucketTasks = tasks.map(_.copy(scheduledTime = currentTimeBucket))
    val nextBucketTasks = tasks.map(_.copy(scheduledTime = nextTimeBucket))
    (currentBucketTasks, nextBucketTasks)
  }

  def makeTaskAttempt(attemptNumber: Int, taskResult: CompletionResult) = {
    val now = Time.now.floor(1.millisecond)
    val hadException = taskResult match {
      case CompletionResult.Success => false
      case _ => true
    }
    TaskAttempt(
      attemptNumber,
      startedAt = now - 100.millis,
      finishedAt = now - 50.millis,
      taskResult,
      taskResultUpdatedAt = now,
      exceptionClass = if (hadException) Some("YourTaskFailedException") else None,
      exceptionMessage = if (hadException) Some("Something went wrong.") else None,
      exceptionStackTrace = if (hadException) Some("com.pagerduty.app.YourTask.run") else None
    )
  }
}
