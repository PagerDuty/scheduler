package com.pagerduty.scheduler.model

import com.pagerduty.scheduler.model.Task.PartitionId
import java.time.Instant
import java.util.UUID
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

/**
  * A task for scheduling.
  *
  * @param orderingId The key of a logical ordering with which to associate the task. Ordering of
  *                 tasks is defined only for tasks with the same orderingId, and only when this
  *                 method is called in the same order as the scheduled times. That is, task1 is
  *                 only guaranteed to execute before task2 if they have the same orderingId, task1
  *                 has a scheduledTime before the scheduledTime of task2, and scheduleTask is
  *                 called for task1 before it is called for task2.
  *
  *                 As a result of this ordering guarantee note that if a task with a given
  *                 orderingId is blocked, this will not allow other tasks with the same orderingId to
  *                 execute.
  *
  *                DO NOT put commas in this string! It will break parsing the key from a string.
  * @param scheduledTime The time at which to run the task.
  * @param uniquenessKey The (orderingId, scheduledTime, uniquenessKey) tuple is the ID for a task.
  *                      When a task is scheduled with an existing ID, it will overwrite the
  *                      existing task. If the original task has been completed, the new task will
  *                      not be re-run. This key can be used to schedule tasks with the same
  *                      orderingId and scheduledTime.
  *
  *                     DO NOT put commas in this string!
  * @param taskData Application-defined task data. This likely includes a task identifier,
  *                 and possibly task state.
  * @param version Serialization version number so we can make changes in the future.
  *
  */
final case class Task(
    orderingId: Task.OrderingId,
    scheduledTime: Instant,
    uniquenessKey: Task.UniquenessKey,
    taskData: Task.TaskData,
    version: Int = 1) {
  def taskKey: TaskKey = TaskKey(scheduledTime, orderingId, uniquenessKey)

  def taskDataString: String = {
    compact(render(taskData))
  }

  implicit val formats = DefaultFormats + new TaskKeyTimeSerializer

  def toJson: String = write(this)

  override def toString: String = {
    s"Task($taskKey,$taskData)"
  }

  def partitionId(numPartitions: Int): PartitionId = taskKey.partitionId(numPartitions)
}

object Task {
  type OrderingId = String

  type UniquenessKey = String
  type TaskData = Map[String, String]
  type PartitionId = Int

  def apply(taskKey: TaskKey, taskDataString: String): Task = {
    val taskData = read[TaskData](taskDataString)
    Task(taskKey.orderingId, taskKey.scheduledTime, taskKey.uniquenessKey, taskData)
  }

  implicit val formats = DefaultFormats + new TaskKeyTimeSerializer

  def fromJson(task: String): Task = read[Task](task)

  /** Spit out an example task. Handy for tests and maybe some interactive work */
  def example: Task = Task(UUID.randomUUID().toString, Instant.now(), UUID.randomUUID().toString, Map())
}
