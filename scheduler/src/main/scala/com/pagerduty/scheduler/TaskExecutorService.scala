package com.pagerduty.scheduler

import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.model.Task.PartitionId
import scala.concurrent.Future

/**
  * An interface use by Scheduler to execute tasks.
  * Scheduler will create a new TaskExecutorService using provided factory when new partitions are
  * assigned. When partitions are revoked, Scheduler will shutdown the TaskExecutorService.
  */
trait TaskExecutorService {

  /**
    * Execute target task assigned to the given partition. This method must be non-blocking.
    * @param partitionId partition id where the task was assigned
    * @param task target task
    * @return future that will complete when the task is done
    */
  def execute(partitionId: PartitionId, task: Task): Future[Unit]

  /**
    * Shutdown the service. This method will be called when partitions are revoked.
    */
  def shutdown(): Unit
}
