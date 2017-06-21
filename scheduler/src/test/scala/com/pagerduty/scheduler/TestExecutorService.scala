package com.pagerduty.scheduler

import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.model.Task.PartitionId
import scala.concurrent.Future

/**
  * This service is used to execute tasks scheduled by users of the Scheduler library. It is backed
  * by a fixed-size thread pool. Take care to configure the thread pool to have an appropriate size
  * for the expected workload.
  *
  * @param threadPoolSize The size of the thread pool of the executor service.
  */
class TestExecutorService(threadPoolSize: Int, taskRunner: Task => Unit) extends TaskExecutorService {
  protected val executionContext = FixedSizeExecutionContext(threadPoolSize)

  def execute(partitionId: PartitionId, task: Task): Future[Unit] = {
    Future(
      taskRunner(task)
    )(executionContext)
  }

  def shutdown(): Unit = {
    executionContext.shutdown()
  }
}
