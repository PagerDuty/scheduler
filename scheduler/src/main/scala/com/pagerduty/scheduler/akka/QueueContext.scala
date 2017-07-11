package com.pagerduty.scheduler.akka

import com.pagerduty.scheduler.{Scheduler, TaskExecutorService}
import com.pagerduty.scheduler.dao.{AttemptHistoryDao, TaskStatusDao, TaskScheduleDao}

/**
  * Simple wrapper to capture repeated arguments.
  */
case class QueueContext(
    taskScheduleDao: TaskScheduleDao,
    taskStatusDao: TaskStatusDao,
    taskExecutorService: TaskExecutorService,
    logging: Scheduler.Logging)
