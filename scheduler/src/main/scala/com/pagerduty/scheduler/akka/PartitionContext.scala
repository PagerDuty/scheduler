package com.pagerduty.scheduler.akka

import akka.actor.ActorRef
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.{Scheduler, TaskExecutorService}

/**
  * A wrapper for all the important references and objects that are passed downstream on each
  * partition.
  */
case class PartitionContext(
    partitionId: PartitionId,
    taskStatusTracker: ActorRef,
    taskPersistence: ActorRef,
    taskExecutorService: TaskExecutorService,
    logging: Scheduler.Logging)
