package com.pagerduty.scheduler.akka

import akka.actor._
import com.pagerduty.scheduler._
import com.pagerduty.scheduler.akka.TaskPersistence.PersistTasks
import com.pagerduty.scheduler.model.Task.PartitionId

object PartitionSupervisor {

  def props(settings: Settings, queueContext: QueueContext, partitionId: PartitionId): Props = {
    val taskPersistenceFactory = (context: ActorRefFactory, args: TaskPersistenceArgs) => {
      context.actorOf(TaskPersistence.props(args), s"taskPersistence$partitionId")
    }
    Props(new PartitionSupervisor(settings, queueContext, partitionId, taskPersistenceFactory))
  }
}

/**
  * The root supervisor for all the partition related actors for a give `partitionId`.
  * On creation, spawns all the children and subsequently forwards messages to TaskPersistence
  * actor.
  *
  * @param settings
  * @param queueContext
  * @param partitionId
  * @param taskPersistenceFactory
  */
class PartitionSupervisor(
    settings: Settings,
    queueContext: QueueContext,
    partitionId: PartitionId,
    taskPersistenceFactory: (ActorRefFactory, TaskPersistenceArgs) => ActorRef)
    extends Actor
    with ActorLogging {
  override val supervisorStrategy = Supervision.AlwaysEscalateStrategy
  val taskStatusTracker: ActorRef = {
    val statusTrackerProps = TaskStatusTracker.props(partitionId, queueContext.taskStatusDao)
    val tracker = context.actorOf(statusTrackerProps, s"taskStatusTracker$partitionId")

    val retrierProps = Retrier.props(tracker, settings.maxDataAccessAttempts, queueContext.logging)
    context.actorOf(retrierProps, s"taskStatusRetrier$partitionId")
  }
  val throughputController: ActorRef = {
    val throughputControllerProps = ThroughputController.props(
      partitionId,
      settings,
      queueContext.logging
    )
    context.actorOf(throughputControllerProps, s"throughputController$partitionId")
  }
  val partitionExecutor: ActorRef = {
    val partitionExecutorProps = PartitionExecutor.props(settings)
    context.actorOf(partitionExecutorProps, s"partitionExecutor$partitionId")
  }
  val partitionScheduler: ActorRef = {
    val partitionSchedulerProps = PartitionScheduler.props(
      partitionId,
      partitionExecutor,
      queueContext.logging
    )
    context.actorOf(partitionSchedulerProps, s"partitionScheduler$partitionId")
  }
  val taskPersistence = {
    val taskPersistenceArgs = TaskPersistenceArgs(
      settings,
      partitionId,
      queueContext.taskScheduleDao,
      partitionScheduler,
      throughputController
    )
    val persister = taskPersistenceFactory(context, taskPersistenceArgs)

    val retrierProps = Retrier.props(persister, settings.maxDataAccessAttempts, queueContext.logging)
    context.actorOf(retrierProps, s"taskPersistenceRetrier$partitionId")
  }

  def init(): Unit = {
    val partitionContext = PartitionContext(
      partitionId,
      taskStatusTracker,
      taskPersistence,
      queueContext.taskExecutorService,
      queueContext.logging
    )
    partitionExecutor ! PartitionExecutor.Initialize(partitionContext)

    val inProgressTaskOwners = Set(partitionExecutor, partitionScheduler)
    throughputController ! ThroughputController.Initialize(taskPersistence, inProgressTaskOwners)
  }
  init()

  override def receive = {
    case message: PersistTasks => {
      taskPersistence.forward(message)
    }
  }
}
