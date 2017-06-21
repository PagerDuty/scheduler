package com.pagerduty.scheduler.akka

import akka.actor._
import com.pagerduty.scheduler.akka.TaskPersistence.PersistTasks
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.model.Task.PartitionId
import scala.concurrent.TimeoutException
import scala.concurrent.duration._

object TopicSupervisor {

  /**
    * A request to durable store and subsequently process incoming tasks. TopicSupervisor will
    * respond with `TasksPersisted` message or a `Status.Failure` message.
    *
    * @param taskBatch
    */
  case class ProcessTaskBatch(taskBatch: Map[PartitionId, Seq[Task]])

  /**
    * A response to ProcessTaskBatch, sent after a task batch has been durably stored.
    */
  case object TaskBatchProcessed

  /**
    * A response to ProcessTaskBatch, sent if the task batch was not durably stored.
    */
  case class TaskBatchNotProcessed(throwable: Throwable)

  def props(settings: Settings, queueContext: QueueContext, partitions: Set[PartitionId]): Props = {
    val partitionSupervisorFactory = {
      (context: ActorRefFactory, partitionId: PartitionId, queueContext: QueueContext) =>
        {
          val partitionProps = PartitionSupervisor.props(settings, queueContext, partitionId)
          context.actorOf(partitionProps, s"partitionSupervisor$partitionId")
        }
    }
    Props(
      new TopicSupervisor(
        settings,
        queueContext,
        partitions,
        partitionSupervisorFactory
      )
    )
  }

  private def makePersistRequestActorProps(
      partitionSupervisors: Map[PartitionId, ActorRef],
      taskBatch: Map[PartitionId, Seq[Task]],
      requestTimeout: FiniteDuration,
      replyTo: ActorRef
    ): Props = {
    Props(new PersistRequestActor(partitionSupervisors, taskBatch, requestTimeout, replyTo))
  }

  private class PersistRequestActor(
      partitionSupervisors: Map[PartitionId, ActorRef],
      taskBatch: Map[PartitionId, Seq[Task]],
      requestTimeout: FiniteDuration,
      replyTo: ActorRef)
      extends Actor
      with ActorLogging {
    import context.dispatcher

    for ((partitionId, tasks) <- taskBatch) {
      partitionSupervisors(partitionId) ! PersistTasks(tasks)
    }

    var remainingReplies = taskBatch.keySet
    val timer = context.system.scheduler.scheduleOnce(requestTimeout)(self ! ReceiveTimeout)
    replyAndStopWhenReady()

    def receive = {
      case TaskPersistence.TasksPersisted(partitionId) => {
        remainingReplies -= partitionId
        replyAndStopWhenReady()
      }
      case TaskPersistence.TasksNotPersisted(throwable) => {
        replyTo ! TaskBatchNotProcessed(throwable)
        stop()
      }
      case ReceiveTimeout => {
        val partitionString = remainingReplies.toSeq.sorted.mkString("[", ", ", "]")
        val errorMessage = s"Persist request timed out waiting for partitions: $partitionString."
        replyTo ! TaskBatchNotProcessed(new TimeoutException(errorMessage))
        stop()
      }
    }

    def replyAndStopWhenReady(): Unit = {
      if (remainingReplies.isEmpty) {
        replyTo ! TaskBatchProcessed
        stop()
      }
    }
    def stop(): Unit = {
      timer.cancel()
      context.stop(self)
    }
  }
}

/**
  * Main point of entry into the actor system, and the root supervisor. This actor splits each
  * multi-partition message into several per-partition messages and forwards them to
  * corresponding PartitionSupervisors.
  *
  * @param partitions
  * @param queueContext
  * @param partitionSupervisorFactory
  */
class TopicSupervisor(
    settings: Settings,
    queueContext: QueueContext,
    partitions: Set[PartitionId],
    partitionSupervisorFactory: (ActorRefFactory, PartitionId, QueueContext) => ActorRef)
    extends Actor
    with ActorLogging {
  import TopicSupervisor._
  override val supervisorStrategy =
    Supervision.makeAlwaysEscalateTopicSupervisorStrategy(queueContext.logging)
  val persistRequestTimeout = settings.persistRequestTimeout

  val partitionSupervisors: Map[PartitionId, ActorRef] = {
    partitions.map { partitionId =>
      val partitionSupervisor = partitionSupervisorFactory(context, partitionId, queueContext)
      partitionId -> partitionSupervisor
    }.toMap
  }

  def receive = {
    case ProcessTaskBatch(taskBatch) => {
      sendToPartitions(taskBatch)
    }
  }

  def sendToPartitions(taskBatch: Map[PartitionId, Seq[Task]]): Unit = {
    val perRequestActorProps = makePersistRequestActorProps(
      partitionSupervisors,
      taskBatch,
      persistRequestTimeout,
      sender
    )
    context.actorOf(perRequestActorProps)
  }
}
