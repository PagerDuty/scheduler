package com.pagerduty.scheduler.akka

import akka.actor.{ActorRef, ActorRefFactory, Props, Stash}
import com.pagerduty.scheduler.model.{Task, TaskKey}

import scala.collection.immutable.SortedMap

object OrderingExecutor {

  /**
    * Request for a task to be executed.
    * @param task
    */
  case class ExecuteOrderingTask(task: Task)

  /**
    * Receives this message from an OrderingExecutor when the OrderingExecutor has finished executing
    * as task
    *
    * @param taskKey
    */
  case class OrderingTaskExecuted(taskKey: TaskKey)

  /**
    * Reply upon successful task execution. Tasks are retried indefinitely, so we never return
    * a failure.
    * @param taskKey
    */
  case class TaskExecuted(taskKey: TaskKey)

  sealed trait State
  case object Idle extends State
  case object Executing extends State

  case class Data(taskQueue: SortedMap[TaskKey, Task])

  def props(
      settings: Settings,
      partitionContext: PartitionContext,
      orderingId: Task.OrderingId,
      partitionExecutor: ActorRef
    ): Props = {
    val taskExecutorFactory = { (context: ActorRefFactory, task: Task, orderingExecutorRef: ActorRef) =>
      {
        context.actorOf(TaskExecutor.props(settings, partitionContext, task, orderingExecutorRef))
      }
    }
    Props(new OrderingExecutor(orderingId, partitionExecutor, taskExecutorFactory))
  }
}

/**
  * OrderingExecutor manages task queue for a given `orderingId`. Tasks are executed in sequence ordered
  * by TaskKeys, one task at a time. A new TaskExecutor child is created for each task.
  * @param orderingId
  * @param partitionExecutor
  * @param taskExecutorFactory
  */
class OrderingExecutor(
    orderingId: Task.OrderingId,
    partitionExecutor: ActorRef,
    taskExecutorFactory: (ActorRefFactory, Task, ActorRef) => ActorRef)
    extends ExtendedLoggingFSM[OrderingExecutor.State, OrderingExecutor.Data]
    with Stash {

  import OrderingExecutor._
  import PartitionExecutor._

  startWith(Idle, Data(SortedMap.empty))
  override val supervisorStrategy = Supervision.AlwaysEscalateStrategy

  when(Idle) {
    case Event(ExecuteOrderingTask(task), data) if task.orderingId == orderingId => {
      assert(data.taskQueue.isEmpty)
      execute(task)
      goto(Executing)
    }
  }

  when(Executing) {
    case Event(ExecuteOrderingTask(task), data) if task.orderingId == orderingId => {
      stay() using Data(data.taskQueue + (task.taskKey -> task))
    }
    case Event(TaskExecuted(taskKey), data) if taskKey.orderingId == orderingId => {
      partitionExecutor ! OrderingTaskExecuted(taskKey)
      if (data.taskQueue.isEmpty) {
        goto(Idle)
      } else {
        val (taskKey, task) = data.taskQueue.head
        val remaining = data.taskQueue.tail
        execute(task)
        stay() using Data(remaining)
      }
    }
  }

  def execute(task: Task): Unit = {
    taskExecutorFactory(context, task, context.self)
  }
}
