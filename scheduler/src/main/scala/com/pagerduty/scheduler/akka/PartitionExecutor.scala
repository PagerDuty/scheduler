package com.pagerduty.scheduler.akka

import com.pagerduty.scheduler.akka.OrderingExecutor.ExecuteOrderingTask
import akka.actor._
import com.pagerduty.scheduler.model.{Task, TaskKey}

import scala.collection.mutable.{Map, Set}
import scala.language.postfixOps

object PartitionExecutor {
  case class Initialize(partitionContext: PartitionContext)

  /**
    * Request for a task to be executed
    *
    * @param task
    */
  case class ExecutePartitionTask(task: Task)

  def props(settings: Settings): Props = {
    val orderingExecutorFactory = {
      (context: ActorRefFactory,
       partitionContext: PartitionContext,
       orderingId: Task.OrderingId,
       partitionExecutor: ActorRef) =>
        {
          val orderingExecutorProps = OrderingExecutor.props(
            settings,
            partitionContext,
            orderingId,
            partitionExecutor
          )
          context.actorOf(orderingExecutorProps)
        }
    }
    Props(new PartitionExecutor(orderingExecutorFactory))
  }
}

/**
  * Partition executor manages the execution of tasks for a partition. It spawns OrderingExecutors
  * for each unique `orderingId` and also sends the throughput controller the number of tasks in flight
  * for the partition.
  *
  * @param orderingExecutorFactory
  */
class PartitionExecutor(
    orderingExecutorFactory: (ActorRefFactory, PartitionContext, Task.OrderingId, ActorRef) => ActorRef)
    extends Actor
    with ActorLogging
    with Stash {
  case class OrderingExecutorData(tasks: Set[TaskKey], orderingExecutor: ActorRef)
  import PartitionExecutor._
  var partitionContext: PartitionContext = _
  def partitionId = partitionContext.partitionId
  def logging = partitionContext.logging

  override val supervisorStrategy = Supervision.AlwaysEscalateStrategy
  val orderingExecutors: Map[Task.OrderingId, OrderingExecutorData] = Map()

  def receive = {
    case init: Initialize => {
      partitionContext = init.partitionContext
      context.become(initialized)
      unstashAll()
    }
    case _ => {
      stash()
    }
  }

  def initialized: Receive = {
    case ExecutePartitionTask(task) => {
      val orderingId = task.orderingId
      if (orderingExecutors contains orderingId) {
        getOrderingTasks(orderingId) += task.taskKey
      } else {
        orderingExecutors += (orderingId -> createOrderingExecutor(task))
      }
      getOrderingExecutor(orderingId) ! ExecuteOrderingTask(task)
      reportInMemoryTaskCount()
    }
    case OrderingExecutor.OrderingTaskExecuted(taskKey) if orderingExecutors contains taskKey.orderingId => {
      val orderingId = taskKey.orderingId
      val orderingTasks = getOrderingTasks(orderingId)
      orderingTasks -= taskKey

      if (orderingTasks isEmpty) {
        context stop getOrderingExecutor(orderingId)
        orderingExecutors -= orderingId
      }
      reportInMemoryTaskCount()
    }
    case ThroughputController.FetchInProgressTaskCount => {
      sender ! ThroughputController.InProgressTaskCountFetched(inFlightTaskCount)
    }
  }
  private def createOrderingExecutorActor(orderingId: Task.OrderingId): ActorRef = {
    orderingExecutorFactory(context, partitionContext, orderingId, context.self)
  }

  private def createOrderingExecutor(task: Task): OrderingExecutorData = {
    OrderingExecutorData(Set(task.taskKey), createOrderingExecutorActor(task.orderingId))
  }

  private def getOrderingExecutor(orderingId: Task.OrderingId): ActorRef = {
    orderingExecutors(orderingId).orderingExecutor
  }
  private def getOrderingTasks(orderingId: Task.OrderingId): Set[TaskKey] = {
    orderingExecutors(orderingId).tasks
  }
  private def inFlightTaskCount: Int = {
    orderingExecutors.map { case (_, orderingData) => orderingData.tasks.size }.sum
  }
  private def reportInMemoryTaskCount(): Unit = {
    logging.reportInMemoryTaskCount(partitionId, "partitionExecutor", inFlightTaskCount)
  }
}
