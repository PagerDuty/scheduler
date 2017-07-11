package com.pagerduty.scheduler

import com.pagerduty.eris.ClusterCtx
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.model.Task.PartitionId
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * This service is used to execute tasks scheduled by users of the Scheduler library. It is backed
  * by a fixed-size thread pool. Take care to configure the thread pool to have an appropriate size
  * for the expected workload.
  *
  * Scheduler will create a new TaskExecutorService using provided factory when new partitions are
  * assigned. When partitions are revoked, Scheduler will shutdown the TaskExecutorService.
  *
  * @param threadPoolSize the size of the thread pool of the executor service.
  * @param managedTaskRunner special task runner that also has resource management hooks
  * @tparam Resource application-specific resource
  */
class CassandraTaskExecutorService[Resource](
    threadPoolSize: Int,
    managedTaskRunner: ManagedCassandraTaskRunner[Resource])
    extends TaskExecutorService {

  protected val executionContext = FixedSizeExecutionContext(threadPoolSize)
  protected val clusterCtx = {
    val clusterCtx = managedTaskRunner.makeClusterCtx()
    clusterCtx.start()
    clusterCtx
  }
  protected val resource = managedTaskRunner.makeManagedResource(clusterCtx)

  def execute(partitionId: PartitionId, task: Task): Future[Unit] = {
    Future(
      managedTaskRunner.runTask(task, resource)
    )(executionContext)
  }

  def shutdown(): Unit = {
    executionContext.shutdown()
    blockingWait(CassandraTaskExecutorService.ShutdownGracePeriod)
    clusterCtx.shutdown()
    blockingWait(CassandraTaskExecutorService.CassandraClockDriftAllowance)
  }
}

object CassandraTaskExecutorService {
  val ShutdownGracePeriod = 500.milliseconds
  val CassandraClockDriftAllowance = 100.milliseconds

  /**
    * Returns a factory that can create a new CassandraTaskExecutorService. Used in Scheduler
    * to create disposable resources when new partitions are assigned. Scheduler will then
    * shutdown managed resources when partitions are revoked.
    * @param workersPerPartition
    * @param managedTaskRunner
    * @tparam Resource
    * @return
    */
  def factory[Resource](
      workersPerPartition: Int,
      managedTaskRunner: ManagedCassandraTaskRunner[Resource]
    ): Set[PartitionId] => TaskExecutorService = { partitions =>
    val workerCount = workersPerPartition * math.max(1, partitions.size)
    new CassandraTaskExecutorService(workerCount, managedTaskRunner)
  }
}

/**
  * This trait formalizes relationship between tasks and cassandra dependent resources.
  * @tparam Resource application-specific resource
  */
trait ManagedCassandraTaskRunner[Resource] {

  /**
    * Creates a new clusterCtx instance when partitions are assigned. The returned clusterCtx
    * will be shut down when partitions are revoked.
    * @return newly created clusterCtx instance
    */
  def makeClusterCtx(): ClusterCtx

  /**
    * Created an application-specific resource using a newly created clusterCtx instance.
    * @param clusterCtx
    * @return newly created application specific resource
    */
  def makeManagedResource(clusterCtx: ClusterCtx): Resource

  /**
    * Runs target task using managed application-specific resource.
    * @param task
    * @param resource
    */
  def runTask(task: Task, resource: Resource): Unit
}
