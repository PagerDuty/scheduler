package com.pagerduty.scheduler

import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.eris.dao.ErisSettings
import com.pagerduty.eris.schema.SchemaLoader
import com.pagerduty.eris.{ClusterCtx, TimeUuid}
import com.pagerduty.eris.serializers._
import com.pagerduty.eris.dao._
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.widerow.{Bound, EntryColumn}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object CassandraTaskExecutorTestService {
  val KeyspaceName = "CassandraTaskExecutorTestService"
  val WorkersPerPartition = 2

  class LogEntryDao(
      protected val cluster: Cluster,
      protected val keyspace: Keyspace,
      override protected val settings: ErisSettings)
      extends Dao {

    protected implicit val executor: ExecutionContextExecutor = ExecutionContext.Implicits.global

    protected val logEntries = new WideRowMap(
      columnFamily[String, TimeUuid, String]("LogEntries"),
      pageSize = 100
    )

    private val rowKey = "mainLog"

    def insert(logEntry: String): Future[Unit] = {
      logEntries(rowKey).queueInsert(EntryColumn(TimeUuid(), logEntry)).executeAsync()
    }
  }

  private def makeLogEntryDao(cluster: Cluster) = {
    val keyspace = cluster.getKeyspace(KeyspaceName)
    new LogEntryDao(cluster, keyspace, new ErisSettings())
  }

  def schemaLoader(cluster: Cluster): SchemaLoader = {
    val dao = makeLogEntryDao(cluster)
    new SchemaLoader(cluster, dao.columnFamilyDefs)
  }

  def cassTestExecutorFactory(
      mkClusterCtx: () => ClusterCtx,
      taskRunner: Task => Unit
    ): Set[PartitionId] => TaskExecutorService = {
    val managedTaskRunner = new ManagedCassandraTaskRunner[LogEntryDao] {
      def makeClusterCtx(): ClusterCtx = mkClusterCtx()
      def makeManagedResource(clusterCtx: ClusterCtx): LogEntryDao = {
        makeLogEntryDao(clusterCtx.cluster)
      }
      def runTask(task: Task, logEntryDao: LogEntryDao): Unit = {
        taskRunner(task)
        Await.result(logEntryDao.insert(task.taskKey.toString()), Duration.Inf)
      }
    }
    CassandraTaskExecutorService.factory(WorkersPerPartition, managedTaskRunner)
  }
}
