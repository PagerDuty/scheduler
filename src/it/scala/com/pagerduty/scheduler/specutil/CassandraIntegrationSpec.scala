package com.pagerduty.scheduler.specutil

import com.pagerduty.eris.ClusterCtx
import com.pagerduty.eris.config.{AstyanaxConfigBuilder, ConnectionPoolConfigBuilder}
import com.pagerduty.eris.custom.{ErisPdSettings, PdConnectionPoolMonitorImpl}
import com.pagerduty.eris.schema.SchemaLoader
import com.pagerduty.scheduler.dao.{AttemptHistoryDaoImpl, TaskStatusDaoImpl, TaskScheduleDaoImpl}
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike}

import scala.util.control.NonFatal

trait CassandraIntegrationSpec extends FreeSpecLike with BeforeAndAfterAll {
  def makeClusterCtx() = new ClusterCtx(
    clusterName = "CassCluster",
    astyanaxConfig = AstyanaxConfigBuilder.build(
      asyncThreadPoolName = "AstyanaxAsync",
      asyncThreadPoolSize = 50
    ),
    connectionPoolConfig = ConnectionPoolConfigBuilder.build(
      connectionPoolName = "CassConnectionPool",
      cassPort = 9160,
      hosts = "localhost:9160",
      maxConnectionsPerHost = 50
    ),
    connectionPoolMonitor = new PdConnectionPoolMonitorImpl
  )
  lazy val clusterCtx = makeClusterCtx()

  val cluster = clusterCtx.cluster
  val keyspace = cluster.getKeyspace("SchedulerIntegrationSpec")
  val erisPdSettings = new ErisPdSettings()
  lazy val attemptHistoryDaoImpl = new AttemptHistoryDaoImpl(cluster, keyspace, erisPdSettings)

  override def beforeAll() {
    dropAndLoadCassSchema()
    super.beforeAll()
  }

  override def afterAll() {
    super.afterAll()
    clusterCtx.shutdown()
  }

  def dropAndLoadCassSchema(): Unit = {
    val schemaLoader = getSchemaLoader()
    try {
      schemaLoader.dropSchema()
    }
    catch {
      case NonFatal(e) => println(s"Error dropping schema: $e")
    }
    try {
      schemaLoader.loadSchema()
    }
    catch {
      case NonFatal(e) => println(s"Error loading schema: $e")
    }
  }

  protected def getSchemaLoader(): SchemaLoader = {
    val taskScheduleDaoImpl = new TaskScheduleDaoImpl(cluster, keyspace, erisPdSettings)
    val taskStatusDaoImpl = new TaskStatusDaoImpl(cluster, keyspace, erisPdSettings)

    val columnFamilyDefs = {
      taskScheduleDaoImpl.columnFamilyDefs ++
        taskStatusDaoImpl.columnFamilyDefs ++
        attemptHistoryDaoImpl.columnFamilyDefs
    }
    new SchemaLoader(cluster, columnFamilyDefs)
  }
}
