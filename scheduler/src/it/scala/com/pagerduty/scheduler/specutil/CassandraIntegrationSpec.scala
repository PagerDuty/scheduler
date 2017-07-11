package com.pagerduty.scheduler.specutil

import com.pagerduty.scheduler.dao.TestClusterCtx
import com.pagerduty.eris.dao.ErisSettings
import com.pagerduty.eris.schema.SchemaLoader
import com.pagerduty.scheduler.dao.{AttemptHistoryDaoImpl, TaskStatusDaoImpl, TaskScheduleDaoImpl}
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike}

import scala.util.control.NonFatal

trait CassandraIntegrationSpec extends FreeSpecLike with BeforeAndAfterAll {
  def makeClusterCtx() = new TestClusterCtx
  lazy val clusterCtx = makeClusterCtx()

  val cluster = clusterCtx.cluster
  val keyspace = cluster.getKeyspace("SchedulerIntegrationSpec")
  val erisSettings = new ErisSettings()
  lazy val attemptHistoryDaoImpl = new AttemptHistoryDaoImpl(cluster, keyspace, erisSettings)

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
    } catch {
      case NonFatal(e) => println(s"Error dropping schema: $e")
    }
    try {
      schemaLoader.loadSchema()
    } catch {
      case NonFatal(e) => println(s"Error loading schema: $e")
    }
  }

  protected def getSchemaLoader(): SchemaLoader = {
    val taskScheduleDaoImpl = new TaskScheduleDaoImpl(cluster, keyspace, erisSettings)
    val taskStatusDaoImpl = new TaskStatusDaoImpl(cluster, keyspace, erisSettings)

    val columnFamilyDefs = {
      taskScheduleDaoImpl.columnFamilyDefs ++
        taskStatusDaoImpl.columnFamilyDefs ++
        attemptHistoryDaoImpl.columnFamilyDefs
    }
    new SchemaLoader(cluster, columnFamilyDefs)
  }
}
