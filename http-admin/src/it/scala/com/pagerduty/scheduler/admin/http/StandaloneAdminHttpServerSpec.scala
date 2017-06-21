package com.pagerduty.scheduler.admin.http

import com.pagerduty.metrics.NullMetrics
import com.pagerduty.scheduler._
import com.pagerduty.scheduler.admin.http.standalone.AdminHttpServer
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.specutil.CassandraIntegrationSpec
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers}

import scalaj.http.Http

class StandaloneAdminHttpServerSpec extends CassandraIntegrationSpec with Matchers with BeforeAndAfterAll {

  val config = ConfigFactory.load()

  def taskRunner(task: Task): Unit = {}

  val port = 4011
  val namespace = "scheduler"
  val aSettings = Settings(port, namespace)
  val settings = SchedulerSettings(ConfigFactory.load())
  val executorFactory = SchedulerIntegrationSpecBase.simpleTestExecutorFactory(taskRunner)
  val scheduler = new SchedulerImpl(settings, config, NullMetrics, cluster, keyspace, executorFactory)()
  val adminServer = new AdminHttpServer(aSettings, scheduler.adminService)
  adminServer.start()

  override def afterAll(): Unit = {
    adminServer.stop()
  }

  "The standalone admin HTTP server" - {

    "should initialize properly and serve requests" in {
      val response = Http(s"http://localhost:$port/$namespace/api/v1/status").asString
      response.code should equal(200)
      response.body should include("Scheduler Admin API is running")
    }
  }
}
