package com.pagerduty.scheduler.admin

import com.pagerduty.metrics.NullMetrics
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.specutil.CassandraIntegrationSpec
import com.pagerduty.scheduler._
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers}

import scalaj.http.Http

class StandaloneAdminHttpServerSpec
  extends CassandraIntegrationSpec
  with Matchers
  with BeforeAndAfterAll {

  val config = ConfigFactory.load()

  def taskRunner(task: Task): Unit = {}

  val port = 4011
  val namespace = "scheduler"
  val aSettings = Settings(true, port, namespace)
  val settings = SchedulerSettings(ConfigFactory.load()).copy(adminSettings = aSettings)
  val executorFactory = SchedulerIntegrationSpecBase.simpleTestExecutorFactory(taskRunner)
  val scheduler = new SchedulerImpl(settings, config, NullMetrics, cluster, keyspace, executorFactory)()
  scheduler.start()

  override def afterAll(): Unit = {
    scheduler.shutdown()
  }

  "The standalone admin HTTP server" - {

    "should initialize properly and serve requests" in {
      val response = Http(s"http://localhost:$port/$namespace/api/v1/status").asString
      response.code should equal (200)
      response.body should include ("Scheduler Admin API is running")
    }

    "should parse a TaskKey that has escaped characters in the URL properly" in {
      val key = "TaskKey(2016-04-18T20:50:10.000Z,!@#$%^&*(),20160418-2048_Offcall_PGDWO5O_PALYC1L)"
      val response = Http(s"http://localhost:$port/$namespace/api/v1/task")
                       .params("key" -> key)
                       .asString
      response.code should equal (200)
    }
  }
}
