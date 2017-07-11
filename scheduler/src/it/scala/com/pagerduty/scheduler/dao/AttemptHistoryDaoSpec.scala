package com.pagerduty.scheduler.dao

import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.eris.dao._
import com.pagerduty.scheduler.model.{CompletionResult, TaskAttempt, TaskKey}
import com.pagerduty.scheduler.specutil.{TaskAttemptFactory, TaskFactory, TestTimer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{MustMatchers, fixture}
import scala.concurrent.duration._

class AttemptHistoryDaoSpec
    extends fixture.WordSpec
    with MustMatchers
    with DaoFixture
    with TestTimer
    with ScalaFutures {
  import TaskAttemptFactory.makeTaskAttempt
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(3, Seconds)))

  type FixtureParam = AttemptHistoryDaoImpl
  val columnTtl = 2.seconds
  override protected def mkFixtureDao(cluster: Cluster, keyspace: Keyspace): FixtureParam = {
    new AttemptHistoryDaoImpl(cluster, keyspace, new ErisSettings, columnTtl)
  }
  val limit = 100
  val partitionId = 5

  "AttemptHistoryDao" should {
    "insert task attempts with a TTL" in { dao =>
      val taskKey = TaskFactory.makeTaskKey()
      val taskAttempt1 = makeTaskAttempt(attemptNumber = 1, CompletionResult.Incomplete)
      val taskAttempt2 = makeTaskAttempt(attemptNumber = 2, CompletionResult.Success)
      dao.insert(partitionId, taskKey, taskAttempt1).futureValue
      dao.insert(partitionId, taskKey, taskAttempt2).futureValue
      dao.loadAllAttempts(taskKey) mustEqual Seq(taskAttempt1, taskAttempt2)

      Thread.sleep(columnTtl.toMillis) // wait for TTL to expire
      dao.loadAllAttempts(taskKey) mustBe empty
    }

    "load task attempts" in { dao =>
      val taskKey = TaskFactory.makeTaskKey()
      val attemptCount = 10
      val taskAttempts = for (i <- 1 to attemptCount) yield {
        makeTaskAttempt(attemptNumber = i, CompletionResult.Incomplete)
      }
      taskAttempts.foreach(dao.insert(partitionId, taskKey, _).futureValue)
      dao.load(partitionId, taskKey, 5).futureValue mustEqual taskAttempts.take(5)
    }
  }

  private implicit class ExtendedDao(dao: AttemptHistoryDaoImpl) {
    def loadAllAttempts(taskKey: TaskKey): Seq[TaskAttempt] = {
      dao.load(partitionId, taskKey, limit = 1000).futureValue
    }
  }
}
