package com.pagerduty.scheduler.dao

import scala.util.Random
import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.eris.dao._
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{CompletionResult, Task, TaskKey, TaskStatus}
import com.pagerduty.scheduler.specutil.TaskFactory
import com.pagerduty.scheduler.specutil.TestTimer
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.scalatest.{MustMatchers, fixture}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import scala.concurrent.duration._

class TaskStatusDaoSpec extends fixture.WordSpec with MustMatchers with DaoFixture with TestTimer with ScalaFutures {
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(3, Seconds)))

  type FixtureParam = TaskStatusDaoImpl
  val columnTtl = 3.seconds
  override protected def mkFixtureDao(cluster: Cluster, keyspace: Keyspace): FixtureParam = {
    new TaskStatusDaoImpl(cluster, keyspace, new ErisSettings, columnTtl)
  }
  val successStatus: TaskStatus = TaskStatus.successful(2)
  val failureStatus: TaskStatus = TaskStatus.failed(3)
  val droppedStatus: TaskStatus = TaskStatus.Dropped
  val limit = 100
  val partitionId = 2

  "TaskStatusDao" should {
    "insert task key with a TTL" in { dao =>
      val task = TaskFactory.makeTask()

      val initialState = dao.loadPlusMinusOneSecondFrom(task)
      initialState mustBe empty
      dao.insert(partitionId, task.taskKey, successStatus).futureValue
      val persistedTaskKeys = dao.loadPlusMinusOneSecondFrom(task)
      persistedTaskKeys mustEqual Seq(task.taskKey)

      Thread.sleep(columnTtl.toMillis) // wait for TTL to expire

      dao.loadPlusMinusOneSecondFrom(task) mustBe empty
    }

    "load task keys" in { dao =>
      val tasks = for (i <- 0 to 5) yield TaskFactory.makeTask()
      val initialState = dao.loadPlusMinusOneSecondFrom(tasks.head)
      initialState mustBe empty

      for (task <- tasks) dao.insert(partitionId, task.taskKey, successStatus).futureValue
      val persistedTaskKeys = dao.loadPlusMinusOneSecondFrom(tasks.head)
      persistedTaskKeys mustBe tasks.map(_.taskKey)
    }

    "load status for a single task" when {
      "it does exist" in { dao =>
        val tasks = for (i <- 0 to 5) yield TaskFactory.makeTask()
        val initialState = dao.loadPlusMinusOneSecondFrom(tasks.head)
        initialState mustBe empty

        for (task <- tasks) dao.insert(partitionId, task.taskKey, successStatus).futureValue
        val taskKey = Random.shuffle(tasks).head.taskKey
        dao.getStatus(partitionId, taskKey).futureValue mustBe successStatus
      }

      "it doesn't exist" in { dao =>
        val tasks = for (i <- 0 to 5) yield TaskFactory.makeTask()
        val initialState = dao.loadPlusMinusOneSecondFrom(tasks.head)
        initialState mustBe empty

        for (task <- tasks) dao.insert(partitionId, task.taskKey, successStatus).futureValue

        tasks.foreach { task =>
          val scheduledTime = task.scheduledTime
          val closeKey = task.taskKey.copy(scheduledTime = scheduledTime + 1.second)
          dao.getStatus(partitionId, closeKey).futureValue mustBe TaskStatus.NeverAttempted
        }
      }
    }

    "dedup task keys correctly" in { dao =>
      val task = TaskFactory.makeTask()
      dao.insert(partitionId, task.taskKey, successStatus).futureValue
      dao.insert(partitionId, task.taskKey, successStatus).futureValue

      val persistedTaskKeys = dao.loadPlusMinusOneSecondFrom(task)
      persistedTaskKeys mustBe Seq(task.taskKey)
    }

    "insert failed task status" in { dao =>
      val task = TaskFactory.makeTask()
      dao.insert(partitionId, task.taskKey, failureStatus).futureValue
      val persistedTaskEntries = dao.loadPlusMinusOneSecondWithStatusFrom(task)
      persistedTaskEntries mustBe Seq(task.taskKey -> failureStatus)
    }

    "insert successful task status" in { dao =>
      val task = TaskFactory.makeTask()
      dao.insert(partitionId, task.taskKey, successStatus).futureValue
      val persistedTaskEntries = dao.loadPlusMinusOneSecondWithStatusFrom(task)
      persistedTaskEntries mustBe Seq(task.taskKey -> successStatus)
    }

    "insert incomplete task status" in { dao =>
      val task = TaskFactory.makeTask()
      val nextAttemptAt = Some(Instant.now().truncatedTo(ChronoUnit.MILLIS))
      val status = TaskStatus(2, CompletionResult.Incomplete, nextAttemptAt)
      dao.insert(partitionId, task.taskKey, status).futureValue
      val persistedTaskEntries = dao.loadPlusMinusOneSecondWithStatusFrom(task)
      persistedTaskEntries mustBe Seq(task.taskKey -> status)
    }

    "drop task on provided partitions" in { dao =>
      val selectedPartitions = Set(1, 3, 6)
      val task = TaskFactory.makeTask()
      dao.dropTaskOnPartitions(selectedPartitions, task.taskKey).futureValue

      val allPartitions = (selectedPartitions.min to selectedPartitions.max).toSet
      for (partitionId <- allPartitions) {
        val persistedTaskEntries = dao.loadPlusMinusOneSecondWithStatusFrom(partitionId, task)
        if (selectedPartitions.contains(partitionId)) {
          persistedTaskEntries mustBe Seq(task.taskKey -> droppedStatus)
        } else {
          persistedTaskEntries mustBe Seq.empty
        }
      }
    }
  }

  private implicit class ExtendedDao(dao: TaskStatusDaoImpl) {
    def loadPlusMinusOneSecondFrom(task: Task): Seq[TaskKey] = {
      val from = task.scheduledTime - 1.second
      val to = task.scheduledTime + 1.second
      val results = dao.load(partitionId, from, to, limit).futureValue
      results.map { case (key, _) => key }
    }

    def loadPlusMinusOneSecondWithStatusFrom(task: Task): Seq[(TaskKey, TaskStatus)] = {
      loadPlusMinusOneSecondWithStatusFrom(partitionId, task)
    }
    def loadPlusMinusOneSecondWithStatusFrom(partitionId: PartitionId, task: Task): Seq[(TaskKey, TaskStatus)] = {
      val from = task.scheduledTime - 1.second
      val to = task.scheduledTime + 1.second
      dao.load(partitionId, from, to, limit).futureValue
    }
  }
}
