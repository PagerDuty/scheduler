package com.pagerduty.scheduler.dao

import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.eris.TimeUuid
import com.pagerduty.eris.dao._
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{Task, TaskKey}
import com.pagerduty.scheduler.specutil.TaskFactory
import com.pagerduty.scheduler.specutil.TestTimer
import java.time.temporal.ChronoUnit
import java.time.Instant
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{MustMatchers, fixture}
import scala.concurrent.duration._
import scala.util.Random

class TaskScheduleDaoSpec extends fixture.WordSpec with MustMatchers with DaoFixture with TestTimer with ScalaFutures {
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(3, Seconds)))

  type FixtureParam = TaskScheduleDaoImpl
  override protected def mkFixtureDao(cluster: Cluster, keyspace: Keyspace): FixtureParam = {
    new TaskScheduleDaoImpl(cluster, keyspace, new ErisSettings)
  }

  val partitionId: PartitionId = 1
  val limit = 1000

  "TaskScheduleDao" should {
    "insert and load tasks" in { dao =>
      val tasks = for (i <- 0 to 5) yield TaskFactory.makeTask()

      val initialState = dao.loadPlusMinusOneDayFrom(tasks.head)
      initialState mustBe empty

      dao.insert(partitionId, tasks).futureValue
      val persistedTasks = dao.loadPlusMinusOneDayFrom(tasks.head)
      persistedTasks mustBe tasks
    }

    "load edge case correctly" in { dao =>
      def taskFromTime(time: Instant) = {
        val taskKey = TaskKey.lowerBound(time)
        Task(taskKey, "{}")
      }
      val fromTime = Instant.now()
      val toTime = fromTime + 1.millisecond
      val tasks = Seq(taskFromTime(fromTime), taskFromTime(toTime))

      val initialState = dao.loadPlusMinusOneDayFrom(tasks.head)
      initialState mustBe empty

      dao.insert(partitionId, tasks).futureValue

      val from = TaskKey.lowerBound(fromTime)
      val persistedTasks = dao.load(partitionId, from, toTime, limit).futureValue
      persistedTasks mustBe Seq(tasks(0))
    }

    "load tasks when given a lower bound with full key" in { dao =>
      val now = Instant.now()
      val key1 = TaskKey(now, "1", "1")
      val key2 = TaskKey(now, "2", "1")
      val key3 = TaskKey(now, "2", "2")

      val t1 = Task(key1, "{}")
      val t2 = Task(key2, "{}")
      val t3 = Task(key3, "{}")

      val tasks = Seq(t1, t2, t3)

      val initialState = dao.loadPlusMinusOneDayFrom(tasks.head)
      initialState mustBe empty

      dao.insert(partitionId, tasks).futureValue

      val from = TaskKey.lowerBound(key3.scheduledTime, Some(key3.orderingId), Some(key3.uniquenessKey))
      val persistedTasks1 = dao.load(partitionId, from, now + 1.second, limit).futureValue
      val persistedTasks2 = dao
        .load(
          partitionId,
          from.scheduledTime,
          Some(from.orderingId),
          Some(from.uniquenessKey),
          now + 1.second,
          limit
        )
        .futureValue

      persistedTasks1 mustBe Seq(tasks(2))
      persistedTasks2 mustBe Seq(tasks(2))
    }

    "validate range arguments for load()" in { dao =>
      val from = TaskKey.lowerBound(Instant.now())
      val to = from.scheduledTime
      an[IllegalArgumentException] should be thrownBy {
        dao.load(partitionId, from, to, limit).futureValue
      }
    }

    "insert tasks into correct buckets" in { dao =>
      val (currentBucketTasks, nextBucketTasks) = {
        TaskFactory.makeTasksInConsecutiveBuckets(dao.rowTimeBucketDuration)
      }
      val allTasks = currentBucketTasks ++ nextBucketTasks

      val initialState = dao.loadPlusMinusOneDayFrom(allTasks.head)
      initialState mustBe empty

      dao.insert(partitionId, allTasks).futureValue
      val persistedTasks = dao.loadPlusMinusOneDayFrom(allTasks.head)
      persistedTasks mustEqual allTasks

      def testFor(bucketTime: Instant, expectedResult: Seq[Task]): Unit = {
        val from = TaskKey.lowerBound(bucketTime.truncatedTo(ChronoUnit.HOURS))
        val to = from.scheduledTime + dao.rowTimeBucketDuration
        val loadedTasks = dao.load(partitionId, from, to, limit).futureValue
        loadedTasks mustEqual expectedResult
      }

      testFor(currentBucketTasks.head.scheduledTime, currentBucketTasks)
      testFor(nextBucketTasks.head.scheduledTime, nextBucketTasks)
    }

    "remove task" in { dao =>
      val task = TaskFactory.makeTask()

      dao.insertAndBlock(task)
      val initialState = dao.loadPlusMinusOneDayFrom(task)
      initialState mustEqual Seq(task)

      dao.remove(partitionId, task.taskKey).futureValue
      val persistedTasks = dao.loadPlusMinusOneDayFrom(task)
      persistedTasks mustBe empty
    }

    "override tasks with the same uniqueness key" in { dao =>
      val task = TaskFactory.makeTask()

      dao.insertAndBlock(task)
      val initialState = dao.loadPlusMinusOneDayFrom(task)
      initialState mustEqual Seq(task)

      dao.insertAndBlock(task)
      val persistedTasks = dao.loadPlusMinusOneDayFrom(task)
      persistedTasks mustEqual Seq(task)
    }

    "insert multiple tasks when uniqueness keys are different" in { dao =>
      val task1 = TaskFactory.makeTask()

      dao.insertAndBlock(task1)
      val initialState = dao.loadPlusMinusOneDayFrom(task1)
      initialState mustEqual Seq(task1)

      val task2 = task1.copy(uniquenessKey = TimeUuid().toString)
      dao.insertAndBlock(task2)
      val persistedTasks = dao.loadPlusMinusOneDayFrom(task1)
      persistedTasks mustEqual Seq(task1, task2)
    }

    "load tasks from a given set of partitions" in { dao =>
      val partitionIds: Set[PartitionId] = Seq(5, 70).toSet
      var manuallyMappedTasks = Map[PartitionId, IndexedSeq[Task]]()
      val tasks = TaskFactory.makeTasks(partitionIds.size)
      (partitionIds, tasks).zipped.foreach { (id, task) =>
        {
          dao.insert(id, Seq(task)).futureValue
          manuallyMappedTasks += (id -> IndexedSeq(task))
        }
      }
      var tasksFromCassandra = Map[PartitionId, IndexedSeq[Task]]()
      tasksFromCassandra =
        dao.loadTasksFromPartitions(partitionIds, Instant.now() - 2.hours, Instant.now(), 10).futureValue

      tasksFromCassandra.foreach {
        case (key, _) =>
          tasksFromCassandra(key) mustEqual manuallyMappedTasks(key)
      }
    }
    "load count of tasks from a given set of partitions" in { dao =>
      val partitionIds: Set[PartitionId] = (75 until 90).toSet
      val tasks = TaskFactory.makeTasks(partitionIds.size)
      (partitionIds, tasks).zipped.foreach { (id, task) =>
        {
          dao.insert(id, Seq(task)).futureValue
        }
      }

      //Edge conditions. These tasks should not be counted when getting the task count
      val reallyNewTask = TaskFactory.makeTask(Instant.now() + 30.seconds)
      val reallyOldTask = TaskFactory.makeTask(Instant.now() - (2.hours + 30.seconds))
      dao.insert(partitionIds.head, Seq(reallyOldTask, reallyNewTask)).futureValue

      val totalTaskCount =
        dao.getTotalTaskCount(partitionIds, Instant.now() - 2.hours, Instant.now(), 100).futureValue

      totalTaskCount mustEqual tasks.size
    }

    "find a single task" when {
      "it does exist" in { dao =>
        val tasks = for (i <- 0 to 5) yield TaskFactory.makeTask()
        val initialState = dao.loadPlusMinusOneDayFrom(tasks.head)
        initialState mustBe empty

        for (task <- tasks) dao.insert(partitionId, List(task)).futureValue
        val taskToFind = Random.shuffle(tasks).head
        dao.find(partitionId, taskToFind.taskKey).futureValue must equal(Some(taskToFind))
      }

      "it doesn't exist" in { dao =>
        val tasks = for (i <- 0 to 5) yield TaskFactory.makeTask()
        for (task <- tasks) dao.insert(partitionId, List(task)).futureValue

        val taskToFind = Random.shuffle(tasks).head.copy(orderingId = "non-existent-id")
        dao.find(partitionId, taskToFind.taskKey).futureValue must equal(None)
      }
    }
  }

  private implicit class ExtendedDao(dao: TaskScheduleDaoImpl) {
    def insertAndBlock(task: Task): Unit = {
      dao.insert(partitionId, Seq(task)).futureValue
    }
    def loadPlusMinusOneDayFrom(task: Task): Seq[Task] = {
      val from = TaskKey.lowerBound(task.scheduledTime - 1.day)
      val to = task.scheduledTime + 1.day
      val future = dao.load(partitionId, from, to, limit)
      future.futureValue
    }
  }
}
