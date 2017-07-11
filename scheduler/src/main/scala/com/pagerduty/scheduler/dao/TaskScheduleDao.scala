package com.pagerduty.scheduler.dao

import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ByteBufferRange
import com.netflix.astyanax.util.RangeBuilder
import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.eris.FutureConversions._
import com.pagerduty.eris.dao._
import com.pagerduty.eris.serializers._
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{Task, TaskKey}
import com.pagerduty.widerow.{Bound, EntryColumn}
import java.time.Instant
import scala.concurrent.Future

trait TaskScheduleDao {

  /**
    * Insert all tasks into the schedule index.
    * @param tasks
    * @return
    */
  def insert(partitionId: PartitionId, tasks: Iterable[Task]): Future[Unit]

  /**
    * Remove task with given taskKey from the schedule index.
    * @param taskKey
    * @return
    */
  def remove(partitionId: PartitionId, taskKey: TaskKey): Future[Unit]

  /**
    * Loads all the tasks in the range.
    * @param from start of the range, inclusive, scheduled time must be less than `to`
    * @param to end of the range, exclusive
    * @param limit maximum number of results
    * @return
    */
  def load(partitionId: PartitionId, from: TaskKey, to: Instant, limit: Int): Future[IndexedSeq[Task]]

  def find(partitionId: PartitionId, key: TaskKey): Future[Option[Task]]

  /**
    * Loads tasks from a set of partitions over a given time period
    * @param partitionIds The ids of the partitions you want to query for
    * @param from
    * @param to
    * @return A map which links PartitionId -> IndexedSeq[Task] for the given partitions
    */
  def loadTasksFromPartitions(
      partitionIds: Set[PartitionId],
      from: Instant,
      to: Instant,
      limitPerPartition: Int
    ): Future[Map[PartitionId, IndexedSeq[Task]]] = {
    loadTasksFromPartitions(
      partitionIds,
      from,
      None,
      None,
      to,
      limitPerPartition
    )
  }

  def loadTasksFromPartitions(
      partitionIds: Set[PartitionId],
      from: Instant,
      fromOrderingId: Option[Task.OrderingId],
      fromUniquenessKey: Option[Task.UniquenessKey],
      to: Instant,
      limitPerPartition: Int
    ): Future[Map[PartitionId, IndexedSeq[Task]]]

  /**
    * Gets the total task count across all partitions
    * @param partitions set of partition ids
    * @param from
    * @param to
    * @param limitPerPartition the max count of tasks that can be returned per partition
    * @return
    */
  def getTotalTaskCount(partitions: Set[PartitionId], from: Instant, to: Instant, limitPerPartition: Int): Future[Int]

}

class TaskScheduleDaoImpl(
    protected val cluster: Cluster,
    protected val keyspace: Keyspace,
    override protected val settings: ErisSettings)
    extends TaskScheduleDao
    with TaskDaoImpl
    with Dao {

  type RowKey = (PartitionId, TimeBucketKey)

  protected val scheduleIndex = new WideRowMap(
    columnFamily[RowKey, TaskKey, String]("TaskSchedule"),
    pageSize = 100
  )

  protected val scheduleView = {
    scheduleIndex.map {
      case EntryColumn(taskKey, taskDataString) => Task(taskKey, taskDataString)
    }
  }

  def insert(partitionId: PartitionId, tasks: Iterable[Task]): Future[Unit] = {
    val tasksByBucket = tasks.groupBy(task => getTimeBucketKey(task.scheduledTime))
    val futures = tasksByBucket.map {
      case (timeBucketKey, tasks) => insertTasksIntoTimeBucket(partitionId, timeBucketKey, tasks)
    }
    Future.sequence(futures).map(_ => Unit)
  }

  private def insertTasksIntoTimeBucket(
      partitionId: PartitionId,
      timeBucketKey: TimeBucketKey,
      tasks: Iterable[Task]
    ): Future[Unit] = {
    val rowKey = (partitionId, timeBucketKey)
    val batchUpdater = scheduleIndex(rowKey)
    for (task <- tasks) {
      val column = EntryColumn(task.taskKey, task.taskDataString)
      batchUpdater.queueInsert(column)
    }
    batchUpdater.executeAsync()
  }

  def remove(partitionId: PartitionId, taskKey: TaskKey): Future[Unit] = {
    val rowKey = (partitionId, getTimeBucketKey(taskKey.scheduledTime))
    scheduleIndex(rowKey).queueRemove(taskKey).executeAsync()
  }

  def load(partitionId: PartitionId, from: TaskKey, to: Instant, limit: Int): Future[IndexedSeq[Task]] = {
    load(partitionId, from.scheduledTime, Some(from.orderingId), Some(from.uniquenessKey), to, limit)
  }

  def load(partitionId: PartitionId, from: Instant, to: Instant, limit: Int): Future[IndexedSeq[Task]] = {
    load(partitionId, from, None, None, to, limit)
  }

  def load(
      partitionId: PartitionId,
      from: Instant,
      fromOrderingId: Option[Task.OrderingId],
      fromUniquenessKey: Option[Task.UniquenessKey],
      to: Instant,
      limit: Int
    ): Future[IndexedSeq[Task]] = {
    require(from.compareTo(to) < 0, "`from` time must be less than `to`")
    val rowKeys = getRowKeysExclusive(partitionId, from, to)
    val fromBound = Bound(TaskKey.lowerBound(from, fromOrderingId, fromUniquenessKey))
    val toBound = Bound(TaskKey.lowerBound(to), inclusive = false)
    scheduleView.get(
      colLimit = Some(limit),
      rowKeys = rowKeys,
      lowerBound = fromBound,
      upperBound = toBound
    )
  }

  def find(partitionId: PartitionId, taskKey: TaskKey): Future[Option[Task]] = {
    scheduleView.get(
      colLimit = Some(1),
      rowKeys = Seq(makeRowKey(partitionId, taskKey)),
      lowerBound = Bound(taskKey),
      upperBound = Bound(taskKey)
    ) map (_.headOption)
  }

  def loadTasksFromPartitions(
      partitionIds: Set[PartitionId],
      from: Instant,
      fromOrderingId: Option[Task.OrderingId],
      fromUniquenessKey: Option[Task.UniquenessKey],
      to: Instant,
      limitPerPartition: Int
    ): Future[Map[PartitionId, IndexedSeq[Task]]] = {
    val results = Map[PartitionId, IndexedSeq[Task]]()

    def recursivelyLoadTasks(
        partitionIds: Set[PartitionId],
        from: Instant,
        fromOrderingId: Option[Task.OrderingId],
        fromUniquenessKey: Option[Task.UniquenessKey],
        to: Instant,
        limitPerPartition: Int,
        results: Map[PartitionId, IndexedSeq[Task]]
      ): Future[Map[PartitionId, IndexedSeq[Task]]] = {
      if (partitionIds.nonEmpty) {
        val id = partitionIds.head
        val subsetOfIds = partitionIds.drop(1)
        load(id, from, fromOrderingId, fromUniquenessKey, to, limitPerPartition) flatMap { res =>
          recursivelyLoadTasks(
            subsetOfIds,
            from,
            fromOrderingId,
            fromUniquenessKey,
            to,
            limitPerPartition,
            results + (id -> res)
          )
        }
      } else {
        Future(results)
      }
    }

    recursivelyLoadTasks(partitionIds, from, fromOrderingId, fromUniquenessKey, to, limitPerPartition, results)
  }

  private def getTaskCount(partitionId: PartitionId, from: Instant, to: Instant, limit: Int): Future[Int] = {
    val rowKeys = getRowKeysExclusive(partitionId, from, to)
    val columnFamilyModel = scheduleIndex.columnFamilyModel
    val timeRange =
      buildRange(TaskKey.lowerBound(from), TaskKey.lowerBound(to), limit)
    val taskCountFutureList: Seq[Future[OperationResult[Integer]]] =
      rowKeys.map { rowKey =>
        val future: Future[OperationResult[Integer]] =
          columnFamilyModel.keyspace
            .prepareQuery(columnFamilyModel.columnFamily)
            .getKey(rowKey)
            .withColumnRange(timeRange)
            .getCount
            .executeAsync()
        future
      }
    Future.fold(taskCountFutureList)(0)(_ + _.getResult.intValue())
  }

  private def buildRange(from: TaskKey, to: TaskKey, limit: Int): ByteBufferRange = {
    val columnFamilyModel = scheduleIndex.columnFamilyModel
    val builder = new RangeBuilder().setLimit(limit)
    builder.setStart(from, columnFamilyModel.colNameSerializer)
    builder.setEnd(to, columnFamilyModel.colNameSerializer)
    builder.build()
  }

  def getTotalTaskCount(
      partitions: Set[PartitionId],
      from: Instant,
      to: Instant,
      limitPerPartition: Int
    ): Future[Int] = {
    require(from.compareTo(to) < 0, "`from` time must be less than `to`")
    val taskCountPerPartition = partitions.toSeq.map(getTaskCount(_, from, to, limitPerPartition))
    Future.fold(taskCountPerPartition)(0) { _ + _ }
  }

  private def makeRowKey(partitionId: PartitionId, taskKey: TaskKey): RowKey = {
    (partitionId, getTimeBucketKey(taskKey.scheduledTime))
  }
}
