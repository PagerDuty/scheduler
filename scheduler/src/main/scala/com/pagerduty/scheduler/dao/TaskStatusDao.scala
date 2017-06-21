package com.pagerduty.scheduler.dao

import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.eris.dao._
import com.pagerduty.eris.serializers._
import com.pagerduty.scheduler.model.{TaskKey, TaskStatus}
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.widerow.{Bound, EntryColumn}
import java.time.Instant
import scala.concurrent.duration._
import scala.concurrent.Future

trait TaskStatusDao {

  /**
    * Update task status.
    * @param partitionId
    * @param taskKey
    * @return
    */
  def insert(partitionId: PartitionId, taskKey: TaskKey, status: TaskStatus): Future[Unit]

  /**
    * Load task status.
    * @param partitionId
    * @param taskKey
    * @return
    */
  def getStatus(partitionId: PartitionId, taskKey: TaskKey): Future[TaskStatus]

  /**
    * Load all task status entries for the given range and partitionId.
    * @param partitionId
    * @param from
    * @param to
    * @param limit
    * @return
    */
  def load(partitionId: PartitionId, from: Instant, to: Instant, limit: Int): Future[Seq[(TaskKey, TaskStatus)]]

  /**
    * Mark selected task as `Dropped` on all partitions provided.
    * @param partitions
    * @param taskKey
    * @return
    */
  def dropTaskOnPartitions(partitions: Set[PartitionId], taskKey: TaskKey): Future[Unit]
}

class TaskStatusDaoImpl(
    protected val cluster: Cluster,
    protected val keyspace: Keyspace,
    override protected val settings: ErisSettings,
    columnTtl: Duration = TaskStatusDao.ColumnTtl)
    extends TaskStatusDao
    with TaskDaoImpl
    with Dao {
  type RowKey = (PartitionId, TimeBucketKey)

  protected implicit val taskStatusSerializer = {
    new ProxySerializer[TaskStatus, String](
      toRepresentation = _.toJson,
      fromRepresentation = TaskStatus.fromJson,
      new InferredSerializer
    )
  }

  protected val statusMap = new WideRowMap(
    columnFamily[RowKey, TaskKey, TaskStatus]("TaskStatus"),
    pageSize = 100
  )

  protected val statusMapView = statusMap.map { column =>
    column.name -> column.value
  }
  def insert(partitionId: PartitionId, taskKey: TaskKey, status: TaskStatus): Future[Unit] = {
    statusMap(makeRowKey(partitionId, taskKey))
      .queueInsert(EntryColumn(taskKey, status, Some(columnTtl.toSeconds.toInt)))
      .executeAsync()
  }

  def getStatus(partitionId: PartitionId, taskKey: TaskKey): Future[TaskStatus] = {
    val results = statusMap.get(
      colLimit = Some(1),
      rowKeys = Seq(makeRowKey(partitionId, taskKey)),
      lowerBound = Bound(taskKey),
      upperBound = Bound(taskKey)
    )
    val resultOption: Future[Option[TaskStatus]] = results.map(_.headOption.map(_.value))
    resultOption.map(_.getOrElse(TaskStatus.NeverAttempted))
  }

  def load(partitionId: PartitionId, from: Instant, to: Instant, limit: Int): Future[Seq[(TaskKey, TaskStatus)]] = {
    require(from.compareTo(to) < 0, "`from` must be less than `to`")
    val rowKeys = getRowKeysExclusive(partitionId, from, to)
    val fromBound = Bound(TaskKey(from, null, null))
    val toBound = Bound(TaskKey(to, null, null), inclusive = false)
    statusMapView.get(
      colLimit = Some(limit),
      rowKeys = rowKeys,
      lowerBound = fromBound,
      upperBound = toBound
    )
  }

  def dropTaskOnPartitions(partitions: Set[PartitionId], taskKey: TaskKey): Future[Unit] = {
    val dropResults = partitions.map { partitionId =>
      insert(partitionId, taskKey, TaskStatus.Dropped)
    }
    Future.sequence(dropResults).map(_ => Unit)
  }

  private def makeRowKey(partitionId: PartitionId, taskKey: TaskKey): RowKey = {
    (partitionId, getTimeBucketKey(taskKey.scheduledTime))
  }
}

object TaskStatusDao {
  val ColumnTtl = 14.days
}
