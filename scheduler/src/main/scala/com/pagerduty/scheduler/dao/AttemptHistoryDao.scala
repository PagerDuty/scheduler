package com.pagerduty.scheduler.dao

import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.eris.dao._
import com.pagerduty.eris.serializers._
import com.pagerduty.scheduler._
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{TaskAttempt, TaskKey}
import com.pagerduty.widerow.{Bound, EntryColumn}
import scala.concurrent.duration._
import scala.concurrent.Future

trait AttemptHistoryDao {

  /**
    * Persist a task attempt.
    * @param partitionId
    * @param taskKey
    * @return
    */
  def insert(partitionId: PartitionId, taskKey: TaskKey, taskAttempt: TaskAttempt): Future[Unit]

  /**
    * Load all the task attempts for a given task key.
    * @param partitionId
    * @param taskKey
    * @return
    */
  def load(partitionId: PartitionId, taskKey: TaskKey, limit: Int): Future[Seq[TaskAttempt]]
}

object AttemptHistoryDao {
  val ColumnTtl = 14.days
}

class AttemptHistoryDaoImpl(
    protected val cluster: Cluster,
    protected val keyspace: Keyspace,
    override protected val settings: ErisSettings,
    columnTtl: Duration = AttemptHistoryDao.ColumnTtl)
    extends AttemptHistoryDao
    with TaskDaoImpl
    with Dao {
  type RowKey = (PartitionId, TimeBucketKey)

  protected implicit val taskAttemptSerializer = {
    new ProxySerializer[TaskAttempt, String](
      toRepresentation = _.toJson,
      fromRepresentation = TaskAttempt.fromJson,
      new InferredSerializer
    )
  }

  protected val attemptsMap = new WideRowMap(
    columnFamily[RowKey, (TaskKey, Int), TaskAttempt]("AttemptHistory"),
    pageSize = 100
  )
  protected val attemptsView = attemptsMap.map(_.value)

  def insert(partitionId: PartitionId, taskKey: TaskKey, taskAttempt: TaskAttempt): Future[Unit] = {
    val columnName = (taskKey, taskAttempt.attemptNumber)
    attemptsMap(makeRowKey(partitionId, taskKey))
      .queueInsert(EntryColumn(columnName, taskAttempt, Some(columnTtl.toSeconds.toInt)))
      .executeAsync()
  }

  def load(partitionId: PartitionId, taskKey: TaskKey, limit: Int): Future[Seq[TaskAttempt]] = {
    val rowKeys = Seq(makeRowKey(partitionId, taskKey))
    val fromBound = Bound(taskKey -> Int.MinValue)
    val toBound = Bound(taskKey -> Int.MaxValue)
    attemptsView.get(
      colLimit = Some(limit),
      rowKeys = rowKeys,
      lowerBound = fromBound,
      upperBound = toBound
    )
  }

  private def makeRowKey(partitionId: PartitionId, taskKey: TaskKey): RowKey = {
    (partitionId, getTimeBucketKey(taskKey.scheduledTime))
  }
}
