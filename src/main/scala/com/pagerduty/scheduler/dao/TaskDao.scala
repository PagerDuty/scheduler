package com.pagerduty.scheduler.dao

import com.pagerduty.eris.custom._
import com.pagerduty.eris.serializers._
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{ Task, TaskKey }
import com.twitter.util.Time
import com.twitter.conversions.time._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor }

/**
 * Common base for TaskScheduleDao and TaskStatusDao.
 */
protected[dao] trait TaskDaoImpl {
  /**
   * This is hardcoded within implementation and data layout.
   */
  final val rowTimeBucketDuration = 1.hour

  protected implicit val executor: ExecutionContextExecutor = ExecutionContext.Implicits.global
  protected type TimeBucketKey = String
  protected implicit val taskKeySerializer = {
    // Cassandra composites erase distinction between "" and null.
    // So, we choose to interpret null as "" to avoid dealing with nulls in our code.
    def taskKeyFromTuple(tuple: (Time, Task.OrderingId, Task.UniquenessKey)): TaskKey = {
      val (scheduledTime, orderingId, uniquenessKey) = tuple
      TaskKey(
        scheduledTime,
        if (orderingId == null) "" else orderingId,
        if (uniquenessKey == null) "" else uniquenessKey
      )
    }
    new ProxySerializer[TaskKey, (Time, Task.OrderingId, Task.UniquenessKey)](
      toRepresentation = taskKey => taskKey.asTuple,
      fromRepresentation = taskKeyFromTuple,
      new InferredSerializer
    )
  }

  /**
   * Convert time to a string time bucket key used as row key.
   * @param time
   * @return
   */
  protected def getTimeBucketKey(time: Time): TimeBucketKey = TimeBasedKey.hourKeyFor(time)

  /**
   * Get a sequence of time bucket keys for specified time range.
   * @param from start of the range, inclusive, must be less than `to`
   * @param to end of the range, exclusive
   * @return
   */
  protected def getTimeBucketKeysExclusive(from: Time, to: Time): Seq[TimeBucketKey] = {
    import com.twitter.conversions.time._

    val inclusiveTo = to - 1.millisecond
    for (hour <- from.inHours to inclusiveTo.inHours) yield {
      val bucketTimeStamp = Time.fromSeconds(hour * 3600)
      getTimeBucketKey(bucketTimeStamp)
    }
  }

  /**
   * Get a sequence of row keys for specified time range.
   * @param from start of the range, inclusive, must be less than `to`
   * @param to end of the range, exclusive
   * @return
   */
  protected def getRowKeysExclusive(partitionId: PartitionId, from: Time, to: Time): Seq[(PartitionId, TimeBucketKey)] = {
    getTimeBucketKeysExclusive(from, to).map(timeBucketKey => (partitionId, timeBucketKey))
  }
}
