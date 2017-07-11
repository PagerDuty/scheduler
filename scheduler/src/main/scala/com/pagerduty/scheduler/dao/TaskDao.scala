package com.pagerduty.scheduler.dao

import com.pagerduty.eris.dao._
import com.pagerduty.eris.serializers._
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.model.Task.PartitionId
import com.pagerduty.scheduler.model.{Task, TaskKey}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
  * Common base for TaskScheduleDao and TaskStatusDao.
  */
protected[dao] trait TaskDaoImpl {
  private val hourFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH").withZone(ZoneOffset.UTC)

  /**
    * Returns a key that uses years, months, days and hours (24-hour-based) from the given time.
    */
  private def hourKeyFor(time: Instant) = hourFormat.format(time)

  /**
    * This is hardcoded within implementation and data layout.
    */
  final val rowTimeBucketDuration = 1.hour

  protected implicit val executor: ExecutionContextExecutor = ExecutionContext.Implicits.global
  protected type TimeBucketKey = String

  protected implicit val TimeSerializer = new TimeSerializer

  protected implicit val taskKeySerializer = {
    // Cassandra composites erase distinction between "" and null.
    // So, we choose to interpret null as "" to avoid dealing with nulls in our code.
    def taskKeyFromTuple(tuple: (Instant, Task.OrderingId, Task.UniquenessKey)): TaskKey = {
      val (scheduledTime, orderingId, uniquenessKey) = tuple
      TaskKey(
        scheduledTime,
        if (orderingId == null) "" else orderingId,
        if (uniquenessKey == null) "" else uniquenessKey
      )
    }
    new ProxySerializer[TaskKey, (Instant, Task.OrderingId, Task.UniquenessKey)](
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
  protected def getTimeBucketKey(time: Instant): TimeBucketKey = hourKeyFor(time)

  /**
    * Get a sequence of time bucket keys for specified time range.
    * @param from start of the range, inclusive, must be less than `to`
    * @param to end of the range, exclusive
    * @return
    */
  protected def getTimeBucketKeysExclusive(from: Instant, to: Instant): Seq[TimeBucketKey] = {
    val inclusiveTo = to - 1.millisecond
    for (hour <- from.inHours to inclusiveTo.inHours) yield {
      val bucketTimeStamp = Instant.ofEpochSecond(hour * 3600, 0)
      getTimeBucketKey(bucketTimeStamp)
    }
  }

  /**
    * Get a sequence of row keys for specified time range.
    * @param from start of the range, inclusive, must be less than `to`
    * @param to end of the range, exclusive
    * @return
    */
  protected def getRowKeysExclusive(
      partitionId: PartitionId,
      from: Instant,
      to: Instant
    ): Seq[(PartitionId, TimeBucketKey)] = {
    getTimeBucketKeysExclusive(from, to).map(timeBucketKey => (partitionId, timeBucketKey))
  }
}
