package com.pagerduty.scheduler.model

import com.twitter.util.Time
import java.util.UUID
import org.scalatest.{ Matchers, WordSpecLike }

class TaskKeySpec extends WordSpecLike with Matchers {
  "TaskKey" should {

    "parse from a string" in {
      val taskKey = TaskKey(Time.now, UUID.randomUUID().toString, UUID.randomUUID.toString)
      val taskKeyString = taskKey.toString()

      val parsedTaskKey = TaskKey.fromString(taskKeyString)

      parsedTaskKey should equal(taskKey)
    }

    "parse from a string with all sorts of weird characters (except commas)" in {
      val taskKey = TaskKey(Time.now, "{}()-_=+!@#$%^&*:\"|\\:;./<>?~", "|\\:;./<>?~'")
      val formattedTimeString = TaskKey.TimeFormat.format(taskKey.scheduledTime)
      val string = s"TaskKey($formattedTimeString,${taskKey.orderingId},${taskKey.uniquenessKey})"

      val parsedTaskKey = TaskKey.fromString(string)

      parsedTaskKey should equal(taskKey)
    }

    "have somewhat evenly distributed partition IDs within the correct range" in {
      val numKeys = 100000

      val numPartitions = 64

      var partitionCounts = Map[Int, Int]()

      (0 until numKeys) foreach { n =>
        val key = TaskKey(Time.now, UUID.randomUUID().toString, UUID.randomUUID.toString)
        val id = key.partitionId(numPartitions)
        id should be >= 0
        id should be < numPartitions
        partitionCounts = if (partitionCounts.contains(id)) {
          val currentCount = partitionCounts(id)
          partitionCounts + (id -> (currentCount + 1))
        } else {
          partitionCounts + (id -> 1)
        }
      }

      val expectedAvgCount = numKeys / numPartitions
      val acceptableDev = expectedAvgCount / 3

      partitionCounts foreach {
        case (_, count) =>
          // this check could fail with a very low probability, if it does, adjust acceptableDev
          count should be((numKeys / numPartitions) +- acceptableDev)
      }
    }
  }
}
