package com.pagerduty.scheduler.model

import com.twitter.util.Time
import java.util.UUID
import org.scalatest.{ Matchers, WordSpecLike }

class TaskSpec extends WordSpecLike with Matchers {
  "A Task" should {
    val scheduledTime = Time.fromMilliseconds(1454982072959L)
    val dataKey = "data"
    val dataValue = "moreData"
    val taskData = Map(dataKey -> dataValue)
    val task = Task(
      orderingId = UUID.randomUUID().toString,
      scheduledTime = scheduledTime,
      uniquenessKey = UUID.randomUUID().toString,
      taskData = taskData
    )

    "serialize to JSON" in {
      val expectedJson = s"""
        {
          "orderingId":"${task.orderingId}",
          "scheduledTime":"2016-02-09T01:41:12.959Z",
          "uniquenessKey":"${task.uniquenessKey}",
          "taskData": {
            "$dataKey":"$dataValue"
          },
          "version": 1
        }
      """.replaceAll("[ \n]", "")
      task.toJson should equal(expectedJson)
    }

    "deserialize properly" in {
      val taskFromJson = Task.fromJson(task.toJson)
      task shouldEqual taskFromJson
    }
  }
}
