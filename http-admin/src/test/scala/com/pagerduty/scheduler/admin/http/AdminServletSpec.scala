package com.pagerduty.scheduler.admin.http

import com.pagerduty.metrics.Metrics
import com.pagerduty.scheduler.datetimehelpers._
import com.pagerduty.scheduler.admin.AdminService
import com.pagerduty.scheduler.admin.model.{AdminTask, TaskDetails}
import com.pagerduty.scheduler.model.{CompletionResult, Task, TaskAttempt, TaskKey}
import com.pagerduty.scheduler.specutil.TaskFactory
import com.pagerduty.scheduler.{Scheduler, SchedulerSettings}
import com.typesafe.config.ConfigFactory
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import org.scalamock.scalatest.MockFactory
import org.scalatest.FreeSpecLike
import org.scalatra.test.scalatest.ScalatraSuite
import scala.concurrent.duration._
import scala.concurrent.Future

class AdminServletSpec extends ScalatraSuite with FreeSpecLike with MockFactory {

  val mockAdminService = mock[AdminService]
  addServlet(
    new AdminServlet(mockAdminService),
    "/*"
  )

  "AdminServlet" - {
    val namespace = "/api/v1"
    val timeFormatPattern = TaskKey.ScheduledTimeFormat
    val timeFormat = DateTimeFormatter.ofPattern(timeFormatPattern).withZone(ZoneOffset.UTC)

    "should handle a GET to /status" in {
      get(s"$namespace/status") {
        status should equal(200)
        body should include("Scheduler Admin API is running")
      }
    }

    "should handle a GET to /task" - {
      "when given a valid key" - {
        val key = TaskKey(Instant.now(), "orderingId", "uniquenessKey")

        "and the task is fetched successfully" in {
          val start = Instant.now()
          val finish = start + 1.second
          val updateAt = finish + 1.second

          val attempt = TaskAttempt(
            1,
            start,
            finish,
            CompletionResult.Success,
            updateAt,
            Some("com.pagerduty.Exception"),
            Some("Thisisatestexception"),
            None
          )
          val attempts = List(attempt)
          val details = TaskDetails(attempts)

          val task = AdminTask(
            Some(TaskFactory.makeTaskKey()),
            Some(1),
            Some(Map("someKey" -> "someValue")),
            Some(1),
            Some(CompletionResult.Success),
            Some(details)
          )

          val expectedJson = s"""
          {
            "task": {
              "key": "${task.key.get}",
              "partitionId": ${task.partitionId.get},
              "data": {
                "someKey": "someValue"
              },
              "numberOfAttempts": ${task.numberOfAttempts.get},
              "status": "${task.status.get}",
              "details": {
                "attempts": [
                  {
                    "attemptNumber": ${attempt.attemptNumber},
                    "startedAt": "${timeFormat.format(attempt.startedAt)}",
                    "finishedAt": "${timeFormat.format(attempt.finishedAt)}",
                    "taskResult": "${attempt.taskResult.toString}",
                    "taskResultUpdatedAt": "${timeFormat.format(attempt.taskResultUpdatedAt)}",
                    "exceptionClass": "${attempt.exceptionClass.get}",
                    "exceptionMessage": "${attempt.exceptionMessage.get}"
                  }
                ]
              }
            },
            "errors": []
          }
          """.replaceAll("[ \n]", "")

          (mockAdminService
            .fetchTaskWithDetails(_: TaskKey, _: Int))
            .expects(key, 20)
            .returns(Future.successful(task))

          get(s"$namespace/task", Map("key" -> key.toString)) {
            status should equal(200)
            body should equal(expectedJson)
          }
        }

        "and the task is not fetched successfully" in {
          (mockAdminService
            .fetchTaskWithDetails(_: TaskKey, _: Int))
            .expects(key, 20)
            .returns(Future.failed(new Exception))

          get(s"$namespace/task", Map("key" -> key.toString)) {
            status should equal(500)
          }
        }
      }

      "when given an invalid key" in {
        get(s"$namespace/task", Map("key" -> "notakey")) {
          status should equal(400)
          body should include("Unable to parse provided task key")
        }
      }
    }

    "should handle a GET to /tasks" - {
      val fromString = "2016-04-07T01:00:00.000Z"
      val from = Instant.parse(fromString)
      val toString = "2016-04-07T02:00:00.000Z"
      val to = Instant.parse(toString)

      val limit = 10
      val limitString = limit.toString

      "when given correct minimum params" - {
        val task = AdminTask(
          Some(TaskFactory.makeTaskKey()),
          Some(1),
          None,
          Some(1),
          Some(CompletionResult.Failure),
          None
        )

        val expectedJson = s"""
        {
          "tasks": [
            {
              "key": "${task.key.get}",
              "partitionId": ${task.partitionId.get},
              "numberOfAttempts": ${task.numberOfAttempts.get},
              "status": "${task.status.get}"
            }
          ],
          "errors": []
        }
        """.replaceAll("[ \n]", "")

        "and the fetch succeeds" in {
          (mockAdminService
            .fetchIncompleteTasks(
              _: Instant,
              _: Option[Task.OrderingId],
              _: Option[Task.UniquenessKey],
              _: Instant,
              _: Int
            ))
            .expects(from, None, None, to, limit)
            .returns(Future.successful(List(task)))

          get(s"$namespace/tasks", Seq("from" -> fromString, "to" -> toString, "limit" -> limitString)) {
            status should equal(200)
            body should equal(expectedJson)
          }
        }

        "and some non-required params are added, and the fetch succeeds" in {
          val fromOrderingIdString = "orderingId"
          val fromUniquenessKeyString = "uniquenessKey"

          (mockAdminService
            .fetchIncompleteTasks(
              _: Instant,
              _: Option[Task.OrderingId],
              _: Option[Task.UniquenessKey],
              _: Instant,
              _: Int
            ))
            .expects(
              from,
              Some(fromOrderingIdString),
              Some(fromUniquenessKeyString),
              to,
              limit
            )
            .returns(Future.successful(List(task)))

          val params = Seq(
            "from" -> fromString,
            "fromOrderingId" -> fromOrderingIdString,
            "fromUniquenessKey" -> fromUniquenessKeyString,
            "to" -> toString,
            "limit" -> limitString
          )

          get(s"$namespace/tasks", params) {
            status should equal(200)
            body should equal(expectedJson)
          }
        }

        "and the fetch fails" in {
          (mockAdminService
            .fetchIncompleteTasks(
              _: Instant,
              _: Option[Task.OrderingId],
              _: Option[Task.UniquenessKey],
              _: Instant,
              _: Int
            ))
            .expects(from, None, None, to, limit)
            .returns(Future.failed(new Exception("test")))

          get(s"$namespace/tasks", Seq("from" -> fromString, "to" -> toString, "limit" -> limitString)) {
            status should equal(500)
          }
        }
      }

      "when missing a param" in {
        get(s"$namespace/tasks", Seq("to" -> toString, "limit" -> limitString)) {
          status should equal(400)
          body should include("are required params")
        }
      }

      "when given an unparseable date" in {
        get(s"$namespace/tasks", Seq("from" -> "asdf", "to" -> toString, "limit" -> limitString)) {
          status should equal(400)
          body should include("could not be parsed")
        }
      }

      "when given an unparseable limit" in {
        get(s"$namespace/tasks", Seq("from" -> fromString, "to" -> toString, "limit" -> "asdf")) {
          status should equal(400)
          body should include("Unparseable limit")
        }
      }

      "when given a from date after the to date" in {
        get(s"$namespace/tasks", Seq("from" -> toString, "to" -> fromString, "limit" -> limitString)) {
          status should equal(400)
          body should include("'from' must be before 'to'")
        }
      }
    }

    "should handle a PUT to /task" - {
      "when given an invalid key" in {
        put(s"$namespace/task?key=notakey") {
          status should equal(400)
        }
      }

      "when given a valid key" - {
        val key = TaskKey(Instant.now(), "orderingId", "uniquenessKey")

        "and no JSON body" in {
          put(s"$namespace/task?key=$key") {
            status should equal(400)
          }
        }

        "and a correct JSON body" - {
          val body = "{\"task\":{\"status\":\"Dropped\"}}"

          "and the drop succeeds" in {
            (mockAdminService
              .dropTask(_: TaskKey))
              .expects(key)
              .returns(Future.successful(()))
            put(s"$namespace/task?key=$key", body) {
              status should equal(204)
            }
          }

          "and the drop fails" in {
            (mockAdminService
              .dropTask(_: TaskKey))
              .expects(key)
              .returns(Future.failed(new Exception("test")))
            put(s"$namespace/task?key=$key", body) {
              status should equal(500)
            }
          }
        }

        "and an incorrect JSON body" in {
          val body = "{\"task\":{\"status\":\"asdf\"}}"

          put(s"$namespace/task?key=$key", body) {
            status should equal(400)
          }
        }

        "and an unparseable JSON body" in {
          val body = "{\"task\":"

          put(s"$namespace/task?key=$key", body) {
            status should equal(400)
          }
        }
      }
    }
  }
}
