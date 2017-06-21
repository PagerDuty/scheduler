package com.pagerduty.scheduler.admin.http

import com.pagerduty.scheduler.admin.AdminService
import com.pagerduty.scheduler.admin.model._
import com.pagerduty.scheduler.model.{CompletionResult, Task, TaskKey}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.NoSuchElementException
import java.util.concurrent.Executors
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * This class is used by the standalone AdminHttpServer, or it can be mounted into a service's
  * existing Scalatra context (if the service is already doing HTTP via Scalatra).
  */
class AdminServlet(adminService: AdminService) extends ScalatraServlet with JacksonJsonSupport with FutureSupport {
  import AdminServlet._

  val log = LoggerFactory.getLogger(getClass)

  before() {
    lazy val requestLog =
      s"Scheduler API: ${request.getMethod} to ${request.getRequestURI} with params ${request.getQueryString}"
    log.info(requestLog)
    contentType = formats("json")
  }

  protected implicit lazy val jsonFormats: Formats =
    DefaultFormats + (new CompletionStatusSerializer) + (new TimeSerializer) + (new TaskKeySerializer)

  protected implicit def executor: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool)

  private val apiPath = s"/api/${VersionString}"

  get(s"$apiPath/status") {
    logResponse(Ok(GetStatusResponse(s"Scheduler Admin API is running! The time is: ${Instant.now()}")))
  }

  get(s"$apiPath/task") {
    parseTaskKeyFromParams(
      { key =>
        adminService.fetchTaskWithDetails(key) map { task =>
          logResponse(Ok(GetTaskResponse(Some(task), List.empty)))
        } recover internalServerError
      },
      badGetTaskResponse
    )
  }

  get(s"$apiPath/tasks") {
    val fromOrderingId: Option[Task.OrderingId] = params.get("fromOrderingId")
    val fromUniquenessKey: Option[Task.UniquenessKey] = params.get("fromUniquenessKey")

    val paramParsing = for {
      fromString <- Try(params("from"))
      toString <- Try(params("to"))
      limitString <- Try(params("limit"))
      from <- parseTime(fromString)
      to <- parseTime(toString)
      limit <- Try(limitString.toInt)
      _ <- Try(require(from.compareTo(to) < 0, "'from' must be before 'to'"))
    } yield adminService.fetchIncompleteTasks(from, fromOrderingId, fromUniquenessKey, to, limit)

    paramParsing match {
      case Success(futureList) =>
        futureList map { list =>
          logResponse(Ok(GetTasksResponse(Some(list), List.empty)))
        } recover internalServerError
      case Failure(t) =>
        t match {
          case e: NoSuchElementException =>
            badGetTasksResponse("'from', 'to', and 'limit' are required params")
          case e: NumberFormatException =>
            badGetTasksResponse("Unparseable limit")
          case NonFatal(e) =>
            badGetTasksResponse(e.getMessage)
        }
    }
  }

  put(s"$apiPath/task") {
    val putTaskRequest = parsedBody.extract[PutTaskRequest]
    val task = putTaskRequest.task

    parseTaskKeyFromParams(
      { key =>
        task.status match {
          case Some(CompletionResult.Dropped) =>
            adminService.dropTask(key) map (_ => logResponse(NoContent())) recover
              internalServerError
          case _ =>
            badPutTaskResponse(
              s"The only valid value for status is ${CompletionResult.Dropped}"
            )
        }
      },
      badPutTaskResponse
    )
  }

  private def parseTaskKeyFromParams(
      successAction: TaskKey => Future[ActionResult],
      failureBuilder: String => Future[ActionResult]
    ) = {
    val parseTaskKey = Try(TaskKey.fromString(params("key")))

    parseTaskKey match {
      case Success(key) =>
        successAction(key)
      case Failure(_) => failureBuilder("Unable to parse provided task key")
    }
  }

  private def internalServerError: PartialFunction[Throwable, ActionResult] = {
    case _ => logResponse(InternalServerError())
  }

  private def badPutTaskResponse(error: String): Future[ActionResult] = {
    badRequest(PutTaskResponse(None, List(error)))
  }

  private def badGetTaskResponse(error: String): Future[ActionResult] = {
    badRequest(GetTaskResponse(None, List(error)))
  }

  private def badGetTasksResponse(error: String): Future[ActionResult] = {
    badRequest(GetTasksResponse(None, List(error)))
  }

  private def badRequest(response: Any): Future[ActionResult] = {
    Future.successful(logResponse(BadRequest(response)))
  }

  private def parseTime(timeStr: String): Try[Instant] = {
    Try(Instant.parse(timeStr))
  }

  // it would be better to do this in an `after` callback, but it seems Scalatra might have a bug
  // see https://github.com/scalatra/scalatra/issues/148,
  // or possibly https://github.com/scalatra/scalatra/issues/561
  private def logResponse(actionResult: ActionResult): ActionResult = {
    log.info(s"Scheduler API: Returning ${actionResult.status}")
    actionResult
  }
}

object AdminServlet {
  val VersionString = "v1"
  val TimeFormatPattern = TaskKey.ScheduledTimeFormat // use the same format for consistency
  val TimeFormat = DateTimeFormatter.ofPattern(TimeFormatPattern).withZone(ZoneOffset.UTC)
}
