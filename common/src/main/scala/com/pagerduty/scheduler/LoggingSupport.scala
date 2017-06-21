package com.pagerduty.scheduler

import com.pagerduty.metrics.Metrics
import com.pagerduty.scheduler.model.Task
import org.slf4j.Logger
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Some logging code shared between scheduler and scheduler-scala-api
  */
object LoggingSupport {

  /**
    * Report results on a future
    * @param metrics Metrics instance to write metrics to
    * @param log Logger instance to write logging to
    * @param name Name for the future
    * @param logString Description of what the future does
    * @param future The future itself
    * @param additionalTags Any additional datadog tags to write out
    * @tparam T The return type of the future
    */
  def reportFutureResults[T](
      metrics: Metrics,
      log: Logger,
      name: String,
      logString: Option[String],
      future: Future[T],
      additionalTags: Seq[(String, String)] = Seq.empty[(String, String)]
    )(implicit ec: ExecutionContext
    ): Unit = {

    val startTime = System.currentTimeMillis()
    def timeTaken() = (System.currentTimeMillis() - startTime).toInt
    logString.foreach(str => log.info(s"Attempting: ${str}"))

    future.onComplete {
      case Success(_) =>
        logString.foreach(str => log.info(s"Succeeded: ${str}"))
        val tags = additionalTags :+ ("result" -> "success")
        metrics.histogram(
          name,
          timeTaken(),
          tags: _*
        )
      case Failure(e) =>
        logString.foreach(str => log.error(s"Failed: ${str}.", e))
        val tags = additionalTags :+ ("result" -> "failure") :+ ("exception" -> e.getClass.getSimpleName)
        metrics.histogram(
          name,
          timeTaken(),
          tags: _*
        )
    }
  }

  /**
    * Setup additional tags based on the Task
    * @param task The Task to generate tags from
    * @param taskDataTagNames Task data keys to use for tags
    * @return Tags pulled out of task.taskData appearing in taskDataDatNames.
    */
  def additionalTags(task: Task, taskDataTagNames: Set[String]): Seq[(String, String)] =
    task.taskData.filterKeys(taskDataTagNames.contains).toSeq
}
