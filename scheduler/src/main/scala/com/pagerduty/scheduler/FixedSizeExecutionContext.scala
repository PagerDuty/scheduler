package com.pagerduty.scheduler

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

private[scheduler] object FixedSizeExecutionContext {
  def apply(threadPoolSize: Int): ExecutionContextExecutorService = {
    val executorService = Executors.newFixedThreadPool(threadPoolSize)
    ExecutionContext.fromExecutorService(executorService)
  }
}
