package com.pagerduty.scheduler.akka

import akka.actor.{FSM, LoggingFSM}

/**
  * Trait for capturing abnormal FSM failures
  * Any FSM Actor using this trait will output a log of the events leading up to the failure,
  *  as well as other state information.
  *
  * @tparam State
  * @tparam Data
  */
trait ExtendedLoggingFSM[State, Data] extends LoggingFSM[State, Data] {
  override def logDepth = 10

  onTermination {
    case StopEvent(FSM.Failure(_), state, data) => {
      val lastEvents = getLog.mkString("\n\t")
      // This is for low-level actor debugging, so we are not using Scheduler.Logging.
      log.error(s"Failure in $state with $data.\nEvent log:\n\t$lastEvents")
    }
  }
}
