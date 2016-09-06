package com.pagerduty.scheduler.akka

import akka.actor.Props
import akka.testkit._
import com.pagerduty.scheduler.specutil.ActorPathFreeSpec

class ExtendedLoggingFSMSpec extends ActorPathFreeSpec("ExtendedLoggingFSMSpec") {

  private val data = "JunkData"

  import LoggingFSMTestActor._
  class LoggingFSMTestActor extends ExtendedLoggingFSM[State, String] {
    startWith(Idle, data)
    when(Idle) {
      case Event(JunkEvent, _) => goto(InvalidState)
      case Event(KillEvent, _) => stop()
      case _ => stay()
    }
    initialize()
  }

  object LoggingFSMTestActor {
    // states used by the FSM Actor
    sealed trait State
    case object Idle extends State
    case object InvalidState extends State

    case object JunkEvent
    case object KillEvent
  }

  "An actor that extends ExtendedLoggingFSM" - {

    "should log when the actor terminates due to a failed state transition" in {
      val loggingFSMTestActor = system.actorOf(Props(new LoggingFSMTestActor))
      EventFilter.error(
        source = loggingFSMTestActor.path.toString,
        pattern = s"Failure in ${Idle} with ${data}.\nEvent log:.*",
        occurrences = 1
      ) intercept {
        loggingFSMTestActor ! JunkEvent
      }
    }

    "should not log an error during a normal termination sequence" in {
      val loggingFSMTestActor = system.actorOf(Props(new LoggingFSMTestActor))
      EventFilter.error(
        occurrences = 0
      ) intercept {
        loggingFSMTestActor ! KillEvent
      }
    }
  }
}
