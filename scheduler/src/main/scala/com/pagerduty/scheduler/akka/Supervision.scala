package com.pagerduty.scheduler.akka

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{AllForOneStrategy, SupervisorStrategy, SupervisorStrategyConfigurator}
import com.pagerduty.scheduler.Scheduler

object Supervision {
  val AlwaysEscalateStrategy = AllForOneStrategy() {
    case _: Throwable => Escalate
  }

  /**
    * The supervisor strategy for the queue supervisor, the top most actor of this system and the
    * child of the User Guardian. If this actor crashes, it will cause the error to propagate to
    * the User Guardian, which will shut down the actor system by escalating to the Root Guardian.
    * @param logger Scheduler logging object to notify the restart to
    * @return an AllForOneStrategy that escalates and reports upon receiving a throwable
    */
  def makeAlwaysEscalateTopicSupervisorStrategy(logger: Scheduler.Logging): AllForOneStrategy = {
    AllForOneStrategy() {
      case throwable: Throwable => {
        logger.reportActorSystemRestart(throwable)
        Escalate
      }
    }
  }
}

final class UserGuardianEscalateStrategy extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = Supervision.AlwaysEscalateStrategy
}
