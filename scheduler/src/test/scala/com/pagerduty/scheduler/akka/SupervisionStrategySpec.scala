package com.pagerduty.scheduler.akka

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, Props}
import akka.testkit.{TestProbe, TestActorRef}
import com.pagerduty.scheduler.Scheduler
import com.pagerduty.scheduler.akka.TestSupervisionStrategy.ThrowException
import com.pagerduty.scheduler.specutil.ActorPathFreeSpec
import org.scalamock.scalatest.{PathMockFactory}

import scala.concurrent.duration._

case object TestSupervisionStrategy {
  case class ThrowException()

}
private class TestSupervisionStrategy() extends Actor {
  import TestSupervisionStrategy._
  override val supervisorStrategy = Supervision.AlwaysEscalateStrategy
  def receive = {
    case ThrowException() => {
      throw new RuntimeException("Test Exception")
    }
  }
}

private class TestTopicSupervisorStrategy(mockLogger: Scheduler.Logging) extends TestSupervisionStrategy {
  override val supervisorStrategy = Supervision.makeAlwaysEscalateTopicSupervisorStrategy(mockLogger)
}
class SupervisionStrategySpec extends ActorPathFreeSpec("SupervisionStrategySpec") with PathMockFactory {

  "SupervisionStrategy" - {
    "should escalate an exception from children upon receiving an exception" in {
      val supervisor = TestActorRef[TestSupervisionStrategy](Props(new TestSupervisionStrategy()))
      val strategy = supervisor.underlyingActor.supervisorStrategy.decider
      strategy(new RuntimeException("Test Exception")) shouldBe Escalate
      strategy(new Throwable) shouldBe Escalate
    }

    "should escalate the issues to the root guardian and thus shutdown the system" in {
      val parent = system.actorOf(Props(new TestSupervisionStrategy()))
      parent ! ThrowException()
      system.awaitTermination(5.seconds)
      system.isTerminated shouldBe true
    }

    "should cause the queue supervisor to escalate upon receiving a throwable" in {
      val logger = mock[Scheduler.Logging]
      val queueSupervisor = system.actorOf(Props(new TestTopicSupervisorStrategy(logger)))
      queueSupervisor ! ThrowException()
      (logger.reportActorSystemRestart _).expects(new RuntimeException("Test Exception"))
      system.awaitTermination(5.seconds)
      system.isTerminated shouldBe true

    }
  }

}
