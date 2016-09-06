package com.pagerduty.scheduler.akka

import akka.testkit.TestProbe
import com.pagerduty.scheduler.Scheduler
import com.pagerduty.scheduler.specutil.ActorPathFreeSpec
import org.scalamock.scalatest.PathMockFactory

class RetrierSpec extends ActorPathFreeSpec("TopicSupervisorSpec") with PathMockFactory {
  "Retrier" - {
    val requestHandler = TestProbe()
    val mockRequestHandler = requestHandler.ref

    val retrier = system.actorOf(Retrier.props(mockRequestHandler, 2, stub[Scheduler.Logging]))

    "should transparently proxy non-retryable messages" in {
      case object Request
      case object Reply

      retrier ! Request
      requestHandler.expectMsg(Request)
      requestHandler.reply(Reply)
      expectMsg(Reply)
    }

    "should retry retryable requests" - {
      case object FetchStuff extends RetryableRequest
      case object StuffFetched extends SuccessResponse
      case object StuffNotFetched extends FailureResponse

      retrier ! FetchStuff

      "and return the success message if the request succeeds" in {
        requestHandler.expectMsg(FetchStuff)
        requestHandler.reply(StuffNotFetched)
        requestHandler.expectMsg(FetchStuff)
        requestHandler.reply(StuffFetched)
        expectMsg(StuffFetched)
      }

      "and return the failure message if the max number of attempts is reached" in {
        requestHandler.expectMsg(FetchStuff)
        requestHandler.reply(StuffNotFetched)
        requestHandler.expectMsg(FetchStuff)
        requestHandler.reply(StuffNotFetched)
        expectMsg(StuffNotFetched)
      }
    }
  }

}
