package com.pagerduty.scheduler.akka

import akka.actor.{Actor, ActorRef}
import akka.actor.Props

import com.pagerduty.scheduler.Scheduler

trait RetryableRequest
trait SuccessResponse
trait FailureResponse

object Retrier {
  def props(requestHandler: ActorRef, maxAttempts: Int, logging: Scheduler.Logging): Props =
    Props(new Retrier(requestHandler, maxAttempts, logging))

  private object RequestRetrier {
    sealed trait State
    case object AwaitingResponse extends State

    sealed trait Data
    case class RetryData(numberOfAttempts: Int) extends Data

    def props(message: Any, from: ActorRef, to: ActorRef, maxAttempts: Int, logging: Scheduler.Logging): Props =
      Props(new RequestRetrier(message, from, to, maxAttempts, logging))
  }

  private class RequestRetrier(
      message: Any,
      from: ActorRef,
      to: ActorRef,
      maxAttempts: Int,
      logging: Scheduler.Logging)
      extends ExtendedLoggingFSM[RequestRetrier.State, RequestRetrier.Data] {

    import RequestRetrier._

    to ! message
    startWith(AwaitingResponse, RetryData(1))

    when(AwaitingResponse) {
      case Event(response: SuccessResponse, _) if sender() == to => {
        forwardMessageAndStop(response)
      }

      case Event(response: FailureResponse, data: RetryData) if sender() == to => {
        val willRetry = data.numberOfAttempts < maxAttempts
        logging.reportRetryableRequestFailed(message, response, willRetry)
        if (willRetry) {
          to ! message
          goto(AwaitingResponse) using RetryData(data.numberOfAttempts + 1)
        } else {
          forwardMessageAndStop(response)
        }
      }
    }

    private def forwardMessageAndStop(message: Any): State = {
      from ! message
      stop()
    }
  }
}

class Retrier(requestHandler: ActorRef, maxAttempts: Int, logging: Scheduler.Logging) extends Actor {
  import Retrier._

  def receive = {
    case request: RetryableRequest =>
      context.actorOf(RequestRetrier.props(request, sender(), requestHandler, maxAttempts, logging))
    case message: Any =>
      requestHandler.forward(message)
  }
}
