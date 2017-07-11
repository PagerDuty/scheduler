package com.pagerduty.scheduler.model

import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString

/**
  * Task completion result.
  */
sealed trait CompletionResult {
  protected def stringValue: String
  override def toString(): String = stringValue
  def isComplete: Boolean
}

object CompletionResult {

  /**
    * Task is not ye completed.
    */
  case object Incomplete extends CompletionResult {
    override protected def stringValue = "Incomplete"
    override def isComplete: Boolean = false
  }

  /**
    * Task succeeded.
    */
  case object Success extends CompletionResult {
    override protected def stringValue = "Success"
    override def isComplete: Boolean = true
  }

  /**
    * Task failed.
    */
  case object Failure extends CompletionResult {
    override protected def stringValue = "Failure"
    override def isComplete: Boolean = true
  }

  /**
    * Task was forcefully dropped.
    */
  case object Dropped extends CompletionResult {
    override protected def stringValue = "Dropped"
    override def isComplete: Boolean = true
  }

  def fromString(completionResult: String): Option[CompletionResult] = {
    completionResult.toLowerCase match {
      case "incomplete" => Some(Incomplete)
      case "success" => Some(Success)
      case "failure" => Some(Failure)
      case "dropped" => Some(Dropped)
      case _ => None
    }
  }
}

class CompletionResultSerializer
    extends CustomSerializer[CompletionResult](
      format =>
        ({
          case JString(s) => CompletionResult.fromString(s).getOrElse(CompletionResult.Failure)
        }, {
          case cr: CompletionResult => JString(cr.toString)
        })
    )
