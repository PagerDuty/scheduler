package com.pagerduty.scheduler.specutil

import org.scalatest.WordSpecLike
import org.scalatest.{FreeSpecLike, Matchers}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

// TODO deprecate this eventually
trait UnitSpec extends WordSpecLike with Matchers with TestTimer with Eventually {
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))
}

trait FreeUnitSpec extends FreeSpecLike with Matchers with TestTimer with Eventually {
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))
}
