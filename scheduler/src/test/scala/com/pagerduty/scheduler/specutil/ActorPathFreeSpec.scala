package com.pagerduty.scheduler.specutil

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, path}
import org.slf4j.bridge.SLF4JBridgeHandler

abstract class ActorPathFreeSpec(actorSystemName: String)
    extends TestKit(ActorSystem(actorSystemName, ConfigFactory.load().getConfig("scheduler")))
    // New actor system per test!? And, how do we shut it down?!
    with ImplicitSender
    with path.FreeSpecLike
    with Matchers {
  LoggingUtil.initJavaUtilLoggingBridge()
}

object LoggingUtil {
  def initJavaUtilLoggingBridge(): Unit = {
    initJavaUtilLoggingBridgeOnce
  }
  private lazy val initJavaUtilLoggingBridgeOnce = redirectJavaUtilLogging()
  private def redirectJavaUtilLogging(): Unit = {
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()
  }
}
