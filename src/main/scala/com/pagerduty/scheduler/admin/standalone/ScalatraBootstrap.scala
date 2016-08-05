package com.pagerduty.scheduler.admin.standalone

import javax.servlet.ServletContext

import com.pagerduty.scheduler.Scheduler
import com.pagerduty.scheduler.admin.{ AdminService, AdminServlet, Settings }
import org.scalatra.LifeCycle

/**
 * This class mounts our AdminServlet into the AdminHttpServer. It bridges the gap between
 * Scalatra and Jetty, including necessary dependency injection.
 */
class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    val adminService = context.getAttribute(AdminHttpServer.AdminServiceAttributeKey)
    val logging = context.getAttribute(AdminHttpServer.LoggingAttributeKey)
    val settings = context.getAttribute(AdminHttpServer.SettingsAttributeKey)

    (adminService, logging, settings) match {
      case (service: AdminService, logging: Scheduler.Logging, settings: Settings) =>
        context mount (new AdminServlet(service, logging), s"/${settings.apiNamespace}/*")
      case _ =>
        throw new RuntimeException("Couldn't start Admin HTTP server without dependencies!")
    }

  }
}
