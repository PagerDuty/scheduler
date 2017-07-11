package com.pagerduty.scheduler.admin.http.standalone

import com.pagerduty.scheduler.admin.AdminService
import com.pagerduty.scheduler.admin.http.Settings
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

/**
  * A wrapper for a standalone embedded Jetty HTTP server.
  *
  * Meant to be enabled by services (via config) that are not already serving HTTP via Scalatra,
  * or services that would prefer to delegate this setup to the Scheduler library.
  *
  * @param settings
  * @param adminService
  */
class AdminHttpServer(settings: Settings, adminService: AdminService) {

  import AdminHttpServer._

  val ScalatraBootstrapClass = "com.pagerduty.scheduler.admin.http.standalone.ScalatraBootstrap"

  private val server = new Server(settings.httpPort)

  def start(): Unit = {
    val context = new WebAppContext()
    context.setContextPath("/")
    context.setResourceBase("/dev/null") // don't have any static resources to serve
    context.setInitParameter(ScalatraListener.LifeCycleKey, ScalatraBootstrapClass)
    context.setAttribute(AdminServiceAttributeKey, adminService)
    context.setAttribute(SettingsAttributeKey, settings)
    context.addEventListener(new ScalatraListener)
    context.addServlet(classOf[DefaultServlet], "/")

    server.setHandler(context)

    server.start
  }

  def running: Boolean = server.isRunning

  def stop(): Unit = {
    server.stop()
  }
}

object AdminHttpServer {
  val AdminServiceAttributeKey = "adminService"
  val SettingsAttributeKey = "settings"
}
