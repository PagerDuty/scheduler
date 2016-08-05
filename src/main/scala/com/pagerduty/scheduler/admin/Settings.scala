package com.pagerduty.scheduler.admin

import com.typesafe.config.{ Config, ConfigFactory }

case class Settings(
  enabled: Boolean,
  httpPort: Int,
  apiNamespace: String
)

object Settings {
  private val ConfigPrefix = "scheduler.admin"

  def apply(config: Config = ConfigFactory.load()): Settings = {
    config.checkValid(ConfigFactory.defaultReference(), ConfigPrefix)

    val libConfig = config.getConfig(ConfigPrefix)

    Settings(
      enabled = libConfig.getBoolean("enabled"),
      httpPort = libConfig.getInt("http-port"),
      apiNamespace = libConfig.getString("api-namespace")
    )
  }
}
