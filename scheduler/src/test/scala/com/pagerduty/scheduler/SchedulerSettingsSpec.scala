package com.pagerduty.scheduler

import com.pagerduty.scheduler.specutil.UnitSpec
import com.typesafe.config.ConfigFactory

class SchedulerSettingsSpec extends UnitSpec {
  "Scheduler" should {
    val settings = SchedulerSettings(ConfigFactory.load("test"))

    // quick verify we're picking up the correct setup
    "read correct file" in {
      settings.kafkaTopic should be("scheduler_settings_spec")
    }

    "convert kafka consumer properties correctly" in {
      val props = settings.kafkaProperties
      props.getProperty("fetch.min.bytes") should be("5")
      props.getProperty("fetch.max.wait.ms") should be("3")
    }
  }
}
