package com.pagerduty.scheduler.gauge

import com.pagerduty.metrics.gauge.Gauge
import com.pagerduty.scheduler.SchedulerKafkaConsumer

class StaleTasksGauge(kafkaConsumer: SchedulerKafkaConsumer) extends Gauge[Int] {
  def sample = kafkaConsumer.countStaleTasks
}
