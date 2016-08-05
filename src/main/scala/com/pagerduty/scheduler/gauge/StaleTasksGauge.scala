package com.pagerduty.scheduler.gauge

import com.pagerduty.scheduler.SchedulerKafkaConsumer

class StaleTasksGauge(kafkaConsumer: SchedulerKafkaConsumer) extends Gauge[Int] {
  protected def doSample = kafkaConsumer.countStaleTasks
}
