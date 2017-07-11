package com.pagerduty.scheduler

import com.pagerduty.scheduler.model.Task._
import org.apache.kafka.common.utils.{Utils => KafkaUtils}

/**
  * Created by cees on 2016/06/08.
  */
object Partitioner {

  /**
    * This method calculates a Kafka partitionId for the given bytes. It is a direct copy of the
    * partitioning logic found in org.apache.kafka.clients.producer.internals.DefaultPartitioner.
    */
  def partitionId(bytes: Array[Byte], numPartitions: Int): PartitionId = {
    toPositive(KafkaUtils.murmur2(bytes)) % numPartitions
  }

  /**
    * Again, this is a direct copy of the function in org.apache.kafka.clients.producer.internals.DefaultPartitioner
    */
  private def toPositive(number: Int): Int = number & 0x7fffffff
}
