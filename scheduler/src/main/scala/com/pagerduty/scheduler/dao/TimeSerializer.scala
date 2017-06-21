package com.pagerduty.scheduler.dao

import com.pagerduty.eris.serializers._
import java.time.Instant
import java.util.Date

object TimeSerializer {
  def toDate(time: Instant): Date = Date.from(time)
  def toLocalDateTime(d: Date): Instant = d.toInstant()
}

class TimeSerializer
    extends ProxySerializer[Instant, Date](
      toRepresentation = TimeSerializer.toDate(_),
      fromRepresentation = TimeSerializer.toLocalDateTime(_),
      DateSerializer
    )
