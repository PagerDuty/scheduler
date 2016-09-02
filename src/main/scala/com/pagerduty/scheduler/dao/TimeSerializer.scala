package com.pagerduty.scheduler.dao

import com.pagerduty.eris.serializers._
import com.twitter.util.Time
import java.util.Date

class TimeSerializer extends ProxySerializer[Time, Date](
  toRepresentation = _.toDate,
  fromRepresentation = Time(_),
  DateSerializer
)
