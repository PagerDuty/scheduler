package com.pagerduty.scheduler.gauge

trait Gauge[SampleType] {
  @volatile private var callbacks: Set[(SampleType) => Unit] = Set()

  private object RegisterCallbackLock

  def sample: SampleType = {
    val sample = doSample
    callbacks foreach (_.apply(sample))
    sample
  }

  protected def doSample: SampleType

  def registerOnSampleCallback(callback: (SampleType) => Unit): Unit = {
    RegisterCallbackLock.synchronized(callbacks += callback)
  }
}
