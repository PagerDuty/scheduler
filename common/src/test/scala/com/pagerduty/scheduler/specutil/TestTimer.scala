package com.pagerduty.scheduler.specutil

import com.pagerduty.metrics.Stopwatch
import org.scalatest._

/**
  * Provides timing functions so we can identify slow tests.
  */
trait TestTimer extends BeforeAndAfterAll with BeforeAndAfterEachTestData { this: Suite =>

  final val AnsiReset = "\u001B[0m"
  final val AnsiBlue = "\u001B[34m"
  final val AnsiYellow = "\u001B[33m"
  final val AnsiRed = "\u001B[31m"

  var overAllStopwatch: Stopwatch = _
  var individualTestStopwatch: Stopwatch = _

  override def beforeAll() {
    println(AnsiBlue + "Running tests for " + this.getClass.getName + AnsiReset)
    overAllStopwatch = Stopwatch.start()
    super.beforeAll()
  }

  override def afterAll() {
    super.afterAll()
    println(
      AnsiBlue + "Total time for " + this.getClass.getName + " (" +
        overAllStopwatch.elapsed().toSeconds + " seconds)" + AnsiReset
    )
  }

  override def beforeEach(td: TestData) {
    individualTestStopwatch = Stopwatch.start()
    super.beforeEach(td) // To be stackable, must call super.beforeEach(TestData)
  }

  override def afterEach(td: TestData) {
    super.afterEach(td) // To be stackable, must call super.afterEach(TestData)
    val elapsed = individualTestStopwatch.elapsed()
    val elapsedMillis = elapsed.toMillis
    val elapsedSeconds = elapsedMillis / 1000.0
    if (elapsedMillis > 10000) {
      println(
        AnsiRed + "[SUPER SLOW] " + td.name + ". Time taken: " +
          elapsedSeconds + " seconds" + AnsiReset
      )
    } else if (elapsedMillis > 1000) {
      println(
        AnsiYellow + "[SLOW] " + td.name + ". Time taken: " +
          elapsedSeconds + " seconds" + AnsiReset
      )
    }
  }

}
