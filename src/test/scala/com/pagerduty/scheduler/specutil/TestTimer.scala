package com.pagerduty.specutil

import com.google.common.base.Stopwatch
import java.util.concurrent.TimeUnit.MILLISECONDS
import org.scalatest._

/**
 * Provides timing functions so we can identify slow tests.
 */
trait TestTimer extends BeforeAndAfterAll with BeforeAndAfterEachTestData { this: Suite =>

  final val AnsiReset = "\u001B[0m"
  final val AnsiBlue = "\u001B[34m"
  final val AnsiYellow = "\u001B[33m"
  final val AnsiRed = "\u001B[31m"

  val overAllStopwatch = Stopwatch.createUnstarted()
  val individualTestStopwatch = Stopwatch.createUnstarted()

  override def beforeAll() {
    println(AnsiBlue + "Running tests for " + this.getClass.getName + AnsiReset)
    overAllStopwatch.reset.start
    super.beforeAll()
  }

  override def afterAll() {
    super.afterAll()
    println(AnsiBlue + "Total time for " + this.getClass.getName + " (" +
      overAllStopwatch.toString + ")" + AnsiReset)
  }

  override def beforeEach(td: TestData) {
    individualTestStopwatch.reset.start
    super.beforeEach(td) // To be stackable, must call super.beforeEach(TestData)
  }

  override def afterEach(td: TestData) {
    super.afterEach(td) // To be stackable, must call super.afterEach(TestData)
    individualTestStopwatch.stop
    if (individualTestStopwatch.elapsed(MILLISECONDS) > 10000) {
      println(AnsiRed + "[SUPER SLOW] " + td.name + ". Time taken: " +
        individualTestStopwatch.toString + AnsiReset)
    } else if (individualTestStopwatch.elapsed(MILLISECONDS) > 1000) {
      println(AnsiYellow + "[SLOW] " + td.name + ". Time taken: " +
        individualTestStopwatch.toString + AnsiReset)
    }
  }

}
