package com.wavesenterprise

import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Millis, Span}

import scala.concurrent.duration._

trait AsyncTest extends ScalaFutures {
  val atMost: PatienceConfiguration.Timeout            = PatienceConfiguration.Timeout(10 seconds)
  val interval: PatienceConfiguration.Interval         = PatienceConfiguration.Interval(Span(200, Millis))
  implicit override val patienceConfig: PatienceConfig = PatienceConfig(atMost.value, interval.value)
  implicit val scheduler: SchedulerService             = Scheduler.computation()
}
