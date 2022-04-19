package com.wavesenterprise.generator

import cats.Show

import scala.concurrent.duration.FiniteDuration

case class WorkerSettings(autoReconnect: Boolean,
                          iterations: Int,
                          workingTime: FiniteDuration,
                          utxLimit: Int,
                          delay: FiniteDuration,
                          reconnectDelay: FiniteDuration,
                          stopWhenLoadEnds: Boolean = true)

object WorkerSettings {
  implicit val toPrintable: Show[WorkerSettings] = { x =>
    import x._

    s"""number of iterations: $iterations
       |delay between iterations: $delay
       |auto reconnect: ${if (autoReconnect) "enabled" else "disabled"}
       |reconnect delay: $reconnectDelay
       |stop when load ends: $stopWhenLoadEnds""".stripMargin
  }
}
