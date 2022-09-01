package com.wavesenterprise.metrics

import com.wavesenterprise.database.Key
import kamon.Kamon
import kamon.metric.{HistogramMetric, MeasurementUnit, TimerMetric}

object DBStats {
  implicit class DbHistogramExt(val h: HistogramMetric) {
    def recordTagged(key: Key[_], value: Array[Byte]): Unit = recordTagged(key.name, value)

    def recordTagged(tag: String, value: Array[Byte]): Unit =
      h.refine("key", tag).record(Option(value).map(_.length.toLong).getOrElse(0))

    def recordTagged(tag: String, totalBytes: Long): Unit =
      h.refine("key", tag).record(totalBytes)
  }

  implicit class TimerExt(private val timer: TimerMetric) extends AnyVal {

    def measure[A](key: Key[_])(f: => A): A = {
      val start  = timer.refine("key" -> key.name).start()
      val result = f
      start.stop()
      result
    }
  }

  val read: HistogramMetric  = Kamon.histogram("node.db.read", MeasurementUnit.information.bytes)
  val write: HistogramMetric = Kamon.histogram("node.db.write", MeasurementUnit.information.bytes)

  val readLatency: TimerMetric               = Kamon.timer("node.db.read.latency")
  val existLatency: TimerMetric              = Kamon.timer("node.db.exist.latency")
  val existNotDefinitelyLatency: TimerMetric = Kamon.timer("node.db.exist.not-definitely-latency")
}
