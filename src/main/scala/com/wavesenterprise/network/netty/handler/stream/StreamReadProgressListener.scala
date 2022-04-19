package com.wavesenterprise.network.netty.handler.stream

import StreamProgressListener.DefaultReportProgressBytes
import com.wavesenterprise.utils.ScorexLogging
import monix.execution.atomic.AtomicLong
import org.apache.commons.io.FileUtils.byteCountToDisplaySize

class StreamReadProgressListener(name: String,
                                 start: Long = 0,
                                 maybeTotal: Option[Long] = None,
                                 reportProgressBytes: Long = DefaultReportProgressBytes)
    extends ScorexLogging {
  require(start >= 0, "start bytes count must not be negative")
  require(maybeTotal.forall(_ > 0), "total bytes count must be positive")
  require(reportProgressBytes > 0, "report progress bytes count must be positive")

  private[this] val innerProgress: AtomicLong = AtomicLong(start)
  private[this] val lastReport: AtomicLong    = AtomicLong(-1)

  def progress: Long = innerProgress.get()

  def progressed(size: Long, isLast: Boolean = false): Unit = {
    innerProgress.add(size)
    val progressChunk = innerProgress.get / reportProgressBytes
    if (progressChunk > lastReport.getAndSet(progressChunk) || isLast) {
      val totalPart = maybeTotal.fold("")(total => s" of ${byteCountToDisplaySize(total)}")
      log.debug(s"$name ${if (isLast) "completed" else "progress"}: ${byteCountToDisplaySize(innerProgress.get)}$totalPart")
    }
  }
}
