package com.wavesenterprise.network.netty.handler.stream

import com.wavesenterprise.network.netty.handler.stream.StreamProgressListener.DefaultReportProgressBytes
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.{ChannelProgressiveFuture, ChannelProgressiveFutureListener}
import monix.execution.atomic.AtomicLong
import org.apache.commons.io.FileUtils.byteCountToDisplaySize

class StreamWriteProgressListener(name: String, reportProgressBytes: Long = DefaultReportProgressBytes)
    extends ChannelProgressiveFutureListener
    with ScorexLogging {
  require(reportProgressBytes > 0, "report progress bytes count must be positive")

  private[this] val innerProgress: AtomicLong = AtomicLong(0)
  private[this] val lastReport: AtomicLong    = AtomicLong(-1)

  def progress: Long = innerProgress.get

  override def operationProgressed(future: ChannelProgressiveFuture, progress: Long, rawTotal: Long) {
    innerProgress.update(progress)
    val maybeTotal = Option(rawTotal).filter(_ > 0)

    val progressChunk = progress / reportProgressBytes
    if (progressChunk > lastReport.getAndSet(progressChunk)) {
      log.debug(s"$name progress: ${byteCountToDisplaySize(progress)}${maybeTotal.fold("")(total => s" of ${byteCountToDisplaySize(total)}")} ")
    }
  }

  override def operationComplete(future: ChannelProgressiveFuture) {
    if (future.isSuccess) {
      val maybeTotal = Option(innerProgress.get).filter(_ > 0).map(byteCountToDisplaySize)
      log.debug(s"$name successfully completed${maybeTotal.fold("")(total => s", total $total")}")
    } else {
      log.error(s"$name failed, progress ${byteCountToDisplaySize(innerProgress.get)}", future.cause())
    }
  }
}
