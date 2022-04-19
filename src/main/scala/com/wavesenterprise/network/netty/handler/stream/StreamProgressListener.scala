package com.wavesenterprise.network.netty.handler.stream

import org.apache.commons.io.FileUtils

object StreamProgressListener {
  val DefaultReportProgressBytes: Long = 10 * FileUtils.ONE_MB
}
