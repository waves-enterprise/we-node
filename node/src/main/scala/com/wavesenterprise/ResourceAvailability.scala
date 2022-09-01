package com.wavesenterprise

import com.wavesenterprise.utils.ScorexLogging
import org.apache.commons.io.FileUtils

import java.nio.file.{Files, Paths}
import scala.util.Try

object ResourceAvailability extends ScorexLogging {

  private def spaceInfo: String =
    Try(Files.getFileStore(Paths.get("").toAbsolutePath).getTotalSpace)
      .fold(
        ex => s"Unable to check available space, $ex",
        bytes => s"${bytes / FileUtils.ONE_GB} GB"
      )

  def logResources(): Unit =
    log.debug(
      "Available system resources\n" +
        s"\tCPUs: ${Runtime.getRuntime.availableProcessors}\n" +
        s"\tMemory: ${Runtime.getRuntime.maxMemory.toDouble / FileUtils.ONE_GB} GB\n" +
        s"\tSpace: $spaceInfo")
}
