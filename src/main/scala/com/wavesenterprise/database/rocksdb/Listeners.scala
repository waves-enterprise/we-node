package com.wavesenterprise.database.rocksdb

import com.wavesenterprise.utils.ScorexLogging
import org.rocksdb.{AbstractEventListener, BackgroundErrorReason, Status, TableFileCreationBriefInfo, TableFileCreationInfo}

import java.util
import scala.collection.JavaConverters.seqAsJavaList

object Listeners extends ScorexLogging {
  val listeners: util.List[AbstractEventListener] = {
    val errorRecoveryListener: AbstractEventListener = new AbstractEventListener {
      override def onErrorRecoveryBegin(backgroundErrorReason: BackgroundErrorReason, backgroundError: Status): Boolean = {
        log.info(s"RocksDB has started recovery from [${backgroundError.getCodeString}] because of: [${backgroundErrorReason.toString}]")
        true
      }

      override def onErrorRecoveryCompleted(oldBackgroundError: Status): Unit = {
        log.info(s"RocksDB has completed recovery from [${oldBackgroundError.getCodeString}]")
      }

      override def onTableFileCreationStarted(tableFileCreationBriefInfo: TableFileCreationBriefInfo): Unit =
        log.debug(s"RocksDB has started SST file with brief [$tableFileCreationBriefInfo]")

      override def onTableFileCreated(tableFileCreationInfo: TableFileCreationInfo): Unit = {
        log.debug(s"RocksDB has created SST file with brief [$tableFileCreationInfo]")
        super.onTableFileCreated(tableFileCreationInfo)
      }
    }

    seqAsJavaList(Seq(errorRecoveryListener))
  }
}
