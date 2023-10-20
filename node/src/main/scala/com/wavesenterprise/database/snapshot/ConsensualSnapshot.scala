package com.wavesenterprise.database.snapshot

import cats.Show
import cats.data.EitherT
import cats.implicits.showInterpolator
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.database.rocksdb.MainRocksDBStorage
import com.wavesenterprise.docker.deferEither
import com.wavesenterprise.settings.{ConsensusType, PositiveInt, WEConfigReaders}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{BlockchainUpdater, LastBlockInfo, ValidationError}
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Observable, Observer}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

sealed trait ConsensualSnapshotSettings extends Product with Serializable

case object DisabledSnapshot extends ConsensualSnapshotSettings

case class Backoff(maxRetries: Int, delay: FiniteDuration)

object Backoff {
  implicit val configReader: ConfigReader[Backoff] = deriveReader
  implicit val toPrintable: Show[Backoff] = { x =>
    import x._
    s"""
       |maxRetries: $maxRetries
       |delay: $delay
       """.stripMargin
  }
}

case class EnabledSnapshot(snapshotHeight: PositiveInt, waitBlocksCount: PositiveInt, backOff: Backoff, consensusType: ConsensusType)
    extends ConsensualSnapshotSettings {

  require(consensusType != ConsensusType.PoS, "Consensus type 'PoS' is not supported for new networks made from snapshots")

  val snapshotTakingHeight: Int  = snapshotHeight.value + 1
  val snapshotSendingHeight: Int = snapshotHeight.value + waitBlocksCount.value
}

object EnabledSnapshot extends WEConfigReaders {

  import pureconfig.module.enumeratum._
  import pureconfig.generic.auto._
  implicit val configReader: ConfigReader[EnabledSnapshot] = deriveReader
}

object ConsensualSnapshotSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[ConsensualSnapshotSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      enableCursor <- objectCursor.atKey("enable")
      isEnable     <- enableCursor.asBoolean
      settings     <- if (isEnable) EnabledSnapshot.configReader.from(objectCursor) else Right(DisabledSnapshot)
    } yield settings
  }

  implicit val toPrintable: Show[ConsensualSnapshotSettings] = {
    case DisabledSnapshot => "enable: false"
    case es: EnabledSnapshot =>
      import es._
      s"""
         |enable: true
         |snapshotHeight: ${snapshotHeight.value}
         |waitBlocksCount: ${waitBlocksCount.value}
         |backOff:
         |  ${show"$backOff".replace("\n", "\n--")}
         |consensusType: $consensusType
       """.stripMargin
  }
}

class ConsensualSnapshot(val settings: EnabledSnapshot,
                         snapshotDirectory: String,
                         miner: PublicKeyAccount,
                         lastBlockInfo: Observable[LastBlockInfo],
                         blockchain: Blockchain,
                         storage: MainRocksDBStorage,
                         val statusObserver: Observer[SnapshotStatus],
                         snapshotGenesis: SnapshotGenesis)(implicit val scheduler: Scheduler)
    extends ScorexLogging
    with SnapshotStatusObserver
    with AutoCloseable {

  private[this] val cancelable = lastBlockInfo
    .find { blockInfo =>
      isReadyToTakeSnapshot(blockInfo)
    }
    .observeOn(scheduler)
    .mapEval { _ =>
      consensualSnapshotTask(snapshotDirectory)
    }
    .executeOn(scheduler)
    .logErr
    .subscribe()

  private def buildAndAppendGenesis: Task[Either[ValidationError, Unit]] = {
    EitherT(snapshotGenesis.build(settings.consensusType)).flatMapF { genesis =>
      snapshotGenesis.append(genesis)
    }.value
  }

  private def takeSnapshot(snapshotDirectory: String): Task[Unit] = {
    deferEither(storage.takeSnapshot(snapshotDirectory)).onErrorRestartLoop(settings.backOff)(onSnapshotErrorRestartLoop)
  }

  private def consensualSnapshotTask(snapshotDirectory: String): Task[Unit] = {
    Task(StateSnapshot.exists(snapshotDirectory))
      .flatMap { exists =>
        if (exists) {
          Task(Left(GenericError(s"Snapshot already exists in the directory '$snapshotDirectory'")))
        } else {
          onStatus(InProgress) >> takeSnapshot(snapshotDirectory) >> buildAndAppendGenesis
        }
      }
      .map {
        case Right(_)                  => Verified
        case Left(GenericError(error)) => Failed(error)
        case Left(error)               => Failed(error.toString)
      }
      .flatMap { status =>
        onStatus(status)
      }
      .doOnFinish {
        case None     => Task.unit
        case Some(ex) => onStatus(Failed(ex))
      }
  }

  private def isReadyToTakeSnapshot(blockInfo: LastBlockInfo): Boolean = {
    blockInfo.height >= settings.snapshotTakingHeight && blockchain.blockHeaderAt(settings.snapshotHeight.value).exists(_.sender == miner)
  }

  private def onSnapshotErrorRestartLoop(err: Throwable, state: Backoff, retry: Backoff => Task[Unit]): Task[Unit] = {
    log.error("An error occurred while taking snapshot, retry...", err)

    val Backoff(maxRetries, delay) = state
    if (maxRetries > 0) {
      retry(Backoff(maxRetries - 1, delay * 2)).delayExecution(delay)
    } else {
      log.warn("No retries left, rethrow the error")
      Task.raiseError(err)
    }
  }

  override def close(): Unit = {
    cancelable.cancel()
  }
}

object ConsensualSnapshot {

  def apply(settings: EnabledSnapshot,
            snapshotDirectory: String,
            miner: PublicKeyAccount,
            blockchain: BlockchainUpdater with Blockchain,
            storage: MainRocksDBStorage,
            statusObserver: Observer[SnapshotStatus],
            snapshotGenesis: SnapshotGenesis)(implicit scheduler: Scheduler): ConsensualSnapshot = {
    new ConsensualSnapshot(settings, snapshotDirectory, miner, blockchain.lastBlockInfo, blockchain, storage, statusObserver, snapshotGenesis)
  }
}
