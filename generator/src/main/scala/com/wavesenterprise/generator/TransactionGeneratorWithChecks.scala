package com.wavesenterprise.generator

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Level, LoggerContext}
import com.typesafe.config.ConfigFactory
import com.wavesenterprise.api.http.assets.TransferV2Request
import com.wavesenterprise.generator.TransactionGeneratorWithChecks._
import com.wavesenterprise.generator.common.{NodeInfo, TransactionChecker}
import com.wavesenterprise.settings.WEConfigReaders._
import com.wavesenterprise.transaction.transfer.TransferTransactionV2
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.execution.Scheduler.io
import monix.execution.schedulers.SchedulerService
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.NameMapper
import org.slf4j.LoggerFactory
import play.api.libs.json.Json.toJson
import play.api.libs.json.{JsObject, Json}

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration._

class TransactionGeneratorWithChecks(config: TransactionCheckerConfig)(implicit system: ActorSystem)
    extends TransactionChecker(config.nodeInfo, 40.seconds)
    with ScorexLogging {

  val scheduler: SchedulerService = io("transaction-checker")

  val transferAmount: Long = 0.0000001.west
  val transferFee: Long    = 0.01.west

  @volatile var shutdownRequested: Boolean = false

  sys.addShutdownHook {
    log.info("Shutdown requested. Stopping all tasks...")
    shutdownRequested = true
  }

  def start(): Unit = {

    /**
      * Checker uses node's REST Api and its key to sign and broadcast txs.
      * For every sent tx, checker should check these states to happen accordingly:
      *  1. Tx is found in "/transactions/unconfirmed/info/{id}"
      *  2. If not found, then it should be definitely found in "/transactions/info/{id}"
      *
      * On failure, these checks are repeated once again (to avoid false negatives because of NG).
      * If the failure is confirmed, it is logged as failure with failed tx id.
      */
    log.info("Starting the sending process")

    val pipeline = Task
      .defer {
        val txJson = generateTxRequestJson(TransferTransactionV2.typeId, config.senderAddress, config.recipientAddress, config.password)
        broadcastTxTask(txJson)
          .flatMap {
            case Right(broadcastedTx) =>
              log.info(s"Broadcasted tx with id '${broadcastedTx.id().base58}'")
              Task.now(Some(broadcastedTx.id().base58))
            case Left(apiError) =>
              log.error(s"Encountered an error during broadcast: '$apiError'")
              Task.now(None)
          }
      }
      .flatMap {
        case Some(id) => txCheckProcess(id)
        case None     => Task.unit
      }
      .timeout(5.minutes)
      .restartUntil(_ => shutdownRequested)
      .onErrorHandle { _ =>
        log.error("Triggering ERROR flag")
        shutdownRequested = true
      }

    val process = Task.parTraverseUnordered(1 to config.parallelism)(_ => pipeline).runToFuture(scheduler)

    Await.ready(process, Duration.Inf)
    Await.ready(system.terminate(), 5.seconds)
  }

  /**
    * To generate different tx requests
    */
  def generateTxRequestJson(txTypeByte: Byte, sourceAddress: String, recipient: String, passwordOpt: Option[String]): JsObject = txTypeByte match {
    case transferTypeId @ TransferTransactionV2.typeId =>
      Json.obj("type" -> transferTypeId) ++
        toJson(
          TransferV2Request(version = 2,
                            assetId = None,
                            transferAmount,
                            feeAssetId = None,
                            transferFee,
                            sourceAddress,
                            attachment = None,
                            recipient,
                            password = passwordOpt)).as[JsObject]

    case otherType =>
      throw new RuntimeException(s"Tx typeId '$otherType' is unsupported")
  }

}

object TransactionGeneratorWithChecks {

  private implicit val readConfigInHyphen: NameMapper = net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase

  case class TransactionCheckerConfig(loggingLevel: String,
                                      nodeInfo: NodeInfo,
                                      senderAddress: String,
                                      recipientAddress: String,
                                      password: Option[String],
                                      parallelism: Int)

  def main(args: Array[String]): Unit = {
    val configPath   = args.headOption.fold(exitWithError("Configuration file path not specified!"))(identity)
    val parsedConfig = ConfigFactory.parseFile(new File(configPath))
    val config       = ConfigFactory.load(parsedConfig).as[TransactionCheckerConfig]("transaction-checker")

    val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    lc.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(Level.valueOf(config.loggingLevel))

    val system: ActorSystem = ActorSystem("TransactionGeneratorWithChecksSystem", parsedConfig)

    new TransactionGeneratorWithChecks(config)(system).start()
  }
}
