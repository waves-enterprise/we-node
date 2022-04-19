package com.wavesenterprise.generator.atomic

import akka.actor.ActorSystem
import cats.data.EitherT
import cats.implicits.{catsSyntaxOptionId, showInterpolator}
import cats.instances.either._
import cats.instances.list._
import cats.syntax.traverse._
import ch.qos.logback.classic.{Level, LoggerContext}
import com.typesafe.config.ConfigFactory
import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.api.http.docker.{CallContractRequestV4, CreateContractRequestV2}
import com.wavesenterprise.api.http.{AtomicTransactionRequestV1, BroadcastRequest, CreatePolicyRequestV3, PrivacyDataInfo, SendDataRequest}
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.generator.atomic.AtomicGenerator._
import com.wavesenterprise.generator.common.RestTooling.ApiError
import com.wavesenterprise.generator.common.{NodeInfo, TransactionChecker}
import com.wavesenterprise.generator.exitWithError
import com.wavesenterprise.generator.privacy.NodeConfig
import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.settings.CryptoSettings
import com.wavesenterprise.transaction.{AtomicBadge, Transaction}
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.utils.{Base64, ScorexLogging}
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}
import scala.language.postfixOps
import scala.util.{Failure, Random, Success, Try}

case class AtomicGenerator(settings: AtomicGeneratorSettings)(implicit val system: ActorSystem) extends BroadcastRequest with ScorexLogging {
  implicit val scheduler: SchedulerService = Scheduler.computation(name = "privacy-generator-scheduler")

  private val utxLimit = settings.utxLimit
  private val transactionCheckersByNode = settings.nodes.map { node =>
    node.address -> new TransactionChecker(NodeInfo(node.apiUrl, node.privacyApiKey.some), settings.maxWaitForTxLeaveUtx)
  }.toMap

  def start(): Unit = {
    log.info("Starting AtomicGenerator")

    val cryptoSettings: CryptoSettings = CryptoSettings.WavesCryptoSettings
    CryptoInitializer.init(cryptoSettings).left.foreach(err => exitWithError(err.message))
    AddressScheme.setAddressSchemaByte(settings.chainId.head)

    log.info("Crypto has been initialized")

    val mainTask = Task
      .parTraverseUnordered(settings.nodes)(processSender)
      .void
      .onErrorHandle(err => log.error("Shutting down because of error:", err))
      .runToFuture

    Await.result(mainTask, Duration.Inf)
    Try(Await.result(system.terminate(), 10 seconds)) match {
      case Success(_) => log.debug("Actor system shutdown successful")
      case Failure(e) => log.error("Failed to terminate actor system", e)
    }
  }

  private def processSender(sender: NodeConfig) = {
    (for {
      contractId <- settings.contract.id.map(EitherT.rightT[Task, ApiError].apply).getOrElse(createNewContract(sender).map(_.id().base58))
      _          <- EitherT.right[ApiError].apply[Task, Unit](mainLoop(sender, contractId))
    } yield ())
      .leftMap(apiError => new RuntimeException(apiError.toString))
      .value
      .flatMap(Task.fromEither(_))
  }

  private def atomicWithNewPolicy(sender: NodeConfig, contractId: String) =
    for {
      policyTx       <- createNewPolicy(sender)
      callContractTx <- signedCallContractTx(sender, contractId)
      pdhTxs         <- signedPdhTxs(sender, policyTx.id().base58)
      atomicTx       <- broadcastAtomicTx(sender, callContractTx :: policyTx :: pdhTxs)
    } yield atomicTx -> policyTx

  private def mainLoop(sender: NodeConfig, contractId: String) = {
    if (settings.policy.lifespan < 1) {
      Task
        .parSequenceN(settings.parallelism) {
          Array.fill(settings.parallelism) {
            atomicWithNewPolicy(sender, contractId)
              .leftMap(apiError => new RuntimeException(apiError.toString))
              .value
              .delayExecution(settings.atomicDelay)
              .flatMap(Task.fromEither(_))
              .loopForever
          }
        }
        .void
    } else {
      (for {
        atomicWithPolicy <- atomicWithNewPolicy(sender, contractId)
        (firstAtomicTx, policyTx) = atomicWithPolicy
        _ <- isTxMined(sender, firstAtomicTx)
        _ <- processSenderInParallel(sender, contractId, policyTx.id().base58)
      } yield ())
        .leftMap(apiError => new RuntimeException(apiError.toString))
        .value
        .flatMap(Task.fromEither(_))
        .loopForever
        .void
    }
  }

  private def processSenderInParallel(sender: NodeConfig, contractId: String, policyId: String) = EitherT {
    Task
      .parTraverseN(settings.parallelism)(1 to settings.policy.lifespan) { _ =>
        (for {
          callContractTx <- signedCallContractTx(sender, contractId)
          pdhTxs         <- signedPdhTxs(sender, policyId)
          atomicTx       <- broadcastAtomicTx(sender, callContractTx +: pdhTxs)
        } yield atomicTx).value.delayExecution(settings.atomicDelay)
      }
      .map(_.sequence)
  }

  private def isTxMined(sender: NodeConfig, txToCheck: Transaction) =
    EitherT.right[ApiError](transactionCheckersByNode(sender.address).txCheckProcess(txToCheck.id().base58))

  private def createNewContract(sender: NodeConfig) = {
    for {
      req <- EitherT.right(Task.eval {
        CreateContractRequestV2(
          2,
          sender.address,
          settings.contract.image,
          settings.contract.imageHash,
          "contract",
          settings.contract.createParams,
          CreateContractFee,
          feeAssetId = None,
          password = sender.password
        )
      })
      tx <- EitherT(transactionCheckersByNode(sender.address).broadcastTxTask(req.toJson))
      _  <- isTxMined(sender, tx)
    } yield {
      log.info(s"New contractId '${tx.id().base58}' for node '$sender.address'")
      logReceivedTx(tx, sender.address)
    }
  }

  private def signedCallContractTx(sender: NodeConfig, contractId: String) = {
    for {
      req <- EitherT.right(Task.eval {
        CallContractRequestV4(
          4,
          sender.address,
          contractId,
          settings.contract.version,
          settings.contract.callParams,
          CallContractFee,
          None,
          password = sender.password,
          atomicBadge = AtomicBadge().some
        )
      })
      tx <- EitherT(transactionCheckersByNode(sender.address).signTxTask(req.toJson)).map(logReceivedTx(_, sender.address))
    } yield tx
  }

  private def logReceivedTx(tx: Transaction, sender: String, header: String = "Signed") = {
    val txId = tx.id().base58
    log.debug(s"$header tx '$txId' of type '${tx.builder.typeId}' by '$sender'")
    tx
  }

  private def signedPdhTxs(sender: NodeConfig, policyId: String) = {
    EitherT[Task, ApiError, List[Transaction]] {
      Task
        .parTraverseUnordered(1 to settings.policy.dataTxsCount) { _ =>
          for {
            dataRequest <- generateSendDataRequest(policyId, sender.address, sender.password)
            pdhTx <- transactionCheckersByNode(sender.address)
              .sendDataTask(dataRequest.toJson, broadcast = false)
              .map(_.map(logReceivedTx(_, sender.address)))
          } yield pdhTx
        }
        .map(_.sequence)
    }
  }

  private def broadcastAtomicTx(sender: NodeConfig, txs: List[Transaction]) = {
    val atomicReq = AtomicTransactionRequestV1(sender.address, txs.map(_.json()), None, sender.password)
    val checker   = transactionCheckersByNode(sender.address)
    for {
      _        <- EitherT.right(checker.waitForUtxAvailability(utxLimit))
      atomicTx <- EitherT(checker.broadcastTxTask(atomicReq.toJson)).map(logReceivedTx(_, sender.address, "Broadcasted"))
    } yield atomicTx
  }

  private def createNewPolicy(sender: NodeConfig) = {
    for {
      req <- EitherT.right(Task.eval {
        CreatePolicyRequestV3(
          sender.address,
          "generated_policy",
          "generated_policy_desc",
          Set(sender.address),
          Set(sender.address),
          None,
          CreatePolicyFee,
          None,
          AtomicBadge().some,
          sender.password
        )
      })
      tx <- EitherT(transactionCheckersByNode(sender.address).signTxTask(req.toJson))
    } yield {
      log.info(s"New policy '${tx.id().base58}' for node '${sender.address}'")
      logReceivedTx(tx, sender.address)
    }
  }

  private def generateSendDataRequest(policyId: String, sender: String, password: Option[String]): Task[SendDataRequest] = Task {
    val data = Array.ofDim[Byte](settings.policy.dataSize.toBytes.toInt)
    Random.nextBytes(data)

    SendDataRequest(
      3,
      sender,
      policyId,
      Base64.encode(data).some,
      PolicyDataHash.fromDataBytes(data).toString,
      PrivacyDataInfo("some data", data.length, System.currentTimeMillis(), sender, ""),
      PolicyDataHashFee,
      None,
      AtomicBadge().some,
      password
    )
  }
}

object AtomicGenerator {
  val CreatePolicyFee: Long             = 1 west
  val UpdatePolicyFee: Long             = 0.5 west
  val CreateContractFee: Long           = 1 west
  val CallContractFee: Long             = 0.1 west
  val PolicyDataHashFee: Long           = 0.05.west
  val AtomicGeneratorConfigPath: String = "atomic-generator"

  def main(args: Array[String]): Unit = {
    val configPath          = args.headOption.fold(exitWithError("Configuration file path not specified!"))(identity)
    val config              = ConfigFactory.parseFile(new File(configPath))
    val settings            = ConfigSource.fromConfig(config).at(AtomicGeneratorConfigPath).loadOrThrow[AtomicGeneratorSettings]
    val system: ActorSystem = ActorSystem("AtomicGeneratorSystem", config)

    val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    lc.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(Level.valueOf(settings.loggingLevel))
    LoggerFactory.getLogger(getClass).info(show"$settings")

    AtomicGenerator(settings)(system).start()
  }
}
