package com.wavesenterprise.generator.privacy

import akka.actor.ActorSystem
import cats.implicits.{catsSyntaxOptionId, showInterpolator}
import ch.qos.logback.classic.{Level, LoggerContext}
import com.typesafe.config.ConfigFactory
import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.api.http.{BroadcastRequest, CreatePolicyRequestV1, PrivacyDataInfo, SendDataRequest}
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.generator.common.{NodeInfo, TransactionChecker}
import com.wavesenterprise.generator.exitWithError
import com.wavesenterprise.generator.privacy.PrivacyGenerator._
import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.settings.CryptoSettings
import com.wavesenterprise.transaction.{CreatePolicyTransactionV1, PolicyDataHashTransactionV1}
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.utils.{Base64, ScorexLogging}
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.cancelables.SerialCancelable
import monix.execution.schedulers.SchedulerService
import monix.reactive.Observable
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Random, Success, Try}

case class PrivacyGenerator(settings: PrivacyGeneratorSettings)(implicit val system: ActorSystem) extends BroadcastRequest with ScorexLogging {
  private val mainPipelineTask           = SerialCancelable()
  private val utxLimit                   = settings.request.utxLimit
  private val participantCombinations    = settings.nodes.map(_.address).combinations(settings.policy.participantsCount).map(_.toSet).toSeq
  private val nodeSettingsByOwnerAddress = settings.nodes.map(n => n.address -> n).toMap
  private val transactionCheckersByNode = settings.nodes.map { node =>
    node.address -> new TransactionChecker(NodeInfo(node.apiUrl, node.privacyApiKey.some), settings.policy.maxWaitForTxLeaveUtx)
  }.toMap

  implicit val scheduler: SchedulerService = Scheduler.computation(name = "privacy-generator-scheduler")

  type Address  = String
  type PolicyId = String

  sys.addShutdownHook {
    log.info("Shutdown requested. Stopping all tasks...")
    mainPipelineTask.cancel
    Thread.sleep(2000)
  }

  def start(): Unit = {
    log.info("Starting PrivacyGenerator")

    val cryptoSettings: CryptoSettings = CryptoSettings.WavesCryptoSettings
    CryptoInitializer.init(cryptoSettings).left.foreach(err => exitWithError(err.message))
    AddressScheme.setAddressSchemaByte(settings.chainId.head)

    log.info("Crypto has been initialized")

    val sendDataTask = SerialCancelable()

    val mainPipeline = Observable
      .repeatEvalF(generateCreatePolicyRequests flatMap signAndBroadcastAll)
      .takeWhile(_ => !sendDataTask.isCanceled)
      .mapEval { recipientPolicies =>
        Task {
          sendDataTask := sendDataInParallel(recipientPolicies).onErrorHandle { error =>
            log.error("Shutting down SendData task because of error: ", error)
            sendDataTask.cancel
          }.runToFuture
        }.void
      }
      .delayOnNext(settings.policy.recreateInterval)
      .onErrorHandle { t =>
        log.error("Shutting main pipeline down because of error: ", t)
      }
      .completedL
      .runToFuture

    mainPipelineTask := mainPipeline

    Await.result(mainPipeline, Duration.Inf)
    Try(Await.result(system.terminate(), 10 seconds)) match {
      case Success(_) => log.debug("Actor system shutdown successful")
      case Failure(e) => log.error("Failed to terminate actor system", e)
    }
  }

  private def generateCreatePolicyRequests: Task[Seq[CreatePolicyRequestV1]] =
    Task {
      participantCombinations.map { participants =>
        val sender = settings.nodes(Random.nextInt(participants.size))
        CreatePolicyRequestV1(
          sender.address,
          "generated_policy",
          "generated_policy_desc",
          participants,
          participants + sender.address,
          System.currentTimeMillis().some,
          CreatePolicyFee,
          sender.password
        )
      }
    } <* Task(log.debug(s"Generated '${participantCombinations.size}' CreatePolicy requests"))

  private def signAndBroadcastAll(requests: Seq[CreatePolicyRequestV1]): Task[Map[Address, List[PolicyId]]] = {
    val requestsBySender = requests.groupBy(_.sender).toSeq
    Task
      .parTraverseUnordered(requestsBySender) {
        case (sender, senderRequests) =>
          val checker     = transactionCheckersByNode(sender)
          val requestsItr = senderRequests.iterator
          Observable
            .repeatEvalF(for {
              utxSize       <- if (requestsItr.hasNext) checker.waitForUtxAvailability(utxLimit) else Task(utxLimit)
              requestsBatch <- Task.eval(requestsItr.take(utxLimit - utxSize).toList)
              recipientsWithPolicyId <- Task.parTraverseN(settings.request.parallelism)(requestsBatch) { req =>
                checker.broadcastWithRetry(req.toJson, sender, CreatePolicyTransactionV1.typeId).map { tx =>
                  tx.asInstanceOf[CreatePolicyTransactionV1].recipients.map(r => r.address -> tx.id().base58)
                }
              }
            } yield recipientsWithPolicyId)
            .takeWhile(_.nonEmpty)
            .toListL
            .map(_.flatten.flatten)
      }
      .map(_.flatten
        .groupBy {
          case (address, _) => address
        }
        .mapValues(_.map {
          case (_, policyId) => policyId
        })) <* Task(log.info(s"Successfully broadcasted '${requests.size}' CreatePolicy requests"))
  }

  /**
    * Simultaneously sends data to every node in N threads for each node at the same time.
    * The number of threads for each node is specified in [[settings.request.parallelism]].
    * Each request sends with delay specified in [[settings.request.delay]]
    */
  private def sendDataInParallel(recipientPolicies: Map[Address, List[PolicyId]]) =
    Task
      .parTraverseUnordered(recipientPolicies.toList) {
        case (recipient, policies) =>
          (for {
            utxSize  <- transactionCheckersByNode(recipient).waitForUtxAvailability(utxLimit)
            requests <- generateSendDataRequests(utxLimit - utxSize, policies, recipient)
            _        <- sendDataToNodeInParallel(recipient, requests)
          } yield ()).loopForever
      }

  private def sendDataToNodeInParallel(recipient: Address, requests: List[SendDataRequest]) =
    Task.parTraverseN(settings.request.parallelism)(requests) { request =>
      Task
        .defer {
          transactionCheckersByNode(recipient)
            .sendDataTask(request.toJson)
            .map {
              case Right(broadcastedTx) =>
                log.debug(s"Broadcasted tx '${broadcastedTx.id().base58}' of type '${broadcastedTx.builder.typeId}' to '${request.sender}'")
              case Left(apiError) =>
                log.info(
                  s"Encountered an error during broadcast tx of type '${PolicyDataHashTransactionV1.typeId}' to '${request.sender}': '$apiError'")
            }
        }
        .delayExecution(settings.request.delay)
    }

  private def generateSendDataRequests(count: Int, policies: List[PolicyId], recipient: Address) =
    Task.parTraverseUnordered(1 to count)(_ => generateSendDataRequest(policies, recipient)) <*
      Task(log.info(s"Generated '$count' SendData requests for '$recipient'. Broadcasting..."))

  private def generateSendDataRequest(policies: List[PolicyId], recipient: Address): Task[SendDataRequest] = Task {
    val policyId = policies(Random.nextInt(policies.size))
    val data     = Array.ofDim[Byte](settings.request.dataSize.toBytes.toInt)
    Random.nextBytes(data)

    SendDataRequest(
      3,
      recipient,
      policyId,
      Base64.encode(data).some,
      PolicyDataHash.fromDataBytes(data).toString,
      PrivacyDataInfo("some data", data.length, System.currentTimeMillis(), recipient, ""),
      PolicyDataHashFee,
      None,
      None,
      nodeSettingsByOwnerAddress(recipient).password
    )
  }
}

object PrivacyGenerator {
  val CreatePolicyFee: Long      = 1 west
  val PolicyDataHashFee: Long    = 0.05.west
  val PrivacyGeneratorConfigPath = "privacy-generator"

  def main(args: Array[String]): Unit = {
    val configPath          = args.headOption.fold(exitWithError("Configuration file path not specified!"))(identity)
    val config              = ConfigFactory.parseFile(new File(configPath))
    val settings            = ConfigSource.fromConfig(config).at(PrivacyGeneratorConfigPath).loadOrThrow[PrivacyGeneratorSettings]
    val system: ActorSystem = ActorSystem("PrivacyGeneratorSystem", config)

    val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    lc.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(Level.valueOf(settings.loggingLevel))
    LoggerFactory.getLogger(getClass).info(show"$settings")

    PrivacyGenerator(settings)(system).start()
  }
}
