package com.wavesenterprise.anchoring

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.util.ByteString
import cats.kernel.Eq
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.Block
import com.wavesenterprise.crypto.internals.{WavesPrivateKey, WavesPublicKey}
import com.wavesenterprise.network.TxBroadcaster
import com.wavesenterprise.settings.HeightCondition
import com.wavesenterprise.state.{BinaryDataEntry, Blockchain, DataEntry, IntegerDataEntry, StringDataEntry}
import com.wavesenterprise.transaction.{DataTransactionV1, Transaction, ValidationError}
import com.wavesenterprise.utils.{NTP, ScorexLogging}
import monix.eval.Task
import monix.execution.Cancelable
import monix.reactive.Observable
import play.api.libs.json.Json.parse
import play.api.libs.json.{JsResultException, Json, OFormat, Reads}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

sealed trait AnchoringConfiguration
object AnchoringConfiguration {
  case class EnabledAnchoringCfg(
      targetnetAuthTokenProvider: TargetnetAuthTokenProvider,
      targetnetNodeAddress: Uri,
      targetnetSchemeByte: Byte,
      currentChainPrivateKey: PrivateKeyAccount,
      targetnetRecipientAddress: String,
      targetnetPublicKey: WavesPublicKey,
      targetnetPrivateKey: WavesPrivateKey,
      targetnetFee: Long,
      currentChainFee: Long,
      heightCondition: HeightCondition,
      threshold: Int,
      txMiningCheckDelay: Option[FiniteDuration],
      txMiningCheckCount: Option[Int]
  ) extends AnchoringConfiguration

  case object DisabledAnchoringCfg extends AnchoringConfiguration
}

/**
  * Actual anchoring scheduler.
  * Anchoring flow:
  * 1. Chooses block to anchor in sidechain
  * 2. Create a transaction for targetnet
  * 3. Broadcast (2) to targetnet
  * 4. Wait until targetnet reaches minimal required height (height above)
  * 5. Check that transaction created in targetnet exists on the same height as it was created in (3), do that to be sure that transaction wasn't rollbacked
  * 6. Create transaction for sidechain with data from (2)
  * 7. Broadcast (5) to sidechain
  * 8. If error happens during any stage - create a data transaction in sidechain with information about error
  *
  */
class AnchoringScheduler(
    val anchoringCfg: AnchoringConfiguration.EnabledAnchoringCfg,
    blockchainUpdater: Blockchain,
    time: NTP,
    txBroadcaster: TxBroadcaster
)(
    implicit
    system: ActorSystem,
    materializer: Materializer,
    scheduler: monix.execution.Scheduler
) extends ScorexLogging {

  private case class HttpResponseWithBody(response: HttpResponse, body: String)
  private case class BlockOnHeight(block: Block, height: Int)

  private val heightRangeCondition               = anchoringCfg.heightCondition.range
  private val defaultTargetnetGetTxHeightRetries = 20
  private val defaultDelayBetweenSteps           = 5 seconds

  private val targetnetAnchorTxRetries         = 10
  private val targetnetGetCurrentHeightRetries = 100
  private val delayOnHeightWaiting             = 30 seconds
  private val httpClient                       = Http()

  def run(): Cancelable = {
    mainLogic.subscribe()
  }

  private def blockWithHeight(height: Int): Option[BlockOnHeight] = {
    blockchainUpdater.blockAt(height).map(block => BlockOnHeight(block, height))
  }

  private val mainLogic: Observable[Unit] = {
    Observable
      .repeatEval(blockchainUpdater.height)
      .distinctUntilChanged(Eq.fromUniversalEquals)
      .filter { height =>
        val heightRemainder = height % heightRangeCondition
        val result          = height > anchoringCfg.threshold && heightRemainder == 0
        if (height % 10 == 0) {
          log.trace(s"current height: '$height', heightRangeCondition: '$heightRangeCondition', remainder: '$heightRemainder'")
        }
        result
      }
      .map(currHeight => blockWithHeight(currHeight - anchoringCfg.threshold))
      .collect { case Some(blockOnHeight) => blockOnHeight }
      .mapEval { bOnH =>
        createAnchorTransactionsFlow(bOnH)
          .map(_ => ())
          .onErrorHandleWith(createAnchoringTransactionOnError(_, bOnH))
      }
      .onErrorRestartUnlimited
  }

  private def createAnchoringTransactionOnError(ex: Throwable, blockOnHeight: BlockOnHeight): Task[Unit] = {
    val anchoringErrorType = ex match {
      case anchErr: AnchoringException => anchErr.errorType
      case _ =>
        log.error(s"Unknown exception: '${ex.getClass.getSimpleName}'", ex)
        AnchoringErrorType.UnknownError
    }

    val txData = List(
      IntegerDataEntry("error-code", anchoringErrorType.code),
      StringDataEntry("error-message", anchoringErrorType.reason),
      BinaryDataEntry("signature", blockOnHeight.block.signerData.signature),
      IntegerDataEntry("height", blockOnHeight.height)
    )

    val ourChainDataTx = createSidechainTx(txData)

    ourChainDataTx.flatMap(txBroadcaster.broadcastIfNew(_)) match {
      case Left(validationError) =>
        log.error(s"Failed to create AnchoringError dataTx in current blockchain. Reason: '$validationError'")
        Task.raiseError(new RuntimeException)
      case Right(tx) =>
        log.info(s"AnchoringError transaction successfully created, tx.id: '${tx.id().base58}'")
        Task.pure(())
    }
  }

  private def createAnchorTransactionsFlow(blockOnHeight: BlockOnHeight): Task[Either[ValidationError, Transaction]] = {
    for {
      dataTransaction         <- mkDataTxInTargetnet(blockOnHeight)
      responseWithBody        <- sendAnchorTxToTargetnet(dataTransaction)
      _                       <- handleTxCreationResponse(dataTransaction, responseWithBody)
      targetnetAnchorTxHeight <- waitForTxToBeMained(dataTransaction)
      _                       <- waitForTargetnetHeight(targetnetAnchorTxHeight)
      _                       <- ensureTargetnetTxStillExists(dataTransaction)
      _                       <- ensureAnchoredTxDidntChange(blockOnHeight)
      sidechainTx             <- broadcastSidechainTx(dataTransaction)
    } yield sidechainTx
  }

  private def mkDataTxInTargetnet(blockOnHeight: BlockOnHeight): Task[TargetnetDataTransaction] = {
    val targetnetDataTx = TargetnetDataTransaction.create(
      sender = anchoringCfg.targetnetPublicKey,
      author = anchoringCfg.targetnetPublicKey,
      data = List(
        BinaryDataEntry("signature", blockOnHeight.block.signerData.signature),
        IntegerDataEntry("height", blockOnHeight.height)
      ),
      feeAmount = anchoringCfg.targetnetFee,
      timestamp = time.getTimestamp(),
      anchoringCfg.targetnetPrivateKey,
      anchoringCfg.targetnetSchemeByte
    )
    targetnetDataTx match {
      case Left(validationError) =>
        log.warn(s"Failed to create anchoring transaction for targetnet, error: $validationError")
        Task.raiseError(new AnchoringException(AnchoringErrorType.TargetnetTransactionCreationError))
      case Right(tx) => Task.eval(tx)
    }
  }

  private def sendAnchorTxToTargetnet(dataTransaction: TargetnetDataTransaction): Task[HttpResponseWithBody] = {
    val txDataString = dataTransaction.dataTransaction.data.mkString("[", ", ", "]")
    val requestTask = for {
      _ <- Task.eval(log.info(
        s"Sending anchoring transaction to targetnet '${anchoringCfg.targetnetNodeAddress}'. txId: '${dataTransaction.id.base58}', txData: $txDataString"))
      request <- Task.eval {
        val authTokenHeaders = anchoringCfg.targetnetAuthTokenProvider.authHeaders
        HttpRequest(
          uri = buildRequestPath("/transactions/broadcast"),
          method = HttpMethods.POST,
          entity = HttpEntity(ContentTypes.`application/json`, ByteString(dataTransaction.json().toString()))
        ).withHeaders(authTokenHeaders)
      }
      req <- createRequestTask(request)
    } yield req

    requestTask
      .flatMap { response =>
        Task.deferFuture(responseBody(response)).map { body =>
          HttpResponseWithBody(response, body)
        }
      }
      .delayExecution(defaultDelayBetweenSteps)
      .onErrorRestart(targetnetAnchorTxRetries)
  }

  private def createRequestTask(request: HttpRequest): Task[HttpResponse] = {
    Task
      .deferFuture(httpClient.singleRequest(request))
      .onErrorHandleWith { ex =>
        log.error(s"Failed to send anchoring transaction", ex)
        Task.raiseError(new AnchoringException(AnchoringErrorType.SendingTransactionToTargetnetError))
      }
  }

  private def handleTxCreationResponse(originDataTx: TargetnetDataTransaction, responseWithBody: HttpResponseWithBody): Task[DataTransactionInfo] = {
    val response     = responseWithBody.response
    val responseBody = responseWithBody.body
    if (response.status != StatusCodes.OK) {
      val errorInfo = tryParseError(responseWithBody.body)
      log.error(s"Anchoring request returned invalid status. Message: '${errorInfo.map(_.message).getOrElse("-")}', response: '$responseBody'")
      Task.raiseError(new AnchoringException(AnchoringErrorType.TargetnetReturnWrongResponseStatus(response.status.intValue())))
    } else {
      val tryParse = Try(parse(responseBody).as[DataTransactionInfo])
      Task
        .fromTry(tryParse)
        .onErrorHandleWith { _ =>
          log.error(s"Failed to parse DataTransactionInfo from response on targetnet anchoring broadcast:\n'$responseBody'")
          Task.raiseError(new AnchoringException(AnchoringErrorType.TargetnetReturnWrongResponse))
        }
        .flatMap { dataTxInfo =>
          val originDataTxId = originDataTx.id.base58
          if (dataTxInfo.id != originDataTxId) {
            log.error(
              s"Targetnet returned transaction with id='${dataTxInfo.id}', but it differs from transaction that was sent id='$originDataTxId'")
            Task.raiseError(new AnchoringException(AnchoringErrorType.TargetnetTxIdDifferFromSentTx(dataTxInfo.id, originDataTxId)))
          } else {
            log.info(s"Anchoring transaction in targetnet created! txId: '${dataTxInfo.id}'")
            Task.eval(dataTxInfo)
          }
        }
    }
  }

  private def waitForTxToBeMained(dataTx: TargetnetDataTransaction): Task[Long] = {
    val targetnetGetTxHeightRetries = anchoringCfg.txMiningCheckCount.getOrElse(defaultTargetnetGetTxHeightRetries)
    val delayBetweenSteps           = anchoringCfg.txMiningCheckDelay.getOrElse(defaultDelayBetweenSteps)
    targetnetTxHeight(dataTx, AnchoringErrorType.TargetnetTxInfoError, targetnetGetTxHeightRetries, delayBetweenSteps)
  }

  private def ensureTargetnetTxStillExists(dataTx: TargetnetDataTransaction): Task[Long] = {
    targetnetTxHeight(dataTx, AnchoringErrorType.AnchoringTxInTargetnetDisappear)
  }

  private def targetnetTxHeight(dataTx: TargetnetDataTransaction,
                                errorType: AnchoringErrorType,
                                tryCount: Int = defaultTargetnetGetTxHeightRetries,
                                localDelayBetweenSteps: FiniteDuration = defaultDelayBetweenSteps): Task[Long] = {
    Task
      .eval(log.debug(s"Asking targetnet about information for transaction with id '${dataTx.id}'..."))
      .flatMap { _ =>
        val authTokenHeaders = anchoringCfg.targetnetAuthTokenProvider.authHeaders
        val request = HttpRequest(uri = buildRequestPath(s"/transactions/info/${dataTx.id}"))
          .withHeaders(authTokenHeaders)
        Task.deferFuture(httpClient.singleRequest(request))
      }
      .flatMap(response => Task.deferFuture(responseBody(response)))
      .delayExecution(localDelayBetweenSteps) //if error happens - we don't want to restart task immediately
      .map { responseBody =>
        val txHeight = parseNodeResponse[HeightResponse](responseBody).height
        log.info(s"Anchoring transaction in targetnet found as expected, tx.id: '${dataTx.id}', height: '$txHeight'")
        txHeight
      }
      .onErrorHandleWith { ex =>
        log.error(s"Failed to get info about transaction '${dataTx.id}' from targetnet, message: '${ex.getMessage}'")
        Task.raiseError(new AnchoringException(errorType))
      }
      .onErrorRestart(tryCount)
  }

  private def waitForTargetnetHeight(anchorTxHeight: Long): Task[Long] = {
    val requiredTargetnetHeight = anchoringCfg.heightCondition.above + anchorTxHeight

    val targetnetHeight: Task[Long] = Task
      .eval(log.debug("Asking targetnet for current height..."))
      .flatMap { _ =>
        val authTokenHeaders = anchoringCfg.targetnetAuthTokenProvider.authHeaders
        val request          = HttpRequest(uri = buildRequestPath("/blocks/height")).withHeaders(authTokenHeaders)
        Task.deferFuture(httpClient.singleRequest(request))
      }
      .flatMap { response =>
        Task
          .deferFuture(responseBody(response))
      }
      .delayExecution(defaultDelayBetweenSteps) //if error happens - we don't want to restart task immediately
      .map { responseBody =>
        val currentHeight = parseNodeResponse[HeightResponse](responseBody).height
        if (currentHeight < requiredTargetnetHeight) {
          log.debug(s"Current height in targetnet is '$currentHeight'. Waiting until it reaches at least '$requiredTargetnetHeight'")
        } else {
          log.debug(
            s"Current height in targetnet is '$currentHeight'. Reached required height '$requiredTargetnetHeight', continuing anchoring process")
        }

        currentHeight
      }
      .onErrorHandleWith { ex =>
        log.error(s"Failed to get current height from targetnet, message: '${ex.getMessage}'")
        Task.raiseError(new AnchoringException(AnchoringErrorType.TargetnetHeightError))
      }
      .onErrorRestart(targetnetGetCurrentHeightRetries)

    targetnetHeight
      .delayResult(delayOnHeightWaiting)
      .restartUntil(_ >= requiredTargetnetHeight)
  }

  private def ensureAnchoredTxDidntChange(blockOnHeight: BlockOnHeight): Task[Any] = {
    blockchainUpdater.blockAt(blockOnHeight.height) match {
      case None =>
        log.error(s"Couldn't find the block at height '${blockOnHeight.height}' on sidechain. Possibly a rollback has happened.")
        Task.raiseError(new AnchoringException(AnchoringErrorType.AnchoredTxChanged))
      case Some(block) =>
        if (blockOnHeight.block.signerData.signature != block.signerData.signature) {
          log.error(s"Anchored block signature at height '${blockOnHeight.height}' has been changed. Possibly a rollback has happened.")
          Task.raiseError(new AnchoringException(AnchoringErrorType.AnchoredTxChanged))
        } else {
          Task.pure(())
        }
    }
  }

  private def broadcastSidechainTx(dataTransaction: TargetnetDataTransaction): Task[Either[ValidationError, Transaction]] = {
    val txData = dataTransaction.dataTransaction.data ++ List(
      StringDataEntry("targetnet-tx-id", dataTransaction.id.base58),
      IntegerDataEntry("targetnet-tx-timestamp", dataTransaction.dataTransaction.timestamp)
    )

    val ourChainDataTx = createSidechainTx(txData)

    ourChainDataTx.flatMap(txBroadcaster.broadcastIfNew(_)) match {
      case Left(validationErr) =>
        log.error(s"Failed to create anchoring dataTx in current blockchain. Reason: '$validationErr'")
        Task.raiseError(new AnchoringException(AnchoringErrorType.SidechainTxCreationFailed))
      case right @ Right(tx) =>
        log.info(
          s"Successfully created anchoring transactions! Targetnet tx.id: '${dataTransaction.id.base58}', currentChain tx.td: '${tx.id().base58}'")
        Task.eval(right)
    }
  }

  private def createSidechainTx(dataEntries: List[DataEntry[_]]): Either[ValidationError, DataTransactionV1] = {
    DataTransactionV1.signed(
      sender = anchoringCfg.currentChainPrivateKey,
      author = anchoringCfg.currentChainPrivateKey,
      data = dataEntries,
      timestamp = time.getTimestamp(),
      fee = anchoringCfg.currentChainFee,
      signer = anchoringCfg.currentChainPrivateKey
    )
  }

  private def buildRequestPath(requestPath: String): Uri = {
    anchoringCfg.targetnetNodeAddress.withPath(anchoringCfg.targetnetNodeAddress.path + requestPath)
  }

  private def responseBody(response: HttpResponse): Future[String] = {
    response.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
  }

  private def parseNodeResponse[T: Reads](responseBody: String): T = {
    Try(parse(responseBody).as[T]) match {
      case Failure(ex) =>
        log.error(s"Failed to parse responseBody: '$responseBody'. Reason: '${ex.getMessage}'")
        ex match {
          case _: JsResultException =>
            val errorReason = parse(responseBody).as[NodeErrorResponse].details
            throw new Exception(errorReason)
          case _ => throw ex
        }
      case Success(result) => result
    }
  }

  private def tryParseError(responseBody: String): Option[NodeErrorMessage] = {
    Try(parse(responseBody).as[NodeErrorMessage]) match {
      case Failure(_)            => None
      case Success(errorMessage) => Some(errorMessage)
    }
  }
}

case class NodeErrorMessage(error: Int, message: String)
object NodeErrorMessage {
  implicit val format: OFormat[NodeErrorMessage] = Json.format
}

case class NodeErrorResponse(status: String, details: String)
object NodeErrorResponse {
  implicit val format: OFormat[NodeErrorResponse] = Json.format
}

case class HeightResponse(height: Long)
object HeightResponse {
  implicit val format: OFormat[HeightResponse] = Json.format
}

case class DataTransactionInfo(`type`: Int,
                               id: String,
                               senderPublicKey: String,
                               authorPublicKey: String,
                               fee: Long,
                               timestamp: Long,
                               version: Int,
                               proofs: List[String])
object DataTransactionInfo {
  implicit val format: OFormat[DataTransactionInfo] = Json.format
}
