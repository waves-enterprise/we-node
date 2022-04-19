package com.wavesenterprise.generator.common

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.{Http, HttpExt}
import akka.util.ByteString
import com.wavesenterprise.generator.common.RestTooling.{ApiError, MinedTxIdResponse, TxIdResponse, _}
import com.wavesenterprise.generator.common.TransactionChecker._
import com.wavesenterprise.http.ApiMarshallers.playJsonUnmarshaller
import com.wavesenterprise.http.api_key
import com.wavesenterprise.protobuf.service.transaction.UtxSize
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import play.api.libs.json.JsObject
import pureconfig.ConfigConvert.catchReadError
import pureconfig.ConfigReader

import scala.concurrent.duration._
import scala.language.postfixOps

class TransactionChecker(nodeInfo: NodeInfo, maxWaitForTxLeaveUtx: FiniteDuration)(implicit val system: ActorSystem)
    extends RestTooling
    with ScorexLogging {

  override val httpSender: HttpExt                = Http()
  override val RequestTimeout: FiniteDuration     = 20.seconds
  val MaxRetriesToGetUtxSize: Int                 = 5
  val MaxBroadcastRetries                         = 5
  val BroadcastWithRetriesTimeout: FiniteDuration = 5 minutes

  /**
    * Full process definition
    */
  def txCheckProcess(txId: String): Task[Unit] = {
    Task.eval(log.info(s"Checking presence of transaction '$txId' in UTX'")) >>
      waitForTxToLeaveUtx(txId) >>
      Task.eval(log.info(s"Checking presence of transaction '$txId' in blockchain")) >>
      isTxMined(txId)
        .delayExecution(1.seconds)
        .onErrorHandleWith(_ => debounceNgEffect(txId))
  }

  /**
    * Polls UTX until transaction is not found there
    */
  def waitForTxToLeaveUtx(txId: String): Task[Unit] = {
    isTxInUtx(txId)
      .delayExecution(1.seconds)
      .restartUntil(isInUtx => !isInUtx)
      .timeoutWith(maxWaitForTxLeaveUtx, new RuntimeException(s"Transaction '$txId' is in UTX for more than $maxWaitForTxLeaveUtx"))
      .map(_ => ())
  }

  /**
    * In case if transaction has left UTX, but not found in blockchain, let's check, that it is not back in UTX again
    * (this case may be caused by microblock rollbacks)
    */
  def debounceNgEffect(txId: String): Task[Unit] = {
    Task.eval(log.info(s"Transaction '$txId' not found in blockchain, performing debounce")) >>
      isTxInUtx(txId)
        .flatMap {
          case true =>
            log.debug(s"Transaction '$txId' is back in UTX for some reason, starting the process over'")
            txCheckProcess(txId)
          case false =>
            isTxMined(txId)
              .delayExecution(5.seconds)
        }
  }

  /**
    * Check if txId is in UTX or not
    */
  def isTxInUtx(txId: String): Task[Boolean] = {
    val request = HttpRequest(HttpMethods.GET, nodeInfo.apiUri.withPath(nodeInfo.apiUri.path / "transactions" / "unconfirmed" / "info" / txId))

    log.trace(s"Request: '$request'")
    sendRequest[TxIdResponse](request, timeout = RequestTimeout).value.map {
      case Right(_) =>
        log.debug(s"Transaction '$txId' is still in UTX")
        true

      case Left(apiError) if apiError.details == "Transaction is not in UTX" =>
        log.info(s"Transaction '$txId' has left UTX")
        false
    }
  }

  /**
    * Check if txId was mined
    * This check is performed after tx leaves UTX pool, so it is expected to appear in blockchain
    */
  def isTxMined(txId: String, lastCheck: Boolean = false): Task[Unit] = {
    val request = HttpRequest(HttpMethods.GET, nodeInfo.apiUri.withPath(nodeInfo.apiUri.path / "transactions" / "info" / txId))

    log.trace(s"Request: '$request'")

    sendRequest[MinedTxIdResponse](request, timeout = RequestTimeout).value.flatMap {
      case Right(_) if lastCheck =>
        log.info(s"Transaction '$txId' is mined")
        Task.now(Unit)

      case Right(_) =>
        Task.eval(log.debug(s"Transaction '$txId' found in blockchain. Double checking in 2 seconds...")) >>
          isTxMined(txId, lastCheck = true)
            .delayExecution(2.seconds)

      case Left(apiError) =>
        log.error(s"Transaction '$txId' is not found in blockchain. Api response: '$apiError'")
        Task.raiseError(new RuntimeException("Transaction left UTX, but not found in blockchain"))
    }
  }

  def signTxTask(txJson: JsObject): Task[Either[ApiError, Transaction]] =
    sendRequestTask(txJson, SignApiPath)

  def broadcastTxTask(txJson: JsObject): Task[Either[ApiError, Transaction]] =
    sendRequestTask(txJson, SignAndBroadcastApiPath)

  def sendDataTask(txJson: JsObject, broadcast: Boolean = true): Task[Either[ApiError, Transaction]] = {
    val apiPath = if (!broadcast) SendDataApiPath + "?broadcast=false" else SendDataApiPath
    sendRequestTask(txJson, apiPath, privacyRequest = true, broadcast)
  }

  def broadcastWithRetry(request: JsObject, sender: String, typeId: Byte): Task[Transaction] = {
    def go(retries: Int): Task[Transaction] = {
      broadcastTxTask(request)
        .flatMap {
          case Right(broadcastedTx) =>
            log.debug(s"Broadcasted tx '${broadcastedTx.id()}' of type '${broadcastedTx.builder.typeId}' to '${sender}'")
            txCheckProcess(broadcastedTx.id().base58).map(_ => broadcastedTx)
          case Left(apiError) =>
            if (retries > 0) {
              log.warn(s"Encountered an error during broadcast tx of type '$typeId' to '$sender': '$apiError'")
              Task.defer(go(retries - 1))
            } else {
              Task.raiseError {
                new RuntimeException(s"Error after '$MaxBroadcastRetries' broadcast retries to '$sender': $apiError")
              }
            }
        }
        .timeout(BroadcastWithRetriesTimeout)
    }

    go(MaxBroadcastRetries)
  }

  /**
    * Send a tx request to `apiPath` endpoint
    */
  def sendRequestTask(txJson: JsObject,
                      path: String,
                      privacyRequest: Boolean = false,
                      broadcast: Boolean = true): Task[Either[ApiError, Transaction]] = {
    val baseUri = nodeInfo.apiUri.withPath(nodeInfo.apiUri.path + path)
    val uri     = if (broadcast) baseUri else baseUri.withQuery(Uri.Query("broadcast=false"))
    val baseRequest = HttpRequest(HttpMethods.POST, uri)
      .withEntity(HttpEntity(ContentTypes.`application/json`, ByteString(txJson.toString())))
    val request: HttpRequest = nodeInfo.privacyApiKey
      .collect {
        case key if privacyRequest => baseRequest.withHeaders(RawHeader(api_key.name, key))
      }
      .getOrElse(baseRequest)

    log.trace(s"Request: '$request'")

    sendRequest[Transaction](request, timeout = RequestTimeout).value
  }

  def waitForUtxAvailability(utxLimit: Int): Task[Int] =
    Task
      .defer(nodeUtxSize)
      .delayExecution(2.seconds)
      .restartUntil(_ < utxLimit)
      .timeout(maxWaitForTxLeaveUtx)

  /**
    * Send UTX size request to `/transactions/unconfirmed/size`
    */
  def nodeUtxSize: Task[Int] = {
    val request = HttpRequest(HttpMethods.GET, nodeInfo.apiUri.withPath(nodeInfo.apiUri.path / "transactions" / "unconfirmed" / "size"))

    log.trace(s"Request: '$request'")

    import com.wavesenterprise.api.http.TransactionsApiRoute.format

    sendRequest[UtxSize](request, timeout = RequestTimeout).value.map {
      case Right(transactionsSize) =>
        log.trace(s"UTX size is '${transactionsSize.size}'")
        transactionsSize.size
      case Left(apiError) =>
        throw new RuntimeException(s"Couldn't get UTX size because of error: '$apiError'")
    }
  }
}

object TransactionChecker {
  val SignApiPath             = "/transactions/sign"
  val SignAndBroadcastApiPath = "/transactions/signAndBroadcast"
  val SendDataApiPath         = "/privacy/sendData"
}

case class NodeInfo(apiUri: Uri, privacyApiKey: Option[String] = None) {
  implicit val akkaApiUriReader: ConfigReader[Uri] = ConfigReader.fromNonEmptyString[Uri](catchReadError(Uri(_)))
}
