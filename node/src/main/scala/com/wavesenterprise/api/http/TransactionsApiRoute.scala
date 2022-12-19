package com.wavesenterprise.api.http

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import cats.data.EitherT
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.TransactionsApi
import com.wavesenterprise.api.http.ApiError.{CustomValidationError, InvalidSignature, TooBigArrayAllocation}
import com.wavesenterprise.api.http.Jsonify._
import com.wavesenterprise.api.http.auth.WithAuthFromContract
import com.wavesenterprise.api.http.service.certChainFromJson
import com.wavesenterprise.docker.ContractAuthTokenService
import com.wavesenterprise.network.TxBroadcaster
import com.wavesenterprise.privacy.PolicyStorage
import com.wavesenterprise.protobuf.service.transaction.UtxSize
import com.wavesenterprise.settings.{ApiSettings, CryptoSettings, NodeMode}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.validation.FeeCalculator
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wallet.Wallet
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import play.api.libs.json._

import scala.concurrent.ExecutionContext
import scala.util.Success

class TransactionsApiRoute(val settings: ApiSettings,
                           val feeCalculator: FeeCalculator,
                           val wallet: Wallet,
                           val blockchain: Blockchain,
                           val utx: UtxPool,
                           val time: Time,
                           val contractAuthTokenService: Option[ContractAuthTokenService],
                           val nodeOwner: Address,
                           val policyStorage: PolicyStorage,
                           val txBroadcaster: TxBroadcaster,
                           val nodeMode: NodeMode,
                           val scheduler: SchedulerService,
                           val cryptoSettings: CryptoSettings)(implicit ex: ExecutionContext)
    extends ApiRoute
    with TransactionsApi
    with WithAuthFromContract
    with TxApiFunctions
    with NonWatcherFilter
    with AdditionalDirectiveOps {

  import TransactionsApiRoute._

  protected val jsonTransactionParser = new JsonTransactionParser()

  override lazy val route: Route =
    pathPrefix("transactions") {
      unconfirmed ~ addressLimit ~ info ~ sign ~ calculateFee ~ broadcast ~ signAndBroadcast
    }

  private val userAuth = withAuth()

  /**
    * GET /transactions/address/{address}/limit/{limit}
    *
    * Get list of transactions where specified address has been involved"
    **/
  def addressLimit: Route = {
    def getResponse(address: Address, limit: Int, fromId: Option[ByteStr]): Either[String, JsArray] =
      blockchain
        .addressTransactions(address, Set.empty, limit, fromId)
        .map(_.map { case (h, tx) => txToCompactJson(address, tx) + ("height" -> JsNumber(h)) })
        .map(txs => Json.arr(JsArray(txs)))

    (get & pathPrefix("address" / Segment) & (withContractAuth | userAuth)) { address =>
      withExecutionContext(scheduler) {
        Address.fromString(address) match {
          case Left(e) => complete(ApiError.fromCryptoError(e))
          case Right(a) =>
            (path("limit" / IntNumber) & parameter('after.?)) { (limit, after) =>
              if (limit > settings.rest.transactionsByAddressLimit) complete(TooBigArrayAllocation)
              else
                after match {
                  case Some(t) =>
                    ByteStr.decodeBase58(t) match {
                      case Success(id) =>
                        getResponse(a, limit, Some(id)).fold(
                          _ => complete(StatusCodes.NotFound -> Json.obj("status" -> "error", "details" -> "Transaction is not in blockchain")),
                          complete(_))
                      case _ => complete(CustomValidationError(s"Unable to decode transaction id $t"))
                    }
                  case None =>
                    getResponse(a, limit, None)
                      .fold(_ => complete(StatusCodes.NotFound), complete(_))
                }
            } ~ complete(CustomValidationError("invalid.limit"))
        }
      }
    }
  }

  /**
    * GET /transactions/info/{id}
    **/
  def info: Route = (pathPrefix("info") & get & (withContractAuth | userAuth)) {
    pathEndOrSingleSlash {
      complete(InvalidSignature)
    } ~
      path(Segment) { encoded =>
        withExecutionContext(scheduler) {
          ByteStr.decodeBase58(encoded) match {
            case Success(id) =>
              blockchain.transactionInfo(id) match {
                case Some((h, tx)) => complete(txToExtendedJson(tx) + ("height" -> JsNumber(h)))
                case None          => complete(StatusCodes.NotFound -> Json.obj("status" -> "error", "details" -> "Transaction is not in blockchain"))
              }
            case _ => complete(InvalidSignature)
          }
        }
      }
  }

  /**
    * GET /transactions/unconfirmed
    **/
  def unconfirmed: Route = (pathPrefix("unconfirmed") & get & userAuth) {
    pathEndOrSingleSlash {
      withExecutionContext(scheduler) {
        complete(JsArray(utx.all.map(txToExtendedJson)))
      }
    } ~ utxSize ~ utxTransactionInfo
  }

  /**
    * GET /transactions/unconfirmed/size
    **/
  def utxSize: Route = (pathPrefix("size") & get & userAuth) {
    complete(utx.size)
  }

  /**
    * GET /transactions/unconfirmed/info/{id}
    **/
  def utxTransactionInfo: Route =
    (pathPrefix("info") & get & userAuth) {
      pathEndOrSingleSlash {
        complete(InvalidSignature)
      } ~
        path(Segment) { encoded =>
          withExecutionContext(scheduler) {
            ByteStr.decodeBase58(encoded) match {
              case Success(id) =>
                utx.transactionById(id) match {
                  case Some(tx) =>
                    complete(txToExtendedJson(tx))
                  case None =>
                    complete(StatusCodes.NotFound -> Json.obj("status" -> "error", "details" -> "Transaction is not in UTX"))
                }
              case _ => complete(InvalidSignature)
            }
          }
        }
    }

  /**
    * POST /transactions/calculateFee
    *
    * Calculates minimal fee for a transaction
    **/
  def calculateFee: Route = (pathPrefix("calculateFee") & post & userAuth) {
    pathEndOrSingleSlash {
      withExecutionContext(scheduler) {
        json[JsObject] { jsv =>
          val senderPk = (jsv \ "senderPublicKey").as[String]
          // Just for converting the request to the transaction
          val enrichedJsv = jsv ++ Json.obj(
            "fee"    -> 1234567,
            "sender" -> senderPk
          )

          jsonTransactionParser
            .createTransaction(senderPk, enrichedJsv)
            .fold(
              error => ToResponseMarshallable(error),
              tx => feeCalculator.calculateMinFee(blockchain.height, tx).map(feeHolderJsonify.json)
            )
        }
      }
    }
  }

  /**
    * POST /transactions/sign
    **/
  def sign: Route = (pathPrefix("sign") & post & userAuth & blockchainUpdaterGuard) {
    pathEndOrSingleSlash {
      withExecutionContext(scheduler) {
        json[JsObject] { jsv =>
          log.trace(s"Received json to sign: '$jsv'")
          sign(jsv)
        }
      }
    }
  }

  protected def sign(jsv: JsObject): Either[ApiError, JsObject] =
    jsonTransactionParser.signTransaction(jsv, wallet, time, checkCerts = false).map(_.json())

  /**
    * POST /transactions/signAndBroadcast
    **/
  def signAndBroadcast: Route =
    (pathPrefix("signAndBroadcast") & post & nonWatcherFilter & userAuth & blockchainUpdaterGuard & addedGuard) {
      pathEndOrSingleSlash {
        withExecutionContext(scheduler) {
          json[JsObject] { jsv =>
            (for {
              provenTx       <- EitherT.fromEither[Task](jsonTransactionParser.signTransaction(jsv, wallet, time, checkCerts = false)) // all checks will be done after broadcast
              tx             <- EitherT.fromEither[Task](TransactionFactory.fromSignedRequest(provenTx.json())).leftMap(ApiError.fromValidationError)
              _              <- EitherT.fromEither[Task](broadcastCertificatesCheck((jsv \ "certificates").toOption, tx))
              maybeCertChain <- EitherT.fromEither[Task](certChainFromJson(jsv))
              validatedTx    <- validateAndBroadcastTransaction(tx, maybeCertChain)
            } yield validatedTx).value.runToFuture(Scheduler(ex))
          }
        }
      }
    }

  /**
    * POST /transactions/broadcast
    **/
  def broadcast: Route = (pathPrefix("broadcast") & post & nonWatcherFilter & userAuth & blockchainUpdaterGuard) {
    withExecutionContext(scheduler) {
      json[JsObject] { jsv =>
        (for {
          tx             <- EitherT.fromEither[Task](TransactionFactory.fromSignedRequest(jsv)).leftMap(ApiError.fromValidationError)
          _              <- EitherT.fromEither[Task](broadcastCertificatesCheck((jsv \ "certificates").toOption, tx))
          maybeCertChain <- EitherT.fromEither[Task](certChainFromJson(jsv))
          _              <- validateAndBroadcastTransaction(tx, maybeCertChain)
        } yield tx).value.runToFuture(Scheduler(ex))
      }
    }
  }

  protected def broadcastCertificatesCheck(certificates: Option[JsValue], tx: Transaction): Either[ApiError, Unit] = Right(())

  /**
    * Produces compact representation for large transactions by stripping unnecessary data.
    * Currently implemented for MassTransfer transaction only.
    */
  private def txToCompactJson(address: Address, tx: Transaction): JsObject = {
    import com.wavesenterprise.transaction.transfer._
    tx match {
      case mtt: MassTransferTransaction if mtt.sender.toAddress != address =>
        val addresses = Set(address) ++ blockchain.aliasesIssuedByAddress(address)
        mtt.json() ++ Json.obj(
          "transfers" -> mtt.transfers.filter(transfer => addresses.contains(transfer.recipient)).map(_.toDescriptor)
        )
      case _ => txToExtendedJson(tx)
    }
  }
}

object TransactionsApiRoute {
  implicit val format: OFormat[UtxSize] = new OFormat[UtxSize] {
    override def writes(o: UtxSize): JsObject = JsObject {
      Seq(
        "size"        -> JsNumber(o.size),
        "sizeInBytes" -> JsNumber(o.sizeInBytes)
      )
    }

    override def reads(json: JsValue): JsResult[UtxSize] = {
      json \ "size" match {
        case JsDefined(JsNumber(size)) =>
          json \ "sizeInBytes" match {
            case JsDefined(JsNumber(sizeInBytes)) =>
              try {
                JsSuccess(UtxSize(size.toIntExact, sizeInBytes.toLongExact))
              } catch {
                case _: ArithmeticException =>
                  JsError(
                    s"value $size is too big for integer (max value = ${Int.MaxValue}) or value $sizeInBytes is too big for long (max value = ${Long.MaxValue}) or there is not number")
              }
            case _ => JsError("key 'sizeInBytes' is missing")
          }
        case _ => JsError("key 'size' is missing")
      }
    }
  }
}
