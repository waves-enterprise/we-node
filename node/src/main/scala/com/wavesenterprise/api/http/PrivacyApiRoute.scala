package com.wavesenterprise.api.http

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, Multipart}
import akka.http.scaladsl.server.{Directive0, Route}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{Attributes, Materializer}
import akka.util.ByteString
import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.service.ConnectionIdsHolder
import com.wavesenterprise.api.http.ApiError.{CustomValidationError, PrivacyIsSwitchedOff, PrivacyLargeObjectFeatureIsNotActivated, WrongJson}
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.PrivacyApiKeyProtection
import com.wavesenterprise.api.http.auth.AuthRole.PrivacyUser
import com.wavesenterprise.api.http.auth.WithAuthFromContract
import com.wavesenterprise.api.http.service.PrivacyApiService
import com.wavesenterprise.api.http.service.PrivacyApiService.ValidSendDataSetup
import com.wavesenterprise.docker.ContractAuthTokenService
import com.wavesenterprise.settings.privacy.PrivacyServiceSettings
import com.wavesenterprise.settings.{ApiSettings, NodeMode}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.{PolicyDataHashTransaction, Transaction}
import com.wavesenterprise.utils.{Base64, Time}
import monix.execution.schedulers.SchedulerService
import monix.reactive.{Observable, OverflowStrategy}
import org.apache.commons.io.FileUtils
import play.api.libs.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object PrivacyApiRoute {
  // each char is 8 bytes, so we skip multiplication by 8
  final val sendDataEntityMaxSize: Long      = 28 * FileUtils.ONE_MB
  final val sendLargeDataEntityMaxSize: Long = 5 * FileUtils.ONE_GB
}

class PrivacyApiRoute(val privacyService: PrivacyApiService,
                      val settings: ApiSettings,
                      val serviceSettings: PrivacyServiceSettings,
                      val time: Time,
                      val privacyEnabled: Boolean,
                      val contractAuthTokenService: Option[ContractAuthTokenService],
                      val nodeOwner: Address,
                      val nodeMode: NodeMode,
                      val scheduler: SchedulerService)(implicit val mat: Materializer)
    extends ApiRoute
    with WithAuthFromContract
    with NonWatcherFilter
    with ConnectionIdsHolder {

  protected def buildRoute(): Route = pathPrefix("privacy") {
    policyRecipients ~ policyOwners ~ policyHashes ~ policyItemData ~ policyItemLargeData ~ policyItemInfo ~ policyItemsInfo ~ sendData ~ sendDataV2 ~ forceSync ~ forceSyncByPolicyId ~ sendLargeData
  }

  override val route: Route = buildRoute()

  private val userAuth              = withAuth()
  private val privacyUserAuth       = withAuth(PrivacyApiKeyProtection, PrivacyUser)
  private val contractOrUserAuth    = withContractAuth | userAuth
  private val contractOrPrivacyAuth = withContractAuth | privacyUserAuth

  private def withPrivacyEnabled: Directive0 = if (privacyEnabled) pass else complete(PrivacyIsSwitchedOff)
  private def withLargeObjectFeatureActivated: Directive0 = {
    if (privacyService.isLargeObjectFeatureActivated()) pass else complete(PrivacyLargeObjectFeatureIsNotActivated)
  }

  /**
    * GET /privacy/{policy-id}/recipients
    *
    * Get list of policy recipients by policy id
    **/
  def policyRecipients: Route = {
    (get & path(Segment / "recipients") & contractOrUserAuth) { policyId =>
      complete(privacyService.policyRecipients(policyId))
    }
  }

  /**
    * GET /privacy/{policy-id}/owners
    *
    * Get list of policy owners by policy id
    **/
  def policyOwners: Route = {
    (get & path(Segment / "owners") & userAuth) { policyId =>
      complete(privacyService.policyOwners(policyId))
    }
  }

  /**
    * GET /privacy/{policy-id}/hashes
    *
    * Get all policy data hashes
    **/
  def policyHashes: Route = {
    (get & path(Segment / "hashes") & contractOrUserAuth) { policyId =>
      complete(privacyService.policyHashes(policyId))
    }
  }

  /**
    * GET /privacy/{policy-id}/getData/{policy-item-hash}
    *
    * Get policy item data
    **/
  def policyItemData: Route = {
    (get & path(Segment / "getData" / Segment) & contractOrPrivacyAuth & withPrivacyEnabled) {
      case (policyId, policyItemHash) =>
        import mat.executionContext
        complete {
          privacyService
            .policyItemData(policyId, policyItemHash)
            .map { byteStrOrError =>
              byteStrOrError.map(byteStr => Base64.encode(byteStr.arr))
            }
        }
    }
  }

  /**
    * GET /privacy/{policy-id}/getLargeData/{policy-item-hash}
    *
    * Get policy item data
    **/
  def policyItemLargeData: Route = {
    (get & path(Segment / "getLargeData" / Segment) & contractOrPrivacyAuth & withPrivacyEnabled & withLargeObjectFeatureActivated) {
      case (policyId, policyItemHash) =>
        complete {
          privacyService
            .policyItemLargeData(policyId, policyItemHash)
            .map { bytesStreamOrError =>
              bytesStreamOrError.map(bytesStream => {
                val resSource = Source.fromPublisher(bytesStream.map(ByteString(_)).toReactivePublisher(scheduler))
                HttpEntity(ContentTypes.`application/octet-stream`, resSource)
              })
            }(scheduler)
        }
    }
  }

  /**
    * GET /privacy/{policy-id}/getInfo/{policy-item-hash}
    *
    * Get policy item info
    **/
  def policyItemInfo: Route = {
    (get & path(Segment / "getInfo" / Segment) & contractOrPrivacyAuth & withPrivacyEnabled) {
      case (policyId, policyItemHash) =>
        complete(privacyService.policyItemInfo(policyId, policyItemHash))
    }
  }

  /**
    * POST /privacy/getInfos
    *
    * Get policy item info
    **/
  def policyItemsInfo: Route = {
    (path("getInfos") & post & privacyUserAuth) {
      json[PoliciesMetaInfoRequest](privacyService.policyItemsInfo)
    }
  }

  /**
    * POST /privacy/sendData
    *
    * Upload policy private data
    **/
  def sendData: Route =
    withSizeLimit(PrivacyApiRoute.sendDataEntityMaxSize) {
      (pathPrefix("sendData") & post & parameter("broadcast".as[Boolean] ? true) &
        privacyUserAuth &
        nonWatcherFilter &
        withPrivacyEnabled) { broadcast =>
        json[JsObject] { jsv =>
          sendPolicyItem(jsvToSendDataRequest(jsv, broadcast).flatMap(_.toPolicyItem.leftMap(ApiError.fromValidationError)), broadcast)
        }
      }
    }

  /**
    * POST /privacy/sendDataV2
    *
    * Upload policy private data
    **/
  def sendDataV2: Route =
    withSizeLimit(PrivacyApiRoute.sendDataEntityMaxSize) {
      (pathPrefix("sendDataV2") & post & parameter("broadcast".as[Boolean] ? true) &
        privacyUserAuth &
        nonWatcherFilter &
        withPrivacyEnabled) { broadcast =>
        entity(as[Multipart.FormData]) { formData =>
          import mat.executionContext

          val parsedFormData = formData.parts
            .mapAsyncUnordered[Either[ApiError, (String, SendDataDTO)]](Runtime.getRuntime.availableProcessors)(parseBodyPart(_, broadcast))
            .runFold(Map.empty[String, SendDataDTO].asRight[ApiError]) {
              case (acc, part) =>
                for {
                  processedMap  <- acc
                  processedPart <- part
                } yield processedMap + processedPart
            }

          val tx = for {
            policyItemOrError <- parsedFormData.map(_.map(mergeSendDataMap))
            transaction       <- sendPolicyItem(policyItemOrError, broadcast)
          } yield transaction

          complete(tx)
        }
      }
    }

  sealed trait MultipartProcessingAccumulator

  object MultipartProcessingAccumulator {
    case object Empty                                                                 extends MultipartProcessingAccumulator
    case class MetadataAccumulated(policyItem: PolicyItem, setup: ValidSendDataSetup) extends MultipartProcessingAccumulator
    case class Completed(tx: PolicyDataHashTransaction)                               extends MultipartProcessingAccumulator
  }

  private val requestPartName = "request"
  private val dataPartName    = "data"

  case class ApiErrorException(apiError: ApiError) extends Throwable

  private def handleApiError[T](ei: Either[ApiError, T]): Future[T] = {
    ei match {
      case Left(apiError) => Future.failed(ApiErrorException(apiError))
      case Right(value)   => Future.successful(value)
    }
  }

  /**
    * POST /privacy/sendLargeData
    * Upload policy private large data
    */
  def sendLargeData: Route = withSizeLimit(PrivacyApiRoute.sendLargeDataEntityMaxSize) {
    (pathPrefix("sendLargeData") & post & parameter("broadcast".as[Boolean] ? true) &
      privacyUserAuth &
      withPrivacyEnabled &
      nonWatcherFilter &
      withLargeObjectFeatureActivated) { toBroadcast =>
      entity(as[Multipart.FormData]) { formData =>
        import MultipartProcessingAccumulator._
        import mat.executionContext
        val connectionId = generateUniqueConnectionId
        log.debug(s"[$connectionId] Starting data loading process")

        val streamProcessing = formData.parts
          .runFoldAsync[MultipartProcessingAccumulator](Empty) {
            case (Empty, bodyPart) =>
              handleApiError(validateRequestPart(bodyPart.name.toLowerCase)) >>
                bodyPart.toStrict(serviceSettings.metaDataAccumulationTimeout).flatMap { bp =>
                  handleApiError {
                    for {
                      jsv             <- bodyPartToJsv(bp)
                      sendDataRequest <- jsvToSendDataRequest(jsv, toBroadcast)
                      policyItem      <- sendDataRequest.toPolicyItem.leftMap(ApiError.fromValidationError)
                      setup           <- privacyService.validateLargePolicyItem(policyItem).leftMap(ApiError.fromValidationError)
                    } yield {
                      log.trace(s"[$connectionId] Received metadata for policy '${sendDataRequest.policyId}' with hash '${sendDataRequest.hash}'")
                      MetadataAccumulated(policyItem, setup)
                    }
                  }
                }
            case (MetadataAccumulated(item, setup), bodyPart) =>
              handleApiError(validateDataPart(bodyPart.name.toLowerCase)) >> {
                val dataStream = Observable
                  .fromReactivePublisher {
                    bodyPart.entity.dataBytes.addAttributes(Attributes.inputBuffer(1, 1)).runWith(Sink.asPublisher(fanout = false))
                  }
                  .flatMap(Observable.fromIterable(_))
                  .asyncBoundary(OverflowStrategy.BackPressure(serviceSettings.requestBufferSize.toBytes.toInt))

                Future(log.trace(s"[$connectionId] Start stream data loading for policy '${item.policyId}' with hash '${item.hash}'")) >>
                  privacyService
                    .sendLargeData(item, setup, dataStream, toBroadcast)
                    .flatMap { ei =>
                      handleApiError(ei.map(Completed))
                    }
              }
            case (state, bodyPart) =>
              log.trace(s"[$connectionId] Skipping unexpected form data part '${bodyPart.name}'")
              Future.successful(state)
          }
          .map[Either[ApiError, PolicyDataHashTransaction]] {
            case Completed(tx) =>
              log.debug(s"[$connectionId] Data successfully persisted")
              Right(tx)
            case _ => Left(ApiError.CustomValidationError("Unexpected state"))
          }
          .recover {
            case ApiErrorException(apiError) =>
              Left(apiError)
            case NonFatal(unhandled) =>
              log.error("Unhandled exception", unhandled)
              Left(ApiError.CustomValidationError(s"Unexpected error: ${unhandled.getMessage}"))
          }

        complete(streamProcessing)
      }
    }
  }

  private def validateRequestPart(partName: String): Either[ApiError, Unit] = {
    Either.cond(partName == requestPartName,
                (),
                ApiError.CustomValidationError(s"'$requestPartName' part must be in the head of the Multipart, got '$partName'"))
  }

  private def validateDataPart(partName: String): Either[ApiError, Unit] = {
    Either.cond(partName.name.toLowerCase == dataPartName,
                (),
                ApiError.CustomValidationError(s"'$dataPartName' part must be followed by the '$requestPartName' part"))
  }

  private def parseBodyPart(part: Multipart.FormData.BodyPart, toBroadcast: Boolean)(implicit ex: ExecutionContext) =
    part.name.toLowerCase match {
      case `dataPartName` =>
        part.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).map(bt => Right(dataPartName -> PolicyItem(Right(ByteStr(bt.toArray)))))
      case `requestPartName` =>
        part.toStrict(2 seconds).map { bodyPart =>
          for {
            jsv             <- bodyPartToJsv(bodyPart)
            sendDataRequest <- jsvToSendDataRequest(jsv, toBroadcast)
          } yield requestPartName -> sendDataRequest
        }
    }

  private def jsvToSendDataRequest(jsv: JsObject, toBroadcast: Boolean): Either[ApiError, SendDataRequest] =
    (jsv \ "version").validateOpt[Byte](versionReads) match {
      case JsError(errors) => Left(WrongJson(None, errors))
      case JsSuccess(valueOpt, _) =>
        val finalJsv = valueOpt.map(_ => jsv).getOrElse(jsv + ("version" -> JsNumber(1)))
        valueOpt.getOrElse(1: Byte) match {
          case 3 => Right(finalJsv.as[SendDataRequest])
          case 1 | 2 =>
            Either.cond(toBroadcast,
                        finalJsv.as[SendDataRequest],
                        CustomValidationError(s"SendDataRequest V1/V2 with 'broadcast=false' is not allowed. Use V3 instead"))
          case otherVersion =>
            Left(CustomValidationError(s"Unknown version '$otherVersion' provided for '/privacy/sendData'"))
        }
    }

  private def bodyPartToJsv(part: Multipart.FormData.BodyPart.Strict): Either[WrongJson, JsObject] = {
    val jsv = Json.parse(part.entity.data.utf8String)
    Json.fromJson[JsObject](jsv).asEither.leftMap(WrongJson(None, _))
  }

  private def sendPolicyItem(policyItem: Either[ApiError, PolicyItem], broadcast: Boolean): Future[Either[ApiError, Transaction]] =
    policyItem match {
      case Left(err)         => Future.successful(Left(err))
      case Right(policyItem) => privacyService.sendData(policyItem, broadcast)
    }

  private val mergeSendDataMap: Map[String, SendDataDTO] => PolicyItem =
    map => {
      val data = map("data").asInstanceOf[PolicyItem]
      val r    = map("request").asInstanceOf[SendDataRequest]
      data.mergeWithRequest(r)
    }

  /**
    * POST /privacy/forceSync/
    *
    * Force node to download all policy data that it should store
    **/
  def forceSync: Route = {
    (pathPrefix("forceSync") & post & privacyUserAuth & withPrivacyEnabled) {
      complete(privacyService.forceSync())
    }
  }

  /**
    * GET /privacy/forceSync/{policyId}
    *
    * Force node to download policy data by policy id that it should store
    **/
  def forceSyncByPolicyId: Route = {
    (get & path("forceSync" / Segment) & privacyUserAuth & withPrivacyEnabled) { policyId =>
      complete(privacyService.forceSync(policyId, nodeOwner))
    }
  }
}
