package com.wavesenterprise.http

import akka.http.scaladsl.server.Route
import ch.qos.logback.classic.{Level, LoggerContext}
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.api.http.ApiError.CustomValidationError
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.ApiKeyProtection
import com.wavesenterprise.api.http.auth.AuthRole.Administrator
import com.wavesenterprise.api.http.{ApiError, ApiRoute, CommonApiFunctions, ExternalStatusResponse, Response}
import com.wavesenterprise.http.NodeApiRoute._
import com.wavesenterprise.http.service.{MetricsApiService, MetricsStatus}
import com.wavesenterprise.protobuf.constants.CryptoType
import com.wavesenterprise.protobuf.service.util.NodeConfigResponse.{BlockTiming => PbBlockTiming}
import com.wavesenterprise.protobuf.service.util.{
  NodeConfigResponse => PbNodeConfigResponse,
  PoaRoundInfo => PbPoaRoundInfo,
  PosRoundInfo => PbPosRoundInfo
}
import com.wavesenterprise.serialization.ProtoAdapter.scalaDurationToProto
import com.wavesenterprise.settings.ConsensusSettings.{CftSettings, PoASettings, PoSSettings}
import com.wavesenterprise.settings.{ApiSettings, ConsensusType, FeeSettings, VersionConstants, WESettings}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.{ScorexLogging, Time}
import com.wavesenterprise.{Version, protobuf}
import org.slf4j.LoggerFactory
import play.api.libs.json._

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class NodeApiRoute(nodeSetting: WESettings,
                   blockchain: Blockchain,
                   val time: Time,
                   ownerPublicAccount: PublicKeyAccount,
                   healthChecker: HealthChecker)
    extends ApiRoute
    with CommonApiFunctions
    with ScorexLogging {

  val nodeOwner: Address = ownerPublicAccount.toAddress

  override val settings: ApiSettings = nodeSetting.api

  override lazy val route: Route = pathPrefix("node") {
    status ~ healthcheck ~ version ~
      withAuth() {
        nodeOwnerRoute ~ config ~ loggingReadRoute ~ metricsStatus
      } ~
      adminAuth {
        stop ~ loggingEditRoute ~ editMetricsStatus
      }
  }

  private val adminAuth = withAuth(ApiKeyProtection, Administrator)

  /**
    * GET /node/version
    **/
  def version: Route = (get & path("version")) {
    complete(Json.obj("version" -> VersionConstants.AgentName))
  }

  /**
    * POST /node/stop
    **/
  def stop: Route = (post & path("stop")) {
    log.info("Request to stop application")
    asyncStop()
    complete(Json.obj("stopped" -> true))
  }

  private def asyncStop(): Unit = {
    val runnable = new Runnable {
      override def run(): Unit = {
        sys.exit()
      }
    }
    val thread = new Thread(runnable)
    thread.start()
  }

  /**
    * GET /node/status
    **/
  def status: Route = (get & path("status")) {
    complete(healthChecker.nodeStatus)
  }

  /**
    * GET /node/healthcheck
    */
  def healthcheck: Route = (get & path("healthcheck") & parameter("service".as[String].?)) { maybeServiceName =>
    complete {
      maybeServiceName.fold {
        Future.successful[Either[ApiError, ExternalStatusResponse]](Left(CustomValidationError("Service param is required")))
      } { serviceName =>
        healthChecker.externalStatusByName(serviceName)
      }
    }
  }

  /**
    * GET /node/config
    **/
  def config: Route = (get & path("config")) {
    complete(NodeConfigRawResponse.build(nodeSetting, blockchain).toRest)
  }

  /**
    * GET /node/logging
    *
    * Returns logging levels of various loggers
    **/
  def loggingReadRoute: Route = (path("logging") & get) {
    val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    complete(
      lc.getLoggerList.asScala.map(logger => s"${logger.getName}-${logger.getEffectiveLevel.levelStr}").mkString("\n")
    )
  }

  /**
    * POST /node/logging
    *
    * Change logging levels
    **/
  def loggingEditRoute: Route = (path("logging") & post) {
    json[ChangeLoggerLevelRequest] { req =>
      val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
      lc.getLogger(req.logger).setLevel(Level.valueOf(req.level))
      Response.OK + (req.logger -> req.level)
    }
  }

  /**
    * GET /node/owner
    *
    * Returns node owner's address and public key
    */
  def nodeOwnerRoute: Route = (path("owner") & get) {
    complete(NodeOwnerResponse(nodeOwner.address, ownerPublicAccount.publicKeyBase58))
  }

  /**
    * GET /node/metrics
    *
    * Returns metrics status
    */
  def metricsStatus: Route = (path("metrics") & get) {
    val metricsApiService = new MetricsApiService()
    complete(metricsApiService.getStatus)
  }

  /**
    * POST /node/metrics
    *
    * Updates metrics status
    */
  def editMetricsStatus: Route = (path("metrics") & post) {
    json[MetricsStatus] { newStatus =>
      val metricsApiService = new MetricsApiService()
      metricsApiService.updateStatus(newStatus)
      metricsApiService.getStatus
    }
  }
}

object NodeApiRoute {
  case class ChangeLoggerLevelRequest(logger: String, level: String)
  implicit val changeLoggerLevelRequestFormat: OFormat[ChangeLoggerLevelRequest] = Json.format

  case class NodeOwnerResponse(address: String, publicKey: String)
  implicit val nodeOwnerResponseFormat: OFormat[NodeOwnerResponse] = Json.format

  implicit val finiteDurationFormat: Format[FiniteDuration] = Format(
    Reads { jsValue =>
      Try(jsValue.as[Long])
        .map(millis => JsSuccess(FiniteDuration(millis, TimeUnit.MILLISECONDS)))
        .getOrElse(JsError(s"Couldn't parse given '$jsValue' as a FiniteDuration"))
    },
    Writes(millis => JsNumber(millis.toMillis))
  )

  sealed trait BlockTiming extends Product with Serializable {
    def toProto: PbBlockTiming
  }
  object BlockTiming {
    case class PoaRoundInfo(roundDuration: FiniteDuration, syncDuration: FiniteDuration) extends BlockTiming {
      override def toProto: PbBlockTiming = {
        PbBlockTiming.PoaRoundInfo(PbPoaRoundInfo(Some(roundDuration), Some(syncDuration)))
      }
    }
    case class PosRoundInfo(averageBlockDelay: FiniteDuration) extends BlockTiming {
      override def toProto: PbBlockTiming = {
        PbBlockTiming.PosRoundInfo(PbPosRoundInfo(Some(averageBlockDelay)))
      }
    }
  }

  implicit val poaRoundInfoFormat: Format[BlockTiming.PoaRoundInfo] = Json.format
  implicit val posRoundInfoFormat: Format[BlockTiming.PosRoundInfo] = Json.format

  implicit val blockTimingFormat: Format[BlockTiming] = Format(
    { jsValue =>
      jsValue
        .asOpt[BlockTiming.PosRoundInfo]
        .orElse(jsValue.asOpt[BlockTiming.PoaRoundInfo])
        .map(JsSuccess.apply(_))
        .getOrElse(JsError(s"Couldn't parse given '$jsValue' as a BlockTiming"))
    }, {
      case poa: BlockTiming.PoaRoundInfo => poaRoundInfoFormat.writes(poa)
      case pos: BlockTiming.PosRoundInfo => posRoundInfoFormat.writes(pos)
    }
  )

  case class NodeConfigRawResponse(version: String,
                                   chainId: Char,
                                   consensus: ConsensusType,
                                   minimumFee: Map[String, Long],
                                   additionalFee: Map[String, Long],
                                   maxTransactionsInMicroBlock: Int,
                                   minMicroBlockAge: FiniteDuration,
                                   microBlockInterval: FiniteDuration,
                                   blockTiming: BlockTiming) {
    def toRest: NodeConfigResponse =
      NodeConfigResponse(
        version,
        chainId.toString,
        consensus.value.toUpperCase,
        minimumFee,
        additionalFee,
        maxTransactionsInMicroBlock,
        minMicroBlockAge,
        microBlockInterval,
        blockTiming
      )

    def toProto: PbNodeConfigResponse = {
      val protoConsensusType = consensus match {
        case ConsensusType.PoA => protobuf.constants.ConsensusType.POA
        case ConsensusType.PoS => protobuf.constants.ConsensusType.POS
        case ConsensusType.CFT => protobuf.constants.ConsensusType.CFT
      }

      PbNodeConfigResponse(
        version,
        CryptoType.CURVE_25519,
        chainId.toInt,
        protoConsensusType,
        minimumFee,
        additionalFee,
        maxTransactionsInMicroBlock,
        Some(minMicroBlockAge),
        Some(microBlockInterval),
        blockTiming.toProto
      )
    }
  }

  case class NodeConfigResponse(version: String,
                                chainId: String,
                                consensus: String,
                                minimumFee: Map[String, Long],
                                additionalFee: Map[String, Long],
                                maxTransactionsInMicroBlock: Int,
                                minMicroBlockAge: FiniteDuration,
                                microBlockInterval: FiniteDuration,
                                blockTiming: BlockTiming)

  object NodeConfigResponse {
    implicit val nodeConfigResponse: OFormat[NodeConfigResponse] = Json.format
  }

  object NodeConfigRawResponse {
    def build(nodeSettings: WESettings, blockchain: Blockchain): NodeConfigRawResponse = {
      val feeSettings        = nodeSettings.blockchain.fees.resolveActual(blockchain, blockchain.height)
      val blockchainSettings = nodeSettings.blockchain

      val txByteToMinFee: Map[String, Long] = FeeSettings.txConfNameToTypeByte.map {
        case (_, txByteId) =>
          txByteId.toString -> feeSettings.forTxType(txByteId)
      } ++ FeeSettings.tempConstantFees
      val txByteToAdditionalFee: Map[String, Long] = FeeSettings.txTypesRequiringAdditionalFees.map {
        case (_, txParser) =>
          txParser.typeId.toString -> feeSettings.forTxTypeAdditional(txParser.typeId)
      }

      val blockTiming = blockchainSettings.consensus match {
        case CftSettings(roundDuration, syncDuration, _, _, _, _, _, _) =>
          BlockTiming.PoaRoundInfo(roundDuration, syncDuration)
        case PoASettings(roundDuration, syncDuration, _, _, _) =>
          BlockTiming.PoaRoundInfo(roundDuration, syncDuration)
        case PoSSettings =>
          BlockTiming.PosRoundInfo(blockchainSettings.custom.genesis.toPlainSettingsUnsafe.averageBlockDelay)
      }

      NodeConfigRawResponse(
        Version.VersionString,
        blockchainSettings.custom.addressSchemeCharacter,
        blockchainSettings.consensus.consensusType,
        txByteToMinFee,
        txByteToAdditionalFee,
        nodeSettings.miner.maxTransactionsInMicroBlock,
        nodeSettings.miner.minMicroBlockAge,
        nodeSettings.miner.microBlockInterval,
        blockTiming
      )
    }
  }
}
