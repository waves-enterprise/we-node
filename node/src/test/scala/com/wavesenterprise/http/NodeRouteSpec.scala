package com.wavesenterprise.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.PingCmd
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.anchoring.TargetnetAuthTokenProvider
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.PoSLikeConsensusBlockData
import com.wavesenterprise.crypto.internals.pki.Models.ExtendedKeyUsage
import com.wavesenterprise.database.rocksdb.MainRocksDBStorage
import com.wavesenterprise.database.{MainDBKey, Keys}
import com.wavesenterprise.history._
import com.wavesenterprise.http.ApiMarshallers._
import com.wavesenterprise.http.NodeApiRoute.{BlockTiming, NodeConfigResponse, NodeOwnerResponse}
import com.wavesenterprise.http.service.MetricsStatus
import com.wavesenterprise.lagonaki.mocks.TestBlock.randomOfLength
import com.wavesenterprise.metrics.{Metrics, MetricsType}
import com.wavesenterprise.privacy.{EmptyPolicyStorage, PolicyStorage}
import com.wavesenterprise.settings.{ConsensusSettings, CryptoSettings, FeeSettings, PlainGenesisSettings, WESettings}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{CryptoHelpers, TestTime, Version}
import monix.eval.Task
import org.apache.commons.io.FileUtils
import org.scalamock.scalatest.PathMockFactory
import play.api.libs.json.{JsObject, Json}
import pureconfig.ConfigSource

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._

class NodeRouteSpec extends RouteSpec("/node") with ApiSettingsHelper with PathMockFactory {
  private val sampleSettings   = DefaultWESettings
  val testingAddressChar: Char = 'Z'
  protected val testBlockchainSettings =
    sampleSettings.blockchain
      .copy(
        custom = sampleSettings.blockchain.custom.copy(addressSchemeCharacter = testingAddressChar),
        consensus = ConsensusSettings.PoSSettings
      )
  protected val weSettings = ConfigSource
    .fromConfig(config)
    .at(WESettings.configPath)
    .loadOrThrow[WESettings]
    .copy(api = restAPISettings, blockchain = testBlockchainSettings)

  protected val cryptoSettings: CryptoSettings = ConfigSource.fromConfig(config).at(WESettings.configPath).loadOrThrow[CryptoSettings]

  val ownerAccount: PrivateKeyAccount = CryptoHelpers.generatePrivateKey

  val mockBlock: Block = Block
    .buildAndSign(
      Block.NgBlockVersion,
      System.currentTimeMillis(),
      randomSig,
      PoSLikeConsensusBlockData(0L, randomOfLength(Block.GeneratorSignatureLength)),
      List.empty,
      ownerAccount,
      Set.empty
    )
    .explicitGet()

  protected val blockchain = mock[Blockchain]
  (blockchain.height _).expects().returning(1).anyNumberOfTimes()
  (blockchain.blockHeaderAndSize(_: Int)).expects(*).returning(Some(mockBlock.blockHeader -> 1)).anyNumberOfTimes()

  private val scheduler = monix.execution.Scheduler.global

  private val authProvider  = TargetnetAuthTokenProvider.NoToken
  private val policyStorage = mock[PolicyStorage]
  (policyStorage.healthCheck _).expects().returning(Task.pure(Right(()))).anyNumberOfTimes()
  private val dockerClient = mock[DockerClient]
  private val pingCmd      = mock[PingCmd]
  (dockerClient.pingCmd _).expects().returning(pingCmd).anyNumberOfTimes()
  (pingCmd.exec _).expects().returning(null).anyNumberOfTimes()

  private val rocksDB = mock[MainRocksDBStorage]
  (rocksDB.get(_: MainDBKey[Option[Int]])).expects(Keys.schemaVersion).returning(Some(1)).anyNumberOfTimes()

  private val fileStorage = mock[FileStorageService]
  (fileStorage.freeSpace _).expects(*).returning(600 * 1024 * 1024).anyNumberOfTimes()

  private val isFrozen = new AtomicBoolean(false)

  protected val healthChecker = HealthChecker(
    sampleSettings.healthCheck,
    blockchain,
    15.seconds,
    2 * FileUtils.ONE_MB,
    rocksDB,
    ".",
    policyStorage,
    Some(authProvider),
    Some(dockerClient),
    isFrozen,
    fileStorage
  )(scheduler)

  val route: Route =
    new NodeApiRoute(weSettings, cryptoSettings, blockchain, new TestTime, ownerAccount, healthChecker).route

  routePath("/config") - {
    "check for PoS settings" in {
      Get(routePath("/config")) ~> route ~> check {
        response.status shouldBe StatusCodes.OK
        val parsedResponse = responseAs[NodeConfigResponse]
        parsedResponse.version shouldBe Version.VersionString
        parsedResponse.cryptoType shouldBe cryptoSettings.name
        parsedResponse.chainId shouldBe testingAddressChar.toString
        parsedResponse.consensus shouldBe ConsensusSettings.PoSSettings.consensusType.toString.toUpperCase
        val feeMap = FeeSettings.txConfNameToTypeByte.map {
          case (_, txByteId) =>
            txByteId.toString -> weSettings.blockchain.fees.asInstanceOf[FeeSettings].forTxType(txByteId)
        } ++ FeeSettings.tempConstantFees
        parsedResponse.minimumFee shouldBe feeMap

        val additionalFeeMap = FeeSettings.txTypesRequiringAdditionalFees.map {
          case (_, txParser) =>
            val txByteId = txParser.typeId
            txByteId.toString -> weSettings.blockchain.fees.asInstanceOf[FeeSettings].forTxTypeAdditional(txByteId)
        }
        parsedResponse.additionalFee shouldBe additionalFeeMap

        weSettings.blockchain.custom.genesis shouldBe a[PlainGenesisSettings]
        val actualBlockDelay = weSettings.blockchain.custom.genesis.toPlainSettingsUnsafe.averageBlockDelay
        parsedResponse.blockTiming shouldBe a[BlockTiming.PosRoundInfo]
        parsedResponse.blockTiming.asInstanceOf[BlockTiming.PosRoundInfo].averageBlockDelay shouldBe actualBlockDelay
      }
    }
    "check for PoA settings" in {
      val roundDuration     = 17 seconds
      val syncDuration      = 3 seconds
      val banDurationBlocks = 50
      val warningsForBan    = 5
      val maxBansPercentage = 20
      val poaSettings       = ConsensusSettings.PoASettings(roundDuration, syncDuration, banDurationBlocks, warningsForBan, maxBansPercentage)
      val settings          = weSettings.copy(blockchain = weSettings.blockchain.copy(consensus = poaSettings))
      val route: Route =
        new NodeApiRoute(settings, cryptoSettings, blockchain, new TestTime, ownerAccount, healthChecker).route
      Get(routePath("/config")) ~> route ~> check {
        response.status shouldBe StatusCodes.OK
        val parsedResponse = responseAs[NodeConfigResponse]
        parsedResponse.blockTiming shouldBe a[BlockTiming.PoaRoundInfo]
        val poaRoundInfo = parsedResponse.blockTiming.asInstanceOf[BlockTiming.PoaRoundInfo]
        poaRoundInfo.roundDuration shouldBe roundDuration
        poaRoundInfo.syncDuration shouldBe syncDuration
      }
    }

    "pki settings" - {
      "check disabled" in {
        Get(routePath("/config")) ~> route ~> check {
          response.status shouldBe StatusCodes.OK
          val parsedResponse = responseAs[NodeConfigResponse]

          val (pkiMode, requiredOids, crlChecksEnabled) = ("OFF", Seq.empty[String], false)

          val actualRequiredOids = ExtendedKeyUsage.parseStrings(parsedResponse.requiredOids: _*)

          parsedResponse.pkiMode shouldBe pkiMode
          actualRequiredOids shouldBe 'right
          actualRequiredOids.right.get should contain theSameElementsAs requiredOids
          parsedResponse.crlChecksEnabled shouldBe crlChecksEnabled
        }
      }
    }
  }

  routePath("/owner") - {
    "returns correct node owner's address and public key" in {
      Get(routePath("/owner")) ~> route ~> check {
        val ownerResponse = responseAs[NodeOwnerResponse]
        ownerResponse.address shouldBe ownerAccount.toAddress.address
        ownerResponse.publicKey shouldBe ownerAccount.publicKeyBase58
      }
    }
  }

  routePath("/status") in {
    Get(routePath("/status")) ~> route ~> check {
      response.status shouldBe StatusCodes.OK
      val responseJson = responseAs[JsObject]
      (responseJson \ "blockchainHeight").as[Int] shouldBe 1
      (responseJson \ "stateHeight").as[Int] shouldBe 1
    }
  }

  routePath("/healthcheck") - {
    "returns 200" - {
      "on docker service" in {
        Get(routePath("/healthcheck?service=docker")) ~> route ~> check {
          response.status shouldBe StatusCodes.OK
        }
      }

      "on privacy storage service" in {
        Get(routePath("/healthcheck?service=privacy-storage")) ~> route ~> check {
          response.status shouldBe StatusCodes.OK
        }
      }

      "on anchoring auth storage service" in {
        Get(routePath("/healthcheck?service=anchoring-auth")) ~> route ~> check {
          response.status shouldBe StatusCodes.OK
        }
      }
    }

    "returns correct error" - {
      "on unknown service" in {
        Get(routePath("/healthcheck?service=hello-world")) ~> route ~> check {
          response.status shouldBe StatusCodes.BadRequest
          (responseAs[JsObject] \ "message").as[String] shouldBe "Unknown service 'hello-world'"
        }
      }

      "without service param" in {
        Get(routePath("/healthcheck")) ~> route ~> check {
          response.status shouldBe StatusCodes.BadRequest
          (responseAs[JsObject] \ "message").as[String] shouldBe "Service param is required"
        }
      }

      "on disabled service" - {
        val time = new TestTime
        val disabledExternalServicesHealthChecker = HealthChecker(
          sampleSettings.healthCheck,
          blockchain,
          15.seconds,
          2 * FileUtils.ONE_MB,
          rocksDB,
          ".",
          new EmptyPolicyStorage(time),
          None,
          None,
          isFrozen,
          fileStorage
        )(scheduler)
        val disabledExternalServicesRoute: Route =
          new NodeApiRoute(weSettings, cryptoSettings, blockchain, time, ownerAccount, disabledExternalServicesHealthChecker).route

        "docker" in {
          Get(routePath("/healthcheck?service=docker")) ~> disabledExternalServicesRoute ~> check {
            response.status shouldBe StatusCodes.NotFound
            (responseAs[JsObject] \ "message").as[String] shouldBe "Service 'Docker' is disabled"
          }
        }

        "privacy storage" in {
          Get(routePath("/healthcheck?service=privacy-storage")) ~> disabledExternalServicesRoute ~> check {
            response.status shouldBe StatusCodes.NotFound
            (responseAs[JsObject] \ "message").as[String] shouldBe "Service 'Privacy Storage' is disabled"
          }
        }

        "anchoring auth" in {
          Get(routePath("/healthcheck?service=anchoring-auth")) ~> disabledExternalServicesRoute ~> check {
            response.status shouldBe StatusCodes.NotFound
            (responseAs[JsObject] \ "message").as[String] shouldBe "Service 'Anchoring' is disabled"
          }
        }
      }
    }
  }

  routePath("/metrics") in {
    MetricsType.values.foreach(Metrics.registry.enable)
    Get(routePath("/metrics")) ~> route ~> check {
      response.status shouldBe StatusCodes.OK
      val status = responseAs[MetricsStatus]
      status shouldBe MetricsStatus(enabled = MetricsType.values.toSet, disabled = Set.empty)
    }

    val updatedStatus = MetricsStatus(enabled = Set.empty, disabled = Set(MetricsType.Common, MetricsType.Block))
    Post(routePath("/metrics"), Json.toJson(updatedStatus)) ~> api_key(apiKey) ~> route ~> check {
      response.status shouldBe StatusCodes.OK
      val status = responseAs[MetricsStatus]
      status shouldBe MetricsStatus(enabled = MetricsType.values.toSet -- updatedStatus.disabled, disabled = updatedStatus.disabled)
    }
  }
}
