package com.wavesenterprise.api.grpc.service

import akka.actor.ActorSystem
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.MetadataBuilder
import akka.stream.scaladsl.Source
import com.google.protobuf.ByteString
import com.wavesenterprise.api.grpc.utils.ErrorMessageMetadataKey
import com.wavesenterprise.api.http.service.PrivacyApiService
import com.wavesenterprise.api.http.service.PrivacyApiService.ValidSendDataSetup
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.privacy.PolicyItem
import com.wavesenterprise.http.api_key
import com.wavesenterprise.protobuf.service.privacy.{File, PolicyItemFileInfo, SendDataMetadata, SendLargeDataRequest}
import com.wavesenterprise.settings.privacy.PrivacyServiceSettings
import com.wavesenterprise.settings.{AuthorizationSettings, NodeMode}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.PolicyDataHashTransaction
import com.wavesenterprise.utils.Base58
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.{TestTime, TransactionGen, crypto}
import monix.execution.Scheduler
import org.apache.commons.io.FileUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import squants.information.Mebibytes

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.Random

class PrivacyServiceImplSpec
    extends AnyFreeSpec
    with TransactionGen
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with ScalaCheckPropertyChecks {
  implicit val sys: ActorSystem             = ActorSystem("PrivacyServiceImplSpec")
  implicit val ec: ExecutionContextExecutor = sys.dispatcher
  implicit val scheduler: Scheduler         = Scheduler.Implicits.global

  private val ownerAddress           = accountGen.sample.get.toAddress
  private val time                   = new TestTime
  private val apiKey                 = "foo"
  private lazy val apiKeyHash        = Base58.encode(crypto.secureHash(apiKey))
  private val authSetting            = AuthorizationSettings.ApiKey(apiKeyHash, apiKeyHash, apiKeyHash)
  private val privacyServiceSettings = PrivacyServiceSettings(Mebibytes(10), 3.seconds)

  "#sendLargeData" - {
    val policyId         = "random_policy_id"
    val dataHash         = "random_policy_hash"
    val mockedApiService = stub[PrivacyApiService]
    val service =
      new PrivacyServiceImpl(mockedApiService, true, authSetting, privacyServiceSettings, ownerAddress, time, NodeMode.Default)
    val sendDataMetadata = buildSendDataMetadata(policyId, dataHash, ownerAddress.address)
    val data             = randomData(size = FileUtils.ONE_MB.toInt * 3)
    val requestMetadata  = new MetadataBuilder().addText(api_key.name, apiKey).build()
    val metaDataPart     = SendLargeDataRequest(SendLargeDataRequest.Request.Metadata(sendDataMetadata))
    val chunkSize        = FileUtils.ONE_MB.toInt - 100
    val dataParts = data
      .grouped(chunkSize)
      .map { chunk =>
        SendLargeDataRequest(SendLargeDataRequest.Request.File(File(ByteString.copyFrom(chunk))))
      }
      .toList
    "should return proper PDH tx" in {
      forAll(policyDataHashTransactionV1Gen()) {
        case PolicyDataWithTxV1(_, dataHashTx) =>
          (mockedApiService.isLargeObjectFeatureActivated _)
            .when()
            .returning(true)
            .anyNumberOfTimes()

          (mockedApiService.validateLargePolicyItem _)
            .when(*)
            .onCall { (_: PolicyItem) =>
              Right(mock[ValidSendDataSetup])
            }
            .once()

          (mockedApiService.sendLargeData _)
            .when(*, *, *, *)
            .onCall { (_, _, obs, _) =>
              obs.completedL.runToFuture.map[Either[ApiError, PolicyDataHashTransaction]](_ => Right(dataHashTx))(ec)
            }
            .once()

          val stream         = Source(metaDataPart +: dataParts)
          val sendLargeDataF = service.sendLargeData(stream, requestMetadata)
          val resultTx       = Await.result(sendLargeDataF, 10.seconds).tx

          resultTx should not be empty
          ByteStr(resultTx.get.id.toByteArray) shouldBe dataHashTx.id()
      }
    }

    "should return proper error on wrong stream elements order" in {
      (mockedApiService.isLargeObjectFeatureActivated _)
        .when()
        .returning(true)
        .anyNumberOfTimes()

      val stream = Source(dataParts :+ metaDataPart)
      (the[GrpcServiceException] thrownBy {
        val sendLargeDataF = service.sendLargeData(stream, requestMetadata)
        Await.result(sendLargeDataF, 10.seconds)
      }).metadata.getText(ErrorMessageMetadataKey) shouldBe Some("Metadata part must be in the head of the stream")
    }

    "should return proper error on empty chunk" in {
      (mockedApiService.isLargeObjectFeatureActivated _)
        .when()
        .returning(true)
        .anyNumberOfTimes()

      val emptyChunk = SendLargeDataRequest(SendLargeDataRequest.Request.File(File(ByteString.EMPTY)))
      val stream     = Source(metaDataPart :: emptyChunk :: dataParts)
      (the[GrpcServiceException] thrownBy {
        val sendLargeDataF = service.sendLargeData(stream, requestMetadata)
        Await.result(sendLargeDataF, 10.seconds)
      }).metadata.getText(ErrorMessageMetadataKey) shouldBe Some("Empty data stream")
    }
  }

  private def buildSendDataMetadata(policyId: String, dataHash: String, senderAddress: String) =
    new SendDataMetadata(
      senderAddress = senderAddress,
      policyId = policyId,
      dataHash = dataHash,
      info = Some(PolicyItemFileInfo("", 1, 1, "", "")),
      fee = 0.05.west,
      feeAssetId = None,
      atomicBadge = None,
      password = None,
      broadcastTx = false
    )

  private def randomData(size: Int): Array[Byte] =
    (1 to size)
      .map(_ => Random.nextInt(100).toByte)
      .toArray
}
