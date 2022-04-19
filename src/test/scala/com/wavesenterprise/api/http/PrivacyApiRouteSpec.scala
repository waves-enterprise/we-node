package com.wavesenterprise.api.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError.CustomValidationError
import com.wavesenterprise.api.http.service.PrivacyApiService
import com.wavesenterprise.api.http.service.PrivacyApiService.ValidSendDataSetup
import com.wavesenterprise.database.PrivacyLostItemUpdater
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.http.{ApiSettingsHelper, RouteSpec, api_key}
import com.wavesenterprise.network.EnabledTxBroadcaster
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.privacy.NoOpPolicyDataSynchronizer
import com.wavesenterprise.privacy._
import com.wavesenterprise.settings.SynchronizationSettings.TxBroadcasterSettings
import com.wavesenterprise.settings.privacy.PrivacyServiceSettings
import com.wavesenterprise.settings.{ApiSettings, NodeMode, TestFees}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.validation.DisabledFeeCalculator
import com.wavesenterprise.transaction.{PolicyDataHashTransaction, PolicyDataHashTransactionV1}
import com.wavesenterprise.utils.{Base58, Base64}
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.{TestException, TestSchedulers, TestTime, TransactionGen}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.scalacheck.Arbitrary
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.Eventually
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.{JsArray, JsObject, JsValue, Json}
import squants.information.Mebibytes
import tools.GenHelper.ExtendedGen

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class PrivacyApiRouteSpec
    extends RouteSpec("/privacy")
    with PathMockFactory
    with ApiSettingsHelper
    with Eventually
    with TransactionGen
    with ApiRoute
    with ScalaCheckPropertyChecks {

  private val ownerKey             = accountGen.generateSample()
  protected val nodeOwner: Address = ownerKey.toAddress

  val time = new TestTime
  abstract class StateType extends Blockchain with PrivacyLostItemUpdater
  private val state                 = stub[StateType]
  private val wallet                = stub[Wallet]
  private val policyStorage         = stub[PolicyStorage]
  private val utxPool               = stub[UtxPool]
  private val activePeerConnections = stub[ActivePeerConnections]
  private val serviceSettings       = PrivacyServiceSettings(Mebibytes(10), 3.seconds)
  private val txBroadcasterSettings = TxBroadcasterSettings(10000, 20.seconds, 1, 3, 500, 1.second, 20.seconds)
  private val txBroadcaster =
    new EnabledTxBroadcaster(txBroadcasterSettings, utxPool, activePeerConnections, 30)(TestSchedulers.transactionBroadcastScheduler)
  private val service =
    new PrivacyApiService(state, wallet, ownerKey, policyStorage, NoOpPolicyDataSynchronizer, DisabledFeeCalculator, time, txBroadcaster)(
      monix.execution.Scheduler.Implicits.global)
  private val privacyRoute = new PrivacyApiRoute(
    service,
    restAPISettings,
    serviceSettings,
    time,
    true,
    None,
    nodeOwner,
    NodeMode.Default,
    apiComputationsScheduler
  ).route

  override val settings: ApiSettings = restAPISettings
  override val route: Route          = privacyRoute

  initSetup()

  routePath("/{policyId}/recipients") in {
    val recipientsAddress = severalAddressGenerator().sample.get.toSet
    val policyId          = Base58.encode("someRandomPolicyId".getBytes())

    // ===============  ok case  ===============
    val policyIdBytes = ByteStr.decodeBase58(policyId).get
    (state.policyRecipients _)
      .when(policyIdBytes)
      .returning(recipientsAddress)
      .once()

    (state.policyExists _)
      .when(policyIdBytes)
      .returning(true)
      .twice()

    Get(routePath(s"/$policyId/recipients")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[List[String]] should contain theSameElementsAs recipientsAddress.map(_.stringRepr)
    }

    // ===============  invalid case  ===============
    val invalidPolicyId = "lalala"
    Get(routePath(s"/$invalidPolicyId/recipients")) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
      (responseAs[JsObject] \ "message").as[String] shouldEqual s"Failed to decode policyId: '$invalidPolicyId'"
    }

    // ===============  empty result case  ===============
    (state.policyRecipients _)
      .when(policyIdBytes)
      .returning(Set.empty)
      .once()

    Get(routePath(s"/$policyId/recipients")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[List[String]] shouldBe empty
    }
  }

  routePath("/{policyId}/owners") in {
    val ownersAddress = severalAddressGenerator().sample.get.toSet
    val policyId      = Base58.encode("someRandomPolicyId".getBytes())

    // ===============  ok case  ===============
    val policyIdBytes = ByteStr.decodeBase58(policyId).get
    (state.policyOwners _)
      .when(policyIdBytes)
      .returning(ownersAddress)
      .once()

    (state.policyExists _)
      .when(policyIdBytes)
      .returning(true)
      .twice()

    Get(routePath(s"/$policyId/owners")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[List[String]] should contain theSameElementsAs ownersAddress.map(_.stringRepr)
    }

    // ===============  invalid case  ===============
    val invalidPolicyId = "lalala"
    Get(routePath(s"/$invalidPolicyId/owners")) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
      (responseAs[JsObject] \ "message").as[String] shouldEqual s"Failed to decode policyId: '$invalidPolicyId'"
    }

    // ===============  empty result case  ===============
    (state.policyOwners _)
      .when(policyIdBytes)
      .returning(Set.empty)
      .once()

    Get(routePath(s"/$policyId/owners")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[List[String]] shouldBe empty
    }
  }

  routePath("/{policy-id}/getData/{policy-item-hash}") - {
    val policyId      = Base58.encode("someRandomPolicyId".getBytes())
    val policyIdBytes = ByteStr.decodeBase58(policyId).get

    val data        = Array.fill(512)(1: Byte)
    val dataByteStr = ByteStr(data)
    val hash        = PolicyDataHash.fromDataBytes(data)

    "should pass with correct method call" in {
      (state.policyExists _)
        .when(policyIdBytes)
        .returning(true)
        .once()
      (state.privacyItemDescriptor _)
        .when(*, *)
        .returning(None)
        .once()
      (policyStorage.policyItemData _)
        .when(policyId, hash.stringRepr)
        .returning(Task.eval(Right(Some(dataByteStr))))
        .once()
      (policyStorage.policyItemType _)
        .when(policyId, hash.stringRepr)
        .returning(Task.eval(Right(Some(PrivacyDataType.Default))))
        .once()

      Get(routePath(s"/$policyId/getData/$hash")) ~> api_key(privacyApiKey) ~> route ~> check {
        status shouldBe StatusCodes.OK
        val response = responseAs[String]
        response shouldEqual Base64.encode(dataByteStr.arr)
      }
    }

    "should fail with incorrect method call" in {
      (state.policyExists _)
        .when(policyIdBytes)
        .returning(true)
        .once()
      (state.privacyItemDescriptor _)
        .when(*, *)
        .returning(None)
        .once()
      (policyStorage.policyItemData _)
        .when(policyId, hash.stringRepr)
        .returning(Task.eval(Right(Some(dataByteStr))))
        .once()
      (policyStorage.policyItemType _)
        .when(policyId, hash.stringRepr)
        .returning(Task.eval(Right(Some(PrivacyDataType.Large))))
        .once()

      Get(routePath(s"/$policyId/getData/$hash")) ~> api_key(privacyApiKey) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest

        val expectedErrorMsg = s"Policy '$policyId' item '${hash.stringRepr}' data size is too big, you should use " +
          s"'getPolicyItemDataLarge' for grpc and 'policyItemDataLarge' for rest methods instead"
        val errorMsg = (responseAs[JsObject] \ "message").as[String]

        errorMsg shouldEqual expectedErrorMsg
      }
    }
  }

  routePath("/{policy-id}/getLargeData/{policy-item-hash}") - {
    val policyId      = Base58.encode("someRandomPolicyId".getBytes())
    val policyIdBytes = ByteStr.decodeBase58(policyId).get

    val data        = Array.fill(512)(1: Byte)
    val dataByteStr = ByteStr(data)
    val hash        = PolicyDataHash.fromDataBytes(data)

    "should pass with correct method call" in {
      (state.activatedFeatures _)
        .when()
        .returning(Map[Short, Int](BlockchainFeature.PrivacyLargeObjectSupport.id -> 0))
        .anyNumberOfTimes()
      (state.height _)
        .when()
        .returning(1)
        .anyNumberOfTimes()
      (state.policyExists _)
        .when(policyIdBytes)
        .returning(true)
        .once()
      (state.privacyItemDescriptor _)
        .when(*, *)
        .returning(None)
        .once()
      (policyStorage.policyItemType _)
        .when(policyId, hash.stringRepr)
        .returning(Task.eval(Right(Some(PrivacyDataType.Large))))
        .once()
      (policyStorage
        .policyItemDataStream(_: String, _: String)(_: Scheduler))
        .when(policyId, hash.stringRepr, *)
        .returning(Task.eval(Right(Some(Observable.fromIteratorUnsafe(dataByteStr.arr.grouped(5))))))
        .once()

      Get(routePath(s"/$policyId/getLargeData/$hash")) ~> api_key(privacyApiKey) ~> route ~> check {
        status shouldBe StatusCodes.OK

        val responseBytes = chunks.flatMap(_.data()).toArray
        responseBytes shouldEqual dataByteStr.arr
      }
    }

    "should fail with incorrect method call" in {
      (state.policyExists _)
        .when(policyIdBytes)
        .returning(true)
        .once()
      (state.privacyItemDescriptor _)
        .when(*, *)
        .returning(None)
        .once()
      (policyStorage.policyItemType _)
        .when(policyId, hash.stringRepr)
        .returning(Task.eval(Right(Some(PrivacyDataType.Default))))
        .once()
      (policyStorage
        .policyItemDataStream(_: String, _: String)(_: Scheduler))
        .when(policyId, hash.stringRepr, *)
        .returning(Task.eval(Right(Some(Observable.fromIteratorUnsafe(dataByteStr.arr.grouped(5))))))
        .once()

      Get(routePath(s"/$policyId/getLargeData/$hash")) ~> api_key(privacyApiKey) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest

        val expectedErrorMsg = s"Policy '$policyId' item '${hash.stringRepr}' data size is small, you should use " +
          s"'getPolicyItemData' for grpc and 'policyItemData' for rest methods instead"
        val errorMsg = (responseAs[JsObject] \ "message").as[String]

        expectedErrorMsg shouldEqual errorMsg
      }
    }

  }

  routePath("/{policy-id}/getInfo/{policy-item-hash}") in {
    val policyId      = Base58.encode("someRandomPolicyId".getBytes())
    val policyIdBytes = ByteStr.decodeBase58(policyId).get

    val data = Array.fill(512)(1: Byte)
    val hash = PolicyDataHash.fromDataBytes(data)

    val policyMetaData =
      PolicyMetaData(policyId, hash.stringRepr, "some_sender", "some_file", 512, System.currentTimeMillis(), "some_author", "comment", None)
    val policyItemInfo = PolicyItemInfo(
      policyMetaData.sender,
      policyMetaData.policyId,
      PolicyItemFileInfo(policyMetaData.filename, policyMetaData.size, policyMetaData.timestamp, policyMetaData.author, policyMetaData.comment),
      policyMetaData.hash
    )

    (state.policyExists _)
      .when(policyIdBytes)
      .returning(true)
      .once()
    (policyStorage.policyItemMeta _)
      .when(policyId, hash.stringRepr)
      .returning(Task.eval(Right(Some(policyMetaData))))
      .once()

    Get(routePath(s"/$policyId/getInfo/$hash")) ~> api_key(privacyApiKey) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[PolicyItemInfo]
      response shouldEqual policyItemInfo
    }
  }

  routePath("/sendData auth check") in {
    val fee                 = TestFees.defaultFees.forTxType(PolicyDataHashTransactionV1.typeId)
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")
    val sendDataReq         = SendDataRequest(1, "", "", Some(""), "", privacyDataDataInfo, fee)

    val mockedApiService = stub[PrivacyApiService]
    val routeWithMockedService = new PrivacyApiRoute(
      mockedApiService,
      restAPISettings,
      serviceSettings,
      time,
      true,
      None,
      nodeOwner,
      NodeMode.Default,
      apiComputationsScheduler
    ).route

    withClue("sendData should fail if wrong api-key is provided") {
      forAll(Arbitrary.arbString.arbitrary) { invalidApiKey =>
        whenever(invalidApiKey != privacyApiKey) {
          Post(routePath("/sendData"), sendDataReq) ~> api_key(invalidApiKey) ~> routeWithMockedService ~> check {
            response.status shouldBe StatusCodes.Forbidden
            (responseAs[JsValue] \ "message").as[String] shouldBe "Provided privacy API key is not correct"
          }
        }
      }
    }
  }

  routePath("/sendData broadcast parameter check") in {
    val fee                 = TestFees.defaultFees.forTxType(PolicyDataHashTransactionV1.typeId)
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")

    val mockedApiService = stub[PrivacyApiService]
    val routeWithMockedService = new PrivacyApiRoute(
      mockedApiService,
      restAPISettings,
      serviceSettings,
      time,
      true,
      None,
      nodeOwner,
      NodeMode.Default,
      apiComputationsScheduler
    ).route

    val sendDataReqV1 = SendDataRequest(1, "", "", Some(""), "", privacyDataDataInfo, fee)
    val sendDataReqV2 = sendDataReqV1.copy(version = 2)
    val data          = "2,3,5\n7,11,13,17,23\n29,31,37\n"
    val multipartV2 = Multipart.FormData(
      Multipart.FormData.BodyPart.Strict("data", HttpEntity(ContentTypes.`text/plain(UTF-8)`, data), Map("filename" -> "primes.csv")),
      Multipart.FormData.BodyPart.Strict("request", HttpEntity(ContentTypes.`text/plain(UTF-8)`, Json.toJson(sendDataReqV2).toString))
    )

    val dummyTx = policyDataHashTransactionV1Gen().sample.get
    (mockedApiService.sendData _)
      .when(*, *)
      .returning(Future.successful(Right(dummyTx.tx)))
      .anyNumberOfTimes()

    def returnsOK = check {
      status shouldEqual StatusCodes.OK
    }

    withClue("should allow V1 request with missing broadcast param") {
      Post(routePath("/sendData"), sendDataReqV1) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> returnsOK
    }
    withClue("should allow V1 request with broadcast=true") {
      Post(routePath("/sendData?broadcast=true"), sendDataReqV1) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> returnsOK
    }

    withClue("should allow V2 request with missing broadcast param") {
      Post(routePath("/sendDataV2"), multipartV2) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> returnsOK
    }
    withClue("should allow V2 request with broadcast=true") {
      Post(routePath("/sendDataV2?broadcast=true"), multipartV2) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> returnsOK
    }
    withClue("shouldn't allow V1 and V2 requests with broadcast=false") {
      val expectedError = CustomValidationError(s"SendDataRequest V1/V2 with 'broadcast=false' is not allowed. Use V3 instead")
      Post(routePath("/sendData?broadcast=false"), sendDataReqV1) ~> api_key(privacyApiKey) ~> routeWithMockedService should produce(expectedError)
      Post(routePath("/sendDataV2?broadcast=false"), multipartV2) ~> api_key(privacyApiKey) ~> routeWithMockedService should produce(expectedError)
    }

    val sendDataReqV3 = sendDataReqV2.copy(version = 3)
    val multipartV3 = Multipart.FormData(
      Multipart.FormData.BodyPart.Strict("data", HttpEntity(ContentTypes.`text/plain(UTF-8)`, data), Map("filename" -> "primes.csv")),
      Multipart.FormData.BodyPart.Strict("request", HttpEntity(ContentTypes.`text/plain(UTF-8)`, Json.toJson(sendDataReqV3).toString))
    )
    withClue("should allow V3 request with any broadcast setting") {
      Post(routePath("/sendDataV2"), multipartV3) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> returnsOK
      Post(routePath("/sendDataV2?broadcast=false"), multipartV3) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> returnsOK
      Post(routePath("/sendDataV2?broadcast=true"), multipartV3) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> returnsOK
    }
  }

  routePath("/sendDataV2") in {
    val policyId            = "some_policy_id"
    val policyItemHash      = "some_policy_item_hash"
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")
    val fee                 = TestFees.defaultFees.forTxType(PolicyDataHashTransactionV1.typeId)
    val data                = "2,3,5\n7,11,13,17,23\n29,31,37\n"
    val sendDataReq =
      SendDataRequest(1, "some_sender", policyId, None, policyItemHash, privacyDataDataInfo, fee)
    val policyItem = PolicyItem(Right(ByteStr(data.getBytes("UTF-8")))).mergeWithRequest(sendDataReq)
    val multipart = Multipart.FormData(
      Multipart.FormData.BodyPart.Strict("data", HttpEntity(ContentTypes.`text/plain(UTF-8)`, data), Map("filename" -> "primes.csv")),
      Multipart.FormData.BodyPart.Strict("request", HttpEntity(ContentTypes.`text/plain(UTF-8)`, Json.toJson(sendDataReq).toString))
    )

    val mockedApiService = stub[PrivacyApiService]
    val routeWithMockedService = new PrivacyApiRoute(
      mockedApiService,
      restAPISettings,
      serviceSettings,
      time,
      true,
      None,
      nodeOwner,
      NodeMode.Default,
      apiComputationsScheduler
    ).route

    // ===============  OK result case  ===============
    {
      forAll(policyDataHashTransactionV1Gen()) {
        case PolicyDataWithTxV1(_, dataHashTx) =>
          (mockedApiService.sendData _)
            .when(policyItem, *)
            .returning(Future.successful(Right(dataHashTx)))
            .once()

          Post(routePath("/sendDataV2"), multipart) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
            val response      = responseAs[JsObject]
            val transactionId = response \ "id"
            transactionId.as[String] shouldBe dataHashTx.id.value.toString
          }
      }
    }
  }

  routePath("/sendLargeData") - {
    val policyId            = "some_policy_id"
    val policyItemHash      = "some_policy_item_hash"
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")
    val fee                 = TestFees.defaultFees.forTxType(PolicyDataHashTransactionV1.typeId)
    val chunkSize           = 100
    val dataBytes           = new Array[Byte](256)
    Random.nextBytes(dataBytes)
    val data        = Source(dataBytes.grouped(chunkSize).map(ByteString(_)).to[collection.immutable.Iterable])
    val sendDataReq = SendDataRequest(1, "some_sender", policyId, None, policyItemHash, privacyDataDataInfo, fee)
    val policyItem  = sendDataReq.toPolicyItem
    val multipart = Multipart.FormData(
      Source(
        List(
          Multipart.FormData.BodyPart.Strict("request", HttpEntity(ContentTypes.`text/plain(UTF-8)`, Json.toJson(sendDataReq).toString)),
          Multipart.FormData.BodyPart("data", HttpEntity.IndefiniteLength(MediaTypes.`application/octet-stream`, data))
        )
      )
    )

    val mockedApiService = stub[PrivacyApiService]
    val routeWithMockedService = new PrivacyApiRoute(
      mockedApiService,
      restAPISettings,
      serviceSettings,
      time,
      true,
      None,
      nodeOwner,
      NodeMode.Default,
      apiComputationsScheduler
    ).route

    "should pass with correct data" in {
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
            .when(policyItem, *, *, *)
            .onCall { (_, _, obs, _) =>
              obs.completedL
                .runToFuture(monix.execution.Scheduler.Implicits.global)
                .map[Either[ApiError, PolicyDataHashTransaction]](_ => Right(dataHashTx))
            }
            .once()

          Post(routePath("/sendLargeData"), multipart) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
            val response      = responseAs[JsObject]
            val transactionId = response \ "id"
            transactionId.as[String] shouldBe dataHashTx.id.value.toString
          }
      }
    }

    "should fail with wrong multipart order" in {
      val multipartWrong = Multipart.FormData(
        Source(
          List(
            Multipart.FormData.BodyPart("data", HttpEntity.IndefiniteLength(MediaTypes.`application/octet-stream`, data)),
            Multipart.FormData.BodyPart.Strict("request", HttpEntity(ContentTypes.`text/plain(UTF-8)`, Json.toJson(sendDataReq).toString))
          )
        )
      )

      Post(routePath("/sendLargeData"), multipartWrong) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
        val response     = responseAs[JsObject]
        val errorMessage = response \ "message"
        errorMessage.as[String] shouldBe "'request' part must be in the head of the Multipart, got 'data'"
      }
    }

    "should pass with redundant request fields" in {
      forAll(policyDataHashTransactionV1Gen()) {
        case PolicyDataWithTxV1(_, dataHashTx) =>
          (mockedApiService.validateLargePolicyItem _)
            .when(*)
            .onCall { (_: PolicyItem) =>
              Right(mock[ValidSendDataSetup])
            }
            .once()

          (mockedApiService.sendLargeData _)
            .when(policyItem, *, *, *)
            .onCall { (_, _, obs, _) =>
              obs.completedL
                .runToFuture(monix.execution.Scheduler.Implicits.global)
                .map[Either[ApiError, PolicyDataHashTransaction]](_ => Right(dataHashTx))
            }
            .once()

          Post(routePath("/sendLargeData"), multipart) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
            val response      = responseAs[JsObject]
            val transactionId = response \ "id"
            transactionId.as[String] shouldBe dataHashTx.id.value.toString
          }
      }
    }
  }

  routePath("/sendData") in {
    val policyId       = "some_policy_id"
    val policyItemHash = "some_policy_item_hash"
    val policyItemData =
      "TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0aGUgY29udGludWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdlLCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3VyZS4="
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")
    val fee                 = TestFees.defaultFees.forTxType(PolicyDataHashTransactionV1.typeId)
    val sendDataReq =
      SendDataRequest(1, "some_sender", policyId, Some(policyItemData), policyItemHash, privacyDataDataInfo, fee)
    val policyItem = sendDataReq.toPolicyItem

    val mockedApiService = stub[PrivacyApiService]
    val routeWithMockedService = new PrivacyApiRoute(
      mockedApiService,
      restAPISettings,
      serviceSettings,
      time,
      true,
      None,
      nodeOwner,
      NodeMode.Default,
      apiComputationsScheduler
    ).route

    // ===============  OK result case  ===============
    {
      forAll(policyDataHashTransactionV1Gen()) {
        case PolicyDataWithTxV1(_, dataHashTx) =>
          (mockedApiService.sendData _)
            .when(policyItem, *)
            .returning(Future.successful(Right(dataHashTx)))
            .once()

          Post(routePath("/sendData"), sendDataReq) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
            val response      = responseAs[JsObject]
            val transactionId = response \ "id"
            transactionId.as[String] shouldBe dataHashTx.id.value.toString
          }
      }
    }

    // ===============  ERROR result case  ===============
    {
      val errorMessage = "error happens"
      (mockedApiService.sendData _)
        .when(policyItem, *)
        .returning(Future.successful(Left(ApiError.fromValidationError(GenericError(errorMessage)))))
        .once()

      Post(routePath("/sendData"), sendDataReq) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
        (responseAs[JsValue] \ "message").as[String] shouldBe errorMessage
      }
    }

    // ===============  Future return failed ===============
    {
      val errorMessage = ApiErrorHandler.InternalServerErrorMessage
      (mockedApiService.sendData _)
        .when(policyItem, *)
        .returning(Future.failed(TestException(errorMessage)))
        .once()

      Post(routePath("/sendData"), sendDataReq) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
        responseAs[String] shouldBe errorMessage
      }
    }
  }

  routePath("/sendData size check") in {
    def entityOfSize(size: Int): String = "o" * size
    val policyId                        = "some_policy_id"
    val policyItemHash                  = "some_policy_item_hash"
    val privacyDataDataInfo             = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")
    val fee                             = TestFees.defaultFees.forTxType(PolicyDataHashTransactionV1.typeId)

    val mockedApiService = stub[PrivacyApiService]
    val routeWithMockedService = new PrivacyApiRoute(
      mockedApiService,
      restAPISettings,
      serviceSettings,
      time,
      true,
      None,
      nodeOwner,
      NodeMode.Default,
      apiComputationsScheduler
    ).route

    // ===============  OK size result case  ===============
    {
      val policyItemData20MB = entityOfSize(20 * 1024 * 1024)
      val sendDataReq =
        SendDataRequest(1, "some_sender", policyId, Some(policyItemData20MB), policyItemHash, privacyDataDataInfo, fee)
      val policyItem = sendDataReq.toPolicyItem

      val PolicyDataWithTxV1(_, dataHashTx) = policyDataHashTransactionV1Gen().sample.get
      (mockedApiService.sendData _)
        .when(policyItem, *)
        .returning(Future.successful(Right(dataHashTx)))
        .once()

      Post(routePath("/sendData"), sendDataReq) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    // ===============  Too big size result case  ===============
    {
      val bigPolicyItemData = entityOfSize(PrivacyApiRoute.sendDataEntityMaxSize.toInt)
      val sendDataReq =
        SendDataRequest(1, "some_sender", policyId, Some(bigPolicyItemData), policyItemHash, privacyDataDataInfo, fee)
      val policyItem = sendDataReq.toPolicyItem

      val PolicyDataWithTxV1(_, dataHashTx) = policyDataHashTransactionV1Gen().sample.get
      (mockedApiService.sendData _)
        .when(policyItem, *)
        .returning(Future.successful(Right(dataHashTx)))
        .once()

      Post(routePath("/sendData"), sendDataReq) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[ApiErrorResponse]
        response.error shouldBe 611
        response.message should include("HttpEntity too big, actual size")
      }
    }
  }

  routePath("/forceSync") in {
    val mockedApiService = stub[PrivacyApiService]
    val routeWithMockedService = new PrivacyApiRoute(
      mockedApiService,
      restAPISettings,
      serviceSettings,
      time,
      true,
      None,
      nodeOwner,
      NodeMode.Default,
      apiComputationsScheduler
    ).route

    val singeForcedSyncResponse = PrivacyForceSyncResponse(1)
    (mockedApiService.forceSync _).when().returning(Future.apply(Right(singeForcedSyncResponse)))

    Post(routePath("/forceSync")) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
      status shouldEqual StatusCodes.OK
      val response = responseAs[PrivacyForceSyncResponse]
      response shouldBe singeForcedSyncResponse
    }
  }

  routePath("/forceSync/{policyId}") in {
    val mockedApiService = stub[PrivacyApiService]
    val routeWithMockedService = new PrivacyApiRoute(
      mockedApiService,
      restAPISettings,
      serviceSettings,
      time,
      true,
      None,
      nodeOwner,
      NodeMode.Default,
      apiComputationsScheduler
    ).route

    val singeForcedSyncResponse = PrivacyForceSyncResponse(1)
    val randomPolicyId          = Base58.encode("somePolicy".getBytes("UTF-8"))
    (mockedApiService.forceSync(_: String, _: Address)).when(*, *).returning(Future.apply(Right(singeForcedSyncResponse)))

    Get(routePath(s"/forceSync/$randomPolicyId")) ~> api_key(privacyApiKey) ~> routeWithMockedService ~> check {
      status shouldEqual StatusCodes.OK
      val response = responseAs[PrivacyForceSyncResponse]
      response shouldBe singeForcedSyncResponse
    }
  }

  def initSetup(): Unit = {
    (state.activatedFeatures _)
      .when()
      .returning(Map[Short, Int](BlockchainFeature.PrivacyLargeObjectSupport.id -> 0))
      .anyNumberOfTimes()
  }
}
