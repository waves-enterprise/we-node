package com.wavesenterprise.api.http

import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import akka.http.scaladsl.model.StatusCodes
import com.wavesenterprise.TestTime
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.docker._
import com.wavesenterprise.api.http.service.ContractsApiService
import com.wavesenterprise.database.docker.{KeysPagination, KeysRequest}
import com.wavesenterprise.docker._
import com.wavesenterprise.http.{ApiSettingsHelper, RouteSpec}
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.PositiveInt
import com.wavesenterprise.settings.dockerengine.ContractExecutionMessagesCacheSettings
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.{BinaryDataEntry, Blockchain, BooleanDataEntry, ByteStr, DataEntry, Diff, IntegerDataEntry}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.utils.{Base58, Base64}
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wallet.Wallet
import monix.eval.Coeval
import org.apache.commons.codec.digest.DigestUtils
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.Eventually
import play.api.libs.json._

import scala.concurrent.duration._

class ContractsApiRouteSpec extends RouteSpec("/contracts") with PathMockFactory with ApiSettingsHelper with ContractTransactionGen with Eventually {

  private val ownerAddress: Address = accountGen.sample.get.toAddress
  private val blockchain            = stub[Blockchain]
  private val wallet                = stub[Wallet]
  private val utx                   = stub[UtxPool]
  private val activePeerConnections = stub[ActivePeerConnections]

  private val sender = Wallet.generateNewAccount()

  private val messagesCache =
    new ContractExecutionMessagesCache(
      ContractExecutionMessagesCacheSettings(1.hour, 100000, 10, 1.second, 5.minutes, PositiveInt(3)),
      dockerMiningEnabled = false,
      activePeerConnections,
      utx,
      monix.execution.Scheduler.global
    )
  private val service = new ContractsApiService(blockchain, messagesCache)
  private val route = new ContractsApiRoute(
    service,
    restAPISettings,
    new TestTime,
    ownerAddress,
    apiComputationsScheduler
  ).route

  private val image     = "localhost:5000/smart-kv"
  private val imageHash = DigestUtils.sha256Hex("some_data")

  private val contractId = ByteStr.decodeBase58("9ekQuYn92natMnMq8KqeGK3Nn7cpKd3BvPEGgD6fFyyz").get
  private val data = List(
    IntegerDataEntry("int", 24),
    BooleanDataEntry("bool", value = true),
    BinaryDataEntry("blob", ByteStr(Base64.decode("YWxpY2U=").get))
  )
  private val dataMap   = data.map(e => e.key -> e).toMap
  private val contract  = ContractInfo(Coeval.pure(sender), contractId, image, imageHash, 1, active = true)
  private val contracts = Set(contract)

  private val unknownContractId = ByteStr(Base58.encode("unknown".getBytes).getBytes)

  (wallet.privateKeyAccount _)
    .when(sender.toAddress, None)
    .onCall((_: Address, _: Option[Array[Char]]) => Right(sender))
    .anyNumberOfTimes()
  (utx.putIfNew _).when(*, *).returns(Right((true, Diff.empty))).anyNumberOfTimes()

  (blockchain
    .contract(_: ByteStr))
    .when(*)
    .onCall((s: ByteStr) => contracts.find(_.contractId == s))
    .anyNumberOfTimes()
  (blockchain
    .contractKeys(_: KeysRequest, _: ContractReadingContext))
    .when(*, *)
    .onCall((request: KeysRequest, _) =>
      new KeysPagination(data.map(_.key).iterator).paginatedKeys(request.offsetOpt, request.limitOpt, request.keysFilter).toVector)
    .anyNumberOfTimes()
  (blockchain
    .contractData(_: ByteStr, _: ContractReadingContext))
    .when(contractId, *)
    .onCall((_: ByteStr, _) => ExecutedContractData(dataMap))
    .anyNumberOfTimes()
  (blockchain.contracts _)
    .when()
    .onCall(_ => contracts)
    .anyNumberOfTimes()
  (blockchain
    .contractData(_: ByteStr, _: Iterable[String], _: ContractReadingContext))
    .when(contractId, *, *)
    .onCall((_: ByteStr, _keys: Iterable[String], _) => ExecutedContractData(dataMap).filterKeys(_keys.toSet))
    .anyNumberOfTimes()
  (blockchain
    .contractData(_: ByteStr, _: String, _: ContractReadingContext))
    .when(contractId, *, *)
    .onCall((_: ByteStr, _key: String, _) => dataMap.get(_key))
    .anyNumberOfTimes()

  routePath("/") in {
    Get(routePath("")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[Set[ContractInfo]] shouldBe contracts
    }

    val body = Json.obj("contracts" -> Json.arr(contractId.toString))
    Post(routePath(""), body) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response   = responseAs[JsObject]
      val actualData = (response \ contractId.toString).as[List[DataEntry[_]]]
      actualData should contain theSameElementsAs data
    }

    val requestedContracts = Range.inclusive(1, ContractsApiRoute.MaxContractsPerRequest + 1).map(b => ByteStr(Array(b.toByte)))
    val tooBigBody         = Json.obj("contracts" -> requestedContracts)
    Post(routePath(""), tooBigBody) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe "Too big sequences requested"
    }

    val notFoundContractBody = Json.obj("contracts" -> Json.arr(unknownContractId.toString))
    Post(routePath(""), notFoundContractBody) ~> route ~> check {
      status shouldBe StatusCodes.NotFound
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe s"Contract '$unknownContractId' is not found"
    }
  }

  routePath("/{contractId}") in {
    Get(routePath(s"/$contractId")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[List[DataEntry[_]]] shouldBe data
    }

    Get(routePath(s"/$unknownContractId")) ~> route ~> check {
      status shouldBe StatusCodes.NotFound
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe s"Contract '$unknownContractId' is not found"
    }

    val offset = 1
    Get(routePath(s"/$contractId?offset=$offset")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[List[DataEntry[_]]] shouldBe data.drop(offset)
    }

    val negativeOffset = -1
    Get(routePath(s"/$contractId?offset=$negativeOffset")) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
      val error = responseAs[String]
      error should include("Invalid parameters: ['-1' must be positive]")
    }
    val largeOffset = "10000000000000"
    Get(routePath(s"/$contractId?offset=$largeOffset")) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
      val error = responseAs[String]
      error should include("Invalid parameters: [Unable to parse Integer from '10000000000000']")
    }

    val limit = 2
    Get(routePath(s"/$contractId?limit=$limit")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[List[DataEntry[_]]] shouldBe data.take(limit)
    }

    val negativeLimit = -2
    Get(routePath(s"/$contractId?offset=$negativeLimit")) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
      val error = responseAs[String]
      error should include("Invalid parameters: ['-2' must be positive]")
    }

    Get(routePath(s"/$contractId?limit=$limit&offset=$offset")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[List[DataEntry[_]]] shouldBe data.slice(offset, offset + limit)
    }

    Get(routePath(s"/$contractId?limit=$negativeLimit&offset=$negativeOffset")) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
      val error = responseAs[String]
      error should include("Invalid parameters")
    }

    offset + limit should be <= data.length

    val keysFilter = "^b.*"
    val pattern    = keysFilter.r.pattern
    Get(routePath(s"/$contractId?limit=$limit&matches=$keysFilter")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[List[DataEntry[_]]] shouldBe data.filter(e => pattern.matcher(e.key).matches())
    }

    val invalidKeysFilter = "[0-9"
    Get(routePath(s"/$contractId?limit=$limit&matches=$invalidKeysFilter")) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe s"Keys filter parameter has invalid regex pattern '$invalidKeysFilter'"
    }

    Post(routePath(s"/$contractId"), Json.obj("keys" -> dataMap.keySet)) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response.as[List[DataEntry[_]]] should contain theSameElementsAs data
    }
  }

  routePath("/{contractId}/{key}") in {
    val key = "blob"

    Get(routePath(s"/$contractId/$key")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsObject]
      response.as[DataEntry[_]] shouldBe dataMap(key)
    }

    val unknownKey = "unknown"
    Get(routePath(s"/$contractId/$unknownKey")) ~> route ~> check {
      status shouldBe StatusCodes.NotFound
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe "no data for this key"
    }
  }

  routePath("/executed-tx-for/{id}") in {
    val signedTx =
      CreateContractTransactionV2
        .selfSigned(sender, "localhost:5000/smart-kv", imageHash, "contract", data, 0, System.currentTimeMillis(), None)
        .right
        .get

    val signedExecutedTx = ExecutedContractTransactionV1
      .selfSigned(sender, signedTx, data, System.currentTimeMillis())
      .right
      .get

    (blockchain
      .executedTxFor(_: ByteStr))
      .when(signedTx.id())
      .onCall((_: ByteStr) => Some(signedExecutedTx))
      .anyNumberOfTimes()

    Get(routePath(s"/executed-tx-for/${signedTx.id()}")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsObject]
      (response \ "id").as[String] shouldBe signedExecutedTx.id().toString
    }
  }

  routePath("/status/{id}") in {
    val txId = ByteStr("some tx id".getBytes())

    val message =
      ContractExecutionMessage(sender, txId, ContractExecutionStatus.Error, Some(3), "No params found", System.currentTimeMillis())
    messagesCache.put(txId, message)

    Get(routePath(s"/status/$txId")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsArray]
      response shouldBe JsArray(Seq(message.json()))
    }

    val notFoundTxId = ByteStr("some not found tx id".getBytes())

    Get(routePath(s"/status/$notFoundTxId")) ~> route ~> check {
      status shouldBe StatusCodes.NotFound
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe s"Contract execution result is not found for transaction with txId = '$notFoundTxId'"
    }
  }

  routePath("/info/{contractId}") in {
    Get(routePath(s"/info/$contractId")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsObject]
      (response \ "contractId").as[String] shouldBe contract.contractId.toString
      (response \ "version").as[Int] shouldBe contract.version
      (response \ "image").as[String] shouldBe contract.image
      (response \ "imageHash").as[String] shouldBe contract.imageHash
      (response \ "active").as[Boolean] shouldBe contract.active
    }

    Get(routePath(s"/info/$unknownContractId")) ~> route ~> check {
      status shouldBe StatusCodes.NotFound
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe s"Contract '$unknownContractId' is not found"
    }
  }
}
