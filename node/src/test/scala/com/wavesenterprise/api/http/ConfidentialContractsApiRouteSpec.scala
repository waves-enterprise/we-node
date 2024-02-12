package com.wavesenterprise.api.http

import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.docker.confidential.ConfidentialContractsApiRoute
import com.wavesenterprise.api.http.service.ContractsApiService
import com.wavesenterprise.api.http.service.confidentialcontract.ConfidentialContractsApiService
import com.wavesenterprise.database.docker.{KeysPagination, KeysRequest}
import com.wavesenterprise.database.rocksdb.confidential.{ConfidentialRocksDBStorage, PersistentConfidentialState}
import com.wavesenterprise.docker.StoredContract.DockerContract
import com.wavesenterprise.docker._
import com.wavesenterprise.http.{ApiSettingsHelper, RouteSpec, api_key}
import com.wavesenterprise.network.DisabledTxBroadcaster
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.dockerengine.ContractExecutionMessagesCacheSettings
import com.wavesenterprise.settings.PositiveInt
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.{AssetDescription, BinaryDataEntry, Blockchain, BooleanDataEntry, ByteStr, ContractId, Diff, IntegerDataEntry}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.utils.Base64
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.{TestHelpers, TestTime}
import monix.eval.Coeval
import org.apache.commons.codec.digest.DigestUtils
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.Eventually

import java.nio.file.Files
import scala.concurrent.duration._

class ConfidentialContractsApiRouteSpec extends RouteSpec("/confidential-contracts")
    with PathMockFactory
    with ApiSettingsHelper
    with ContractTransactionGen
    with Eventually {

  private val ownerAddress: Address       = accountGen.sample.get.toAddress
  private val blockchain                  = stub[Blockchain]
  private val persistentConfidentialState = stub[PersistentConfidentialState]
  private val wallet                      = stub[Wallet]
  private val utx                         = stub[UtxPool]
  private val activePeerConnections       = stub[ActivePeerConnections]

  private val sender = Wallet.generateNewAccount()

  private val messagesCache =
    new ContractExecutionMessagesCache(
      ContractExecutionMessagesCacheSettings(1.hour, 100000, 10, 1.second, 5.minutes, PositiveInt(3)),
      dockerMiningEnabled = false,
      activePeerConnections,
      utx,
      monix.execution.Scheduler.global
    )

  private val contractsApiService = new ContractsApiService(blockchain, messagesCache)

  private val cscDbPath = Files.createTempDirectory("csc").toAbsolutePath

  private val cscDb = ConfidentialRocksDBStorage.openDB(cscDbPath.toString)

  private val confidentialContractsApiService = new ConfidentialContractsApiService(
    blockchain = blockchain,
    peers = activePeerConnections,
    nodeOwner = sender,
    txBroadcaster = DisabledTxBroadcaster(utx, activePeerConnections),
    persistentConfidentialState = persistentConfidentialState
  )

  private val route = new ConfidentialContractsApiRoute(
    contractsApiService,
    confidentialContractsApiService,
    restAPISettings,
    new TestTime,
    ownerAddress,
    apiComputationsScheduler
  ).route

  private val image     = "localhost:5000/smart-kv"
  private val imageHash = DigestUtils.sha256Hex("some_data")

  private val contractId   = ContractId(ByteStr.decodeBase58("9ekQuYn92natMnMq8KqeGK3Nn7cpKd3BvPEGgD6fFyyz").get)
  private val assetId      = ByteStr.decodeBase58("9ekQuYn92natMnMq8KqeGK3Nn7cpKd3BvPEGgD6fFyyz").get
  private val assetBalance = 10000L
  private val assetDescription =
    AssetDescription(null, 0, 0, "testAsset", "", 4, reissuable = false, BigInt(10000), None, sponsorshipIsEnabled = false)
  private val westBalance = 100000000L

  private val data = List(
    IntegerDataEntry("int", 24),
    BooleanDataEntry("bool", value = true),
    BinaryDataEntry("blob", ByteStr(Base64.decode("YWxpY2U=").get))
  )
  private val dataMap = data.map(e => e.key -> e).toMap
  private val contract =
    ContractInfo(Coeval.pure(sender), contractId.byteStr, DockerContract(image, imageHash, ContractApiVersion.Current), 1, active = true)
  private val contracts = Set(contract)

  (wallet.privateKeyAccount _)
    .when(sender.toAddress, None)
    .onCall((_: Address, _: Option[Array[Char]]) => Right(sender))
    .anyNumberOfTimes()
  (utx.putIfNew _).when(*, *).returns(Right((true, Diff.empty))).anyNumberOfTimes()

  (blockchain
    .contract(_: ContractId))
    .when(*)
    .onCall((s: ContractId) => contracts.find(contract => ContractId(contract.contractId) == s))
    .anyNumberOfTimes()
  (blockchain
    .contractKeys(_: KeysRequest, _: ContractReadingContext))
    .when(*, *)
    .onCall((request: KeysRequest, _) =>
      new KeysPagination(data.map(_.key).iterator).paginatedKeys(request.offsetOpt, request.limitOpt, request.keysFilter).toVector)
    .anyNumberOfTimes()
  (blockchain
    .contractData(_: ByteStr, _: ContractReadingContext))
    .when(contractId.byteStr, *)
    .onCall((_: ByteStr, _) => ExecutedContractData(dataMap))
    .anyNumberOfTimes()
  (blockchain.contracts _)
    .when()
    .onCall(_ => contracts)
    .anyNumberOfTimes()
  (blockchain
    .contractData(_: ByteStr, _: Iterable[String], _: ContractReadingContext))
    .when(contractId.byteStr, *, *)
    .onCall((_: ByteStr, _keys: Iterable[String], _) => ExecutedContractData(dataMap).filterKeys(_keys.toSet))
    .anyNumberOfTimes()
  (blockchain
    .contractData(_: ByteStr, _: String, _: ContractReadingContext))
    .when(contractId.byteStr, *, *)
    .onCall((_: ByteStr, _key: String, _) => dataMap.get(_key))
    .anyNumberOfTimes()

  (blockchain
    .contractBalance(_: ContractId, _: Option[ByteStr], _: ContractReadingContext))
    .when(contractId, None, *)
    .returning(westBalance)
    .anyNumberOfTimes()

  (blockchain
    .contractBalance(_: ContractId, _: Option[ByteStr], _: ContractReadingContext))
    .when(contractId, Some(assetId), *)
    .returning(assetBalance)
    .anyNumberOfTimes()

  (blockchain
    .assetDescription(_: ByteStr))
    .when(assetId)
    .returning(Some(assetDescription))
    .anyNumberOfTimes()

  (blockchain
    .assetDescription(_: ByteStr))
    .when(*)
    .returning(None)
    .anyNumberOfTimes()

  (persistentConfidentialState
    .contractKeys(_: KeysRequest))
    .when(*)
    .onCall((request: KeysRequest) =>
      new KeysPagination(data.map(_.key).iterator).paginatedKeys(request.offsetOpt, request.limitOpt, request.keysFilter).toVector)
    .anyNumberOfTimes()

  (persistentConfidentialState
    .contractKeys(_: KeysRequest))
    .when(*)
    .onCall((request: KeysRequest) =>
      new KeysPagination(data.map(_.key).iterator).paginatedKeys(request.offsetOpt, request.limitOpt, request.keysFilter).toVector)
    .anyNumberOfTimes()

  (persistentConfidentialState
    .contractData(_: ContractId, _: Iterable[String]))
    .when(contractId, *)
    .onCall((_: ContractId, _keys: Iterable[String]) => ExecutedContractData(dataMap).filterKeys(_keys.toSet))
    .anyNumberOfTimes()

  // TODO: confidential-contracts/{transactionId} - create ExecutedTx and get input and output
  routePath("/{transactionId}") in {
    Get(routePath(s"/$contractId")) ~> api_key(confidentialApiKey) ~> route ~> check {
      true shouldBe true
//      status shouldBe StatusCodes.OK
//      val response = responseAs[ConfidentialInputAndOutput]
    }
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
      cscDb.db.close()
    } finally {
      TestHelpers.deleteRecursively(cscDbPath)
    }
  }

}
