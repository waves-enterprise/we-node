package com.wavesenterprise.wasm

import com.google.common.io.ByteArrayDataOutput
import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.account.Address
import com.wavesenterprise.crypto.util.Sha256Hash
import com.wavesenterprise.docker.ContractExecutionSuccessV2
import com.wavesenterprise.docker.StoredContract.WasmContract
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.diffs.docker.ExecutableTransactionGen
import com.wavesenterprise.state.{BinaryDataEntry, Blockchain, BooleanDataEntry, ByteStr, ContractId, DataEntry, IntegerDataEntry, StringDataEntry}
import com.wavesenterprise.transaction.AssetId
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps.toBytes
import com.wavesenterprise.transaction.docker.ContractTransactionGen
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation.{
  ContractBurnV1,
  ContractCancelLeaseV1,
  ContractIssueV1,
  ContractLeaseV1,
  ContractReissueV1,
  ContractTransferOutV1
}
import com.wavesenterprise.wasm.WASMServiceImpl.{BytecodeNotFound, WEVMExecutionException}
import com.wavesenterprise.wasm.core.WASMExecutor
import com.wavesenterprise.{TransactionGen, getWasmContract}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.OneInstancePerTest
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.nio.charset.StandardCharsets.UTF_8

class WASMServiceSpec
    extends AnyFreeSpec
    with Matchers
    with MockFactory
    with ScalaCheckPropertyChecks
    with OneInstancePerTest
    with TransactionGen
    with ContractTransactionGen
    with ExecutableTransactionGen {

  private val contractInfo = contractInfoGen(storedContractGen = wasmContractGen).sample.get

  private val wasmContract = getWasmContract(contractInfo)

  private val contractId = contractInfo.contractId

  private val contractIdBytes = bytes(contractId)

  private val paymentIdBytes = bytes(contractId) ++ Array.fill(8)(0.toByte)

  private val transaction = callContractV7ParamGen(maxPayments = Gen.choose(1, 3)).sample.get

  private val blockchain = stub[Blockchain]

  private val readingContext = ContractReadingContext.TransactionExecution(transaction.id.value)

  val service = new WASMServiceImpl(ContractId(contractInfo.contractId), transaction, blockchain)

  private def bytes(str: ByteStr) = str.arr

  private val exampleContractId = ByteStr.decodeBase58("4WVhw3QdiinpE5QXDG7QfqLiLanM7ewBw4ChX4qyGjs2").get
  private val bytecode          = getClass.getResourceAsStream("/example.wasm").readAllBytes()
  private val hash              = new String(Sha256Hash().update(bytecode).result(), UTF_8)

  private val wasmContractInfo = contractInfo.copy(storedContract = WasmContract(bytecode = bytecode, bytecodeHash = hash))

  "getBytecode" - {

    val dockerContractInfo = contractInfoGen(storedContractGen = dockerContractGen).sample.get

    val dockerContractId = dockerContractInfo.contractId

    val missingContractId = createContractV2ParamGen.map(_.contractId).sample.get

    (blockchain.contract _).when(*).onCall((id: ContractId) => {
      id.byteStr match {
        case `contractId`        => Some(contractInfo)
        case `exampleContractId` => Some(wasmContractInfo)
        case `dockerContractId`  => Some(dockerContractInfo)
        case _                   => None
      }
    }).anyNumberOfTimes()

    "pass on correct bytecode" in {
      service.getBytecode(bytes(contractId)) shouldBe wasmContract.bytecode
    }

    "throw on invalid bytecode" in {

      val bytecodeNotFound = intercept[WEVMExecutionException](
        service.getBytecode(bytes(dockerContractId))
      )
      assert(bytecodeNotFound.code == BytecodeNotFound)

      assertThrows[WEVMExecutionException](service.getBytecode(missingContractId.arr))

      val bytecodeNotFound2 = intercept[WEVMExecutionException](
        service.getBytecode(bytes(missingContractId))
      )
      assert(bytecodeNotFound2.code == BytecodeNotFound)
    }
  }

  "get/set storage" - {
    val contractId2 = bytes32gen.map(ByteStr.apply).sample.get

    val storageMap = Map[(ByteStr, String), DataEntry[_]](
      (contractId, "int")            -> IntegerDataEntry("int", 1337),
      (contractId, "string")         -> StringDataEntry("str", "string"),
      (contractId, "bool")           -> BooleanDataEntry("bool", value = true),
      (contractId, "binary")         -> BinaryDataEntry("binary", ByteStr("binary".getBytes(UTF_8))),
      (contractId2, "int")           -> IntegerDataEntry("int2", 13372),
      (contractId2, "string")        -> StringDataEntry("string", "string2"),
      (exampleContractId, "shard_0") -> IntegerDataEntry("shard_0", 0),
      (exampleContractId, "shard_1") -> IntegerDataEntry("shard_1", 0)
    )

    (blockchain.contractData(_: ByteStr, _: String, _: ContractReadingContext))
      .when(*, *, readingContext)
      .onCall((id: ByteStr, key: String, _: ContractReadingContext) => storageMap.get(id -> key))
      .anyNumberOfTimes()

    "get" in {
      storageMap.keys.foreach { case (id, key) =>
        val keyBytes = key.getBytes(UTF_8)
        service.getStorage(bytes(id), keyBytes) shouldBe toBytes(storageMap(id -> key))
      }
    }

    "current contract set" in {
      val intEntry = toBytes(IntegerDataEntry("int", 1338))
      service.setStorage(contractIdBytes, intEntry)
      val gotInt = service.getStorage(
        bytes(contractId),
        "int".getBytes(UTF_8)
      )
      gotInt shouldBe intEntry

      val byteEntry = toBytes(BinaryDataEntry("binary", ByteStr("binary2".getBytes(UTF_8))))
      service.setStorage(contractIdBytes, byteEntry)
      val gotBytes = service.getStorage(
        bytes(contractId),
        "binary".getBytes(UTF_8)
      )
      gotBytes shouldBe byteEntry
      val newEntry = toBytes(BooleanDataEntry("new", value = true))
      service.setStorage(contractIdBytes, newEntry)
      service.getStorage(bytes(contractId), "new".getBytes(UTF_8)) shouldBe newEntry
    }
  }

  val address: Address          = addressGen.sample.get
  val addressBytes: Array[Byte] = Array[Byte](0) ++ bytes(address.bytes)
  val asset: AssetId            = genAssetId.sample.get
  val assetBytes: Array[Byte]   = bytes(asset)

  "getBalance" - {

    (blockchain.addressBalance _)
      .when(address, Some(asset))
      .returning(1000L)

    service.getBalance(assetBytes, addressBytes) shouldBe 1000L

    (blockchain.addressBalance _)
      .when(address, None)
      .returning(2000L)

    service.getBalance(Array.emptyByteArray, addressBytes) shouldBe 2000L
  }

  "asset ops" - {

    "transfer" in {
      val amount = 100

      (blockchain.contractBalance _)
        .when(ContractId(contractId), Some(asset), ContractReadingContext.TransactionExecution(transaction.id()))
        .returning(1000L)

      service.transfer(contractIdBytes, assetBytes, addressBytes, amount)
      assert(service.getContractExecution.isInstanceOf[ContractExecutionSuccessV2])

      val executed = service.getContractExecution.asInstanceOf[ContractExecutionSuccessV2]
      executed.assetOperations.values.flatten.filter(
        _.isInstanceOf[ContractTransferOutV1]
      ).head shouldBe ContractTransferOutV1(
        assetId = Some(asset),
        recipient = address,
        amount = amount
      )
    }

    "issue, burn, reissue and lease" in {
      (blockchain.asset _).when(*).returning(None)

      val assetId = service.issue(
        contractIdBytes,
        name = "asset".getBytes(UTF_8),
        description = "asset_description".getBytes(UTF_8),
        quantity = 1000000,
        decimals = 5,
        isReissuable = true
      )
      service.burn(
        contractIdBytes,
        assetId = assetId,
        amount = 800
      )
      service.reissue(
        contractIdBytes,
        assetId = assetId,
        amount = 1,
        isReissuable = true
      )

      (blockchain.leaseDetails _).when(*).returning(None)

      val leaseId = service.lease(
        contractIdBytes,
        recipient = addressBytes,
        amount = 800
      )
      service.cancelLease(contractIdBytes, leaseId)
      val assetIdStr = ByteStr(assetId)
      val leaseIdStr = ByteStr(leaseId)

      service.getContractExecution.asInstanceOf[ContractExecutionSuccessV2].assetOperations.values.flatten should contain allOf (
        ContractIssueV1(
          assetId = assetIdStr,
          name = "asset",
          description = "asset_description",
          quantity = 1000000,
          decimals = 5,
          isReissuable = true,
          nonce = 1
        ),
        ContractBurnV1(Some(assetIdStr), amount = 800),
        ContractReissueV1(assetIdStr, quantity = 1, isReissuable = true),
        ContractLeaseV1(leaseIdStr, nonce = 1, recipient = address, amount = 800),
        ContractCancelLeaseV1(leaseIdStr)
      )
    }
  }

  "block ops" - {
    "height" in {
      service.getBlockHeight shouldBe 0
    }
  }

  "tx ops" - {
    "tx sender" in {
      service.getTxSender shouldBe transaction.sender.publicKey.getEncoded
    }

    "tx payments" in {
      service.getTxPayments(paymentIdBytes) shouldBe transaction.payments.size
    }

    "tx payment asset id" in {
      val actualId = transaction.payments.head.assetId.map(_.arr).getOrElse(Array.empty)
      service.getTxPaymentAssetId(paymentIdBytes, 0) shouldBe actualId
    }

    "tx payment amount" in {
      service.getTxPaymentAmount(paymentIdBytes, 0) shouldBe transaction.payments.head.amount
    }
  }

  "execute bytecode" - {
    "run" ignore {
      val executor = new WASMExecutor()
      val service  = new WASMServiceImpl(ContractId(contractInfo.contractId), transaction, blockchain)

      val bytecode = getClass.getResourceAsStream("/example.wasm").readAllBytes()

      val result = executor.runContract(
        "4WVhw3QdiinpE5QXDG7QfqLiLanM7ewBw4ChX4qyGjs2".getBytes(UTF_8),
        bytecode,
        "_constructor",
        Array.empty,
        service
      )
      assert(result == 0)

      service.getContractExecution.asInstanceOf[ContractExecutionSuccessV2].results.values.flatten should contain allOf (
        BinaryDataEntry("binary", ByteStr(Array(0, 1))),
        StringDataEntry("string", "test")
      )
    }
  }
  "execute counter bytecode" - {
    "run" in {

      val bytecode = getClass.getResourceAsStream("/increment.wasm").readAllBytes()
      val executor = new WASMExecutor
      val service  = new WASMServiceImpl(ContractId(contractInfo.contractId), transaction.copy(payments = List.empty), blockchain)

      val result = executor.runContract(
        exampleContractId.arr,
        bytecode,
        "_constructor",
        Array.empty,
        service
      )

      result shouldBe 0
      service.getContractExecution.asInstanceOf[ContractExecutionSuccessV2].results.values.flatten.size shouldBe 1
    }

  }

  def dataEntryWrite(value: DataEntry[_], output: ByteArrayDataOutput): Unit = {
    output.write(toBytes(value))
  }

  def getArgs(params: List[DataEntry[_]]): Array[Byte] = {
    val ndo = newDataOutput()
    BinarySerializer.writeShortIterable(params, dataEntryWrite, ndo)
    ndo.toByteArray
  }

  "bytecode with params" - {
    val bytecode = getClass.getResourceAsStream("/multicounter.wasm").readAllBytes()
    val executor = new WASMExecutor
    val service  = new WASMServiceImpl(ContractId(contractInfo.contractId), transaction, blockchain)
    "create" in { // 10 hardcoded shards
      val result = executor.runContract(
        exampleContractId.arr,
        bytecode,
        "_constructor",
        Array.empty,
        service
      )
      result shouldBe 0
      service.getContractExecution.asInstanceOf[ContractExecutionSuccessV2].results.values.flatten.size shouldBe 10
    }
    "call" in {
      val invokeService = new WASMServiceImpl(ContractId(exampleContractId), transaction, blockchain)
      val invokeResult = executor.runContract(
        exampleContractId.arr,
        bytecode,
        "increment_1",
        getArgs(List(StringDataEntry("shard", "1"))),
        invokeService
      )
      invokeResult shouldBe 0
      invokeService.getContractExecution.asInstanceOf[ContractExecutionSuccessV2].results.values.flatten.head shouldBe {
        IntegerDataEntry("shard_1", 1)
      }
    }
  }
}
