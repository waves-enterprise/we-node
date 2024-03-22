package com.wavesenterprise.api.grpc.service

import akka.grpc.internal.GrpcMetadataImpl
import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import com.wavesenterprise.api.http.service.confidentialcontract.{ConfidentialContractsApiService, ConfidentialTxByExecutableTxIdResponse}
import com.wavesenterprise.crypto.internals.SaltBytes
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.database.rocksdb.confidential.PersistentConfidentialState
import com.wavesenterprise.protobuf.service.contract.ExecutedTxRequest
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.state.contracts.confidential.{ConfidentialInput, ConfidentialOutput}
import com.wavesenterprise.state.{ByteStr, ContractId, IntegerDataEntry}
import com.wavesenterprise.transaction.docker.{ContractTransactionGen, ExecutedContractTransactionV4}
import com.wavesenterprise.transaction.protobuf.{ContractKeysRequest, DataEntry}
import com.wavesenterprise.utils.Base58
import com.wavesenterprise.{TestTime, crypto}
import org.apache.commons.codec.digest.DigestUtils
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object PersistentConfidentialStateMock extends PersistentConfidentialState(null, 0)

class ConfidentialContractsApiServiceMock
    extends ConfidentialContractsApiService(null, null, null, null, PersistentConfidentialStateMock)

class ConfidentialContractServiceImplSpec extends AnyFreeSpec
    with Matchers
    with MockFactory
    with ContractTransactionGen
    with ScalaCheckPropertyChecks {

  private val time                            = new TestTime
  private val ownerAddress                    = accountGen.sample.get.toAddress
  private val apiKey                          = "foo"
  private lazy val apiKeyHash                 = Base58.encode(crypto.secureHash(apiKey))
  private val authSetting                     = AuthorizationSettings.ApiKey(apiKeyHash, apiKeyHash, apiKeyHash)
  private val emptyMetadata                   = new GrpcMetadataImpl(new io.grpc.Metadata())
  private val confidentialContractsApiService = mock[ConfidentialContractsApiServiceMock]

  "#confidentialCall" - {
    "should return confidential contract info" in {
      forAll(executedContractV4ParamGen, confidentialInputGen, confidentialOutputGen, bytes32gen.map(DigestUtils.sha256Hex)) {
        case (tx: ExecutedContractTransactionV4, input: ConfidentialInput, output: ConfidentialOutput, txId: String) =>
          val res = ConfidentialTxByExecutableTxIdResponse(tx, input, output)

          (confidentialContractsApiService.confidentialTxByExecutableTxId _).expects(txId).returns(Right(res))

          val confidentialService = new ConfidentialContractServiceImpl(
            authSetting,
            ownerAddress,
            time,
            confidentialContractsApiService)(apiComputationsScheduler)

          val confidentialTx =
            confidentialService.confidentialExecutedTxByExecutableTxId(ExecutedTxRequest(txId), emptyMetadata)

          val resultTx = Await.result(confidentialTx, 10.seconds)

          resultTx.confidentialInput.get.txId shouldBe input.txId.toString
          resultTx.confidentialOutput.get.txId shouldBe output.txId.toString
          resultTx.transaction.get.tx.get.version shouldBe res.executedContractTransactionV4.tx.version
      }
    }

    "should return confidential contract state" in {
      forAll(executedContractV4ParamGen) {
        tx: ExecutedContractTransactionV4 =>
          val res  = Vector(IntegerDataEntry("sum", 1))
          val res_ = Vector(DataEntry("sum", DataEntry.Value.IntValue(1)))
          (confidentialContractsApiService.contractKeys(_: String, _: Option[Int], _: Option[Int], _: Option[String])).expects(
            tx.tx.contractId.base58,
            *,
            *,
            *).returns(Right(res))

          val confidentialService = new ConfidentialContractServiceImpl(
            authSetting,
            ownerAddress,
            time,
            confidentialContractsApiService)(apiComputationsScheduler)

          val confidentialTx =
            confidentialService.getContractKeys(ContractKeysRequest(tx.tx.contractId.base58), emptyMetadata)

          val resultTx = Await.result(confidentialTx, 10.seconds)

          resultTx.entries shouldBe res_
      }
    }
  }

  val confidentialInputGen: Gen[ConfidentialInput] = for {
    commitmentHash <- bytes32gen.map(ByteStr.apply)
    commitment = Commitment(commitmentHash)
    txId    <- bytes32gen.map(ByteStr.apply)
    byteStr <- bytes32gen.map(ByteStr.apply)
    contractId = ContractId(byteStr)
    bytes <- bytes32gen.map(ByteStr.apply)
    commitmentKey = SaltBytes(bytes)
    entries <- Gen.listOf(longEntryGen(dataAsciiRussianGen))

  } yield ConfidentialInput(commitment, txId, contractId, commitmentKey, entries)

  val confidentialOutputGen: Gen[ConfidentialOutput] = for {
    commitmentHash <- bytes32gen.map(ByteStr.apply)
    commitment = Commitment(commitmentHash)
    txId    <- bytes32gen.map(ByteStr.apply)
    byteStr <- bytes32gen.map(ByteStr.apply)
    contractId = ContractId(byteStr)
    bytes <- bytes32gen.map(ByteStr.apply)
    commitmentKey = SaltBytes(bytes)
    entries <- Gen.listOf(longEntryGen(dataAsciiRussianGen))

  } yield ConfidentialOutput(commitment, txId, contractId, commitmentKey, entries)

}
