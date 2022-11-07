package com.wavesenterprise.api.http.service

import com.wavesenterprise.api.http.ApiError.{CustomValidationError, PolicyDataTooBig}
import com.wavesenterprise.api.http.{
  ApiError,
  PoliciesMetaInfoRequest,
  PoliciesMetaInfoResponse,
  PolicyIdWithDataHash,
  PrivacyDataInfo,
  SendDataRequest
}
import com.wavesenterprise.certs.CertChainStoreGen
import com.wavesenterprise.consensus.Consensus
import com.wavesenterprise.database.PrivacyLostItemUpdater
import com.wavesenterprise.network.EnabledTxBroadcaster
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.privacy.NoOpPolicyDataSynchronizer
import com.wavesenterprise.privacy.s3.PolicyS3StorageService
import com.wavesenterprise.privacy.{EmptyPolicyStorage, PolicyDataHash, PolicyMetaData}
import com.wavesenterprise.settings.SynchronizationSettings.TxBroadcasterSettings
import com.wavesenterprise.settings.TestFees
import com.wavesenterprise.state.{Blockchain, ByteStr, Diff}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.validation.EnabledFeeCalculator
import com.wavesenterprise.transaction.{PolicyDataHashTransactionV1, ValidationError}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.{Base58, Base64}
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.{AsyncTest, TestSchedulers, TestTime, TransactionGen}
import monix.eval.Task
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scorex.crypto.hash.Sha256
import tools.GenHelper.ExtendedGen

import scala.concurrent.duration._

class PrivacyApiServiceSpec
    extends AnyFunSpecLike
    with Matchers
    with MockFactory
    with TransactionGen
    with ScalaCheckPropertyChecks
    with CertChainStoreGen
    with AsyncTest {

  private val blockchainHeight = 42
  abstract class StateType extends Blockchain with PrivacyLostItemUpdater
  private val blockchain            = mock[Blockchain]
  private val consensus             = mock[Consensus]
  private val state                 = mock[StateType]
  private val wallet                = mock[Wallet]
  private val feeCalculator         = mock[EnabledFeeCalculator]
  private val utx                   = mock[UtxPool]
  private val activePeerConnections = mock[ActivePeerConnections]
  private val time                  = new TestTime()
  private val policyStorage         = new EmptyPolicyStorage(time)
  private val txBroadcasterSettings = TxBroadcasterSettings(10000, 20.seconds, 1, 3, 500, 1.second, 20.seconds, 1.second)
  private val owner                 = accountGen.generateSample()
  private val txBroadcaster =
    new EnabledTxBroadcaster(txBroadcasterSettings, blockchain, consensus, utx, activePeerConnections, 30)(
      TestSchedulers.transactionBroadcastScheduler)

  private val privacyApiService =
    new PrivacyApiService(state, wallet, owner, policyStorage, NoOpPolicyDataSynchronizer, feeCalculator, time, txBroadcaster)
  private val fee              = TestFees.defaultFees.forTxType(PolicyDataHashTransactionV1.typeId)
  private val recipientAddress = severalAddressGenerator().generateSample().toSet
  private val encodedCerts     = certsForChainGen().generateSample().map(cert => Base64.encode(cert.getEncoded))

  it("data is hashed by sha256") {
    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")

    forAll(byteArrayGen(14000)) { originDataBytes =>
      val sender     = accountGen.sample.get
      val dataString = Base64.encode(originDataBytes)

      val dataHash       = PolicyDataHash.fromDataBytes(originDataBytes)
      val dataHashString = dataHash.toString

      val request = SendDataRequest(
        1,
        sender.address,
        policyIdByteStr.toString,
        Some(dataString),
        dataHashString,
        privacyDataDataInfo,
        fee,
        certificates = encodedCerts
      ).toPolicyItem.explicitGet()

      (state.policyRecipients _).expects(policyIdByteStr).returning(recipientAddress).once()
      (activePeerConnections.withAddresses _).expects(*, *).returning(Iterable.empty).once()
      (state.policyDataHashExists _).expects(policyIdByteStr, dataHash).returning(false).once()
      (state.height _: () => Int).expects().returning(blockchainHeight).anyNumberOfTimes()
      (state.putItemDescriptor _).expects(*, *, *).returning(()).once()
      (state.activatedFeatures _).expects().returning(Map.empty).once()
      (wallet.privateKeyAccount _).expects(sender.toAddress, *).returning(Right(sender)).once()
      (feeCalculator.validateTxFee _).expects(blockchainHeight, *).returning(Right(())).once
      (utx.txDiffer _).expects(*, *).returning(Right(Diff.empty)).once
      (utx.forcePut _).expects(*, *, *).returning(true).once

      val sendDataResponse = privacyApiService.sendData(request).futureValue
      sendDataResponse shouldBe 'right
      val tx = sendDataResponse.right.get.asInstanceOf[PolicyDataHashTransactionV1]
      tx.policyId shouldBe policyIdByteStr
      tx.dataHash.toString shouldBe dataHashString
    }
  }

  it("there is no senders key in wallet") {
    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")

    val originDataBytes = byteArrayGen(666).sample.get
    val sender          = accountGen.sample.get
    val dataString      = Base64.encode(originDataBytes)

    val dataHash       = Sha256.hash(originDataBytes)
    val dataHashString = Base58.encode(dataHash)

    val request = SendDataRequest(
      1,
      sender.address,
      policyIdByteStr.toString,
      Some(dataString),
      dataHashString,
      privacyDataDataInfo,
      fee,
      certificates = encodedCerts
    ).toPolicyItem.explicitGet()

    (wallet.privateKeyAccount _).expects(sender.toAddress, *).returning(Left(GenericError("wrong password"))).once()

    val sendDataResponse = privacyApiService.sendData(request).futureValue
    sendDataResponse shouldBe 'left
    val validationError = sendDataResponse.left.get.asInstanceOf[CustomValidationError]
    validationError.message should include("Failed to get senders private key")
  }

  it("transaction signed by senders private key") {
    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")

    val originDataBytes = byteArrayGen(666).sample.get
    val sender          = accountGen.sample.get
    val dataString      = Base64.encode(originDataBytes)

    val dataHash       = PolicyDataHash.fromDataBytes(originDataBytes)
    val dataHashString = dataHash.toString

    val request = SendDataRequest(
      1,
      sender.address,
      policyIdByteStr.toString,
      Some(dataString),
      dataHashString,
      privacyDataDataInfo,
      fee,
      certificates = encodedCerts
    ).toPolicyItem.explicitGet()

    val currentTime      = time.correctedTime()
    val policyDataHashTx = PolicyDataHashTransactionV1.selfSigned(sender, dataHash, policyIdByteStr, time.getTimestamp(), fee).right.get
    time.setTime(currentTime)

    (state.policyRecipients _).expects(policyIdByteStr).returning(recipientAddress).once()
    (activePeerConnections.withAddresses _).expects(*, *).returning(Iterable.empty).once()
    (state.policyDataHashExists _).expects(policyIdByteStr, dataHash).returning(false).once()
    (wallet.privateKeyAccount _).expects(sender.toAddress, *).returning(Right(sender)).once()
    (state.height _: () => Int).expects().returning(blockchainHeight).anyNumberOfTimes()
    (state.putItemDescriptor _).expects(*, *, *).returning(()).once()
    (state.activatedFeatures _).expects().returning(Map.empty).once()
    (feeCalculator.validateTxFee _).expects(blockchainHeight, policyDataHashTx).returning(Right(())).once
    (utx.txDiffer _).expects(*, *).returning(Right(Diff.empty)).once
    (utx.forcePut _).expects(*, *, *).returning(true).once

    val sendDataResponse = privacyApiService.sendData(request).futureValue
    sendDataResponse shouldBe 'right
    val tx = sendDataResponse.right.get.asInstanceOf[PolicyDataHashTransactionV1]
    tx.policyId shouldBe policyIdByteStr
    tx.dataHash.toString shouldBe dataHashString
  }

  it("fail to sendData if fee checker return error") {
    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")

    val originDataBytes = byteArrayGen(666).sample.get
    val sender          = accountGen.sample.get
    val dataString      = Base64.encode(originDataBytes)

    val dataHash       = PolicyDataHash.fromDataBytes(originDataBytes)
    val dataHashString = dataHash.toString

    val request = SendDataRequest(
      1,
      sender.address,
      policyIdByteStr.toString,
      Some(dataString),
      dataHashString,
      privacyDataDataInfo,
      fee,
      certificates = encodedCerts
    ).toPolicyItem.explicitGet()

    val currentTime      = time.correctedTime()
    val policyDataHashTx = PolicyDataHashTransactionV1.selfSigned(sender, dataHash, policyIdByteStr, time.getTimestamp(), fee).right.get
    time.setTime(currentTime)

    (state.policyDataHashExists _).expects(policyIdByteStr, dataHash).returning(false).once()
    (state.height _: () => Int).expects().returning(blockchainHeight).anyNumberOfTimes()
    (wallet.privateKeyAccount _).expects(sender.toAddress, *).returning(Right(sender)).once()
    (feeCalculator.validateTxFee _).expects(blockchainHeight, policyDataHashTx).returning(Left(GenericError("invalid fee"))).once

    val sendDataResponse = privacyApiService.sendData(request).futureValue
    sendDataResponse shouldBe 'left
    val validationError = sendDataResponse.left.get.asInstanceOf[CustomValidationError]
    validationError.message should include("invalid fee")
  }

  it("validate policy data size") {
    val sender              = accountGen.sample.get
    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val privacyDataDataInfo = PrivacyDataInfo("big_data.rar", 999666999, 152, "Voldemar", "A am Mathematician, πё©!")

    val dataString = createBigString(PrivacyApiService.maxPolicyDataStringLength.toInt)

    val request = SendDataRequest(
      1,
      sender.address,
      policyIdByteStr.toString,
      Some(dataString),
      "do not care",
      privacyDataDataInfo,
      fee,
      certificates = encodedCerts
    ).toPolicyItem.explicitGet()

    val sendDataResponse = privacyApiService.sendData(request).futureValue
    sendDataResponse shouldBe 'left
    val validationError = sendDataResponse.left.get.asInstanceOf[PolicyDataTooBig]
    validationError.message should include("Policy data too big")
  }

  it("validate feeAssetId with version data size") {
    val sender              = accountGen.sample.get
    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")
    val dataString          = Base64.encode(byteArrayGen(10).sample.get)
    val feeAssetId          = Some("feeAssetId")

    val request = SendDataRequest(
      1,
      sender.address,
      policyIdByteStr.toString,
      Some(dataString),
      "do not care",
      privacyDataDataInfo,
      fee,
      feeAssetId,
      certificates = encodedCerts
    ).toPolicyItem.explicitGet()

    val sendDataResponse = privacyApiService.sendData(request).futureValue
    sendDataResponse shouldBe 'left
    val validationError = sendDataResponse.left.get.asInstanceOf[CustomValidationError]
    validationError.errorMessage shouldEqual "'feeAssetId' field is not allowed for the SendDataRequest version '1'"
  }

  it("validate policy data fileName length") {
    val sender              = accountGen.sample.get
    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val tooBigFileName      = "a" * PrivacyDataInfo.fileNameMaxLength + ".zip"
    val privacyDataDataInfo = PrivacyDataInfo(tooBigFileName, 1, 2, "foo", "bar")
    val request = SendDataRequest(
      1,
      sender.address,
      policyIdByteStr.toString,
      Some("data"),
      "do not care",
      privacyDataDataInfo,
      fee,
      certificates = encodedCerts
    ).toPolicyItem.explicitGet()

    val sendDataResponse = privacyApiService.sendData(request).futureValue
    sendDataResponse shouldBe 'left
    val validationError = sendDataResponse.left.get.asInstanceOf[CustomValidationError]
    validationError.errorMessage should be(s"File name is too long '${tooBigFileName.length}', maximum is '${PrivacyDataInfo.fileNameMaxLength}'")
  }

  it("validate policy data author name length") {
    val sender              = accountGen.sample.get
    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val tooBigAuthor        = "u" * PrivacyDataInfo.authorMaxLength + "u"
    val privacyDataDataInfo = PrivacyDataInfo("data.zip", 1, 2, tooBigAuthor, "foo")
    val request = SendDataRequest(
      1,
      sender.address,
      policyIdByteStr.toString,
      Some("data"),
      "do not care",
      privacyDataDataInfo,
      fee,
      certificates = encodedCerts
    ).toPolicyItem.explicitGet()

    val sendDataResponse = privacyApiService.sendData(request).futureValue
    sendDataResponse shouldBe 'left
    val validationError = sendDataResponse.left.get.asInstanceOf[CustomValidationError]
    validationError.errorMessage should be(s"Author name is too long '${tooBigAuthor.length}', maximum is '${PrivacyDataInfo.authorMaxLength}'")
  }

  it("validate policy data comment length") {
    val sender              = accountGen.sample.get
    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val tooBigComment       = "o" * PrivacyDataInfo.commentMaxLength + "!"
    val privacyDataDataInfo = PrivacyDataInfo("data.zip", 1, 2, "foo", tooBigComment)
    val request = SendDataRequest(
      1,
      sender.address,
      policyIdByteStr.toString,
      Some("data"),
      "do not care",
      privacyDataDataInfo,
      fee,
      certificates = encodedCerts
    ).toPolicyItem.explicitGet()

    val sendDataResponse = privacyApiService.sendData(request).futureValue
    sendDataResponse shouldBe 'left
    val validationError = sendDataResponse.left.get.asInstanceOf[CustomValidationError]
    validationError.errorMessage should be(s"Comment is too long '${tooBigComment.length}', maximum is '${PrivacyDataInfo.commentMaxLength}'")
  }

  it("get policies items info") {
    val policyIdByteStr_1      = ByteStr(genBoundedString(10, 100).sample.get).toString
    val policyIdWithDataHash_1 = PolicyIdWithDataHash(policyIdByteStr_1, Set("a", "b", "c"))

    val policyIdByteStr_2      = ByteStr(genBoundedString(10, 100).sample.get).toString
    val policyIdWithDataHash_2 = PolicyIdWithDataHash(policyIdByteStr_2, Set("1", "2", "3"))

    val request = PoliciesMetaInfoRequest(
      List(
        policyIdWithDataHash_1,
        policyIdWithDataHash_2
      ))

    val nonEmptyPolicyStorage = mock[PolicyS3StorageService]
    (nonEmptyPolicyStorage.policyItemsMetas _)
      .expects(*)
      .returning(
        Task.eval(
          Right(Seq(
            PolicyMetaData(policyIdByteStr_1, "a", "sender", "filename", 10, System.currentTimeMillis(), "author", "comment", None),
            PolicyMetaData(policyIdByteStr_1, "b", "sender", "filename", 10, System.currentTimeMillis(), "author", "comment", None),
            PolicyMetaData(policyIdByteStr_1, "c", "sender", "filename", 10, System.currentTimeMillis(), "author", "comment", None),
            PolicyMetaData(policyIdByteStr_2, "1", "sender", "filename", 10, System.currentTimeMillis(), "author", "comment", None),
            PolicyMetaData(policyIdByteStr_2, "2", "sender", "filename", 10, System.currentTimeMillis(), "author", "comment", None),
            PolicyMetaData(policyIdByteStr_2, "3", "sender", "filename", 10, System.currentTimeMillis(), "author", "comment", None)
          ))
        ))
      .once()

    val owner = accountGen.generateSample()
    val privacyApiServiceWithDB =
      new PrivacyApiService(state, wallet, owner, nonEmptyPolicyStorage, NoOpPolicyDataSynchronizer, feeCalculator, time, txBroadcaster)
    val response = privacyApiServiceWithDB.policyItemsInfo(request).futureValue

    response shouldBe 'right
    val policiesMetaInfoResp: PoliciesMetaInfoResponse = response.right.get
    val cuttedInfo = policiesMetaInfoResp.policiesDataInfo.map { polDataInfo =>
      polDataInfo.policyId -> polDataInfo.datasInfo.map(_.hash)
    }.toMap

    cuttedInfo(policyIdByteStr_1) should contain theSameElementsAs Seq("a", "b", "c")
    cuttedInfo(policyIdByteStr_2) should contain theSameElementsAs Seq("1", "2", "3")
    cuttedInfo.size shouldBe 2
  }

  it("validate certificate bytes") {
    val invalidCerts = certsForChainGen().generateSample().map { cert =>
      val encoded = cert.getEncoded
      encoded(2) = (encoded(2) + 1).toByte
      Base64.encode(encoded)
    }

    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")
    val originDataBytes     = byteArrayGen(14000).generateSample()
    val sender              = accountGen.sample.get
    val dataString          = Base64.encode(originDataBytes)
    val dataHash            = PolicyDataHash.fromDataBytes(originDataBytes)
    val dataHashString      = dataHash.toString

    val request = SendDataRequest(
      1,
      sender.address,
      policyIdByteStr.toString,
      Some(dataString),
      dataHashString,
      privacyDataDataInfo,
      fee,
      certificates = invalidCerts
    ).toPolicyItem.explicitGet()

    val sendDataResponse = privacyApiService.sendData(request).futureValue
    sendDataResponse shouldBe 'left
    val validationError = sendDataResponse.left.get.asInstanceOf[ApiError.CertificateParseError]
    validationError.reason should include("Expected a valid X.509 certificate in DER encoding")
  }

  it("validate base64 certificate") {
    val invalidCerts = certsForChainGen().generateSample().map { cert =>
      Base64.encode(cert.getEncoded) + "A"
    }

    val policyIdByteStr     = ByteStr(genBoundedString(10, 100).sample.get)
    val privacyDataDataInfo = PrivacyDataInfo("britney.jpg", 69, 1998, "Voldemar", "mmm, not bad!")
    val originDataBytes     = byteArrayGen(14000).generateSample()
    val sender              = accountGen.sample.get
    val dataString          = Base64.encode(originDataBytes)
    val dataHash            = PolicyDataHash.fromDataBytes(originDataBytes)
    val dataHashString      = dataHash.toString

    val request = SendDataRequest(
      1,
      sender.address,
      policyIdByteStr.toString,
      Some(dataString),
      dataHashString,
      privacyDataDataInfo,
      fee,
      certificates = invalidCerts
    ).toPolicyItem

    request shouldBe 'left
    val validationError = request.left.get.asInstanceOf[ValidationError.CertificateParseError]
    validationError.reason should include("Encountered an invalid Base64")
  }

  private def createBigString(size: Int): String = {
    val strBld = new StringBuilder(size)
    (0 to size).foreach(_ => strBld.append('a'))
    strBld.mkString
  }

}
