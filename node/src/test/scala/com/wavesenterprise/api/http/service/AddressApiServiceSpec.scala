package com.wavesenterprise.api.http.service

import com.wavesenterprise.api.http.ApiError.InvalidPublicKey
import com.wavesenterprise.api.http.SignedMessage
import com.wavesenterprise.state.{AccountDataInfo, Blockchain}
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.{TransactionGen, crypto}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class AddressApiServiceSpec extends AnyFunSpecLike with Matchers with MockFactory with TransactionGen {

  private lazy val pk1 = crypto.generatePublicKey

  private val address    = pk1.toAddress
  private val addressStr = pk1.toAddress.stringRepr

  private val blockchain      = stub[Blockchain]
  private val wallet          = stub[Wallet]
  private val dataSize        = 10
  private val dataEntries     = Gen.listOfN(dataSize, dataEntryGen(666)).sample.get
  private val dataEntryMap    = Range(0, dataSize).zip(dataEntries).map { case (i, de) => i.toString -> de }.toMap
  private val accountDataInfo = AccountDataInfo(dataEntryMap)
  private def loadMock(from: Int, to: Int) = {
    (blockchain.accountDataSlice _)
      .when(address, from, to)
      .returning(accountDataInfo)
  }
  private val addressApiService = new AddressApiService(blockchain, wallet)

  it("offset & limit is none") {
    loadMock(0, Integer.MAX_VALUE)
    val accountData = addressApiService.accountData(addressStr, None, None)
    accountData shouldBe 'right
    accountData.right.get should contain theSameElementsAs dataEntries
  }

  it("offset is < 0 & limit is none") {
    loadMock(0, Integer.MAX_VALUE)
    val accountData = addressApiService.accountData(addressStr, Some(-10), None)
    accountData shouldBe 'right
    accountData.right.get should contain theSameElementsAs dataEntries
  }

  it("offset is < 0 & limit is Int.Max") {
    loadMock(0, Integer.MAX_VALUE)
    val accountData = addressApiService.accountData(addressStr, Some(-10), Some(Integer.MAX_VALUE))
    accountData shouldBe 'right
    accountData.right.get should contain theSameElementsAs dataEntries
  }

  it("offset is Int.Max & limit is Int.Max - 1") {
    val accountData = addressApiService.accountData(addressStr, Some(Integer.MAX_VALUE), Some(Integer.MAX_VALUE - 1))
    accountData shouldBe 'left
  }

  it("offset is 10 & limit is 20") {
    loadMock(10, 10 + 20)
    val accountData = addressApiService.accountData(addressStr, Some(10), Some(20))
    accountData shouldBe 'right
    accountData.right.get should contain theSameElementsAs dataEntries
  }

  it("offset is 10 & limit is none") {
    loadMock(10, Integer.MAX_VALUE)
    val accountData = addressApiService.accountData(addressStr, Some(10), None)
    accountData shouldBe 'right
    accountData.right.get should contain theSameElementsAs dataEntries
  }

  it("invalid public key") {
    val signedMessage      = SignedMessage("ping_pong", "GmU5d7pjZmQrs5EKgR9CSz5L6Np", "IO123")
    val verificationResult = addressApiService.verifySignedMessage(signedMessage, addressStr, isMessageEncoded = false)
    verificationResult shouldBe 'left
    val validationError = verificationResult.left.get.asInstanceOf[InvalidPublicKey]
    validationError.message should equal("invalid public key: IO123")
  }
}
