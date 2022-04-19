package com.wavesenterprise.api.http

import java.nio.charset.StandardCharsets

import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import akka.http.scaladsl.model.StatusCodes
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.assets.AssetsApiRoute
import com.wavesenterprise.http.{ApiSettingsHelper, RouteSpec}
import com.wavesenterprise.state.{AssetDescription, Blockchain, ByteStr, LeaseBalance, Portfolio}
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.{NoShrink, TestTime, TestWallet, TransactionGen}
import org.scalamock.scalatest.PathMockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json._

class AssetsApiRouteSpec
    extends RouteSpec("/assets")
    with PathMockFactory
    with ScalaCheckPropertyChecks
    with ApiSettingsHelper
    with TestWallet
    with TransactionGen
    with NoShrink {

  private val ownerAddress: Address = accountGen.sample.get.toAddress

  (testWallet.privateKeyAccounts _).expects().returns(walletData.values.toList).atLeastOnce()

  private val allAccounts  = testWallet.privateKeyAccounts
  private val allAddresses = allAccounts.map(_.address)
  private val blockchain   = stub[Blockchain]

  private val route = new AssetsApiRoute(
    restAPISettings,
    mock[UtxPool],
    blockchain,
    new TestTime,
    ownerAddress,
    apiComputationsScheduler
  ).route

  private val smartAssetTx = smartIssueTransactionGen(scriptOptGen = scriptGen.map(Some.apply)).sample.get
  private val smartAssetDesc = AssetDescription(
    issuer = smartAssetTx.sender,
    height = 1,
    timestamp = smartAssetTx.timestamp,
    name = new String(smartAssetTx.name, StandardCharsets.UTF_8),
    description = new String(smartAssetTx.description, StandardCharsets.UTF_8),
    decimals = smartAssetTx.decimals,
    reissuable = smartAssetTx.reissuable,
    totalVolume = smartAssetTx.quantity,
    script = smartAssetTx.script,
    sponsorshipIsEnabled = false
  )
  (blockchain.transactionInfo _).when(smartAssetTx.id()).onCall((_: ByteStr) => Some((1, smartAssetTx)))
  (blockchain.assetDescription _).when(smartAssetTx.id()).onCall((_: ByteStr) => Some(smartAssetDesc))
  routePath(s"/details/${smartAssetTx.id().base58}") in {
    Get(routePath(s"/details/${smartAssetTx.id().base58}")) ~> route ~> check {
      val response = responseAs[JsObject]
      (response \ "assetId").as[String] shouldBe smartAssetTx.id().base58
      (response \ "issueHeight").as[Long] shouldBe 1
      (response \ "issueTimestamp").as[Long] shouldBe smartAssetTx.timestamp
      (response \ "issuer").as[String] shouldBe smartAssetTx.sender.toString
      (response \ "name").as[String] shouldBe new String(smartAssetTx.name, StandardCharsets.UTF_8)
      (response \ "description").as[String] shouldBe new String(smartAssetTx.description, StandardCharsets.UTF_8)
      (response \ "decimals").as[Int] shouldBe smartAssetTx.decimals
      (response \ "reissuable").as[Boolean] shouldBe smartAssetTx.reissuable
      (response \ "quantity").as[BigDecimal] shouldBe smartAssetDesc.totalVolume
      (response \ "sponsorshipIsEnabled").asOpt[Boolean] shouldBe Some(false)
    }
  }

  private val sillyAssetTx = issueGen.sample.get
  private val sillyAssetDesc = AssetDescription(
    issuer = sillyAssetTx.sender,
    height = 1,
    timestamp = sillyAssetTx.timestamp,
    name = new String(sillyAssetTx.name, StandardCharsets.UTF_8),
    description = new String(sillyAssetTx.description, StandardCharsets.UTF_8),
    decimals = sillyAssetTx.decimals,
    reissuable = sillyAssetTx.reissuable,
    totalVolume = sillyAssetTx.quantity,
    script = sillyAssetTx.script,
    sponsorshipIsEnabled = true
  )
  (blockchain.transactionInfo _).when(sillyAssetTx.id()).onCall((_: ByteStr) => Some((1, sillyAssetTx)))
  (blockchain.assetDescription _).when(sillyAssetTx.id()).onCall((_: ByteStr) => Some(sillyAssetDesc))
  routePath(s"/details/${sillyAssetTx.id().base58}") in {
    Get(routePath(s"/details/${sillyAssetTx.id().base58}")) ~> route ~> check {
      val response = responseAs[JsObject]
      (response \ "assetId").as[String] shouldBe sillyAssetTx.id().base58
      (response \ "issueHeight").as[Long] shouldBe 1
      (response \ "issueTimestamp").as[Long] shouldBe sillyAssetTx.timestamp
      (response \ "issuer").as[String] shouldBe sillyAssetTx.sender.toString
      (response \ "name").as[String] shouldBe new String(sillyAssetTx.name, StandardCharsets.UTF_8)
      (response \ "description").as[String] shouldBe new String(sillyAssetTx.description, StandardCharsets.UTF_8)
      (response \ "decimals").as[Int] shouldBe sillyAssetTx.decimals
      (response \ "reissuable").as[Boolean] shouldBe sillyAssetTx.reissuable
      (response \ "quantity").as[BigDecimal] shouldBe sillyAssetDesc.totalVolume
      (response \ "sponsorshipIsEnabled").asOpt[Boolean] shouldBe Some(true)
    }
  }

  routePath("/balance") in {
    val assetId     = bytes32gen.map(ByteStr(_)).sample.get
    val nullAssetId = bytes32gen.map(ByteStr(_)).sample.get

    blockchain.portfolio _ when * returns Portfolio(0, LeaseBalance.empty, Map(assetId -> 100, nullAssetId -> 0))

    val body = Json.obj("addresses" -> JsArray(value = allAddresses.map(JsString)))
    Post(routePath("/balance"), body) ~> route ~> check {
      val values = responseAs[JsObject].value
      values.size shouldBe allAddresses.size
      for (obj <- values) {
        allAddresses should contain(obj._1)
        obj._2.as[JsArray] shouldBe JsArray(Seq(Json.obj("assetId" -> assetId.toString, "balance" -> 100)))
      }
    }

    val invalidBody = Json.obj("addresses" -> JsArray(value = Array(JsString("some_invalid_address"))))
    Post(routePath("/balance"), invalidBody) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
    }
  }

}
