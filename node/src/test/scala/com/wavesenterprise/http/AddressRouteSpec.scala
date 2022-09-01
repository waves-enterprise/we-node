package com.wavesenterprise.http

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import com.wavesenterprise.account.{Address, Alias}
import com.wavesenterprise.api.http.AddressApiRoute.{
  AddressPublicKeyInfo,
  AddressResponse,
  GeneratingBalance,
  ValidateManyReq,
  ValidityMany,
  ValiditySingle
}
import com.wavesenterprise.api.http.ApiError.{InvalidAddress, InvalidPublicKey, MissingSenderPrivateKey, RequestedHeightDoesntExist}
import com.wavesenterprise.api.http.Message._
import com.wavesenterprise.api.http.service.AddressApiService
import com.wavesenterprise.api.http.{AddressApiRoute, ApiErrorResponse, Message}
import com.wavesenterprise.http.ApiMarshallers._
import com.wavesenterprise.lang.v1.compiler.Terms._
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.state.{BalanceSnapshot, Blockchain, LeaseBalance, Portfolio}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.AliasDoesNotExist
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.utils.Base58
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.{NoShrink, TestTime, TestWallet, TransactionGen, crypto}
import org.scalacheck.{Arbitrary, Gen}
import org.scalamock.scalatest.PathMockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json._

import java.nio.charset.StandardCharsets.UTF_8

class AddressRouteSpec
    extends RouteSpec("/addresses")
    with PathMockFactory
    with ScalaCheckPropertyChecks
    with ApiSettingsHelper
    with TestWallet
    with NoShrink
    with TransactionGen {

  private val ownerAddress: Address = accountGen.sample.get.toAddress

  (testWallet.keyStoreAliases _).expects().returns(walletData.keys.toSeq).atLeastOnce()
  (testWallet.publicKeyAccounts _).expects().returns(walletData.values.toList).atLeastOnce()
  (testWallet.privateKeyAccounts _).expects().returns(walletData.values.toList).atLeastOnce()
  (testWallet
    .privateKeyAccount(_: Address, _: Option[Array[Char]]))
    .expects(*, *)
    .onCall { (address: Address, _: Option[Array[Char]]) =>
      walletData.get(address.address).toRight(ValidationError.MissingSenderPrivateKey)
    }
    .atLeastOnce()
  (testWallet.publicKeyAccount _)
    .expects(*)
    .onCall { (address: Address) =>
      walletData.get(address.address).toRight(ValidationError.MissingSenderPrivateKey)
    }
    .atLeastOnce()

  private val allAccounts       = testWallet.privateKeyAccounts
  private val allAddresses      = allAccounts.map(_.address)
  private val validAddresses    = allAddresses.map(addressStr => Address.fromString(addressStr).explicitGet())
  private val blockchain        = stub[Blockchain]
  private val addressApiService = new AddressApiService(blockchain, testWallet)

  private val route = new AddressApiRoute(
    addressApiService,
    restAPISettings,
    new TestTime,
    blockchain,
    mock[UtxPool],
    TestFunctionalitySettings.Stub,
    None,
    ownerAddress,
    apiComputationsScheduler
  ).route

  private val generatedMessages = for {
    account <- Gen.oneOf(allAccounts).label("account")
    length  <- Gen.chooseNum(10, 1000)
    message <- Gen.listOfN(length, Gen.alphaNumChar).map(_.mkString).label("message")
  } yield (account, message)

  private val amount    = 100
  private val portfolio = Portfolio(amount, LeaseBalance(amount, 0), Map.empty)
  (blockchain.balanceSnapshots _).when(*, *, *).returns(Seq(BalanceSnapshot(1, portfolio)))
  (blockchain.balance _).when(*, *).returns(portfolio.balance)
  (blockchain.leaseBalance _).when(*).returns(portfolio.lease)

  routePath("/seq/{from}/{to}") in {
    val r1 = Get(routePath("/seq/0/3")) ~> route ~> check {
      val response = responseAs[Seq[String]]
      response.length shouldBe 4
      allAddresses should contain allElementsOf response
      response
    }

    val r2 = Get(routePath("/seq/4/8")) ~> route ~> check {
      val response = responseAs[Seq[String]]
      response.length shouldBe 5
      allAddresses should contain allElementsOf response
      response
    }

    r1 shouldNot contain allElementsOf r2

    Get(routePath("/seq/7/4")) ~> route ~> check {
      val response = responseAs[String]
      response should include("Invalid interval")
    }

    Get(routePath(s"/seq/0/${MaxAddressesPerRequest + 1}")) ~> route ~> check {
      val response = responseAs[String]
      response should include("Too big sequences requested")
    }

    Get(routePath(s"/seq/-1/3000000000")) ~> route ~> check {
      val response = responseAs[String]
      response should include("Invalid parameters: ['-1' must be non-negative, Unable to parse Integer from '3000000000']")
    }
  }

  routePath("/validate/{addressOrAlias} for addresses") in {
    val t = Table(("address", "valid"), allAddresses.map(_ -> true): _*)

    forAll(t) { (a, v) =>
      Get(routePath(s"/validate/$a")) ~> route ~> check {
        val r = responseAs[AddressApiRoute.ValiditySingle]
        r.addressOrAlias shouldEqual a
        r.valid shouldBe v
      }
    }
  }

  routePath("/validate/{addressOrAlias} for empty string") in {
    Get(routePath(s"/validate/")) ~> route ~> check {
      handled shouldBe false
    }
  }

  routePath("/validate/{addressOrAlias} for aliases in 'alias:T:{name}' format") in {
    forAll(aliasGen, Gen.oneOf(validAddresses)) {
      case (alias, validAddress) =>
        val aliasStr = alias.toString

        (blockchain.resolveAlias _).when(alias).returning(Right(validAddress)).once

        Get(routePath(s"/validate/$aliasStr")) ~> route ~> check {
          val r = responseAs[AddressApiRoute.ValiditySingle]
          r.addressOrAlias shouldEqual aliasStr
          r.valid shouldBe true
        }
    }
  }

  routePath("/validate/{addressOrAlias} for aliases in plain format") in {
    forAll(validAliasStringGen, Gen.oneOf(validAddresses)) {
      case (aliasStr, validAddress) =>
        (blockchain.resolveAlias _)
          .when(Alias.buildWithCurrentChainId(aliasStr).explicitGet())
          .returning(Right(validAddress))
          .once

        Get(routePath(s"/validate/$aliasStr")) ~> route ~> check {
          val r = responseAs[AddressApiRoute.ValiditySingle]
          r.addressOrAlias shouldEqual aliasStr
          r.valid shouldBe true
        }
    }
  }

  routePath("/validate/{addressOrAlias} for non-existing valid aliases") in {
    forAll(aliasGen) { alias =>
      val aliasStr = alias.toString

      (blockchain.resolveAlias(_: Alias)).when(alias).returns(Left(AliasDoesNotExist(alias))).once

      Get(routePath(s"/validate/$aliasStr")) ~> route ~> check {
        val r = responseAs[AddressApiRoute.ValiditySingle]
        r.addressOrAlias shouldEqual aliasStr
        r.valid shouldBe false
      }
    }
  }

  routePath("/validate/{addressOrAlias} for invalid aliases") in {
    forAll(invalidAliasStringGen.map(_.filterNot(c => "`%#&=?".contains(c)))) { aliasStr =>
      Get(routePath(s"/validate/$aliasStr")) ~> route ~> check {
        val r = responseAs[AddressApiRoute.ValiditySingle]
        r.addressOrAlias shouldEqual aliasStr
        r.valid shouldBe false
      }
    }
  }

  routePath("/validateMany for valid addresses") in {
    val genValidateManyRequest: Gen[ValidateManyReq] = Gen.atLeastOne(allAddresses).map(ValidateManyReq.apply)

    forAll(genValidateManyRequest) { req =>
      Post(routePath("/validateMany"), Json.toJson(req)) ~> route ~> check {
        handled shouldBe true
        status shouldBe StatusCodes.OK
        val response = responseAs[ValidityMany]
        response.validations.map(_.addressOrAlias) should contain theSameElementsAs req.addressesOrAliases
        response.validations.map(_.valid).forall(identity) shouldBe true
      }
    }
  }

  routePath("/validateMany for valid aliases") in {
    forAll(Gen.nonEmptyListOf(aliasGen)) { aliases =>
      aliases.foreach { alias =>
        (blockchain.resolveAlias(_: Alias)).when(alias).returns(Right(Address.fromString(allAddresses.head).explicitGet())).once
      }

      val req = ValidateManyReq(aliases.map(_.toString))

      Post(routePath("/validateMany"), Json.toJson(req)) ~> route ~> check {
        val response = responseAs[ValidityMany]
        handled shouldBe true
        status shouldBe StatusCodes.OK
        response.validations.map(_.addressOrAlias) should contain theSameElementsAs req.addressesOrAliases
        response.validations.map(_.valid).forall(identity) shouldBe true
      }
    }
  }

  routePath("/validateMany for non-existing aliases") in {
    forAll(Gen.nonEmptyListOf(aliasGen)) { aliases =>
      aliases.foreach { alias =>
        (blockchain.resolveAlias(_: Alias)).when(alias).onCall((a: Alias) => Left(AliasDoesNotExist(a))).once
      }

      val req = ValidateManyReq(aliases.map(_.toString))

      Post(routePath("/validateMany"), Json.toJson(req)) ~> route ~> check {
        handled shouldBe true
        status shouldBe StatusCodes.OK
        val response = responseAs[ValidityMany]
        response.validations.map(_.addressOrAlias) should contain theSameElementsAs req.addressesOrAliases
        response.validations.foreach {
          case ValiditySingle(_, isValid, reasonOpt) =>
            isValid shouldBe false
            reasonOpt match {
              case Some(reason) if reason.startsWith("AliasDoesNotExist") =>
                succeed
              case otherReason =>
                fail(s"Was expecting Some(AliasDoesNotExist(...)), but got ${otherReason.toString}")
            }
        }
      }
    }
  }

  private def testSign(path: String, encode: Boolean): Unit =
    forAll(generatedMessages) {
      case (account, message) =>
        val uri     = routePath(s"/$path/${account.address}")
        val msgJson = Json.toJson(Message(message, None))

        Post(uri, msgJson) ~> route ~> check {
          val resp      = responseAs[JsObject]
          val signature = Base58.decode((resp \ "signature").as[String]).get

          (resp \ "message").as[String] shouldEqual (if (encode) Base58.encode(message.getBytes(UTF_8)) else message)
          (resp \ "publicKey").as[String] shouldEqual Base58.encode(account.publicKey.getEncoded)

          crypto.verify(signature, message.getBytes(UTF_8), account.publicKey) shouldBe true
        }
    }

  routePath("/sign/{address}") in testSign("sign", true)
  routePath("/signText/{address}") in testSign("signText", false)

  private def testVerify(path: String, encode: Boolean): Unit = {

    forAll(generatedMessages) {
      case (account, message) =>
        val uri          = routePath(s"/$path/${account.address}")
        val messageBytes = message.getBytes(UTF_8)
        val signature    = crypto.sign(account, messageBytes)
        val validBody = Json.obj(
          "message"   -> JsString(if (encode) Base58.encode(messageBytes) else message),
          "publickey" -> JsString(Base58.encode(account.publicKey.getEncoded)),
          "signature" -> JsString(Base58.encode(signature))
        )

        val emptySignature =
          Json.obj("message" -> JsString(""), "publickey" -> JsString(Base58.encode(account.publicKey.getEncoded)), "signature" -> JsString(""))

        Post(uri, emptySignature) ~> route ~> check {
          (responseAs[JsObject] \ "valid").as[Boolean] shouldBe false
        }
        Post(uri, validBody) ~> route ~> check {
          (responseAs[JsObject] \ "valid").as[Boolean] shouldBe true
        }
    }
  }

  routePath("/verifyText/{address}") in testVerify("verifyText", false)
  routePath("/verify/{address}") in testVerify("verify", true)

  routePath(s"/scriptInfo/${allAddresses(1)}") in {
    val script = ScriptV1(TRUE).explicitGet()
    (blockchain.accountScript _).when(allAccounts(1).toAddress).onCall((_: Address) => Some(script))
    Get(routePath(s"/scriptInfo/${allAddresses(1)}")) ~> route ~> check {
      val response = responseAs[JsObject]
      (response \ "address").as[String] shouldBe allAddresses(1)
      (response \ "script").as[String] shouldBe script.bytes().base64
      (response \ "scriptText").as[String] shouldBe "TRUE"
      (response \ "complexity").as[Long] shouldBe 1
    }

    (blockchain.accountScript _).when(allAccounts(2).toAddress).onCall((_: Address) => None)
    Get(routePath(s"/scriptInfo/${allAddresses(2)}")) ~> route ~> check {
      val response = responseAs[JsObject]
      (response \ "address").as[String] shouldBe allAddresses(2)
      (response \ "script").asOpt[String] shouldBe None
      (response \ "scriptText").asOpt[String] shouldBe None
      (response \ "complexity").as[Long] shouldBe 0
    }
  }

  routePath(s"/generatingBalance/{address}/at/{height}") - {
    "returns actual generating balance" in {
      forAll(Gen.oneOf(allAddresses), Gen.posNum[Int]) { (address, height) =>
        (blockchain.height _).when().returning(height + 1).once()
        Get(routePath(s"/generatingBalance/$address/at/$height")) ~> route ~> check {
          status shouldBe StatusCodes.OK
          val response = responseAs[GeneratingBalance]
          response.address shouldBe address
          response.balance shouldBe portfolio.effectiveBalance
        }
      }
    }
    "returns an error if requested height is bigger than actual" in {
      forAll(Gen.oneOf(allAddresses), Gen.posNum[Int]) { (address, requestHeight) =>
        val actualHeight = requestHeight - 1
        (blockchain.height _).when().returning(actualHeight).once()
        Get(routePath(s"/generatingBalance/$address/at/$requestHeight")) ~> route ~> check {
          status shouldBe StatusCodes.BadRequest
          val response = responseAs[JsObject]
          (response \ "message").as[String] shouldBe RequestedHeightDoesntExist(requestHeight, actualHeight).message
        }
      }
    }
    "returns an error if height is invalid integer" in {
      Get(routePath(s"/generatingBalance/$ownerAddress/at/0")) ~> route ~> check {
        val response = responseAs[String]
        response should include("Invalid parameter: '0' must be positive")
      }

      Get(routePath(s"/generatingBalance/$ownerAddress/at/3000000000")) ~> route ~> check {
        val response = responseAs[String]
        response should include("Invalid parameter: Unable to parse Integer from '3000000000'")
      }
    }
  }

  routePath("/balance/details") in {
    val invalidAddress = "foo"
    val checkAddresses = invalidAddress :: allAddresses
    val body           = Json.obj("addresses" -> checkAddresses)

    Post(routePath("/balance/details"), body) ~> route ~> check {
      status shouldBe StatusCodes.OK

      val values = responseAs[JsArray].value

      values.length shouldBe checkAddresses.length

      def findItemByAddress(address: String): Option[JsValue] = {
        values.find(item => (item \ "address").as[String] === address)
      }

      allAddresses.foreach { validAddress =>
        val validItemOpt = findItemByAddress(validAddress)
        validItemOpt should not be empty

        validItemOpt.foreach { validItem =>
          (validItem \ "regular").as[Long] shouldBe portfolio.balance
          (validItem \ "generating").as[Long] shouldBe portfolio.effectiveBalance
          (validItem \ "available").as[Long] shouldBe portfolio.spendableBalance
          (validItem \ "effective").as[Long] shouldBe portfolio.effectiveBalance
        }
      }

      val invalidItemOpt = findItemByAddress(invalidAddress)
      invalidItemOpt should not be empty

      val dummyInvalidAddressErr = InvalidAddress("")

      invalidItemOpt.foreach { invalidItem =>
        (invalidItem \ "error").as[Int] shouldBe dummyInvalidAddressErr.id
        (invalidItem \ "message").as[String] should startWith(dummyInvalidAddressErr.message)
      }
    }
  }

  routePath("/publicKey/{publicKey}") - {
    "rejects invalid public keys" in {
      forAll(Gen.listOf(Arbitrary.arbByte.arbitrary).map(_.toArray)) { generatedBytes =>
        whenever(generatedBytes.nonEmpty && generatedBytes.length != crypto.KeyLength) {
          val encodedBytes = Base58.encode(generatedBytes)
          Get(routePath(s"/publicKey/$encodedBytes")) ~> route ~> check {
            status shouldBe StatusCodes.BadRequest
            val response              = responseAs[ApiErrorResponse]
            val dummyInvalidPubkeyErr = InvalidPublicKey("")
            response.error shouldBe dummyInvalidPubkeyErr.id
            response.message should startWith(dummyInvalidPubkeyErr.message)
          }
        }
      }
    }
    "rejects empty input" in {
      Get(routePath(s"/publicKey/")) ~> route ~> check {
        handled shouldBe false
      }
    }
    "accepts byte arrays of required length" in {
      forAll(Gen.listOfN(crypto.KeyLength, Arbitrary.arbByte.arbitrary).map(_.toArray)) { pubKeyBytes =>
        val pubKeyBytesBase58 = Base58.encode(pubKeyBytes)
        Get(routePath(s"/publicKey/$pubKeyBytesBase58")) ~> route ~> check {
          status shouldBe StatusCodes.OK
          val response = responseAs[AddressResponse]
          Address.fromString(response.address) shouldBe 'right
        }
      }
    }
  }

  routePath("/info/{address}") - {
    "returns correct address and pubkey for a valid address" in {
      val presentAccount    = testWallet.publicKeyAccounts.head
      val presentAddressStr = presentAccount.toAddress.address
      Get(routePath(s"/info/$presentAddressStr")) ~> route ~> check {
        val addressInfo = responseAs[AddressPublicKeyInfo]
        addressInfo.address shouldBe presentAddressStr
        addressInfo.publicKey shouldBe presentAccount.publicKeyBase58
      }
    }
    "returns expected InvalidAddress error" - {
      def expectInvalidAddressResponse(response: HttpResponse) = {
        response.status shouldBe StatusCodes.BadRequest
        val jsonResponse           = responseAs[JsObject]
        val dummyInvalidAddressErr = InvalidAddress("")
        (jsonResponse \ "error").as[Int] shouldBe dummyInvalidAddressErr.id
        (jsonResponse \ "message").as[String] should startWith(dummyInvalidAddressErr.message)
      }

      "on non-address string" in {
        Get(routePath(s"/info/qwerty12345")) ~> route ~> check {
          expectInvalidAddressResponse(response)
        }
      }
      "on address with wrong chain-id" in {
        val wrongByteAddress = accountGen.sample.get.toAddress(Byte.MinValue)
        Get(routePath(s"/info/${wrongByteAddress.address}")) ~> route ~> check {
          expectInvalidAddressResponse(response)
        }
      }
    }
    "returns expected MissingSenderPrivateKey error" - {
      "when being queried for non-existing account" in {
        val nonWalletAddress = accountGen.sample.get.toAddress
        Get(routePath(s"/info/${nonWalletAddress.address}")) ~> route ~> check {
          response.status shouldBe StatusCodes.BadRequest
          val jsonResponse = responseAs[JsObject]
          (jsonResponse \ "message").as[String] shouldBe MissingSenderPrivateKey.message
          (jsonResponse \ "error").as[Int] shouldBe MissingSenderPrivateKey.value
        }
      }
    }
  }

  routePath("/data/{address}") - {
    "returns an error on invalid Integer params" in {
      Get(routePath(s"/data/$ownerAddress?offset=-1&limit=0")) ~> route ~> check {
        val response = responseAs[String]
        response should include("Invalid parameters: ['-1' must be non-negative, '0' must be positive]")
      }
    }
  }

  routePath("/effectiveBalance/{address}/{confirmations}") - {
    "returns an error on invalid Integer params" in {
      Get(routePath(s"/effectiveBalance/$ownerAddress/0")) ~> route ~> check {
        val response = responseAs[String]
        response should include("Invalid parameter: '0' must be positive")
      }
    }
  }
}
