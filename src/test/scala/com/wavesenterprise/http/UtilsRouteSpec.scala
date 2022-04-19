package com.wavesenterprise.http

import akka.http.scaladsl.model.StatusCodes
import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.UtilsApiRoute
import com.wavesenterprise.http.ApiMarshallers._
import com.wavesenterprise.lang.v1.compiler.Terms._
import com.wavesenterprise.lang.v1.evaluator.FunctionIds._
import com.wavesenterprise.lang.v1.evaluator.ctx.impl.PureContext
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.{Base58, Time}
import com.wavesenterprise.{CryptoHelpers, TestTime, TestWallet, crypto}
import org.scalacheck.Gen
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.BeforeAndAfter
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.{JsObject, JsValue}

class UtilsRouteSpec
    extends RouteSpec("/utils")
    with ApiSettingsHelper
    with TestWallet
    with ScalaCheckPropertyChecks
    with PathMockFactory
    with BeforeAndAfter {
  private val ownerAddress: Address = CryptoHelpers.generatePrivateKey.toAddress

  private val route = new UtilsApiRoute(
    new Time {
      def correctedTime(): Long = System.currentTimeMillis()
      def getTimestamp(): Long  = System.currentTimeMillis()
    },
    restAPISettings,
    testWallet,
    new TestTime(),
    ownerAddress,
    apiComputationsScheduler
  ).route

  val script = FUNCTION_CALL(
    function = PureContext.eq.header,
    args = List(CONST_LONG(1), CONST_LONG(2))
  )

  routePath("/script/compile") - {
    "compiles script with expected complexity" in Post(routePath("/script/compile"), "1 == 2") ~> route ~> check {
      val json           = responseAs[JsValue]
      val expectedScript = ScriptV1(script).explicitGet()

      Script.fromBase64String((json \ "script").as[String]) shouldBe Right(expectedScript)
      (json \ "complexity").as[Long] shouldBe 3
    }
  }

  routePath("/script/estimate") - {
    val scriptBase64 = ScriptV1(script).explicitGet().bytes().base64

    "estimates a script properly" in Post(routePath("/script/estimate"), scriptBase64) ~> route ~> check {
      val json = responseAs[JsValue]
      (json \ "script").as[String] shouldBe scriptBase64
      (json \ "scriptText").as[String] shouldBe s"FUNCTION_CALL(Native($EQ),List(CONST_LONG(1), CONST_LONG(2)))"
      (json \ "complexity").as[Long] shouldBe 3
    }

    "checks if given script is empty" in Post(routePath("/script/estimate"), "") ~> route ~> check {
      val json = responseAs[JsValue]
      (json \ "message").as[String] should include("Cannot estimate empty script")
    }

    "checks if parameter is a valid Base64" in Post(routePath("/script/estimate"), "a1bc23df==") ~> route ~> check {
      val json = responseAs[JsValue]
      (json \ "message").as[String] should include("Unable to decode base64")
    }
  }

  routePath("/reload-wallet") - {
    (testWallet.reload _).expects().returning(()).once()

    "returns expected Ok message" in Post(routePath("/reload-wallet")) ~> api_key(apiKey) ~> route ~> check {
      status shouldBe StatusCodes.OK
      (responseAs[JsValue] \ "message").as[String] shouldBe "Wallet reloaded successfully"
    }
  }

  routePath("/hash/") - {
    for ((hash, f) <- Seq[(String, String => Array[Byte])](
           "secure" -> crypto.secureHash,
           "fast"   -> crypto.fastHash
         )) {
      val uri = routePath(s"/hash/$hash")
      uri in {
        forAll(Gen.alphaNumStr) { s =>
          Post(uri, s) ~> route ~> check {
            val r = responseAs[JsObject]
            (r \ "message").as[String] shouldEqual s
            (r \ "hash").as[String] shouldEqual Base58.encode(f(s))
          }
        }
      }
    }
  }
}
