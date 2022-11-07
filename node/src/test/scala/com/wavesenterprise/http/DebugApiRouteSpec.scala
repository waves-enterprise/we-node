package com.wavesenterprise.http

import akka.http.scaladsl.model.StatusCodes
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError.ApiKeyNotValid
import com.wavesenterprise.docker.ContractAuthTokenService
import com.wavesenterprise.history.{DefaultWESettings, config}
import com.wavesenterprise.http.ApiMarshallers._
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.validation.DisabledFeeCalculator
import com.wavesenterprise.{CryptoHelpers, NTPTime, TestWallet}
import org.scalamock.scalatest.MockFactory
import play.api.libs.json.{JsObject, Json}

class DebugApiRouteSpec extends RouteSpec("/debug") with ApiSettingsHelper with TestWallet with NTPTime with MockFactory {
  private val ownerAddress: Address = CryptoHelpers.generatePrivateKey.toAddress

  private val weSettings = DefaultWESettings.copy(api = restAPISettings)
  private val route =
    new DebugApiRoute(
      weSettings,
      ntpTime,
      DisabledFeeCalculator,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      config.root(),
      ownerAddress,
      Some(new ContractAuthTokenService),
      monix.execution.Scheduler.global,
      () => ()
    ).route

  routePath("/configInfo") - {
    "requires api-key header" in {
      Get(routePath("/configInfo?full=true")) ~> route should produce(ApiKeyNotValid)
      Get(routePath("/configInfo?full=false")) ~> route should produce(ApiKeyNotValid)
    }
  }

  routePath("/createGrpcAuth") - {
    val request = Json.obj("contractId" -> ByteStr("some_contract_id".getBytes()).toString)
    "successful auth token creation" in {
      Post(routePath("/createGrpcAuth"), request) ~> api_key(apiKey) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        (responseAs[JsObject] \ "token").isDefined
      }
    }
    "auth token creation requires api-key header" in {
      Post(routePath("/createGrpcAuth"), request) ~> route should produce(ApiKeyNotValid)
    }
  }

  routePath("/threadDump") - {
    "requires api-key header" in {
      Get(routePath("/threadDump")) ~> route should produce(ApiKeyNotValid)
    }

    "valid thread dump" in {
      Get(routePath("/threadDump")) ~> api_key(apiKey) ~> route ~> check {
        response.status shouldBe StatusCodes.OK
        responseAs[String] should include("lockedSynchronizers")
      }
    }
  }
}
