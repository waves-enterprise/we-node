package com.wavesenterprise.api.http.snapshot

import akka.http.scaladsl.model.StatusCodes
import com.wavesenterprise.TestTime
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.{ApiError, _}
import com.wavesenterprise.http.{ApiSettingsHelper, RouteSpec, api_key}
import com.wavesenterprise.transaction.CommonGen
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import play.api.libs.json.JsObject

class DisabledSnapshotApiRouteSpec extends RouteSpec("/snapshot") with PathMockFactory with ApiSettingsHelper with CommonGen with Eventually {
  private val nodeOwner             = accountGen.sample.get
  private val ownerAddress: Address = nodeOwner.toAddress
  private val time                  = new TestTime()
  private val route                 = DisabledSnapshotApiRoute(restAPISettings, time, ownerAddress).route

  routePath("/status") in {
    Get(routePath("/status")) ~> route ~> check {
      expectedResult
    }
  }

  routePath("/genesisConfig") in {
    Get(routePath("/genesisConfig")) ~> route ~> check {
      expectedResult
    }
  }

  routePath("/swapState") in {
    Post(routePath("/swapState?backupOldState=true")) ~> api_key(apiKey) ~> route ~> check {
      expectedResult
    }
  }

  private def expectedResult: Assertion = {
    status shouldBe StatusCodes.Forbidden
    val response = responseAs[JsObject]
    (response \ "message").as[String] shouldBe ApiError.SnapshotFeatureDisabled.message
  }
}
