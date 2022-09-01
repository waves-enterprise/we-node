package com.wavesenterprise.api.http.snapshot

import akka.http.scaladsl.model.StatusCodes
import com.wavesenterprise.TestTime
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError.SnapshotGenesisNotFound
import com.wavesenterprise.api.http._
import com.wavesenterprise.api.http.snapshot.EnabledSnapshotApiRoute.GenesisSettingsFormat
import com.wavesenterprise.database.snapshot._
import com.wavesenterprise.http.{ApiSettingsHelper, RouteSpec, api_key}
import com.wavesenterprise.settings.{ConsensusType, GenesisSettings, GenesisType}
import com.wavesenterprise.transaction.CommonGen
import com.wavesenterprise.utils.EitherUtils.EitherExt
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.Eventually
import play.api.libs.json.JsObject

class EnabledSnapshotApiRouteSpec extends RouteSpec("/snapshot") with PathMockFactory with ApiSettingsHelper with CommonGen with Eventually {

  implicit val scheduler: Scheduler = monix.execution.Scheduler.global

  private val nodeOwner             = accountGen.sample.get
  private val ownerAddress: Address = nodeOwner.toAddress
  private val time                  = new TestTime()

  private val genesis = SnapshotGenesis
    .buildGenesis(ConsensusType.CFT, nodeOwner, System.currentTimeMillis(), senderRoleEnabled = false)
    .explicitGet()

  private val statusHolder = new SnapshotStatusHolder(Observable.empty)
  private val route =
    new EnabledSnapshotApiRoute(statusHolder, Task.pure(Some(genesis)), _ => Task(Right(())), restAPISettings, time, ownerAddress, () => ()).route

  routePath("/status") in {
    statusHolder.setStatus(Exists)
    Get(routePath("/status")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsObject]
      response shouldBe Exists.toJson
    }
  }

  routePath("/genesisConfig") in {
    statusHolder.setStatus(Verified)
    Get(routePath("/genesisConfig")) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsObject]
      (response \ "type").as[GenesisType] shouldBe GenesisType.SnapshotBased
      response.as[GenesisSettings] shouldBe SnapshotGenesis.mapToSnapshotBasedSettings(genesis)
    }

    statusHolder.setStatus(Swapped)
    Get(routePath("/genesisConfig")) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe "Snapshot has been already swapped"
    }

    statusHolder.setStatus(NotExists)
    Get(routePath("/genesisConfig")) ~> route ~> check {
      status shouldBe StatusCodes.NotFound
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe s"Snapshot isn't yet verified. Current status '${NotExists.name}'. Try later"
    }

    statusHolder.setStatus(Verified)
    val emptyGenesisRoute =
      new EnabledSnapshotApiRoute(statusHolder, Task.pure(None), _ => Task(Right(())), restAPISettings, time, ownerAddress, () => ()).route
    Get(routePath("/genesisConfig")) ~> emptyGenesisRoute ~> check {
      status shouldBe StatusCodes.InternalServerError
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe SnapshotGenesisNotFound.message
    }
  }

  routePath("/swapState") in {
    statusHolder.setStatus(Verified)
    Post(routePath("/swapState?backupOldState=true")) ~> api_key(apiKey) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe "Finished swapping old state for snapshot"
    }

    statusHolder.setStatus(Swapped)
    Post(routePath("/swapState?backupOldState=false")) ~> api_key(apiKey) ~> route ~> check {
      status shouldBe StatusCodes.BadRequest
      val response = responseAs[JsObject]
      (response \ "message").as[String] shouldBe "Snapshot has been already swapped"
    }
  }
}
