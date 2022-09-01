package com.wavesenterprise.http

import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.{OpType, PermissionOp, Role}
import com.wavesenterprise.api.http.ApiError.BlockDoesNotExist
import com.wavesenterprise.api.http.consensus.ConsensusApiRoute
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.consensus.PoAConsensusSpec.mockMyBlockchain
import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.http.ApiMarshallers._
import com.wavesenterprise.settings.{ConsensusSettings, FunctionalitySettings}
import com.wavesenterprise.state._
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.{BlockGen, CryptoHelpers, NoShrink, TestTime}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.JsObject

import scala.concurrent.duration._

class ConsensusRouteSpec
    extends RouteSpec("/consensus")
    with ApiSettingsHelper
    with ScalaCheckPropertyChecks
    with BlockGen
    with HistoryTest
    with WithDomain
    with MockFactory
    with NoShrink {

  private val ownerAddress: Address = CryptoHelpers.generatePrivateKey.toAddress

  private val time = new TestTime

  private def routeTestPos(f: (Blockchain, Route) => Any) = withDomain() { d =>
    d.blockchainUpdater.processBlock(genesisBlock, ConsensusPostAction.NoAction)
    1 to 10 foreach { _ =>
      d.blockchainUpdater.processBlock(getNextTestBlock(d.blockchainUpdater), ConsensusPostAction.NoAction)
    }
    f(
      d.blockchainUpdater,
      new ConsensusApiRoute(restAPISettings,
                            time,
                            d.blockchainUpdater,
                            FunctionalitySettings.TESTNET,
                            ConsensusSettings.PoSSettings,
                            ownerAddress,
                            apiComputationsScheduler).route
    )
  }

  routePath("/generationsignature") - {
    "for last block" in routeTestPos { (h, route) =>
      Get(routePath("/generationsignature")) ~> route ~> check {
        (responseAs[JsObject] \ "generationSignature")
          .as[String] shouldEqual h.lastBlock.get.consensusData.asPoSMaybe().explicitGet().generationSignature.base58
      }
    }

    "for existing block" in routeTestPos { (h, route) =>
      val block = h.blockAt(3).get
      Get(routePath(s"/generationsignature/${block.uniqueId.base58}")) ~> route ~> check {
        (responseAs[JsObject] \ "generationSignature")
          .as[String] shouldEqual block.consensusData.asPoSMaybe().explicitGet().generationSignature.base58
      }
    }

    "for non-existent block" in routeTestPos { (_, route) =>
      val testSign = ByteStr("brggwg4wg4g".getBytes)
      Get(routePath(s"/generationsignature/$testSign")) ~> route should produce(BlockDoesNotExist(Left(testSign)))
    }
  }

  routePath("/basetarget") - {
    "for existing block" in routeTestPos { (h, route) =>
      val block = h.blockAt(3).get
      Get(routePath(s"/basetarget/${block.uniqueId.base58}")) ~> route ~> check {
        (responseAs[JsObject] \ "baseTarget").as[Long] shouldEqual block.consensusData.asPoSMaybe().explicitGet().baseTarget
      }
    }

    "for non-existent block" in routeTestPos { (_, route) =>
      val testSign = ByteStr("brggwg4wg4g".getBytes)
      Get(routePath(s"/basetarget/$testSign")) ~> route should produce(BlockDoesNotExist(Left(testSign)))
    }
  }

  routePath("/settings") - {
    "return PoS algorithm name for PoS settings" in routeTestPos { (h, route) =>
      Get(routePath("/settings")) ~> api_key(apiKey) ~> route ~> check {
        (responseAs[JsObject] \ "consensusAlgo").as[String] shouldBe "Fair Proof-of-Stake (FairPoS)"
      }
    }
    "return PoA algorithm name and parameters for PoA settings" in {
      val bc                = mock[Blockchain]
      val roundDuration     = 60.seconds
      val syncDuration      = 15.seconds
      val banDurationBlocks = 100
      val warningsForBan    = 3
      val maxBansPercentage = 50
      val route =
        new ConsensusApiRoute(
          restAPISettings,
          time,
          bc,
          FunctionalitySettings.TESTNET,
          ConsensusSettings.PoASettings(roundDuration, syncDuration, banDurationBlocks, warningsForBan, maxBansPercentage),
          ownerAddress,
          apiComputationsScheduler
        ).route
      Get(routePath("/settings")) ~> api_key(apiKey) ~> route ~> check {
        (responseAs[JsObject] \ "consensusAlgo").as[String] shouldBe "Proof-of-Authority (PoA)"
        (responseAs[JsObject] \ "roundDuration").as[String] shouldBe roundDuration.toString()
        (responseAs[JsObject] \ "syncDuration").as[String] shouldBe syncDuration.toString()
        (responseAs[JsObject] \ "banDurationBlocks").as[Int] shouldBe banDurationBlocks
      }
    }
  }

  routePath("/minersAtHeight/{height}") - {
    "return correct list of miners at given height" in {
      forAll(Gen.choose(1, 20), Gen.choose(1, 1000)) { (numMiners, anyTestHeight) =>
        val addressToPermMap: Map[Address, PermissionOp] = (1 to numMiners).map { num =>
          Wallet.generateNewAccount().toAddress -> PermissionOp(OpType.Add, Role.Miner, num.toLong, None)
        }.toMap
        val bc = mockMyBlockchain(Wallet.generateNewAccount(), addressToPermMap)

        val roundDuration     = 60.seconds
        val syncDuration      = 15.seconds
        val banDurationBlocks = 100
        val warningsForBan    = 3
        val maxBansPercentage = 50
        val route =
          new ConsensusApiRoute(
            restAPISettings,
            time,
            bc,
            FunctionalitySettings.TESTNET,
            ConsensusSettings.PoASettings(roundDuration, syncDuration, banDurationBlocks, warningsForBan, maxBansPercentage),
            ownerAddress,
            apiComputationsScheduler
          ).route

        Get(routePath(s"/minersAtHeight/$anyTestHeight")) ~> route ~> check {
          val json = responseAs[JsObject]
          (json \ "height").as[Int] shouldBe anyTestHeight
          (json \ "miners").as[Seq[String]] should contain theSameElementsAs addressToPermMap.keys.toSeq.map(_.toString)
        }
      }
    }
  }

  routePath("/miners/{timestamp}") - {
    "return correct list of miners at given timestamp" in {
      forAll(Gen.choose(1, 20)) { numMiners =>
        val addressToPermMap: Map[Address, PermissionOp] = (1 to numMiners).map { num =>
          Wallet.generateNewAccount().toAddress -> PermissionOp(OpType.Add, Role.Miner, num.toLong, None)
        }.toMap
        val bc = mockMyBlockchain(Wallet.generateNewAccount(), addressToPermMap)

        val roundDuration     = 60.seconds
        val syncDuration      = 15.seconds
        val banDurationBlocks = 100
        val warningsForBan    = 3
        val maxBansPercentage = 50
        val route =
          new ConsensusApiRoute(
            restAPISettings,
            time,
            bc,
            FunctionalitySettings.TESTNET,
            ConsensusSettings.PoASettings(roundDuration, syncDuration, banDurationBlocks, warningsForBan, maxBansPercentage),
            ownerAddress,
            apiComputationsScheduler
          ).route

        Get(routePath(s"/miners/${Long.MaxValue}")) ~> route ~> check {
          val json = responseAs[JsObject]
          (json \ "timestamp").as[Long] shouldBe Long.MaxValue
          (json \ "miners").as[Seq[String]] should contain theSameElementsAs addressToPermMap.keys.toSeq.map(_.toString)
        }
      }
    }
  }

  routePath("/bannedMiners/{height}") - {
    "return correct list of banned miners at given height" in {
      forAll(Gen.listOf(accountGen), positiveIntGen) { (minerAccounts, testHeight) =>
        val blockchain: Blockchain = mock[Blockchain]
        val minerAddresses         = minerAccounts.map(_.toAddress)

        (blockchain.bannedMiners(_: Int)).expects(testHeight).returns(minerAddresses)

        val roundDuration     = 60.seconds
        val syncDuration      = 15.seconds
        val banDurationBlocks = 100
        val warningsForBan    = 3
        val maxBansPercentage = 50
        val route =
          new ConsensusApiRoute(
            restAPISettings,
            time,
            blockchain,
            FunctionalitySettings.TESTNET,
            ConsensusSettings.PoASettings(roundDuration, syncDuration, banDurationBlocks, warningsForBan, maxBansPercentage),
            ownerAddress,
            apiComputationsScheduler
          ).route

        Get(routePath(s"/bannedMiners/$testHeight")) ~> route ~> check {
          response.status shouldBe StatusCodes.OK
          val json = responseAs[JsObject]
          (json \ "height").as[Long] shouldBe testHeight
          (json \ "bannedMiners").as[Seq[String]] should contain theSameElementsAs minerAddresses.map(_.toString)
        }
      }
    }
  }

}
