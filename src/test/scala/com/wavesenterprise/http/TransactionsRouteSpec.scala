package com.wavesenterprise.http

import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.{Address, PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.api.http.ApiError.{IllegalWatcherActionError, InvalidAddress, InvalidSignature, TooBigArrayAllocation}
import com.wavesenterprise.api.http.{AtomicTransactionRequestV1, TransactionsApiRoute}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.http.ApiMarshallers._
import com.wavesenterprise.lang.ScriptVersion.Versions.V1
import com.wavesenterprise.lang.v1.compiler.Terms.TRUE
import com.wavesenterprise.network.EnabledTxBroadcaster
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.privacy.EmptyPolicyStorage
import com.wavesenterprise.protobuf.service.transaction.UtxSize
import com.wavesenterprise.settings.SynchronizationSettings.TxBroadcasterSettings
import com.wavesenterprise.settings.TestFeeUtils.TestFeeExt
import com.wavesenterprise.settings.{NodeMode, TestFees, TestFunctionalitySettings}
import com.wavesenterprise.state.{AssetDescription, Blockchain, ByteStr}
import com.wavesenterprise.transaction.docker.ContractTransactionGen
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.validation.FeeCalculator
import com.wavesenterprise.transaction.{AtomicBadge, Transaction}
import com.wavesenterprise.utils.Base58
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.{BlockGen, NoShrink, TestSchedulers, TestTime, TransactionGen}
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Assertion, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json._

import scala.concurrent.duration._

class TransactionsRouteSpec
    extends RouteSpec("/transactions")
    with ApiSettingsHelper
    with MockFactory
    with Matchers
    with TransactionGen
    with BlockGen
    with ScalaCheckPropertyChecks
    with NoShrink
    with ContractTransactionGen {

  private val ownerAccount: PrivateKeyAccount = accountGen.sample.get
  private val ownerAddress: Address           = ownerAccount.toAddress
  private val wallet                          = mock[Wallet]

  private val blockchain            = mock[Blockchain]
  private val utx                   = mock[UtxPool]
  private val activePeerConnections = mock[ActivePeerConnections]
  private val feeCalculator         = FeeCalculator(blockchain, TestFunctionalitySettings.Stub, TestFees.defaultFees.toFeeSettings)
  private val txBroadcasterSettings = TxBroadcasterSettings(10000, 20.seconds, 1, 3, 500, 1.second, 20.seconds)
  private val txBroadcaster =
    new EnabledTxBroadcaster(txBroadcasterSettings, utx, activePeerConnections, 30)(TestSchedulers.transactionBroadcastScheduler)
  private val emptyPolicyStorage = new EmptyPolicyStorage(ntpTime)
  private val route =
    new TransactionsApiRoute(
      restAPISettings,
      feeCalculator,
      wallet,
      blockchain,
      utx,
      new TestTime,
      None,
      ownerAddress,
      emptyPolicyStorage,
      txBroadcaster,
      NodeMode.Default,
      apiComputationsScheduler
    ).route

  private val watcherRoute =
    new TransactionsApiRoute(
      restAPISettings,
      feeCalculator,
      wallet,
      blockchain,
      utx,
      new TestTime,
      None,
      ownerAddress,
      emptyPolicyStorage,
      txBroadcaster,
      NodeMode.Watcher,
      apiComputationsScheduler
    ).route

  private val invalidBase58Gen = alphaNumStr.map(_ + "0")

  routePath("/calculateFee") - {
    "transfer with WEST fee" - {
      "TransferTransaction" in {
        val sender: PublicKeyAccount = accountGen.sample.get
        val transferTx = Json.obj(
          "type"            -> 4,
          "version"         -> 2,
          "amount"          -> 1000000,
          "senderPublicKey" -> sender.publicKeyBase58,
          "recipient"       -> accountGen.sample.get.toAddress
        )

        val featuresSettings = TestFunctionalitySettings.Enabled.copy(
          preActivatedFeatures = TestFunctionalitySettings.Enabled.preActivatedFeatures + (BlockchainFeature.FeeSwitch.id -> 100)
        )
        val blockchain = mock[Blockchain]
        (blockchain.height _).expects().returning(1).anyNumberOfTimes()
        (blockchain.hasScript _).expects(sender.toAddress).returning(false).anyNumberOfTimes()
        (blockchain.activatedFeatures _).expects().returning(featuresSettings.preActivatedFeatures).anyNumberOfTimes()

        val feeCalculator = FeeCalculator(blockchain, featuresSettings, TestFees.defaultFees.toFeeSettings)
        val route = new TransactionsApiRoute(
          restAPISettings,
          feeCalculator,
          wallet,
          blockchain,
          utx,
          new TestTime,
          None,
          ownerAddress,
          emptyPolicyStorage,
          txBroadcaster,
          NodeMode.Default,
          apiComputationsScheduler
        ).route

        Post(routePath("/calculateFee"), transferTx) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          (responseAs[JsObject] \ "feeAssetId").asOpt[String] shouldBe empty
          (responseAs[JsObject] \ "feeAmount").as[Long] shouldEqual 1000000
        }
      }

      "MassTransferTransaction" in {
        val sender: PublicKeyAccount = accountGen.sample.get
        val transferTx = Json.obj(
          "type"            -> 11,
          "version"         -> 1,
          "senderPublicKey" -> sender.publicKeyBase58,
          "transfers" -> Json.arr(
            Json.obj(
              "recipient" -> accountGen.sample.get.toAddress,
              "amount"    -> 1000000
            ),
            Json.obj(
              "recipient" -> accountGen.sample.get.toAddress,
              "amount"    -> 2000000
            )
          )
        )

        val featuresSettings = TestFunctionalitySettings.Enabled.copy(
          preActivatedFeatures = TestFunctionalitySettings.Enabled.preActivatedFeatures + (BlockchainFeature.FeeSwitch.id -> 100)
        )
        val blockchain = mock[Blockchain]
        (blockchain.height _).expects().returning(1).anyNumberOfTimes()
        (blockchain.hasScript _).expects(sender.toAddress).returning(false).anyNumberOfTimes()
        (blockchain.activatedFeatures _).expects().returning(featuresSettings.preActivatedFeatures).anyNumberOfTimes()
        val feeCalculator = FeeCalculator(blockchain, featuresSettings, TestFees.defaultFees.toFeeSettings)

        val route = new TransactionsApiRoute(
          restAPISettings,
          feeCalculator,
          wallet,
          blockchain,
          utx,
          new TestTime,
          None,
          ownerAddress,
          emptyPolicyStorage,
          txBroadcaster,
          NodeMode.Default,
          apiComputationsScheduler
        ).route

        Post(routePath("/calculateFee"), transferTx) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          (responseAs[JsObject] \ "feeAssetId").asOpt[String] shouldBe empty
          (responseAs[JsObject] \ "feeAmount").as[Long] shouldEqual 6000000
        }
      }
    }

    "transfer with Asset fee" - {
      "without sponsorship" in {
        val assetId: ByteStr         = issueGen.sample.get.assetId()
        val sender: PublicKeyAccount = accountGen.sample.get
        val transferTx = Json.obj(
          "type"            -> 4,
          "version"         -> 2,
          "amount"          -> 1000000,
          "feeAssetId"      -> assetId.base58,
          "senderPublicKey" -> sender.publicKeyBase58,
          "recipient"       -> accountGen.sample.get.toAddress
        )

        val featuresSettings = TestFunctionalitySettings.Enabled.copy(
          preActivatedFeatures = TestFunctionalitySettings.Enabled.preActivatedFeatures + (BlockchainFeature.FeeSwitch.id -> 100)
        )
        val blockchain = mock[Blockchain]
        (blockchain.height _).expects().returning(1).anyNumberOfTimes()
        (blockchain.hasScript _).expects(sender.toAddress).returning(false).anyNumberOfTimes()
        (blockchain.activatedFeatures _).expects().returning(featuresSettings.preActivatedFeatures).anyNumberOfTimes()
        val feeCalculator = FeeCalculator(blockchain, featuresSettings, TestFees.defaultFees.toFeeSettings)

        val route = new TransactionsApiRoute(
          restAPISettings,
          feeCalculator,
          wallet,
          blockchain,
          utx,
          new TestTime,
          None,
          ownerAddress,
          emptyPolicyStorage,
          txBroadcaster,
          NodeMode.Default,
          apiComputationsScheduler
        ).route

        Post(routePath("/calculateFee"), transferTx) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          (responseAs[JsObject] \ "feeAssetId").asOpt[String] shouldBe empty
          (responseAs[JsObject] \ "feeAmount").as[Long] shouldEqual 1000000
        }
      }

      "with sponsorship" in {
        val assetId: ByteStr         = issueGen.sample.get.assetId()
        val sender: PublicKeyAccount = accountGen.sample.get
        val transferTx = Json.obj(
          "type"            -> 4,
          "version"         -> 2,
          "amount"          -> 1000000,
          "feeAssetId"      -> assetId.base58,
          "senderPublicKey" -> sender.publicKeyBase58,
          "recipient"       -> accountGen.sample.get.toAddress
        )

        val featuresSettings = TestFunctionalitySettings.Enabled.copy(
          preActivatedFeatures = TestFunctionalitySettings.Enabled.preActivatedFeatures +
            (BlockchainFeature.FeeSwitch.id            -> 0) +
            (BlockchainFeature.SponsoredFeesSupport.id -> 0)
        )
        val blockchain = mock[Blockchain]
        (blockchain.height _).expects().returning(featuresSettings.featureCheckBlocksPeriod).once()
        (blockchain.activatedFeatures _).expects().returning(featuresSettings.preActivatedFeatures).anyNumberOfTimes()
        val baseUnitRatio = 1
        (blockchain.assetDescription _)
          .expects(assetId)
          .returning(Some(AssetDescription(
            issuer = accountGen.sample.get,
            height = 1,
            timestamp = System.currentTimeMillis(),
            name = "foo",
            description = "bar",
            decimals = 8,
            reissuable = false,
            totalVolume = Long.MaxValue,
            script = None,
            sponsorshipIsEnabled = true
          )))
          .anyNumberOfTimes()
        val feeCalculator = FeeCalculator(blockchain, featuresSettings, TestFees.defaultFees.toFeeSettings)

        val route = new TransactionsApiRoute(
          restAPISettings,
          feeCalculator,
          wallet,
          blockchain,
          utx,
          new TestTime,
          None,
          ownerAddress,
          emptyPolicyStorage,
          txBroadcaster,
          NodeMode.Default,
          apiComputationsScheduler
        ).route

        Post(routePath("/calculateFee"), transferTx) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          (responseAs[JsObject] \ "feeAssetId").as[String] shouldBe assetId.base58
          (responseAs[JsObject] \ "feeAmount").as[Long] shouldEqual 0.01.west * baseUnitRatio
        }
      }

      "with sponsorship, smart token and smart account" in {
        val assetId: ByteStr         = issueGen.sample.get.assetId()
        val sender: PublicKeyAccount = accountGen.sample.get
        val transferTx = Json.obj(
          "type"            -> 4,
          "version"         -> 2,
          "amount"          -> 1000000,
          "feeAssetId"      -> assetId.base58,
          "senderPublicKey" -> sender.publicKeyBase58,
          "recipient"       -> accountGen.sample.get.toAddress
        )

        val featuresSettings = TestFunctionalitySettings.Enabled.copy(
          preActivatedFeatures = TestFunctionalitySettings.Enabled.preActivatedFeatures +
            (BlockchainFeature.FeeSwitch.id            -> 0) +
            (BlockchainFeature.SponsoredFeesSupport.id -> 0)
        )

        val blockchain = mock[Blockchain]
        (blockchain.height _).expects().returning(featuresSettings.featureCheckBlocksPeriod).once()
        (blockchain.activatedFeatures _).expects().returning(featuresSettings.preActivatedFeatures).anyNumberOfTimes()
        val baseUnitRatio = 1
        (blockchain.assetDescription _)
          .expects(assetId)
          .returning(Some(AssetDescription(
            issuer = accountGen.sample.get,
            height = 1,
            timestamp = System.currentTimeMillis(),
            name = "foo",
            description = "bar",
            decimals = 8,
            reissuable = false,
            totalVolume = Long.MaxValue,
            script = Some(ScriptV1(V1, TRUE, checkSize = false).explicitGet()),
            sponsorshipIsEnabled = true
          )))
          .anyNumberOfTimes()
        val feeCalculator = FeeCalculator(blockchain, featuresSettings, TestFees.defaultFees.toFeeSettings)

        val route = new TransactionsApiRoute(
          restAPISettings,
          feeCalculator,
          wallet,
          blockchain,
          utx,
          new TestTime,
          None,
          ownerAddress,
          emptyPolicyStorage,
          txBroadcaster,
          NodeMode.Default,
          apiComputationsScheduler
        ).route

        Post(routePath("/calculateFee"), transferTx) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          (responseAs[JsObject] \ "feeAssetId").as[String] shouldBe assetId.base58
          (responseAs[JsObject] \ "feeAmount").as[Long] shouldEqual 0.01.west * baseUnitRatio
        }
      }
    }
  }

  "broadcast routes" - {

    routePath("/signAndBroadcast") - {
      "should return error when node mode='Watcher'" in {
        forAll(randomTransactionGen) { tx =>
          Post(routePath("/signAndBroadcast"), tx.json()) ~> watcherRoute should produce(IllegalWatcherActionError)
        }
      }
    }

    routePath("/broadcast") - {
      "should return error when node mode='Watcher'" in {
        forAll(randomTransactionGen) { tx =>
          Post(routePath("/broadcast"), tx.json()) ~> watcherRoute should produce(IllegalWatcherActionError)
        }
      }
    }
  }

  routePath("/address/{address}/limit/{limit}") - {
    val bytes32StrGen          = bytes32gen.map(Base58.encode)
    val addressGen             = accountGen.map(_.toAddress)
    val dummyInvalidAddressErr = InvalidAddress("")

    "handles parameter errors with corresponding responses" - {
      "invalid address" in {
        forAll(bytes32StrGen) { badAddress =>
          Get(routePath(s"/address/$badAddress")) ~> route ~> check {
            status shouldEqual dummyInvalidAddressErr.code
            (responseAs[JsObject] \ "error").as[Int] shouldEqual dummyInvalidAddressErr.id
          }
        }
      }

      "invalid limit" - {
        def assertInvalidLimit(p: String): Assertion = forAll(accountGen) { a =>
          Get(routePath(p)) ~> route ~> check {
            status shouldEqual StatusCodes.BadRequest
            (responseAs[JsObject] \ "message").as[String] shouldEqual "invalid.limit"
          }
        }

        "limit missing" in {
          forAll(addressGen) { a =>
            assertInvalidLimit(s"/address/$a")
          }
        }

        "only trailing slash after address" in {
          forAll(addressGen) { a =>
            assertInvalidLimit(s"/address/$a/")
          }
        }

        "limit could not be parsed as int" in {
          forAll(addressGen) { a =>
            assertInvalidLimit(s"/address/$a/qwe")
          }
        }

        "limit is too big" in {
          forAll(addressGen, choose(MaxTransactionsPerRequest + 1, Int.MaxValue).label("limitExceeded")) {
            case (address, limit) =>
              Get(routePath(s"/address/$address/limit/$limit")) ~> route should produce(TooBigArrayAllocation)
          }
        }
      }

      "invalid after" in {
        forAll(addressGen, choose(1, MaxTransactionsPerRequest).label("limitCorrect"), invalidBase58Gen) {
          case (address, limit, invalidBase58) =>
            Get(routePath(s"/address/$address/limit/$limit?after=$invalidBase58")) ~> route ~> check {
              status shouldEqual StatusCodes.BadRequest
              (responseAs[JsObject] \ "message").as[String] shouldEqual s"Unable to decode transaction id $invalidBase58"
            }
        }
      }
    }

    "returns 200 if correct params provided" - {
      def routeGen: Gen[Route] =
        Gen.const({
          val b = mock[Blockchain]
          (b.addressTransactions _).expects(*, *, *, *).returning(Right(Seq.empty[(Int, Transaction)])).anyNumberOfTimes()
          new TransactionsApiRoute(
            restAPISettings,
            feeCalculator,
            wallet,
            b,
            utx,
            new TestTime,
            None,
            ownerAddress,
            emptyPolicyStorage,
            txBroadcaster,
            NodeMode.Default,
            apiComputationsScheduler
          ).route
        })

      "address and limit" in {
        forAll(routeGen, addressGen, choose(1, MaxTransactionsPerRequest).label("limitCorrect")) {
          case (r, address, limit) =>
            Get(routePath(s"/address/$address/limit/$limit")) ~> r ~> check {
              status shouldEqual StatusCodes.OK
            }
        }
      }

      "address, limit and after" in {
        forAll(routeGen, addressGen, choose(1, MaxTransactionsPerRequest).label("limitCorrect"), bytes32StrGen) {
          case (r, address, limit, txId) =>
            Get(routePath(s"/address/$address/limit/$limit?after=$txId")) ~> r ~> check {
              status shouldEqual StatusCodes.OK
            }
        }
      }
    }
  }

  routePath("/info/{signature}") - {
    "handles invalid signature" in {
      forAll(invalidBase58Gen) { invalidBase58 =>
        Get(routePath(s"/info/$invalidBase58")) ~> route should produce(InvalidSignature)
      }

      Get(routePath(s"/info/")) ~> route should produce(InvalidSignature)
      Get(routePath(s"/info")) ~> route should produce(InvalidSignature)
    }

    "working properly otherwise" in {
      val txAvailability = for {
        tx     <- randomTransactionGen
        height <- posNum[Int]
      } yield (tx, height)

      forAll(txAvailability) {
        case (tx, height) =>
          (blockchain.transactionInfo _).expects(tx.id()).returning(Some((height, tx))).once()
          Get(routePath(s"/info/${tx.id().base58}")) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[JsValue] shouldEqual tx.json() + ("height" -> JsNumber(height))
          }
      }
    }
  }

  routePath("/unconfirmed") - {
    "returns the list of unconfirmed transactions" in {
      val g = for {
        i <- chooseNum(0, 20)
        t <- listOfN(i, randomTransactionGen)
      } yield t

      forAll(g) { txs =>
        (utx.all _).expects().returns(txs).once()
        Get(routePath("/unconfirmed")) ~> route ~> check {
          val resp = responseAs[Seq[JsValue]]
          for ((r, t) <- resp.zip(txs)) {
            if ((r \ "version").as[Int] == 1) {
              (r \ "signature").as[String] shouldEqual t.proofs.proofs(0).base58
            } else {
              (r \ "proofs").as[Seq[String]] shouldEqual t.proofs.proofs.map(_.base58)
            }
          }
        }
      }
    }
  }

  routePath("/unconfirmed/size") - {
    "returns the size of unconfirmed transactions" in {
      val g = for {
        i <- chooseNum(0, 20)
        t <- listOfN(i, randomTransactionGen)
      } yield t

      forAll(g) { txs =>
        (utx.size _).expects().returns(UtxSize(txs.size, txs.map(_.bytes().length).sum)).once()
        Get(routePath("/unconfirmed/size")) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsValue] shouldEqual Json.obj("size" -> JsNumber(txs.size), "sizeInBytes" -> JsNumber(txs.map(_.bytes().length).sum))
        }
      }
    }
  }

  routePath("/unconfirmed/info/{signature}") - {
    "handles invalid signature" in {
      forAll(invalidBase58Gen) { invalidBase58 =>
        Get(routePath(s"/unconfirmed/info/$invalidBase58")) ~> route should produce(InvalidSignature)
      }

      Get(routePath(s"/unconfirmed/info/")) ~> route should produce(InvalidSignature)
      Get(routePath(s"/unconfirmed/info")) ~> route should produce(InvalidSignature)
    }

    "working properly otherwise" in {
      forAll(randomTransactionGen) { tx =>
        (utx.transactionById _).expects(tx.id()).returns(Some(tx)).once()
        Get(routePath(s"/unconfirmed/info/${tx.id().base58}")) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsValue] shouldEqual tx.json()
        }
      }
    }
  }

  "ExecutedTransactions should not be accepted" - {
    "by /broadcast endpoint" in {
      val executedTxV1 = executedContractV1ParamGen.sample.get
      Post(routePath("/broadcast"), executedTxV1) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        (responseAs[JsObject] \ "message").as[String] shouldBe "ExecutedContract transaction is not allowed to be broadcasted"
      }

      val executedTxV2 = executedContractV2ParamGen.sample.get
      Post(routePath("/broadcast"), executedTxV2) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        (responseAs[JsObject] \ "message").as[String] shouldBe "ExecutedContract transaction is not allowed to be broadcasted"
      }
    }
    "by /sign endpoint as a part of AtomicContainer" in {
      val atomicExecutedGen = for {
        callTx         <- callContractV4ParamGen(Gen.const(Some(AtomicBadge(Some(ownerAddress)))))
        executedCallTx <- executedContractV2ParamGen(ownerAccount, callTx)
      } yield executedCallTx

      val innerTxsJsons = innerAtomicTxs2orMoreGen(atomicExecutedGen)
        .map { txs =>
          txs.map(_.json())
        }
        .sample
        .get

      val atomicWithExecutedTxs = AtomicTransactionRequestV1(ownerAddress.address, innerTxsJsons, timestamp = None)

      (wallet.privateKeyAccount _).expects(*, *).returning(Right(ownerAccount)).once()

      Post(routePath("/sign"), atomicWithExecutedTxs.toJson) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        (responseAs[JsObject] \ "message").as[String] shouldBe "ExecutedContract transaction is not allowed to be broadcasted"
      }
    }
  }
}
