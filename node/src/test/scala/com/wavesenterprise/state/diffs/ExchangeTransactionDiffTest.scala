package com.wavesenterprise.state.diffs

import cats.{Order => _, _}
import com.google.common.base.Charsets
import com.wavesenterprise.OrderOps._
import com.wavesenterprise.account.{AddressScheme, PrivateKeyAccount}
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lang.ScriptVersion.Versions.V1
import com.wavesenterprise.lang.directives.DirectiveParser
import com.wavesenterprise.lang.v1.ScriptEstimator
import com.wavesenterprise.lang.v1.compiler.{CompilerContext, CompilerV1}
import com.wavesenterprise.settings.{FunctionalitySettings, TestFees, TestFunctionalitySettings}
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.TransactionDiffer.TransactionValidationError
import com.wavesenterprise.transaction.ValidationError.BalanceErrors
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.assets.exchange._
import com.wavesenterprise.transaction.assets.{IssueTransaction, IssueTransactionV2}
import com.wavesenterprise.transaction.smart.SetScriptTransactionV1
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.smart.script.{Script, ScriptCompiler}
import com.wavesenterprise.transaction.transfer.TransferTransaction
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.utils.SmartContractV1Utils.functionCosts
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatest.{DoNotDiscover, Inside}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.wavesenterprise.state.AssetHolder._

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.Random
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

@DoNotDiscover
class ExchangeTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with Inside with NoShrink {

  val CHAIN_ID: Byte             = AddressScheme.getAddressSchema.chainId
  val MATCHER: PrivateKeyAccount = Wallet.generateNewAccount()

  val fs = TestFunctionalitySettings.Enabled.copy(
    preActivatedFeatures = Map(
      BlockchainFeature.SmartAccounts.id       -> 0,
      BlockchainFeature.SmartAssets.id         -> 0,
      BlockchainFeature.SmartAccountTrading.id -> 0
    )
  )

  val feeSettings = TestFees.defaultFees

  property("preserves WEST invariant, stores match info, rewards matcher") {

    val preconditionsAndExchange: Gen[(GenesisTransaction, GenesisTransaction, IssueTransaction, IssueTransaction, ExchangeTransaction)] = for {
      buyer  <- accountGen
      seller <- accountGen
      ts     <- timestampGen
      gen1: GenesisTransaction = GenesisTransaction.create(buyer.toAddress, ENOUGH_AMT, ts).explicitGet()
      gen2: GenesisTransaction = GenesisTransaction.create(seller.toAddress, ENOUGH_AMT, ts).explicitGet()
      issue1: IssueTransaction <- issueReissueBurnGeneratorP(ENOUGH_AMT, seller).map(_._1)
      issue2: IssueTransaction <- issueReissueBurnGeneratorP(ENOUGH_AMT, buyer).map(_._1)
      maybeAsset1              <- Gen.option(issue1.id())
      maybeAsset2              <- Gen.option(issue2.id()) suchThat (x => x != maybeAsset1)
      exchange                 <- exchangeV2GeneratorP(buyer, seller, maybeAsset1, maybeAsset2)
    } yield (gen1, gen2, issue1, issue2, exchange)

    forAll(preconditionsAndExchange) {
      case (gen1, gen2, issue1, issue2, exchange) =>
        assertDiffAndState(Seq(TestBlock.create(Seq(gen1, gen2, issue1, issue2))), TestBlock.create(Seq(exchange)), fs) {
          case (blockDiff, state) =>
            val totalPortfolioDiff: Portfolio = Monoid.combineAll(blockDiff.portfolios.values)
            totalPortfolioDiff.balance shouldBe 0
            totalPortfolioDiff.effectiveBalance shouldBe 0
            totalPortfolioDiff.assets.values.toSet shouldBe Set(0L)

            blockDiff
              .portfolios(exchange.sender.toAddress.toAssetHolder)
              .balance shouldBe exchange.buyMatcherFee + exchange.sellMatcherFee - exchange.fee
        }
    }
  }

  property("buy WEST without enough money for fee") {
    val preconditions: Gen[(GenesisTransaction, GenesisTransaction, IssueTransaction, ExchangeTransaction)] = for {
      buyer  <- accountGen
      seller <- accountGen
      ts     <- timestampGen
      gen1: GenesisTransaction = GenesisTransaction.create(buyer.toAddress, 1.west, ts).explicitGet()
      gen2: GenesisTransaction = GenesisTransaction.create(seller.toAddress, ENOUGH_AMT, ts).explicitGet()
      issue1   <- issueGen(buyer)
      exchange <- exchangeV2GeneratorP(buyer, seller, None, Some(issue1.id()), fixedMatcherFee = Some(300000))
    } yield {
      (gen1, gen2, issue1, exchange)
    }

    forAll(preconditions) {
      case (gen1, gen2, issue1, exchange) =>
        whenever(exchange.amount > 300000) {
          assertDiffAndState(Seq(TestBlock.create(Seq(gen1, gen2, issue1))), TestBlock.create(Seq(exchange)), fs) {
            case (blockDiff, _) =>
              val totalPortfolioDiff: Portfolio = Monoid.combineAll(blockDiff.portfolios.values)
              totalPortfolioDiff.balance shouldBe 0
              totalPortfolioDiff.effectiveBalance shouldBe 0
              totalPortfolioDiff.assets.values.toSet shouldBe Set(0L)

              blockDiff
                .portfolios(exchange.sender.toAddress.toAssetHolder)
                .balance shouldBe exchange.buyMatcherFee + exchange.sellMatcherFee - exchange.fee
          }
        }
    }
  }

  def createExTx(buy: Order, sell: Order, price: Long, matcher: PrivateKeyAccount, ts: Long): Either[ValidationError, ExchangeTransaction] = {
    val mf     = buy.matcherFee
    val amount = math.min(buy.amount, sell.amount)
    ExchangeTransactionV2.create(
      matcher = matcher,
      buyOrder = buy.asInstanceOf[OrderV1],
      sellOrder = sell.asInstanceOf[OrderV1],
      amount = amount,
      price = price,
      buyMatcherFee = (BigInt(mf) * amount / buy.amount).toLong,
      sellMatcherFee = (BigInt(mf) * amount / sell.amount).toLong,
      fee = buy.matcherFee,
      timestamp = ts
    )
  }

  property("small fee cases") {
    val MatcherFee = 300000L
    val Ts         = 1000L

    val preconditions: Gen[(PrivateKeyAccount, PrivateKeyAccount, PrivateKeyAccount, GenesisTransaction, GenesisTransaction, IssueTransaction)] =
      for {
        buyer   <- accountGen
        seller  <- accountGen
        matcher <- accountGen
        ts      <- timestampGen
        gen1: GenesisTransaction = GenesisTransaction.create(buyer.toAddress, ENOUGH_AMT, ts).explicitGet()
        gen2: GenesisTransaction = GenesisTransaction.create(seller.toAddress, ENOUGH_AMT, ts).explicitGet()
        issue <- issueGen(seller)
      } yield (buyer, seller, matcher, gen1, gen2, issue)

    forAll(preconditions, priceGen) {
      case ((buyer, seller, matcher, gen1, gen2, issue1), price) =>
        val assetPair = AssetPair(Some(issue1.id()), None)
        val buy       = Order.buy(buyer, matcher, assetPair, 1000000L, price, Ts, Ts + 1, MatcherFee)
        val sell      = Order.sell(seller, matcher, assetPair, 1L, price, Ts, Ts + 1, MatcherFee)
        val tx        = createExTx(buy, sell, price, matcher, Ts).explicitGet()
        assertDiffAndState(Seq(TestBlock.create(Seq(gen1, gen2, issue1))), TestBlock.create(Seq(tx)), fs) {
          case (blockDiff, state) =>
            blockDiff.portfolios(tx.sender.toAddress.toAssetHolder).balance shouldBe tx.buyMatcherFee + tx.sellMatcherFee - tx.fee
            state.addressBalance(tx.sender.toAddress) shouldBe 0L
        }
    }
  }

  property("Not enough balance") {
    val MatcherFee = 300000L
    val Ts         = 1000L

    val preconditions: Gen[(PrivateKeyAccount, PrivateKeyAccount, PrivateKeyAccount, GenesisTransaction, GenesisTransaction, IssueTransaction)] =
      for {
        buyer   <- accountGen
        seller  <- accountGen
        matcher <- accountGen
        ts      <- timestampGen
        gen1: GenesisTransaction = GenesisTransaction.create(buyer.toAddress, ENOUGH_AMT, ts).explicitGet()
        gen2: GenesisTransaction = GenesisTransaction.create(seller.toAddress, ENOUGH_AMT, ts).explicitGet()
        issue1 <- issueGen(seller, fixedQuantity = Some(1000L))
      } yield (buyer, seller, matcher, gen1, gen2, issue1)

    forAll(preconditions, priceGen) {
      case ((buyer, seller, matcher, gen1, gen2, issue1), price) =>
        val assetPair = AssetPair(Some(issue1.id()), None)
        val buy       = Order.buy(buyer, matcher, assetPair, issue1.quantity + 1, price, Ts, Ts + 1, MatcherFee)
        val sell      = Order.sell(seller, matcher, assetPair, issue1.quantity + 1, price, Ts, Ts + 1, MatcherFee)
        val tx        = createExTx(buy, sell, price, matcher, Ts).explicitGet()
        assertDiffEither(Seq(TestBlock.create(Seq(gen1, gen2, issue1))), TestBlock.create(Seq(tx)), fs) { totalDiffEi =>
          inside(totalDiffEi) {
            case Left(TransactionValidationError(BalanceErrors(errs, _), _)) =>
              errs should contain key seller.toAddress
          }
        }
    }
  }

  property("Diff for ExchangeTransaction works as expected and doesn't use rounding inside") {
    val MatcherFee = 300000L
    val Ts         = 1000L

    val preconditions
      : Gen[(PrivateKeyAccount, PrivateKeyAccount, PrivateKeyAccount, GenesisTransaction, GenesisTransaction, GenesisTransaction, IssueTransaction)] =
      for {
        buyer   <- accountGen
        seller  <- accountGen
        matcher <- accountGen
        ts      <- timestampGen
        gen1: GenesisTransaction = GenesisTransaction.create(buyer.toAddress, ENOUGH_AMT, ts).explicitGet()
        gen2: GenesisTransaction = GenesisTransaction.create(seller.toAddress, ENOUGH_AMT, ts).explicitGet()
        gen3: GenesisTransaction = GenesisTransaction.create(matcher.toAddress, ENOUGH_AMT, ts).explicitGet()
        issue1 <- issueGen(buyer, fixedQuantity = Some(Long.MaxValue))
      } yield (buyer, seller, matcher, gen1, gen2, gen3, issue1)

    val (buyer, seller, matcher, gen1, gen2, gen3, issue1) = preconditions.sample.get
    val assetPair                                          = AssetPair(None, Some(issue1.id()))

    val buy  = Order.buy(buyer, matcher, assetPair, 3100000000L, 238, Ts, Ts + 1, MatcherFee, version = 1: Byte).asInstanceOf[OrderV1]
    val sell = Order.sell(seller, matcher, assetPair, 425532L, 235, Ts, Ts + 1, MatcherFee, version = 1: Byte).asInstanceOf[OrderV1]
    val tx = ExchangeTransactionV2
      .create(matcher = matcher,
              buyOrder = buy,
              sellOrder = sell,
              amount = 425532,
              price = 238,
              buyMatcherFee = 41,
              sellMatcherFee = 300000,
              fee = buy.matcherFee,
              timestamp = Ts)
      .explicitGet()

    assertDiffEither(Seq(TestBlock.create(Seq(gen1, gen2, gen3, issue1))), TestBlock.create(Seq(tx))) { totalDiffEi =>
      inside(totalDiffEi) {
        case Right(diff) =>
          import diff.portfolios
          portfolios(buyer.toAddress.toAssetHolder).balance shouldBe (-41L + 425532L)
          portfolios(seller.toAddress.toAssetHolder).balance shouldBe (-300000L - 425532L)
          portfolios(matcher.toAddress.toAssetHolder).balance shouldBe (+41L + 300000L - tx.fee)
      }
    }
  }

  val fsV2 = createSettings(
    BlockchainFeature.SmartAccounts       -> 0,
    BlockchainFeature.SmartAccountTrading -> 0,
    BlockchainFeature.SmartAssets         -> 0,
    BlockchainFeature.FeeSwitch           -> 0
  )

  private def createSettings(preActivatedFeatures: (BlockchainFeature, Int)*): FunctionalitySettings =
    TestFunctionalitySettings.Enabled
      .copy(
        preActivatedFeatures = preActivatedFeatures.map { case (k, v) => k.id -> v }(collection.breakOut),
        blocksForFeatureActivation = 1,
        featureCheckBlocksPeriod = 2
      )

  property(s"Exchange transaction with scripted matcher and orders needs extra fee") {
    val allValidP = smartTradePreconditions(
      scriptGen("Order", true),
      scriptGen("Order", true),
      scriptGen("ExchangeTransaction", true)
    )

    forAll(allValidP) {
      case (genesis, transfers, issueAndScripts, etx) =>
        val enoughFee = feeSettings.forTxType(ExchangeTransaction.typeId)
        val smallFee  = enoughFee - 1
        val exchangeWithSmallFee = ExchangeTransactionV2
          .create(MATCHER, etx.buyOrder, etx.sellOrder, 1000000, 1000000, 0, 0, smallFee, etx.timestamp)
          .explicitGet()

        val exchangeWithEnoughFee = ExchangeTransactionV2
          .create(MATCHER, etx.buyOrder, etx.sellOrder, 1000000, 1000000, 0, 0, enoughFee, etx.timestamp)
          .explicitGet()

        val preconBlocks = Seq(
          TestBlock.create(Seq(genesis)),
          TestBlock.create(transfers),
          TestBlock.create(issueAndScripts)
        )

        val blockWithSmallFeeETx  = TestBlock.create(Seq(exchangeWithSmallFee))
        val blockWithEnoughFeeETx = TestBlock.create(Seq(exchangeWithEnoughFee))

        assertLeft(preconBlocks, blockWithSmallFeeETx, fsV2)("does not exceed minimal value of")
        assertDiffEither(preconBlocks, blockWithEnoughFeeETx, fsV2)(_ shouldBe 'right)
    }
  }

  property("ExchangeTransactions valid if all scripts succeeds") {
    val allValidP = smartTradePreconditions(
      scriptGen("Order", true),
      scriptGen("Order", true),
      scriptGen("ExchangeTransaction", true)
    )

    forAll(allValidP) {
      case (genesis, transfers, issueAndScripts, exchangeTx) =>
        val preconBlocks = Seq(
          TestBlock.create(Seq(genesis)),
          TestBlock.create(transfers),
          TestBlock.create(issueAndScripts)
        )
        assertDiffEither(preconBlocks, TestBlock.create(Seq(exchangeTx)), fsV2) { diff =>
          diff.isRight shouldBe true
        }
    }
  }

  property("ExchangeTransactions invalid if buyer scripts fails") {
    val failedOrderScript = smartTradePreconditions(
      scriptGen("Order", false),
      scriptGen("Order", true),
      scriptGen("ExchangeTransaction", true)
    )

    forAll(failedOrderScript) {
      case (genesis, transfers, issueAndScripts, exchangeTx) =>
        val preconBlocks = Seq(TestBlock.create(Seq(genesis)), TestBlock.create(transfers), TestBlock.create(issueAndScripts))
        assertLeft(preconBlocks, TestBlock.create(Seq(exchangeTx)), fsV2)("TransactionNotAllowedByScript")
    }
  }

  property("ExchangeTransactions invalid if seller scripts fails") {
    val failedOrderScript = smartTradePreconditions(
      scriptGen("Order", true),
      scriptGen("Order", false),
      scriptGen("ExchangeTransaction", true)
    )

    forAll(failedOrderScript) {
      case (genesis, transfers, issueAndScripts, exchangeTx) =>
        val preconBlocks = Seq(TestBlock.create(Seq(genesis)), TestBlock.create(transfers), TestBlock.create(issueAndScripts))
        assertLeft(preconBlocks, TestBlock.create(Seq(exchangeTx)), fsV2)("TransactionNotAllowedByScript")
    }
  }

  property("ExchangeTransactions invalid if matcher script fails") {
    val failedMatcherScript = smartTradePreconditions(
      scriptGen("Order", true),
      scriptGen("Order", true),
      scriptGen("ExchangeTransaction", false)
    )

    forAll(failedMatcherScript) {
      case (genesis, transfers, issueAndScripts, exchangeTx) =>
        val preconBlocks = Seq(TestBlock.create(Seq(genesis)), TestBlock.create(transfers), TestBlock.create(issueAndScripts))
        assertLeft(preconBlocks, TestBlock.create(Seq(exchangeTx)), fsV2)("TransactionNotAllowedByScript")
    }
  }

  def badSign(): ByteStr = ByteStr((1 to SignatureLength).map(_ => Random.nextPrintableChar().toByte).toArray)

  property("ExchangeTransaction invalid if order signature invalid") {
    forAll(simpleTradePreconditions) {
      case (gen1, gen2, issue1, issue2, exchange) =>
        val exchangeWithResignedOrder = exchange.copy(sellOrder = exchange.sellOrder.updateProofs(Proofs(Seq(badSign()))))

        val preconBlocks = Seq(
          TestBlock.create(Seq(gen1, gen2)),
          TestBlock.create(Seq(issue1, issue2))
        )

        val blockWithExchange = TestBlock.create(Seq(exchangeWithResignedOrder))

        assertLeft(preconBlocks, blockWithExchange, fs)("Script doesn't exist and proof doesn't validate as signature")
    }
  }

  property("ExchangeTransaction invalid if order contains more than one proofs") {
    forAll(simpleTradePreconditions) {
      case (gen1, gen2, issue1, issue2, exchange) =>
        val newProofs = Proofs(
          Seq(
            badSign(),
            badSign()
          )
        )

        val exchangeWithResignedOrder = exchange.copy(buyOrder = exchange.sellOrder.updateProofs(newProofs))

        val preconBlocks = Seq(
          TestBlock.create(Seq(gen1, gen2)),
          TestBlock.create(Seq(issue1, issue2))
        )

        val blockWithExchange = TestBlock.create(Seq(exchangeWithResignedOrder))

        assertLeft(preconBlocks, blockWithExchange, fs)("Script doesn't exist and proof doesn't validate as signature")
    }
  }

  property("Disable use OrderV1 on SmartAccount") {
    val enoughFee        = 100000000
    val script           = "true"
    val txScriptCompiled = ScriptCompiler(script, isAssetScript = false).explicitGet()._1

    val sellerScript = Some(ScriptCompiler(script, isAssetScript = false).explicitGet()._1)
    val buyerScript  = Some(ScriptCompiler(script, isAssetScript = false).explicitGet()._1)

    val chainId = AddressScheme.getAddressSchema.chainId

    forAll(for {
      buyer  <- accountGen
      seller <- accountGen
      ts     <- timestampGen
      genesis = GenesisTransaction.create(MATCHER.toAddress, Long.MaxValue, ts).explicitGet()
      tr1     = createWestTransfer(MATCHER, buyer.toAddress, Long.MaxValue / 3, enoughFee, ts + 1).explicitGet()
      tr2     = createWestTransfer(MATCHER, seller.toAddress, Long.MaxValue / 3, enoughFee, ts + 2).explicitGet()
      asset1 = IssueTransactionV2
        .selfSigned(chainId, buyer, "Asset#1".getBytes(UTF_8), "".getBytes(UTF_8), 1000000, 8, false, enoughFee, ts + 3, None)
        .explicitGet()
      asset2 = IssueTransactionV2
        .selfSigned(chainId, seller, "Asset#2".getBytes(UTF_8), "".getBytes(UTF_8), 1000000, 8, false, enoughFee, ts + 4, None)
        .explicitGet()
      setMatcherScript = SetScriptTransactionV1
        .selfSigned(CHAIN_ID, MATCHER, Some(txScriptCompiled), "script".getBytes(Charsets.UTF_8), Array.empty[Byte], enoughFee, ts + 5)
        .explicitGet()
      setSellerScript = SetScriptTransactionV1
        .selfSigned(CHAIN_ID, seller, sellerScript, "script".getBytes(Charsets.UTF_8), Array.empty[Byte], enoughFee, ts + 6)
        .explicitGet()
      setBuyerScript = SetScriptTransactionV1
        .selfSigned(CHAIN_ID, buyer, buyerScript, "script".getBytes(Charsets.UTF_8), Array.empty[Byte], enoughFee, ts + 7)
        .explicitGet()
      assetPair = AssetPair(Some(asset1.id()), Some(asset2.id()))
      o1 <- Gen.oneOf(
        OrderV1.buy(seller, MATCHER, assetPair, 1000000, 1000000, ts + 8, ts + 10000, enoughFee),
        OrderV2.buy(seller, MATCHER, assetPair, 1000000, 1000000, ts + 8, ts + 10000, enoughFee)
      )
      o2 <- Gen.oneOf(
        OrderV1.sell(buyer, MATCHER, assetPair, 1000000, 1000000, ts + 9, ts + 10000, enoughFee),
        OrderV2.sell(buyer, MATCHER, assetPair, 1000000, 1000000, ts + 9, ts + 10000, enoughFee)
      )
      exchangeTx = {
        ExchangeTransactionV2
          .create(MATCHER, o1, o2, 1000000, 1000000, enoughFee, enoughFee, enoughFee, ts + 10)
          .explicitGet()
      }
    } yield {
      val pretest = Seq(TestBlock.create(Seq(genesis)),
                        TestBlock.create(Seq(tr1, tr2)),
                        TestBlock.create(Seq(asset1, asset2, setMatcherScript, setSellerScript, setBuyerScript)))
      val test = TestBlock.create(Seq(exchangeTx))
      if (o1.isInstanceOf[OrderV2] && o2.isInstanceOf[OrderV2]) {
        assertDiffEither(pretest, test, fs) { diff =>
          diff.isRight shouldBe true
        }
      } else {
        assertLeft(pretest, test, fs)("Can't process order with signature from scripted account")
      }
    }) { _ =>
      ()
    }
  }

  def scriptGen(caseType: String, v: Boolean): String = {
    s"""
       |match tx {
       | case o: $caseType => $v
       | case _ => ${!v}
       |}
      """.stripMargin
  }

  def compile(scriptText: String, ctx: CompilerContext): Either[String, (Script, Long)] = {
    val compiler = new CompilerV1(ctx)

    val directives = DirectiveParser(scriptText)

    val scriptWithoutDirectives =
      scriptText.linesIterator
        .filter(str => !str.contains("{-#"))
        .mkString("\n")

    for {
      expr       <- compiler.compile(scriptWithoutDirectives, directives)
      script     <- ScriptV1(expr)
      complexity <- ScriptEstimator(ctx.varDefs.keySet, functionCosts(V1), expr)
    } yield (script, complexity)
  }

  def smartTradePreconditions(buyerScriptSrc: String,
                              sellerScriptSrc: String,
                              txScript: String): Gen[(GenesisTransaction, List[TransferTransaction], List[Transaction], ExchangeTransaction)] = {
    val enoughFee = 100000000

    val txScriptCompiled = ScriptCompiler(txScript, isAssetScript = false).explicitGet()._1

    val sellerScript = Some(ScriptCompiler(sellerScriptSrc, isAssetScript = false).explicitGet()._1)
    val buyerScript  = Some(ScriptCompiler(buyerScriptSrc, isAssetScript = false).explicitGet()._1)

    val chainId = AddressScheme.getAddressSchema.chainId

    for {
      buyer  <- accountGen
      seller <- accountGen
      ts     <- timestampGen
      genesis = GenesisTransaction.create(MATCHER.toAddress, Long.MaxValue, ts).explicitGet()
      tr1     = createWestTransfer(MATCHER, buyer.toAddress, Long.MaxValue / 3, enoughFee, ts + 1).explicitGet()
      tr2     = createWestTransfer(MATCHER, seller.toAddress, Long.MaxValue / 3, enoughFee, ts + 2).explicitGet()
      asset1 = IssueTransactionV2
        .selfSigned(chainId, buyer, "Asset#1".getBytes, "".getBytes, 1000000, 8, false, enoughFee, ts + 3, None)
        .explicitGet()
      asset2 = IssueTransactionV2
        .selfSigned(chainId, seller, "Asset#2".getBytes, "".getBytes, 1000000, 8, false, enoughFee, ts + 4, None)
        .explicitGet()
      setMatcherScript = SetScriptTransactionV1
        .selfSigned(CHAIN_ID, MATCHER, Some(txScriptCompiled), "script".getBytes(Charsets.UTF_8), Array.empty[Byte], enoughFee, ts + 5)
        .explicitGet()
      setSellerScript = SetScriptTransactionV1
        .selfSigned(CHAIN_ID, seller, sellerScript, "script".getBytes(Charsets.UTF_8), Array.empty[Byte], enoughFee, ts + 6)
        .explicitGet()
      setBuyerScript = SetScriptTransactionV1
        .selfSigned(CHAIN_ID, buyer, buyerScript, "script".getBytes(Charsets.UTF_8), Array.empty[Byte], enoughFee, ts + 7)
        .explicitGet()
      assetPair = AssetPair(Some(asset1.id()), Some(asset2.id()))
      o1        = OrderV2.buy(seller, MATCHER, assetPair, 1000000, 1000000, ts + 8, ts + 10000, enoughFee)
      o2        = OrderV2.sell(buyer, MATCHER, assetPair, 1000000, 1000000, ts + 9, ts + 10000, enoughFee)
      exchangeTx = {
        ExchangeTransactionV2
          .create(MATCHER, o1, o2, 1000000, 1000000, enoughFee, enoughFee, enoughFee, ts + 10)
          .explicitGet()
      }
    } yield (genesis, List(tr1, tr2), List(asset1, asset2, setMatcherScript, setSellerScript, setBuyerScript), exchangeTx)
  }

  val simpleTradePreconditions: Gen[(GenesisTransaction, GenesisTransaction, IssueTransaction, IssueTransaction, ExchangeTransactionV2)] = for {
    buyer  <- accountGen
    seller <- accountGen
    ts     <- timestampGen
    gen1: GenesisTransaction = GenesisTransaction.create(buyer.toAddress, ENOUGH_AMT, ts).explicitGet()
    gen2: GenesisTransaction = GenesisTransaction.create(seller.toAddress, ENOUGH_AMT, ts).explicitGet()
    issue1: IssueTransaction <- issueReissueBurnGeneratorP(ENOUGH_AMT, seller).map(_._1)
    issue2: IssueTransaction <- issueReissueBurnGeneratorP(ENOUGH_AMT, buyer).map(_._1)
    maybeAsset1              <- Gen.option(issue1.id())
    maybeAsset2              <- Gen.option(issue2.id()) suchThat (x => x != maybeAsset1)
    exchange                 <- exchangeV2GeneratorP(buyer, seller, maybeAsset1, maybeAsset2)
  } yield (gen1, gen2, issue1, issue2, exchange)

}
