package com.wavesenterprise.utx

import com.google.common.base.Charsets
import com.wavesenterprise._
import com.wavesenterprise.account.{AddressScheme, PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.acl.TestPermissionValidator.permissionValidatorNoOp
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.database.snapshot.DisabledSnapshot
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.history.{BlockchainFactory, DefaultWESettings}
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lang.v1.compiler.Terms.EXPR
import com.wavesenterprise.lang.v1.compiler.{CompilerContext, CompilerV1}
import com.wavesenterprise.settings._
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.transaction.smart.SetScriptTransactionV1
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.Time
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import squants.information.Information
import squants.information.InformationConversions._

import scala.concurrent.duration._

class UtxPoolSpecification
    extends FreeSpec
    with Matchers
    with MockFactory
    with ScalaCheckPropertyChecks
    with TransactionGen
    with NoShrink
    with WithDB {

  import UtxPoolSpecification._

  private val memorySize                 = Information("100 GiB").get
  private val defaultTransferBytesLength = 158L
  private val oneTransferPoolLimit       = (defaultTransferBytesLength * 1.2).toLong
  private val cleanUpInterval            = 5.minutes

  private val defaultUtxSettings = UtxSettings(cleanUpInterval, allowTransactionsFromSmartAccounts = true, memorySize)

  private def mkBlockchain(senderAccount: PublicKeyAccount, senderBalance: Long): BlockchainUpdater with PrivacyState with NG = {
    val genesisSigner   = accountGen.sample.get
    val genesisSettings = TestHelpers.buildGenesis(genesisSigner, Map(senderAccount -> senderBalance))

    val origSettings = DefaultWESettings
    val settings = origSettings.copy(
      blockchain = BlockchainSettings(
        Custom(
          FunctionalitySettings.TESTNET.copy(
            preActivatedFeatures = Map(
              BlockchainFeature.MassTransfer.id  -> 0,
              BlockchainFeature.SmartAccounts.id -> 0
            )),
          genesisSettings,
          'T'
        ),
        origSettings.blockchain.fees,
        ConsensusSettings.PoSSettings
      ),
      features = origSettings.features.copy(autoShutdownOnUnsupportedFeature = false)
    )
    val (_, bcu) = BlockchainFactory(settings, storage, new TestTime(), TestSchedulers)
    bcu
      .processBlock(Block.genesis(genesisSettings, ConsensusType.PoS).explicitGet(), ConsensusPostAction.NoAction)
      .explicitGet()
    bcu
  }

  private def transfer(sender: PrivateKeyAccount, maxAmount: Long, time: Time): Gen[TransferTransaction] =
    (for {
      amount    <- chooseNum(1, (maxAmount * 0.9).toLong)
      recipient <- addressGen
      fee       <- const((maxAmount * 0.1).toLong)
    } yield TransferTransactionV2.selfSigned(sender, None, None, time.getTimestamp(), amount, fee, recipient, Array.empty[Byte]).explicitGet())
      .label("transferTransaction")

  private def transferWithRecipient(sender: PrivateKeyAccount, recipient: PublicKeyAccount, maxAmount: Long, time: Time): Gen[TransferTransaction] =
    (for {
      amount <- chooseNum(1, (maxAmount * 0.9).toLong)
      fee    <- const((maxAmount * 0.1).toLong)
    } yield
      TransferTransactionV2
        .selfSigned(sender, None, None, time.getTimestamp(), amount, fee, recipient.toAddress, Array.empty[Byte])
        .explicitGet())
      .label("transferWithRecipient")

  private val stateGen = for {
    sender        <- accountGen.label("sender")
    senderBalance <- positiveLongGen.label("senderBalance")
    if senderBalance > 100000L
  } yield {
    val bcu = mkBlockchain(sender, senderBalance)
    (sender, senderBalance, bcu)
  }

  private def createUtxPool(bcu: BlockchainUpdater with NG, time: Time, utxSettings: UtxSettings = defaultUtxSettings): UtxPoolImpl = {
    new UtxPoolImpl(
      time,
      bcu,
      TestBlockchainSettings.Default,
      utxSettings,
      permissionValidatorNoOp(),
      TestSchedulers.utxPoolSyncScheduler,
      DisabledSnapshot
    )(TestSchedulers.utxPoolBackgroundScheduler)
  }

  private val emptyUtxPool = stateGen
    .map {
      case (sender, _, bcu) =>
        val time    = new TestTime()
        val utxPool = createUtxPool(bcu, time)
        (sender, bcu, utxPool)
    }
    .label("emptyUtxPool")

  private val withValidPayments = (for {
    (sender, senderBalance, bcu) <- stateGen
    recipient                    <- accountGen
    time = new TestTime()
    txs <- Gen.nonEmptyListOf(transferWithRecipient(sender, recipient, senderBalance / 10, time))
  } yield {
    val utxPool = createUtxPool(bcu, time)
    txs.foreach(utxPool.putIfNew)
    (sender, bcu, utxPool, time, defaultUtxSettings)
  }).label("withValidPayments")

  private def utxTest(utxSettings: UtxSettings = defaultUtxSettings, txCount: Int = 10)(
      f: (Seq[TransferTransaction], UtxPool, TestTime) => Unit): Unit =
    forAll(stateGen, chooseNum(2, txCount).label("txCount")) {
      case ((sender, senderBalance, bcu), count) =>
        val time = new TestTime()

        forAll(listOfN(count, transfer(sender, senderBalance / 2, time))) { txs =>
          val utx = createUtxPool(bcu, time, utxSettings)
          f(txs, utx, time)
        }
    }

  private val expr: EXPR = {
    val code =
      """let x = 1
        |let y = 2
        |true""".stripMargin

    val compiler = new CompilerV1(CompilerContext.empty)
    compiler.compile(code, List.empty).explicitGet()
  }

  private val script: Script = ScriptV1(expr).explicitGet()

  private def preconditionsGen(lastBlockId: ByteStr, master: PrivateKeyAccount): Gen[Seq[Block]] =
    ntpTimestampGen.map { ts =>
      val setScript = SetScriptTransactionV1
        .selfSigned(AddressScheme.getAddressSchema.chainId,
                    master,
                    Some(script),
                    "script".getBytes(Charsets.UTF_8),
                    Array.empty[Byte],
                    100000,
                    ts + 1)
        .explicitGet()
      Seq(TestBlock.create(ts + 1, lastBlockId, Seq(setScript)))
    }

  private def withScriptedAccount(scEnabled: Boolean): Gen[(PrivateKeyAccount, Long, UtxPoolImpl, Long)] =
    for {
      (sender, senderBalance, bcu) <- stateGen
      preconditions                <- preconditionsGen(bcu.lastBlockId.get, sender)
    } yield {
      preconditions.foreach(b => bcu.processBlock(b, ConsensusPostAction.NoAction).explicitGet())
      val settings = UtxSettings(1.day, allowTransactionsFromSmartAccounts = scEnabled, memorySize)
      val time     = new TestTime()
      val utx      = createUtxPool(bcu, time, settings)
      (sender, senderBalance, utx, bcu.lastBlock.fold(0L)(_.timestamp))
    }

  private def transactionV2Gen(sender: PrivateKeyAccount, ts: Long, feeAmount: Long): Gen[TransferTransactionV2] = accountGen.map { recipient =>
    TransferTransactionV2.selfSigned(sender, None, None, ts, west(1), feeAmount, recipient.toAddress, Array.emptyByteArray).explicitGet()
  }

  "UTX Pool" - {
    "does not add new transactions when memory is nearly reached its limit" in utxTest(
      defaultUtxSettings.copy(memoryLimit = oneTransferPoolLimit.bytes)) { (txs, utx, _) =>
      withUtxCloseable(utx, {
        utx.putIfNew(txs.head) shouldBe 'right
        all(txs.tail.map(t => utx.putIfNew(t))) should produce("pool bytes size limit")
      })
    }

    "does not broadcast the same transaction twice" in utxTest() { (txs, utx, _) =>
      withUtxCloseable(utx, {
        utx.putIfNew(txs.head) shouldBe 'right
        utx.putIfNew(txs.head) shouldBe 'right
      })
    }

    "portfolio" - {
      "returns a count of assets from the state if there is no transaction" in forAll(emptyUtxPool) {
        case (sender, state, utxPool) =>
          withUtxCloseable(
            utxPool, {
              val basePortfolio = state.portfolio(sender.toAddress)

              utxPool.size.size shouldBe 0
              utxPool.size.sizeInBytes shouldBe 0
              val utxPortfolio = utxPool.portfolio(sender.toAddress)

              basePortfolio shouldBe utxPortfolio
            }
          )
      }

      "taking into account unconfirmed transactions" in forAll(withValidPayments) {
        case (sender, state, utxPool, _, _) =>
          withUtxCloseable(
            utxPool, {
              val basePortfolio = state.portfolio(sender.toAddress)

              utxPool.size.size should be > 0
              utxPool.size.sizeInBytes should be > 0L
              val utxPortfolio = utxPool.portfolio(sender.toAddress)

              utxPortfolio.balance should be <= basePortfolio.balance
              utxPortfolio.lease.out should be <= basePortfolio.lease.out
              // should not be changed
              utxPortfolio.lease.in shouldBe basePortfolio.lease.in
              utxPortfolio.assets.foreach {
                case (assetId, count) =>
                  count should be <= basePortfolio.assets.getOrElse(assetId, count)
              }
            }
          )
      }

      "is changed after transactions with these assets are removed" in forAll(withValidPayments) {
        case (sender, _, utxPool, time, _) =>
          withUtxCloseable(
            utxPool, {
              val utxPortfolioBefore = utxPool.portfolio(sender.toAddress)
              val poolSizeBefore     = utxPool.size

              time.advance(CommonValidation.MaxTimePrevBlockOverTransactionDiff * 2)
              utxPool.cleanup()

              poolSizeBefore.size should be > utxPool.size.size
              poolSizeBefore.sizeInBytes should be > utxPool.size.sizeInBytes
              val utxPortfolioAfter = utxPool.portfolio(sender.toAddress)

              utxPortfolioAfter.balance should be >= utxPortfolioBefore.balance
              utxPortfolioAfter.lease.out should be >= utxPortfolioBefore.lease.out
              utxPortfolioAfter.assets.foreach {
                case (assetId, count) =>
                  count should be >= utxPortfolioBefore.assets.getOrElse(assetId, count)
              }
            }
          )
      }
    }

    "smart accounts" - {
      "any transaction from scripted account is not allowed if smartAccounts disabled in utx pool" - {
        def enoughFeeTxWithScriptedAccount: Gen[(UtxPoolImpl, TransferTransaction)] =
          for {
            (sender, senderBalance, utx, ts) <- withScriptedAccount(false)
            feeAmount                        <- const(senderBalance / 2)
            tx                               <- transactionV2Gen(sender, ts + 1, feeAmount)
          } yield (utx, tx)

        "v2" in {
          val (utx2, tx2) = enoughFeeTxWithScriptedAccount.sample.getOrElse(throw new IllegalStateException("NO SAMPLE"))
          withUtxCloseable(utx2, {
            utx2.putIfNew(tx2) should produce("denied from UTX pool")
          })
        }
      }
    }
  }
}

object UtxPoolSpecification {

  def withUtxCloseable(utxPool: UtxPool, f: => Unit): Unit = {
    try {
      f
    } finally {
      utxPool.close()
    }
  }
}
