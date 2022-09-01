package com.wavesenterprise.db

import com.google.common.base.Charsets
import com.wavesenterprise.account.{AddressScheme, PrivateKeyAccount}
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.history.DefaultWESettings
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.state.{BlockchainUpdaterImpl, _}
import com.wavesenterprise.transaction.smart.SetScriptTransactionV1
import com.wavesenterprise.transaction.smart.script.{Script, ScriptCompiler}
import com.wavesenterprise.transaction.{BlockchainUpdater, GenesisTransaction}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{TestSchedulers, TransactionGen, WithDB}
import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ScriptCacheTest extends AnyFreeSpec with Matchers with WithDB with TransactionGen {
  val CHAIN_ID   = AddressScheme.getAddressSchema.chainId
  val CACHE_SIZE = 1
  val AMOUNT     = 10000000000L
  val FEE        = 5000000

  def mkScripts(num: Int): List[Script] = {
    (0 until num).map { ind =>
      val (script, _) = ScriptCompiler(
        s"""
           |let ind = $ind
           |true
          """.stripMargin,
        isAssetScript = false
      ).explicitGet()

      script
    }.toList
  }

  def blockGen(scripts: List[Script], t: Time): Gen[(Seq[PrivateKeyAccount], Seq[Block])] = {
    val ts = t.correctedTime()
    Gen
      .listOfN(scripts.length, accountGen)
      .map { accounts =>
        for {
          account <- accounts
          i = accounts.indexOf(account)
        } yield (account, GenesisTransaction.create(account.toAddress, AMOUNT, ts + i).explicitGet())
      }
      .map { ag =>
        val (accounts, genesisTxs) = ag.unzip

        val setScriptTxs =
          (accounts zip scripts)
            .map {
              case (account, script) =>
                SetScriptTransactionV1
                  .selfSigned(
                    CHAIN_ID,
                    account,
                    Some(script),
                    "script".getBytes(Charsets.UTF_8),
                    Array.empty[Byte],
                    FEE,
                    ts + accounts.length + accounts.indexOf(account) + 1
                  )
                  .explicitGet()
            }

        val genesisBlock = TestBlock.create(genesisTxs)

        val nextBlock =
          TestBlock
            .create(
              time = setScriptTxs.last.timestamp + 1,
              ref = genesisBlock.uniqueId,
              txs = setScriptTxs
            )

        (accounts, genesisBlock +: nextBlock +: Nil)
      }
  }

  "ScriptCache" - {
    "return correct script after overflow" in {
      val scripts = mkScripts(CACHE_SIZE * 10)

      withBlockchain(blockGen(scripts, _)) {
        case (accounts, bc) =>
          val allScriptCorrect = (accounts zip scripts)
            .map {
              case (account, script) =>
                val address = account.toAddress

                val scriptFromCache =
                  bc.accountScript(address)
                    .toRight(s"No script for acc: $account")
                    .explicitGet()

                scriptFromCache == script && bc.hasScript(address)
            }
            .forall(identity)

          allScriptCorrect shouldBe true
      }
    }

    "Return correct script after rollback" in {
      val scripts @ List(script) = mkScripts(1)

      withBlockchain(blockGen(scripts, _)) {
        case (List(account), bcu) =>
          bcu.accountScript(account.toAddress) shouldEqual Some(script)

          val lastBlock = bcu.lastBlock.get

          val newScriptTx = SetScriptTransactionV1
            .selfSigned(CHAIN_ID, account, None, "script".getBytes(Charsets.UTF_8), Array.empty[Byte], FEE, lastBlock.timestamp + 1)
            .explicitGet()

          val blockWithEmptyScriptTx = TestBlock
            .create(
              time = lastBlock.timestamp + 2,
              ref = lastBlock.uniqueId,
              txs = Seq(newScriptTx)
            )

          bcu
            .processBlock(blockWithEmptyScriptTx, ConsensusPostAction.NoAction)
            .explicitGet()

          bcu.accountScript(account.toAddress) shouldEqual None
          bcu.removeAfter(lastBlock.uniqueId)
          bcu.accountScript(account.toAddress) shouldEqual Some(script)
      }
    }

  }

  def withBlockchain(gen: Time => Gen[(Seq[PrivateKeyAccount], Seq[Block])])(f: (Seq[PrivateKeyAccount], BlockchainUpdater with NG) => Unit): Unit = {
    val settings0     = DefaultWESettings
    val settings      = settings0.copy(features = settings0.features.copy(autoShutdownOnUnsupportedFeature = false))
    val defaultWriter = new RocksDBWriter(storage, TestFunctionalitySettings.Stub, settings.blockchain.consensus, CACHE_SIZE, 2000)
    val bcu           = new BlockchainUpdaterImpl(defaultWriter, settings, ntpTime, TestSchedulers)
    try {
      val (accounts, blocks) = gen(ntpTime).sample.get

      blocks.foreach { block =>
        bcu.processBlock(block, ConsensusPostAction.NoAction).explicitGet()
      }

      f(accounts, bcu)
      bcu.shutdown()
    } finally {
      bcu.shutdown()
    }
  }
}
