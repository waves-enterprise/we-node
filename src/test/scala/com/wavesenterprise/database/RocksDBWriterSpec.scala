package com.wavesenterprise.database

import com.google.common.base.Charsets
import com.wavesenterprise.account.{Address, AddressScheme, PrivateKeyAccount}
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.history.DefaultWESettings
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lang.v1.compiler.Terms
import com.wavesenterprise.settings.{ConsensusSettings, TestFunctionalitySettings}
import com.wavesenterprise.state.diffs.ENOUGH_AMT
import com.wavesenterprise.state.BlockchainUpdaterImpl
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.smart.SetScriptTransactionV1
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.transfer.{TransferTransaction, TransferTransactionV2}
import com.wavesenterprise.transaction.{GenesisTransaction, Transaction}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.{TestSchedulers, TransactionGen, WithDB}
import org.scalacheck.Gen
import org.scalatest.{FreeSpec, Matchers}

class RocksDBWriterSpec extends FreeSpec with Matchers with WithDB with TransactionGen {
  private val chainId = AddressScheme.getAddressSchema.chainId

  "Slice" - {
    "drops tail" in {
      RocksDBWriter.slice(Seq(10, 7, 4), 7, 10) shouldEqual Seq(10, 7)
    }
    "drops head" in {
      RocksDBWriter.slice(Seq(10, 7, 4), 4, 8) shouldEqual Seq(7, 4)
    }
    "includes Genesis" in {
      RocksDBWriter.slice(Seq(10, 7), 5, 11) shouldEqual Seq(10, 7, 1)
    }
  }
  "Merge" - {
    import TestFunctionalitySettings.Enabled
    "correctly joins height ranges" in {
      val fs     = Enabled.copy(preActivatedFeatures = Map(BlockchainFeature.SmartAccountTrading.id -> 0))
      val writer = new RocksDBWriter(storage, fs, ConsensusSettings.PoSSettings, 100000, 2000)
      writer.merge(Seq(15, 12, 3), Seq(12, 5)) shouldEqual Seq((15, 12), (12, 12), (3, 5))
      writer.merge(Seq(12, 5), Seq(15, 12, 3)) shouldEqual Seq((12, 15), (12, 12), (5, 3))
      writer.merge(Seq(8, 4), Seq(8, 4)) shouldEqual Seq((8, 8), (4, 4))
    }

    "preserves compatibility until SmartAccountTrading feature is activated" in {
      val writer = new RocksDBWriter(storage, Enabled, ConsensusSettings.PoSSettings, 100000, 2000)
      writer.merge(Seq(15, 12, 3), Seq(12, 5)) shouldEqual Seq((15, 12), (12, 12), (3, 12), (3, 5))
      writer.merge(Seq(12, 5), Seq(15, 12, 3)) shouldEqual Seq((12, 15), (12, 12), (5, 12), (5, 3))
      writer.merge(Seq(8, 4), Seq(8, 4)) shouldEqual Seq((8, 8), (4, 8), (4, 4))
    }
  }
  "hasScript" - {
    "returns false if a script was not set" in {
      val writer = new RocksDBWriter(storage, TestFunctionalitySettings.Stub, ConsensusSettings.PoSSettings, 100000, 2000)
      writer.hasScript(accountGen.sample.get.toAddress) shouldBe false
    }

    "returns false if a script was set and then unset" in {
      assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
      resetTest { (_, account) =>
        val writer = new RocksDBWriter(storage, TestFunctionalitySettings.Stub, ConsensusSettings.PoSSettings, 100000, 2000)
        writer.hasScript(account.toAddress) shouldBe false
      }
    }

    "returns true" - {
      "if there is a script in db" in {
        assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
        test { (_, account) =>
          val writer = new RocksDBWriter(storage, TestFunctionalitySettings.Stub, ConsensusSettings.PoSSettings, 100000, 2000)
          writer.hasScript(account.toAddress) shouldBe true
        }
      }

      "if there is a script in cache" in {
        assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
        test { (defaultWriter, account) =>
          defaultWriter.hasScript(account.toAddress) shouldBe true
        }
      }
    }

    def gen(ts: Long): Gen[(PrivateKeyAccount, Seq[Block])] = baseGen(ts).map {
      case (master, blocks) =>
        val nextBlock = TestBlock.create(ts + 1, blocks.last.uniqueId, Seq())
        (master, blocks :+ nextBlock)
    }

    def resetGen(ts: Long): Gen[(PrivateKeyAccount, Seq[Block])] = baseGen(ts).map {
      case (master, blocks) =>
        val unsetScriptTx = SetScriptTransactionV1
          .selfSigned(chainId, master, None, "script".getBytes(Charsets.UTF_8), Array.empty[Byte], 5000000, ts + 1)
          .explicitGet()

        val block1 = TestBlock.create(ts + 1, blocks.last.uniqueId, Seq(unsetScriptTx))
        val block2 = TestBlock.create(ts + 2, block1.uniqueId, Seq())
        (master, blocks ++ List(block1, block2))
    }

    def baseGen(ts: Long): Gen[(PrivateKeyAccount, Seq[Block])] = accountGen.map { master =>
      val genesisTx = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      val setScriptTx = SetScriptTransactionV1
        .selfSigned(chainId, master, Some(ScriptV1(Terms.TRUE).explicitGet()), "script".getBytes(Charsets.UTF_8), Array.empty[Byte], 5000000, ts)
        .explicitGet()

      val block = TestBlock.create(ts, Seq(genesisTx, setScriptTx))
      (master, Seq(block))
    }

    def test(f: (RocksDBWriter, PrivateKeyAccount) => Unit): Unit = baseTest(t => gen(t.correctedTime()))(f)

    def resetTest(f: (RocksDBWriter, PrivateKeyAccount) => Unit): Unit = baseTest(t => resetGen(t.correctedTime()))(f)

  }

  def baseTest(gen: Time => Gen[(PrivateKeyAccount, Seq[Block])])(f: (RocksDBWriter, PrivateKeyAccount) => Unit): Unit = {
    val defaultWriter = new RocksDBWriter(storage, TestFunctionalitySettings.Stub, ConsensusSettings.PoSSettings, 100000, 2000)
    val settings0     = DefaultWESettings
    val settings      = settings0.copy(features = settings0.features.copy(autoShutdownOnUnsupportedFeature = false))
    val bcu           = new BlockchainUpdaterImpl(defaultWriter, settings, ntpTime, TestSchedulers)
    try {
      val (account, blocks) = gen(ntpTime).sample.get

      blocks.foreach { block =>
        bcu.processBlock(block, ConsensusPostAction.NoAction).explicitGet()
      }

      bcu.shutdown()
      f(defaultWriter, account)
    } finally {
      bcu.shutdown()
    }
  }

  "addressTransactions" - {
    def preconditions(ts: Long): Gen[(PrivateKeyAccount, List[Block])] = {
      for {
        master    <- accountGen
        recipient <- accountGen
        genesisBlock = TestBlock
          .create(ts, Seq(GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()))
        block1 = TestBlock
          .create(
            ts + 3,
            genesisBlock.uniqueId,
            Seq(
              createTransfer(master, recipient.toAddress, ts + 1),
              createTransfer(master, recipient.toAddress, ts + 2)
            )
          )
        emptyBlock = TestBlock.create(ts + 5, block1.uniqueId, Seq())
      } yield (master, List(genesisBlock, block1, emptyBlock))
    }

    "return txs in correct ordering without fromId" in {
      baseTest(time => preconditions(time.correctedTime())) { (writer, account) =>
        val txs = writer
          .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), 3, None)
          .explicitGet()

        val ordering = Ordering
          .by[(Int, Transaction), (Int, Long)]({ case (h, t) => (-h, -t.timestamp) })

        txs.length shouldBe 2

        txs.sorted(ordering) shouldBe txs
      }
    }

    "correctly applies transaction type filter" in {
      baseTest(time => preconditions(time.correctedTime())) { (writer, account) =>
        val txs = writer
          .addressTransactions(account.toAddress, Set(GenesisTransaction.typeId), 10, None)
          .explicitGet()

        txs.length shouldBe 1
      }
    }

    "return Left if fromId argument is a non-existent transaction" in {
      baseTest(time => preconditions(time.correctedTime())) { (writer, account) =>
        val nonExistentTxId = GenesisTransaction.create(account.toAddress, ENOUGH_AMT, 1).explicitGet().id()

        val txs = writer
          .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), 3, Some(nonExistentTxId))

        txs shouldBe Left(s"Transaction '$nonExistentTxId' does not exist")
      }
    }

    "return txs in correct ordering starting from a given id" in {
      baseTest(time => preconditions(time.correctedTime())) { (writer, account) =>
        // using pagination
        val firstTx = writer
          .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), 1, None)
          .explicitGet()
          .head

        val secondTx = writer
          .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), 1, Some(firstTx._2.id()))
          .explicitGet()
          .head

        // without pagination
        val txs = writer
          .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), 2, None)
          .explicitGet()

        txs shouldBe Seq(firstTx, secondTx)
      }
    }

    "return an empty Seq when paginating from the last transaction" in {
      baseTest(time => preconditions(time.correctedTime())) { (writer, account) =>
        val txs = writer
          .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), 2, None)
          .explicitGet()

        val txsFromLast = writer
          .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), 2, Some(txs.last._2.id()))
          .explicitGet()

        txs.length shouldBe 2
        txsFromLast shouldBe Seq.empty
      }
    }

    def createTransfer(master: PrivateKeyAccount, recipient: Address, ts: Long): TransferTransaction = {
      TransferTransactionV2
        .selfSigned(master, None, None, ts, ENOUGH_AMT / 5, 1000000, recipient, Array.emptyByteArray)
        .explicitGet()
    }
  }
}
