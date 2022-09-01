package com.wavesenterprise.state

import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.history.DefaultWESettings
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{ConsensusSettings, TestFunctionalitySettings}
import com.wavesenterprise.state.diffs.ENOUGH_AMT
import com.wavesenterprise.transaction.transfer.{TransferTransaction, TransferTransactionV2}
import com.wavesenterprise.transaction.{GenesisTransaction, Transaction}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.{NTPTime, TestSchedulers, TransactionGen, WithDB}
import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class BlockchainUpdaterImplSpec extends AnyFreeSpec with Matchers with WithDB with NTPTime with TransactionGen {

  def baseTest(gen: Time => Gen[(PrivateKeyAccount, Seq[Block])])(f: (BlockchainUpdaterImpl, PrivateKeyAccount) => Unit): Unit = {
    val defaultWriter = new RocksDBWriter(storage, TestFunctionalitySettings.Stub, ConsensusSettings.PoSSettings, 100000, 2000)
    val settings      = DefaultWESettings
    val bcu           = new BlockchainUpdaterImpl(defaultWriter, settings, ntpTime, TestSchedulers)
    try {
      val (account, blocks) = gen(ntpTime).sample.get

      blocks.foreach { block =>
        bcu.processBlock(block, ConsensusPostAction.NoAction).explicitGet()
      }

      bcu.shutdown()
      f(bcu, account)
    } finally {
      bcu.shutdown()
    }
  }

  def createTransfer(master: PrivateKeyAccount, recipient: Address, ts: Long): TransferTransaction = {
    TransferTransactionV2
      .selfSigned(master, None, None, ts, ENOUGH_AMT / 5, 1000000, recipient, Array.emptyByteArray)
      .explicitGet()
  }

  "addressTransactions" - {
    def preconditions(ts: Long): Gen[(PrivateKeyAccount, List[Block])] = {
      for {
        master    <- accountGen
        recipient <- accountGen
        genesisBlock = TestBlock
          .create(ts, Seq(GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()))
        b1 = TestBlock
          .create(
            ts + 10,
            genesisBlock.uniqueId,
            Seq(
              createTransfer(master, recipient.toAddress, ts + 1),
              createTransfer(master, recipient.toAddress, ts + 2),
              createTransfer(recipient, master.toAddress, ts + 3),
              createTransfer(master, recipient.toAddress, ts + 4),
              createTransfer(master, recipient.toAddress, ts + 5)
            )
          )
        b2 = TestBlock.create(
          ts + 20,
          b1.uniqueId,
          Seq(
            createTransfer(master, recipient.toAddress, ts + 11),
            createTransfer(recipient, master.toAddress, ts + 12),
            createTransfer(recipient, master.toAddress, ts + 13),
            createTransfer(recipient, master.toAddress, ts + 14)
          )
        )
      } yield (master, List(genesisBlock, b1, b2))
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
      baseTest(time => preconditions(time.correctedTime())) { (updater, account) =>
        val nonExistentTxId = GenesisTransaction.create(account.toAddress, ENOUGH_AMT, 1).explicitGet().id()

        val txs = updater
          .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), 3, Some(nonExistentTxId))

        txs shouldBe Left(s"Transaction '$nonExistentTxId' does not exist")
      }
    }

    "without pagination" in {
      baseTest(time => preconditions(time.correctedTime())) { (updater, account) =>
        val txs = updater
          .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), 10, None)
          .explicitGet()

        val ordering = Ordering
          .by[(Int, Transaction), (Int, Long)]({ case (h, t) => (-h, -t.timestamp) })

        txs.length shouldBe 9
        txs.sorted(ordering) shouldBe txs
      }
    }

    "with pagination" - {
      val LIMIT = 8
      def paginationTest(firstPageLength: Int): Unit = {
        baseTest(time => preconditions(time.correctedTime())) { (updater, account) =>
          // using pagination
          val firstPage = updater
            .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), firstPageLength, None)
            .explicitGet()

          val rest = updater
            .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), LIMIT - firstPageLength, Some(firstPage.last._2.id()))
            .explicitGet()

          // without pagination
          val txs = updater
            .addressTransactions(account.toAddress, Set(TransferTransaction.typeId), LIMIT, None)
            .explicitGet()

          (firstPage ++ rest) shouldBe txs
        }
      }

      "after txs is in the middle of ngState" in paginationTest(3)
      "after txs is the last of ngState" in paginationTest(4)
      "after txs is in db" in paginationTest(6)
    }
  }

}
