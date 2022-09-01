package com.wavesenterprise.network

import com.wavesenterprise.{BlockGen, TransactionGen}
import com.wavesenterprise.certs.CertChainStoreGen
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MessagesWithCertChainSpec
    extends AnyFreeSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with TransactionGen
    with CertChainStoreGen
    with BlockGen {

  val broadcasedTxGen: Gen[BroadcastedTransaction] =
    for {
      tx <- randomTransactionGen
      txWithSize = TransactionWithSize(tx.bytes().length, tx)
      certStore <- certChainStoreGen
    } yield BroadcastedTransaction(txWithSize, certStore)

  val historyBlockGen: Gen[HistoryBlock] =
    for {
      acc       <- accountGen
      txs       <- Gen.nonEmptyListOf(randomTransactionGen)
      block     <- blockGen(txs, acc)
      certStore <- certChainStoreGen
    } yield HistoryBlock(block, certStore)

  "BroadcastedTxSpec" - {
    import com.wavesenterprise.network.message.MessageSpec.BroadcastedTransactionSpec._

    "serialization roundtrip" in forAll(broadcasedTxGen) { tx =>
      deserializeData(serializeData(tx)).get shouldBe tx
    }
  }

  "HistoryBlock" - {
    import com.wavesenterprise.network.message.MessageSpec.HistoryBlockSpec._

    "serialization roundtrip" in forAll(historyBlockGen) { block =>
      deserializeData(serializeData(block)).get shouldBe block
    }
  }
}
