package com.wavesenterprise.generator

import cats.Show
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.generator.DynamicWideTransactionGenerator.Settings
import com.wavesenterprise.generator.utils.Gen
import com.wavesenterprise.settings.FeeSettings.FeesEnabled
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.transfer.TransferTransaction

import java.util.concurrent.atomic.AtomicReference

class DynamicWideTransactionGenerator(settings: Settings, accounts: Seq[PrivateKeyAccount], fees: FeesEnabled) extends TransactionGenerator {
  require(accounts.nonEmpty)

  private val nextTxsNumber       = new AtomicReference[Double](settings.start)
  private val limitedRecipientGen = Gen.address(settings.limitDestAccounts)
  private val minFee              = fees.forTxType(TransferTransaction.typeId).max(settings.minFee)
  private val maxFee              = minFee.max(settings.maxFee)

  override def next(): Iterator[Transaction] = {
    val currTxsNumber = nextTxsNumber.getAndUpdate { x =>
      val newValue = x + settings.growAdder
      settings.maxTxsPerRequest.foldLeft(newValue)(Math.min(_, _))
    }.toInt

    Gen.txs(minFee, maxFee, accounts, limitedRecipientGen).take(currTxsNumber)
  }

}

object DynamicWideTransactionGenerator {

  case class Settings(start: Int, growAdder: Double, maxTxsPerRequest: Option[Int], limitDestAccounts: Option[Int], minFee: Long, maxFee: Long) {
    require(start >= 1)
  }

  object Settings {
    implicit val toPrintable: Show[Settings] = { x =>
      import x._
      s"""txs at start: $start
         |grow adder: $growAdder
         |max txs: $maxTxsPerRequest
         |limit destination accounts: $limitDestAccounts
         |min fee: $minFee
         |max fee: $maxFee""".stripMargin
    }
  }

}
