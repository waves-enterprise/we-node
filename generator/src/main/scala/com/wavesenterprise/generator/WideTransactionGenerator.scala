package com.wavesenterprise.generator

import cats.Show
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.generator.WideTransactionGenerator.Settings
import com.wavesenterprise.generator.utils.Gen
import com.wavesenterprise.settings.FeeSettings.FeesEnabled
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.transfer.TransferTransaction

class WideTransactionGenerator(settings: Settings, accounts: Seq[PrivateKeyAccount], fees: FeesEnabled) extends TransactionGenerator {
  require(accounts.nonEmpty)

  private val limitedRecipientGen = Gen.address(settings.limitDestAccounts)
  private val minFee              = fees.forTxType(TransferTransaction.typeId).max(settings.minFee)
  private val maxFee              = minFee.max(settings.maxFee)

  override def next(): Iterator[Transaction] = {
    Gen.txs(minFee, maxFee, accounts, limitedRecipientGen).take(settings.transactions)
  }

}

object WideTransactionGenerator {

  case class Settings(transactions: Int, limitDestAccounts: Option[Int], minFee: Long, maxFee: Long) {
    require(transactions > 0)
    require(limitDestAccounts.forall(_ > 0))
  }

  object Settings {
    implicit val toPrintable: Show[Settings] = { x =>
      import x._
      s"""transactions per iteration: $transactions
         |number of recipients is ${limitDestAccounts.map(x => s"limited by $x").getOrElse("not limited")}
         |min fee: $minFee
         |max fee: $maxFee""".stripMargin
    }
  }

}
