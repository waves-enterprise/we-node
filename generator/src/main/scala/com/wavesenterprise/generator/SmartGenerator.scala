package com.wavesenterprise.generator

import cats.Show
import com.google.common.base.Charsets
import com.wavesenterprise.account.{AddressScheme, PrivateKeyAccount}
import com.wavesenterprise.generator.utils.Gen
import com.wavesenterprise.settings.FeeSettings.FeesEnabled
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.assets.exchange.{AssetPair, ExchangeTransaction, ExchangeTransactionV2, OrderV2}
import com.wavesenterprise.transaction.smart.SetScriptTransactionV1
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.transfer.{TransferTransaction, TransferTransactionV2}
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.utils.EitherUtils.EitherExt

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration._

class SmartGenerator(settings: SmartGenerator.Settings, val accounts: Seq[PrivateKeyAccount], fees: FeesEnabled) extends TransactionGenerator {

  private val transferTxFee = fees.forTxType(TransferTransaction.typeId)
  private val exchangeTxFee = 0.011.west.max(fees.forTxType(ExchangeTransaction.typeId))

  private def r                                   = ThreadLocalRandom.current
  private def randomFrom[T](c: Seq[T]): Option[T] = if (c.nonEmpty) Some(c(r.nextInt(c.size))) else None

  def ts = System.currentTimeMillis()

  override def next(): Iterator[Transaction] = {
    generate(settings).toIterator
  }

  private def generate(settings: SmartGenerator.Settings): Seq[Transaction] = {
    val bank = randomFrom(accounts).get

    val script: Script = Gen.script(settings.complexity)

    val setScripts = Range(0, settings.scripts) flatMap (_ =>
      accounts.map { i =>
        SetScriptTransactionV1
          .selfSigned(AddressScheme.getAddressSchema.chainId,
                      i,
                      Some(script),
                      "script name".getBytes(Charsets.UTF_8),
                      Array.empty[Byte],
                      1.west,
                      System.currentTimeMillis())
          .explicitGet()
      })

    val txs = Range(0, settings.transfers).map { i =>
      TransferTransactionV2
        .selfSigned(bank, None, None, System.currentTimeMillis(), 1.west - 2 * transferTxFee, transferTxFee, bank.toAddress, Array.emptyByteArray)
        .explicitGet()
    }

    val extxs = Range(0, settings.exchange).map { i =>
      val matcher         = randomFrom(accounts).get
      val seller          = randomFrom(accounts).get
      val buyer           = randomFrom(accounts).get
      val asset           = randomFrom(settings.assets.toSeq)
      val tradeAssetIssue = ByteStr.decodeBase58(asset.get).toOption
      val pair            = AssetPair(None, tradeAssetIssue)
      val sellOrder       = OrderV2.sell(seller, matcher, pair, 100000000L, 1, ts, ts + 30.days.toMillis, 0.003.west)
      val buyOrder        = OrderV2.buy(buyer, matcher, pair, 100000000L, 1, ts, ts + 1.day.toMillis, 0.003.west)

      ExchangeTransactionV2.create(matcher, buyOrder, sellOrder, 100000000, 1, 0.003.west, 0.003.west, exchangeTxFee, ts).explicitGet()
    }

    setScripts ++ txs ++ extxs
  }

}

object SmartGenerator {
  final case class Settings(scripts: Int, transfers: Int, complexity: Boolean, exchange: Int, assets: Set[String]) {
    require(scripts >= 0)
    require(transfers >= 0)
    require(exchange >= 0)
  }

  object Settings {
    implicit val toPrintable: Show[Settings] = { x =>
      import x._
      s"""
         | set-scripts = $scripts
         | transfers = $transfers
         | complexity = $complexity
         | exchange = $exchange
         | assets = $assets
      """.stripMargin
    }

  }
}
