package com.wavesenterprise.generator

import cats.Show
import com.google.common.base.Charsets
import com.wavesenterprise.account.{AddressScheme, PrivateKeyAccount}
import com.wavesenterprise.generator.OracleTransactionGenerator.Settings
import com.wavesenterprise.generator.utils.Gen
import com.wavesenterprise.settings.FeeSettings.FeesEnabled
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.smart.{SetScriptTransaction, SetScriptTransactionV1}
import com.wavesenterprise.transaction.transfer.{TransferTransaction, TransferTransactionV2}
import com.wavesenterprise.transaction.{DataTransaction, DataTransactionV1, Transaction}
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.utils.EitherUtils.EitherExt

class OracleTransactionGenerator(settings: Settings, val accounts: Seq[PrivateKeyAccount], fees: FeesEnabled) extends TransactionGenerator {
  override def next(): Iterator[Transaction] = generate(settings).toIterator

  private val setScriptTxFee = fees.forTxType(SetScriptTransaction.typeId)
  private val dataTxFee      = fees.forTxType(DataTransaction.typeId)
  private val transferTxFee  = fees.forTxType(TransferTransaction.typeId)

  def generate(settings: Settings): Seq[Transaction] = {
    val oracle = accounts.last

    val scriptedAccount = accounts.head

    val script = Gen.oracleScript(oracle, settings.requiredData)

    val setScript: Transaction =
      SetScriptTransactionV1
        .selfSigned(
          AddressScheme.getAddressSchema.chainId,
          scriptedAccount,
          Some(script),
          "script".getBytes(Charsets.UTF_8),
          Array.empty[Byte],
          setScriptTxFee,
          System.currentTimeMillis()
        )
        .explicitGet()

    val setDataTx: Transaction = DataTransactionV1
      .selfSigned(oracle, oracle, settings.requiredData.toList, System.currentTimeMillis(), dataTxFee)
      .explicitGet()

    val transactions: List[Transaction] =
      List
        .fill(settings.transactions) {
          TransferTransactionV2
            .selfSigned(scriptedAccount, None, None, System.currentTimeMillis(), 1.west, transferTxFee, oracle.toAddress, Array.emptyByteArray)
            .explicitGet()
        }

    setScript +: setDataTx +: transactions
  }
}

object OracleTransactionGenerator {
  final case class Settings(transactions: Int, requiredData: Set[DataEntry[_]])

  object Settings {
    implicit val toPrintable: Show[Settings] = { x =>
      s"Transactions: ${x.transactions}\n" +
        s"DataEntries: ${x.requiredData}\n"
    }
  }
}
