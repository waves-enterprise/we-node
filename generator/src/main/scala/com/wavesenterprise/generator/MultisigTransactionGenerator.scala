package com.wavesenterprise.generator

import cats.Show
import com.google.common.base.Charsets
import com.wavesenterprise.account.{AddressScheme, PrivateKeyAccount}
import com.wavesenterprise.crypto
import com.wavesenterprise.generator.utils.Gen
import com.wavesenterprise.settings.FeeSettings.FeesEnabled
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.smart.SetScriptTransactionV1
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.transfer.{TransferTransaction, TransferTransactionV2}
import com.wavesenterprise.transaction.{Proofs, Transaction}
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.utils.EitherUtils.EitherExt

class MultisigTransactionGenerator(settings: MultisigTransactionGenerator.Settings, val accounts: Seq[PrivateKeyAccount], fees: FeesEnabled)
    extends TransactionGenerator {

  private val enoughFee = fees.forTxType(TransferTransaction.typeId)

  override def next(): Iterator[Transaction] = {
    generate(settings).toIterator
  }

  private def generate(settings: MultisigTransactionGenerator.Settings): Seq[Transaction] = {

    val bank   = accounts.head
    val owners = Seq(createAccount(), accounts(1), createAccount(), accounts(2), createAccount(), accounts(3), createAccount(), createAccount())

    val totalAmountOnNewAccount = 1.west

    val script: Script = Gen.multiSigScript(owners, 3)

    val setScript = SetScriptTransactionV1
      .selfSigned(AddressScheme.getAddressSchema.chainId,
                  bank,
                  Some(script),
                  "script".getBytes(Charsets.UTF_8),
                  Array.empty[Byte],
                  enoughFee,
                  System.currentTimeMillis())
      .explicitGet()

    val res = Range(0, settings.transactions).map { i =>
      val tx = TransferTransactionV2
        .create(bank,
                None,
                None,
                System.currentTimeMillis(),
                totalAmountOnNewAccount - 2 * enoughFee - i,
                enoughFee,
                owners(1).toAddress,
                Array.emptyByteArray,
                Proofs.empty)
        .explicitGet()
      val signatures = owners.map(crypto.sign(_, tx.bodyBytes())).map(ByteStr(_))
      tx.copy(proofs = Proofs(signatures))
    }

    println(System.currentTimeMillis())
    println(s"${res.length} tx generated")

    if (settings.firstRun) setScript +: res
    else res
  }

  private def createAccount(): PrivateKeyAccount = PrivateKeyAccount(crypto.generateKeyPair())
}

object MultisigTransactionGenerator {
  final case class Settings(transactions: Int, firstRun: Boolean)

  object Settings {
    implicit val toPrintable: Show[Settings] = { x =>
      s"""
        | transactions = ${x.transactions}
        | firstRun = ${x.firstRun}
      """.stripMargin
    }
  }
}
