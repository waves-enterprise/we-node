package com.wavesenterprise.generator

import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.settings.WalletSettings
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.wallet.Wallet

import java.io.File

case class AccountSettings(storage: String, addresses: Seq[String], password: String) {
  lazy val accounts: Seq[PrivateKeyAccount] = {
    val w = Wallet(WalletSettings(Some(new File(storage)), password))
    addresses.map { address =>
      val r = for {
        a   <- Address.fromString(address)
        pka <- w.privateKeyAccount(a)
      } yield pka
      r.explicitGet()
    }
  }

  def find(address: Address): PrivateKeyAccount = {
    val acc = accounts.find(_.toAddress == address)
    acc match {
      case Some(recipient) => recipient
      case None            => throw new RuntimeException(s"Account with address $address is not found in key store")
    }
  }

  def getKeyWithPassword(address: Address, keyPairPassword: Option[String] = None): Option[PrivateKeyAccount] = {
    val w = Wallet(WalletSettings(Some(new File(storage)), password))
    w.privateKeyAccount(address, keyPairPassword.filter(_.nonEmpty).map(_.toCharArray)).toOption
  }
}
