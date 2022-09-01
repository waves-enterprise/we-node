package com.wavesenterprise.wallet

import com.wavesenterprise.account.{Address, PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.internals.{CryptoError, KeyStoreProvider, KeyPair => KeyPairAbstract}
import com.wavesenterprise.settings.WalletSettings
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.utils._
import com.wavesenterprise.wallet.Wallet.WalletWithKeystore

trait Wallet {
  def nonEmpty: Boolean

  final def isEmpty: Boolean = !nonEmpty

  def privateKeyAccounts: List[PrivateKeyAccount]

  def publicKeyAccounts: List[PublicKeyAccount]

  def generateNewAccounts(howMany: Int): Seq[PublicKeyAccount]

  def generateNewAccount(pwd: Option[Array[Char]] = None): Option[PublicKeyAccount]

  def privateKeyAccount(account: Address, password: Option[Array[Char]] = None): Either[ValidationError, PrivateKeyAccount]

  def publicKeyAccount(account: Address): Either[ValidationError, PublicKeyAccount]

  def keyStoreAliases: Seq[String]

  def containsAlias(alias: String): Either[CryptoError, Boolean]

  def reload(): Unit

  def validated(): WalletWithKeystore
}

trait HasKeyStoreProvider {
  type KeyPairTPE <: KeyPairAbstract

  def toKeyStoreProvider: KeyStoreProvider[KeyPairTPE]
}

object Wallet extends ScorexLogging {
  type WalletWithKeystore = Wallet with HasKeyStoreProvider

  implicit class WalletExtension(w: Wallet) {
    def findPrivateKey(addressString: String, password: Option[Array[Char]] = None): Either[ValidationError, PrivateKeyAccount] =
      for {
        acc        <- Address.fromString(addressString).left.map(ValidationError.fromCryptoError)
        privKeyAcc <- w.privateKeyAccount(acc, password)
      } yield privKeyAcc

    def findPublicKey(addressString: String): Either[ValidationError, PublicKeyAccount] = {
      for {
        acc          <- Address.fromString(addressString).left.map(ValidationError.fromCryptoError)
        publicKeyAcc <- w.publicKeyAccount(acc)
      } yield publicKeyAcc
    }
  }

  /**
    * Used only in tests
    */
  def generateNewAccount(): PrivateKeyAccount = {
    val pair = crypto.generateKeyPair()
    PrivateKeyAccount(pair.getPrivate, pair.getPublic)
  }

  def apply(settings: WalletSettings): WalletWithKeystore = new WalletImpl(settings.file, settings.password)

}
