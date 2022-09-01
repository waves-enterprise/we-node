package com.wavesenterprise.wallet

import java.io.File
import com.wavesenterprise.account.{Address, PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.KeyPair
import com.wavesenterprise.crypto.internals.{CryptoError, KeyStore, KeyStoreProvider}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.wallet.Wallet.WalletWithKeystore
import monix.execution.atomic.{Atomic, AtomicAny}

private[wallet] class WalletImpl(file: Option[File], password: String) extends ScorexLogging with Wallet with HasKeyStoreProvider {

  override type KeyPairTPE = KeyPair

  private val keyStoreRef: AtomicAny[KeyStore[KeyPair]] = Atomic(crypto.keyStore(file, password.toCharArray))

  def toKeyStoreProvider: KeyStoreProvider[KeyPair] = {
    new KeyStoreProvider[KeyPair] {
      def useKeyStore[R](f: KeyStore[KeyPair] => R): R = {
        f(keyStoreRef.get)
      }
    }
  }

  /**
    * A strange method that gets all private keys, that are there without a password
    */
  //TODO: remove???
  override def privateKeyAccounts: List[PrivateKeyAccount] = {
    val keyStore = keyStoreRef.get
    keyStore
      .aliases()
      .iterator
      .flatMap { alias =>
        keyStore
          .getKeyPair(alias, None)
          .map(pair => PrivateKeyAccount(pair.getPrivate, pair.getPublic))
          .toOption
      }
      .toList
  }

  override def publicKeyAccounts: List[PublicKeyAccount] = {
    val keyStore = keyStoreRef.get
    keyStore
      .aliases()
      .iterator
      .filter(Address.fromString(_).isRight)
      .flatMap { alias =>
        /**
          * There's no error handling in Wallet yet, so let's at least log them
          */
        //TODO: add error handling for Wallet
        keyStore.getPublicKey(alias).toOption
      }
      .map(PublicKeyAccount(_))
      .toList
  }

  def nonEmpty: Boolean = keyStoreRef.get.aliases().nonEmpty

  def generateNewAccounts(howMany: Int): Seq[PublicKeyAccount] =
    (1 to howMany).flatMap(_ => generateNewAccount())

  def generateNewAccount(pwd: Option[Array[Char]] = None): Option[PublicKeyAccount] = synchronized {
    keyStoreRef.get.generateAndStore(pwd).map { publicKey =>
      val pka = PublicKeyAccount(publicKey)
      pka
    }
  }

  /**
    * There's some error handling inside CryptoContext, including distinct "Wrong password" or "Expected password" cases
    * TODO: Shouldn't we change Wallet's API to propagate them?
    */
  def privateKeyAccount(account: Address, password: Option[Array[Char]]): Either[ValidationError, PrivateKeyAccount] = {
    keyStoreRef.get.getKeyPair(account.address, password) match {
      case Right(pair) =>
        log.trace(s"Successfully got the PrivateKey from keystore for address '$account'!")
        Right(PrivateKeyAccount(pair.getPrivate, pair.getPublic))

      case Left(cryptoError) =>
        log.info(s"Failed to find the PrivateKey in the keystore for address '$account'! Reason: ${cryptoError.message}")
        Left(ValidationError.MissingSenderPrivateKey)
    }
  }

  override def publicKeyAccount(account: Address): Either[ValidationError, PublicKeyAccount] = {
    val addressWithPrefix = account.address
    keyStoreRef.get.getPublicKey(addressWithPrefix) match {
      case Right(publicKey) =>
        Right(PublicKeyAccount(publicKey))

      case Left(_) =>
        Left(ValidationError.MissingSenderPrivateKey)
    }
  }

  override def keyStoreAliases: Seq[String] = keyStoreRef.get.aliases()

  override def containsAlias(alias: String): Either[CryptoError, Boolean] = keyStoreRef.get.containsAlias(alias)

  override def reload(): Unit = keyStoreRef.transform(_ => crypto.keyStore(file, password.toCharArray))

  override def validated(): WalletWithKeystore = {
    keyStoreRef.get.additionalStorageValidation()
    this
  }
}
