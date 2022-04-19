package com.wavesenterprise

import com.wavesenterprise.account.{Address, AddressScheme, PrivateKeyAccount}
import com.wavesenterprise.crypto.KeyPair
import com.wavesenterprise.crypto.internals.{KeyStoreProvider, WavesAlgorithms}
import com.wavesenterprise.wallet.{HasKeyStoreProvider, Wallet}
import org.scalamock.MockFactoryBase

trait TestWallet extends MockFactoryBase {
  trait TestWalletWithKeyStore extends Wallet with HasKeyStoreProvider {
    override type KeyPairTPE = KeyPair
    override def toKeyStoreProvider: KeyStoreProvider[KeyPair] = ???
  }

  protected val testWallet: TestWalletWithKeyStore = mock[TestWalletWithKeyStore]

  private val chainId = AddressScheme.getAddressSchema.chainId

  protected val walletData: Map[String, PrivateKeyAccount] = (1 to 10).map { _ =>
    val kp    = crypto.generateKeyPair()
    val alias = Address.fromPublicKey(kp.getPublic.getEncoded, chainId, WavesAlgorithms).address
    val pk    = PrivateKeyAccount(kp)
    alias -> pk
  }.toMap
}
