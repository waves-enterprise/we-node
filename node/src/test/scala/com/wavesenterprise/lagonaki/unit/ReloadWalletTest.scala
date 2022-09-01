package com.wavesenterprise.lagonaki.unit

import java.nio.file.Files

import com.wavesenterprise.settings.WalletSettings
import com.wavesenterprise.wallet.Wallet
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ReloadWalletTest extends AnyFunSuite with Matchers with BeforeAndAfter {

  private val walletKeyStore   = Files.createTempFile("wallet_tmp", ".dat").toFile
  private val walletSettings   = WalletSettings(Some(walletKeyStore), "password")
  private val wallet           = Wallet(walletSettings)
  private val reloadableWallet = Wallet(walletSettings)

  val count = 10

  test("reload wallet successfully") {
    checkWalletsEquality()
    wallet.generateNewAccounts(count)
    wallet.publicKeyAccounts.size shouldBe count
    wallet.privateKeyAccounts.size shouldBe count
    reloadableWallet.reload()
    checkWalletsEquality()
  }

  def checkWalletsEquality(): Unit = {
    wallet.publicKeyAccounts shouldBe reloadableWallet.publicKeyAccounts
    wallet.privateKeyAccounts shouldBe reloadableWallet.privateKeyAccounts
  }

  after {
    walletKeyStore.delete()
  }
}
