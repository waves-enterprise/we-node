package com.wavesenterprise.lagonaki.unit

import java.io.File
import java.nio.file.Files

import com.wavesenterprise.settings.WalletSettings
import com.wavesenterprise.wallet.Wallet
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class WalletSpecification extends AnyFunSuite with Matchers with BeforeAndAfter {

  var testKeystoreDir: File = _

  before {
    testKeystoreDir = Files.createTempFile("tempKeystore", ".dat").toFile
  }

  test("without file") {
    (the[IllegalStateException] thrownBy Wallet(WalletSettings(None, "cookies"))).getMessage should include {
      "Wallet file is required for Waves crypto"
    }
  }

  test("non-existent file") {
    (the[IllegalStateException] thrownBy Wallet(WalletSettings(Some(new File("non-existent.file")), "cookies"))).getMessage should include {
      "File doesn't exist"
    }
  }

  test("reopening") {
    val walletFile = Some(testKeystoreDir)

    val w1 = Wallet(WalletSettings(walletFile, "cookies"))
    w1.generateNewAccounts(1)

    val w2 = Wallet(WalletSettings(walletFile, "cookies"))
    w2.privateKeyAccounts.nonEmpty shouldBe true
    w2.privateKeyAccounts shouldEqual w1.privateKeyAccounts
  }

  after {
    FileUtils.forceDelete(testKeystoreDir)
  }
}
