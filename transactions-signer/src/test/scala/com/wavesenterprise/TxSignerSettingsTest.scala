package com.wavesenterprise

import java.io.File
import java.nio.file.Files

import org.scalatest.{EitherValues, FreeSpec, Matchers}
import ch.qos.logback.classic.Level

class TxSignerSettingsTest extends FreeSpec with Matchers with EitherValues {

  private val tmpFilePath     = Files.createTempFile("temporarily-file", null)
  private val tmpExistentFile = tmpFilePath.toFile

  private val tmpDirPath     = Files.createTempDirectory("temporarily-dir")
  private val tmpExistentDir = tmpDirPath.toFile

  val validSettings = TxSignerSettings(
    inputTransactionsFile = tmpExistentFile,
    keystoreFile = tmpExistentFile,
    keystorePassword = "some_pass",
    cryptoType = CryptoType.Waves,
    chainByte = 5.toByte,
    outputTransactionsFile = new File("output.json"),
    loggingLevel = Level.INFO
  )

  "valid settings" in {
    validSettings.validate().right.value shouldBe ((): Unit)
  }

  "invalid settings if input file is a directory" in {
    val invalidSettings = validSettings.copy(inputTransactionsFile = tmpExistentDir)
    invalidSettings.validate().left.value shouldBe s"input file '${tmpExistentDir.getAbsolutePath}' is a directory or doesn't exists"
  }

  "invalid settings if input file doesn't exists" in {
    val nonExistentFile = new File("non-existent-file")
    val invalidSettings = validSettings.copy(inputTransactionsFile = nonExistentFile)
    invalidSettings.validate().left.value shouldBe s"input file '${nonExistentFile.getAbsolutePath}' is a directory or doesn't exists"
  }

  "invalid settings if output file exists" in {
    val invalidSettings = validSettings.copy(outputTransactionsFile = tmpExistentFile)
    invalidSettings.validate().left.value shouldBe s"output file '${tmpExistentFile.getAbsolutePath}' exists"
  }

  "invalid settings if keystore is a directory with waves crypto" in {
    val invalidSettings = validSettings.copy(cryptoType = CryptoType.Waves, keystoreFile = tmpExistentDir)
    invalidSettings
      .validate()
      .left
      .value shouldBe s"bad crypto type 'waves', or unmatched keystore '${tmpExistentDir.getAbsolutePath}' for it - should be file for '${CryptoType.Waves.entryName}'"
  }

}
