package com.wavesenterprise.generator.transaction

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.settings.CryptoSettings
import com.wavesenterprise.state.{BinaryDataEntry, StringDataEntry}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalatest.{FunSpecLike, Matchers}

import java.nio.charset.StandardCharsets.UTF_8

/**
  * Test for [[DataTransactionGenerator]]
  */
class DataTransactionGeneratorTestSuite extends FunSpecLike with Matchers {

  CryptoInitializer.init(CryptoSettings.WavesCryptoSettings).explicitGet()

  private val keyPair = crypto.generateKeyPair()

  private val entryCount         = 10
  private val entryProbabilities = Map("string" -> 0.1, "binary" -> 0.2, "integer" -> 0.3, "boolean" -> 0.4)
  private val entryByteSize      = 100

  private val testSettings = DataTransactionGeneratorSettings(entryCount, entryProbabilities, entryByteSize, entryByteSize)

  private val dataTransactionGenerator = new DataTransactionGenerator(testSettings, fees)

  it("generates correct data transaction") {
    val txEither = dataTransactionGenerator.generateTxV1(PrivateKeyAccount(keyPair), System.currentTimeMillis())

    assert(txEither.isRight)

    val tx = txEither.right.get

    assert(tx.data.size == entryCount)
    tx.data.foreach {
      case StringDataEntry(_, strValue)  => assert(strValue.getBytes(UTF_8).length == entryByteSize)
      case BinaryDataEntry(_, byteValue) => assert(byteValue.arr.length == entryByteSize)
      case _                             =>
    }
  }
}
