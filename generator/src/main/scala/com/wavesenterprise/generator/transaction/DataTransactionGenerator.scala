package com.wavesenterprise.generator.transaction

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.generator.DistributedRandomGenerator
import com.wavesenterprise.settings.FeeSettings.FeesEnabled
import com.wavesenterprise.state.DataEntry.Type
import com.wavesenterprise.state.{BinaryDataEntry, BooleanDataEntry, ByteStr, DataEntry, IntegerDataEntry, StringDataEntry}
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.validation.DataValidation
import org.apache.commons.lang3.{RandomStringUtils, StringUtils}

import java.util.concurrent.ThreadLocalRandom

case class DataTransactionGeneratorSettings(entryCount: Int,
                                            entryTypeProbabilities: Map[String, Double],
                                            stringEntryValueSize: Int,
                                            binaryEntryValueSize: Int) {

  require(entryCount <= DataValidation.MaxEntryCount, s"Entry count must be less than ${DataValidation.MaxEntryCount}")
  require(stringEntryValueSize <= DataTransactionEntryOps.MaxValueSize)
  require(binaryEntryValueSize <= DataTransactionEntryOps.MaxValueSize)
  entryTypeProbabilities.keys.map(StringUtils.capitalize).foreach(DataEntry.Type.withName)

}

object DataTransactionGenerator {
  private def r = ThreadLocalRandom.current

  private def newKey(): String = RandomStringUtils.randomAlphanumeric(5)
}

class DataTransactionGenerator(settings: DataTransactionGeneratorSettings, fees: FeesEnabled) {

  import DataTransactionGenerator._

  private val dataTxBaseFee       = fees.forTxType(DataTransaction.typeId)
  private val dataTxAdditionalFee = fees.forTxTypeAdditional(DataTransaction.typeId)
  private val entryGenerator      = DistributedRandomGenerator(settings.entryTypeProbabilities)

  def generateTxV1(sender: PrivateKeyAccount, ts: Long): Either[ValidationError, DataTransactionV1] = {
    val data = generateData
    val fee  = generateFee(data)
    DataTransactionV1.selfSigned(sender, sender, data, ts, fee)
  }

  def generateTxV2(sender: PrivateKeyAccount, ts: Long): Either[ValidationError, DataTransactionV2] = {
    val data = generateData
    val fee  = generateFee(data)
    DataTransactionV2.selfSigned(sender, sender, data, ts, fee, None)
  }

  private def generateFee(data: List[DataEntry[_]]) = {
    val size = 128 + data.map(DataTransactionEntryOps.toBytes(_).length).sum
    dataTxBaseFee + (size - 1) / 1024 * dataTxAdditionalFee
  }

  def generateData: List[DataEntry[_]] = {
    (for {
      _ <- 0 until settings.entryCount
      key       = newKey()
      entryType = DataEntry.Type.withName(StringUtils.capitalize(entryGenerator.getRandom))
    } yield
      entryType match {
        case Type.Integer => IntegerDataEntry(key, r.nextLong)
        case Type.Boolean => BooleanDataEntry(key, r.nextBoolean)
        case Type.String  => StringDataEntry(key, RandomStringUtils.randomAlphanumeric(settings.stringEntryValueSize))
        case Type.Binary =>
          val b = new Array[Byte](settings.stringEntryValueSize)
          r.nextBytes(b)
          BinaryDataEntry(key, ByteStr(b))
      }).toList
  }
}
