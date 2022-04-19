package com.wavesenterprise.docker.validator

import com.wavesenterprise.metrics.{Metrics, MetricsType}
import enumeratum.EnumEntry.Snakecase
import enumeratum.{EnumEntry, _}
import org.influxdb.dto.Point

import java.util.concurrent.TimeUnit
import scala.collection.immutable

sealed trait ContractValidatorMeasurementType extends EnumEntry with Snakecase

object ContractValidatorMeasurementType extends Enum[ContractValidatorMeasurementType] {

  case object BlocksCount       extends ContractValidatorMeasurementType
  case object TransactionsCount extends ContractValidatorMeasurementType
  case object ResultsCount      extends ContractValidatorMeasurementType

  override def values: immutable.IndexedSeq[ContractValidatorMeasurementType] = findValues
}

object ContractValidatorStoreMetrics {
  val Prefix = "contract_validator_store"

  def writeRawNumber(measurementType: ContractValidatorMeasurementType, value: Int, time: Long = System.currentTimeMillis()): Unit = {
    Metrics.writeWithoutTime(
      MetricsType.ContractValidation,
      Point
        .measurement(s"${Prefix}_${measurementType.entryName}")
        .addField("value", value)
        .time(time, TimeUnit.MILLISECONDS)
    )
  }
}
