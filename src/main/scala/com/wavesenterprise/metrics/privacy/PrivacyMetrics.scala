package com.wavesenterprise.metrics.privacy

import com.wavesenterprise.metrics.{Metrics, MetricsType}
import enumeratum.EnumEntry.Snakecase
import enumeratum._
import kamon.Kamon
import kamon.metric.{MeasurementUnit, RangeSamplerMetric}
import monix.eval.Task
import org.influxdb.dto.Point

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import scala.collection.immutable

sealed trait PrivacyMeasurementType extends EnumEntry with Snakecase

object PrivacyMeasurementType extends Enum[PrivacyMeasurementType] {

  case object SendDataRequestValidation extends PrivacyMeasurementType
  case object SendDataTxValidation      extends PrivacyMeasurementType
  case object SendDataPolicyItemSaving  extends PrivacyMeasurementType

  case object ReplierRequestProcessing extends PrivacyMeasurementType
  case object ReplierDataLoading       extends PrivacyMeasurementType
  case object ReplierDataEncrypting    extends PrivacyMeasurementType

  case object SynchronizerCrawling           extends PrivacyMeasurementType
  case object SynchronizerPullFallback       extends PrivacyMeasurementType
  case object SynchronizerInventoryIteration extends PrivacyMeasurementType
  case object SynchronizerInventoryRequest   extends PrivacyMeasurementType
  case object SynchronizerDataDecrypting     extends PrivacyMeasurementType
  case object SynchronizerDataSaving         extends PrivacyMeasurementType

  override def values: immutable.IndexedSeq[PrivacyMeasurementType] = findValues
}

object PrivacyMetrics {

  val Prefix = "privacy"

  def measureEither[R, L](measurementType: PrivacyMeasurementType, policyId: String, dataHash: String)(either: => Either[L, R]): Either[L, R] = {
    val start = System.currentTimeMillis()
    either.map { result =>
      writeRawTime(measurementType, policyId, dataHash, start)
      result
    }
  }

  def measureTask[A](measurementType: PrivacyMeasurementType, policyId: String, dataHash: String)(task: Task[A]): Task[A] = {
    Task
      .defer {
        val start = System.currentTimeMillis()
        task.map { result =>
          {
            writeRawTime(measurementType, policyId, dataHash, start)
            result
          }
        }
      }
  }

  def writeRawTime(measurementType: PrivacyMeasurementType,
                   policyId: String,
                   dataHash: String,
                   start: Long,
                   end: Long = System.currentTimeMillis(),
                   customTags: Seq[(String, String)] = Seq.empty): Unit = {
    Metrics.writeWithoutTime(
      MetricsType.Privacy, {
        val enrichedPoint = buildPoint(measurementType, policyId, dataHash, customTags)

        enrichedPoint
          .addField("took", end - start)
          .time(start, TimeUnit.MILLISECONDS)
      }
    )
  }

  def writeRawNumber(measurementType: PrivacyMeasurementType,
                     policyId: String,
                     dataHash: String,
                     field: (String, Int),
                     time: Long = System.currentTimeMillis(),
                     customTags: Seq[(String, String)] = Seq.empty): Unit = {
    Metrics.writeWithoutTime(
      MetricsType.Privacy, {
        val enrichedPoint = buildPoint(measurementType, policyId, dataHash, customTags)
        val (name, value) = field

        enrichedPoint
          .addField(name, value)
          .time(time, TimeUnit.MILLISECONDS)
      }
    )
  }

  def writeEvent(measurementType: PrivacyMeasurementType,
                 policyId: String,
                 dataHash: String,
                 time: Long = System.currentTimeMillis(),
                 customTags: Seq[(String, String)] = Seq.empty): Unit = {
    Metrics.writeWithoutTime(
      MetricsType.Privacy, {
        val enrichedPoint = buildPoint(measurementType, policyId, dataHash, customTags)
        enrichedPoint.time(time, TimeUnit.MILLISECONDS)
      }
    )
  }

  private def buildPoint(measurementType: PrivacyMeasurementType,
                         policyId: String,
                         dataHash: String,
                         customTags: Seq[(String, String)]): Point.Builder = {
    val point = Point
      .measurement(s"${Prefix}_${measurementType.entryName}")
      .tag("policy_id", policyId)
      .tag("data_hash", dataHash)

    customTags
      .foldLeft(point) {
        case (acc, (name, value)) =>
          acc.tag(name, value)
      }
  }

  val pendingSizeStats: RangeSamplerMetric =
    Kamon.rangeSampler("privacy_pending_size", MeasurementUnit.none, Duration.of(500, ChronoUnit.MILLIS))

  val crawlingParallelismStats: RangeSamplerMetric =
    Kamon.rangeSampler("policy-crawling-parallelism", MeasurementUnit.none, Duration.of(1, ChronoUnit.SECONDS))
}
