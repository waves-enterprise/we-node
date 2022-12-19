package com.wavesenterprise.metrics

import java.net.URI
import java.util.concurrent.TimeUnit
import cats.Show
import cats.implicits.showInterpolator
import com.wavesenterprise.settings.{ConsensusSettings, WESettings}
import com.wavesenterprise.utils.{ScorexLogging, Time}
import com.wavesenterprise.{Version, crypto}
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import org.influxdb.dto.Point
import org.influxdb.{InfluxDB, InfluxDBFactory}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

object Metrics extends ScorexLogging {

  val configPath = "metrics"

  case class InfluxDbSettings(uri: URI,
                              db: String,
                              username: Option[String],
                              password: Option[String],
                              batchActions: Int,
                              batchFlashDuration: FiniteDuration)
  object InfluxDbSettings {

    implicit val configReader: ConfigReader[InfluxDbSettings] = deriveReader

    implicit val toPrintable: Show[InfluxDbSettings] = { x =>
      import x._

      s"""
         |uri: $uri
         |db: ${x.db}
         |username: $username
         |password: $password
         |batchActions: $batchActions
         |batchFlashDuration: $batchFlashDuration
       """.stripMargin
    }
  }

  case class HttpRequestsCacheSettings(
      maxSize: Int,
      expireAfter: FiniteDuration
  )

  case class CircuitBreakerCacheSettings(
      maxSize: Int,
      expireAfter: FiniteDuration
  )

  object HttpRequestsCacheSettings {

    implicit val configReader: ConfigReader[HttpRequestsCacheSettings] = deriveReader

    implicit val toPrintable: Show[HttpRequestsCacheSettings] = { x =>
      import x._

      s"""
         |maxSize: $maxSize
         |expireAfter: $expireAfter
       """.stripMargin
    }
  }

  case class MetricsSettings(enable: Boolean,
                             nodeId: String,
                             influxDb: InfluxDbSettings,
                             segments: Set[MetricsType],
                             circuitBreakerCache: CircuitBreakerCacheSettings,
                             httpRequestsCache: HttpRequestsCacheSettings)
  object MetricsSettings {

    import pureconfig.module.enumeratum._
    import pureconfig.generic.auto._
    implicit val configReader: ConfigReader[MetricsSettings] = deriveReader

    implicit val toPrintable: Show[MetricsSettings] = { x =>
      import x._

      s"""
         |enable: $enable
         |nodeId: $nodeId
         |influxDbSettings:
         |  ${show"$influxDb".replace("\n", "\n--")}
         |segments: [${segments.mkString(", ")}]
         |httpRequestsCache:
         |  ${show"$httpRequestsCache".replace("\n", "\n--")}
       """.stripMargin
    }
  }

  val registry = new MetricsRegistry

  private implicit val scheduler: SchedulerService = Scheduler.singleThread("metrics")

  private var settings: MetricsSettings = _
  private var time: Time                = _
  private var db: Option[InfluxDB]      = None

  def start(config: MetricsSettings, thatTime: Time): Future[Boolean] =
    Task {
      db.foreach { dbc =>
        try {
          db = None
          dbc.close()
        } catch {
          case NonFatal(e) => log.warn(s"Failed to close InfluxDB (${e.getMessage})")
        }
      }
      settings = config
      time = thatTime
      if (settings.enable) {
        import config.{influxDb => dbSettings}

        settings.segments.foreach(registry.enable)

        log.info(s"Precise metrics are enabled and will be sent to ${dbSettings.uri}/${dbSettings.db}")
        try {
          val influx = dbSettings.username -> dbSettings.password match {
            case (Some(username), Some(password)) =>
              InfluxDBFactory.connect(dbSettings.uri.toString, username, password)
            case _ =>
              InfluxDBFactory.connect(dbSettings.uri.toString)
          }

          influx.setDatabase(dbSettings.db)
          influx.enableBatch(dbSettings.batchActions, dbSettings.batchFlashDuration.toSeconds.toInt, TimeUnit.SECONDS)

          try {
            val pong = influx.ping()
            log.info(s"Metrics will be sent to ${dbSettings.uri}/${dbSettings.db}. Connected in ${pong.getResponseTime}ms.")
            db = Some(influx)
          } catch {
            case NonFatal(e) =>
              log.warn("Can't connect to InfluxDB", e)
          }
        } catch {
          case NonFatal(e) => log.warn(s"Failed to connect to InfluxDB (${e.getMessage})")
        }
      }

      db.nonEmpty
    }.runAsyncLogErr

  def sendInitialMetrics(settings: WESettings): Unit = {
    val addConsensusInfo: Point.Builder => Point.Builder = { p =>
      settings.blockchain.consensus match {
        case ConsensusSettings.PoASettings(roundDuration, syncDuration, banDurationBlocks, warningsForBan, maxBansPercentage) =>
          p.addField("round-duration-millis", roundDuration.toMillis)
            .addField("sync-duration-millis", syncDuration.toMillis)
            .addField("ban-duration-blocks", banDurationBlocks)
            .addField("warnings-for-ban", warningsForBan)
            .addField("max-bans-percentage", maxBansPercentage)

        case ConsensusSettings.CftSettings(roundDuration,
                                           syncDuration,
                                           banDurationBlocks,
                                           warningsForBan,
                                           maxBansPercentage,
                                           maxValidators,
                                           fullVoteSetTimeout,
                                           finalizationTimeout) =>
          p.addField("round-duration-millis", roundDuration.toMillis)
            .addField("sync-duration-millis", syncDuration.toMillis)
            .addField("ban-duration-blocks", banDurationBlocks)
            .addField("warnings-for-ban", warningsForBan)
            .addField("max-bans-percentage", maxBansPercentage)
            .addField("max-validators", maxValidators.value)
            .addField("full-vote-set-timeout", fullVoteSetTimeout.fold(0L)(_.toMillis))
            .addField("finalization-timeout-millis", finalizationTimeout.toMillis)

        case ConsensusSettings.PoSSettings =>
          p.addField("average-block-delay-millis", settings.blockchain.custom.genesis.toPlainSettingsUnsafe.averageBlockDelay.toMillis)
      }
    }

    val addCryptoInfo: Point.Builder => Point.Builder = { p =>
      p.addField("crypto-type", crypto.cryptoSettings.name)
    }

    write(
      MetricsType.Common,
      addConsensusInfo.andThen(addCryptoInfo).apply {
        Point
          .measurement("config")
          .addField("miner-micro-block-interval", settings.miner.microBlockInterval.toMillis)
          .addField("miner-max-transactions-in-micro-block", settings.miner.maxTransactionsInMicroBlock)
          .addField("miner-max-block-size-in-bytes", settings.miner.maxBlockSizeInBytes)
          .addField("miner-min-micro-block-age", settings.miner.minMicroBlockAge.toMillis)
          .addField("mbs-wait-response-timeout", settings.synchronization.microBlockSynchronizer.waitResponseTimeout.toMillis)
          .addField("node-version", Version.VersionString)
          .addField("consensus-type", settings.blockchain.consensus.consensusType.value)
      }
    )
  }

  def write(mType: MetricsType, pointBuilder: => Point.Builder): Unit = {
    writePoint(mType, pointBuilder, withTime = true)
  }

  def writeWithoutTime(mType: MetricsType, pointBuilder: => Point.Builder): Unit = {
    writePoint(mType, pointBuilder)
  }

  private def writePoint(mType: MetricsType, pointBuilder: => Point.Builder, withTime: Boolean = false): Unit = {
    db.filter { _ =>
      registry.isEnabled(mType)
    }
      .foreach { db =>
        val builder = pointBuilder
        builder
          // Should be a tag, but tags are the strings now
          // https://docs.influxdata.com/influxdb/v1.3/concepts/glossary/#tag-value
          .addField("node", settings.nodeId)
          .tag("node", settings.nodeId)
        if (withTime) {
          builder.time(time.getTimestamp(), TimeUnit.MILLISECONDS)
        }
        Task
          .eval(db.write(builder.build()))
          .onErrorRecover {
            case NonFatal(e) => log.warn("Failed to send data to InfluxDB", e)
          }
          .runAsyncLogErr
      }
  }

  def writeManyWithoutTime(mType: MetricsType, points: List[Point.Builder]): Unit = {
    points.foreach(writePoint(mType, _))
  }

  def shutdown(): Unit =
    Task {
      db.foreach(_.close())
      db = None
      scheduler.shutdown()
    }.runAsyncLogErr
}
