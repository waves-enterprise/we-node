package com.wavesenterprise.settings

import com.typesafe.config.ConfigFactory
import com.wavesenterprise.settings.privacy._
import org.scalatest.Assertion
import pureconfig.ConfigSource
import software.amazon.awssdk.regions.Region
import squants.information.Mebibytes

import java.time.{Duration => JDuration}
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrivacySettingsSpec extends AnyFlatSpec with Matchers {
  "Postgres vendor" should "read values" in {
    val jdbcConfigStr =
      """ driver = "org.postgresql.Driver"
        | url = "jdbc:postgresql://localhost:5432/pgdb"
        | user = pguser
        | password = pgpassword
        | connectionPool = HikariCP
        | connectionTimeout = 5000
        | connectionTestQuery = "SELECT 1"
        | queueSize = 10000
        | numThreads = 20
        | upload-chunk-size = 1 MiB
        |""".stripMargin

    val storageConfigStr = s"""
                              | vendor = postgres
                              | schema = "public"
                              | migration-dir = "db/migration"
                              | profile = "slick.jdbc.PostgresProfile$$"
                              | jdbc-config {
                              |  $jdbcConfigStr
                              | }
                              | """.stripMargin

    val config = fullConfigGen(storageConfigStr)

    val settings = config.loadOrThrow[PrivacySettings]
    validateStaticSettings(settings)
    settings.storage shouldBe PostgresPrivacyStorageSettings(
      "public",
      "db/migration",
      "slick.jdbc.PostgresProfile$",
      ConfigFactory.parseString(jdbcConfigStr)
    )
  }

  "S3 vendor" should "read values" in {
    val storageConfigStr = """
                             | vendor = S3
                             | url = "http://localhost:9000/"
                             | bucket = "privacy"
                             | region = "aws-global"
                             | access-key-id = "minio"
                             | secret-access-key = "minio123"
                             | path-style-access-enabled = true
                             | connection-timeout = 30s
                             | connection-acquisition-timeout = 10s
                             | max-concurrency = 200
                             | read-timeout = 0s
                             | upload-chunk-size = 5 MiB
                             | """.stripMargin

    val config = fullConfigGen(storageConfigStr)

    val settings = config.loadOrThrow[PrivacySettings]
    validateStaticSettings(settings)
    settings.storage shouldBe S3PrivacyStorageSettings(
      "http://localhost:9000/",
      "privacy",
      Region.AWS_GLOBAL,
      "minio",
      "minio123",
      pathStyleAccessEnabled = true,
      JDuration.ofSeconds(30),
      JDuration.ofSeconds(10),
      200,
      JDuration.ofSeconds(0)
    )
  }

  "Unknown vendor" should "be as disabled storage" in {
    val config   = fullConfigGen("vendor = none")
    val settings = config.loadOrThrow[PrivacySettings]

    settings.storage shouldBe DisabledPrivacyStorage
  }

  private def fullConfigGen(storageConfigStr: String): ConfigSource =
    ConfigSource.string(s"""
                           |node {
                           |  privacy {
                           |     replier {
                           |      parallelism = 7
                           |      stream-timeout = 3 minutes
                           |      stream-chunk-size = 3MiB
                           |      stream-max-peers-fullness-percentage = 50
                           |    }
                           |
                           |    synchronizer {
                           |      request-timeout = 2 minute
                           |      init-retry-delay = 5 seconds
                           |      inventory-stream-timeout = 10 seconds
                           |      inventory-request-delay = 3 seconds
                           |      inventory-timestamp-threshold = 9 minutes
                           |      crawling-parallelism = 100
                           |      max-attempt-count = 25
                           |      lost-data-processing-delay = 10 minutes
                           |      network-stream-buffer-size = 5
                           |    }
                           |
                           |    inventory-handler {
                           |      max-buffer-time = 500ms
                           |      max-buffer-size = 100
                           |      max-cache-size = 5000
                           |      expiration-time = 5m
                           |      replier-parallelism = 10
                           |    }
                           |
                           |    cache {
                           |      max-size = 100
                           |      expire-after = 10m
                           |    }
                           |
                           |    storage {
                           |      $storageConfigStr
                           |    }
                           |
                           |    cleaner {
                           |      enabled: yes
                           |      interval: 1m
                           |      confirmation-blocks: 99
                           |      pending-time: 3h
                           |    }
                           |
                           |    service {
                           |      request-buffer-size = 100MiB
                           |      meta-data-accumulation-timeout = 7s
                           |    }
                           |  }
                           |}""".stripMargin).at("node.privacy")

  private def validateStaticSettings(settings: PrivacySettings): Assertion = {
    settings.replier shouldBe PrivacyReplierSettings(
      parallelism = PositiveInt(7),
      streamTimeout = 3.minutes,
      streamChunkSize = Mebibytes(3),
      streamMaxPeersFullnessPercentage = PositiveInt(50)
    )

    settings.synchronizer shouldBe PrivacySynchronizerSettings(
      2.minutes,
      initRetryDelay = 5.seconds,
      inventoryStreamTimeout = 10.seconds,
      inventoryRequestDelay = 3.seconds,
      inventoryTimestampThreshold = 9.minutes,
      crawlingParallelism = PositiveInt(100),
      maxAttemptCount = PositiveInt(25),
      lostDataProcessingDelay = 10.minutes,
      networkStreamBufferSize = PositiveInt(5)
    )

    settings.inventoryHandler shouldBe PrivacyInventoryHandlerSettings(
      maxBufferTime = 500.millis,
      maxBufferSize = PositiveInt(100),
      maxCacheSize = PositiveInt(5000),
      expirationTime = 5.minutes,
      replierParallelism = PositiveInt(10)
    )

    settings.service shouldBe PrivacyServiceSettings(Mebibytes(100), 7.seconds)
  }
}
