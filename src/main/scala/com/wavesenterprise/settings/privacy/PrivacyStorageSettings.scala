package com.wavesenterprise.settings.privacy

import cats.Show
import com.typesafe.config.Config
import com.wavesenterprise.settings.WEConfigReaders
import com.wavesenterprise.settings.privacy.PrivacyStorageSettings.{PostgresDefaultChunkSize, S3MaxChunkSize, S3MinChunkSize}
import com.wavesenterprise.settings.privacy.PrivacyStorageVendor.{Postgres, S3, Unknown}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._
import pureconfig.module.squants._
import slick.jdbc.JdbcProfile
import software.amazon.awssdk.regions.Region
import squants.information.{Gibibytes, Information, Mebibytes}

import java.time.Duration

sealed trait PrivacyStorageSettings {
  def vendor: PrivacyStorageVendor
}

case class S3PrivacyStorageSettings(
    url: String,
    bucket: String,
    region: Region,
    accessKeyId: String,
    secretAccessKey: String,
    pathStyleAccessEnabled: Boolean,
    connectionTimeout: Duration,
    connectionAcquisitionTimeout: Duration,
    maxConcurrency: Int,
    readTimeout: Duration,
    uploadChunkSize: Information = S3MinChunkSize
) extends PrivacyStorageSettings {
  require(
    uploadChunkSize >= S3MinChunkSize && uploadChunkSize <= S3MaxChunkSize,
    s"Allowed S3 chunk size must be in between '$S3MinChunkSize' and '$S3MaxChunkSize'"
  )

  override def vendor: PrivacyStorageVendor = S3
}

case class PostgresPrivacyStorageSettings(
    schema: String,
    migrationDir: String,
    profile: String,
    jdbcConfig: Config,
    uploadChunkSize: Information = PostgresDefaultChunkSize
) extends PrivacyStorageSettings {

  val jdbcProfile: JdbcProfile = {
    val profileClass = Class.forName(profile)
    profileClass.getField("MODULE$").get(profileClass).asInstanceOf[slick.jdbc.JdbcProfile]
  }

  override def vendor: PrivacyStorageVendor = Postgres
}

object S3PrivacyStorageSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[S3PrivacyStorageSettings] = deriveReader
}

object PostgresPrivacyStorageSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[PostgresPrivacyStorageSettings] = deriveReader
}

case object DisabledPrivacyStorage extends PrivacyStorageSettings {
  override def vendor: PrivacyStorageVendor = Unknown
}

object PrivacyStorageSettings {

  val PostgresDefaultChunkSize: Information = Mebibytes(1)
  val S3MinChunkSize: Information           = Mebibytes(5)
  val S3MaxChunkSize: Information           = Gibibytes(5)

  implicit val configReader: ConfigReader[PrivacyStorageSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      vendorCursor <- objectCursor.atKey("vendor")
      vendorStr    <- vendorCursor.asString
      vendor = PrivacyStorageVendor.fromStr(vendorStr)
      settings <- vendor match {
        case Postgres => PostgresPrivacyStorageSettings.configReader.from(objectCursor)
        case S3       => S3PrivacyStorageSettings.configReader.from(objectCursor)
        case Unknown  => Right(DisabledPrivacyStorage)
      }
    } yield settings
  }

  implicit val toPrintable: Show[PrivacyStorageSettings] = {
    case x: PostgresPrivacyStorageSettings =>
      import x._
      s"""
         |vendor: $vendor
         |schema: $schema
         |migrationDir: $migrationDir
         |jdbcProfile: $jdbcProfile
         |jdbcConfig: $jdbcConfig
         |uploadChunkSize: $uploadChunkSize
     """.stripMargin

    case x: S3PrivacyStorageSettings =>
      import x._
      s"""
         |vendor: $vendor
         |connectionUrl: $url
         |bucket: $bucket
         |region: $region
         |accessKeyId: $accessKeyId
         |pathStyleAccessEnabled: $pathStyleAccessEnabled
         |connectionTimeout: $connectionTimeout
         |connectionAcquisitionTimeout: $connectionAcquisitionTimeout
         |maxConcurrency: $maxConcurrency
         |readTimeout: $readTimeout
         |uploadChunkSize: $uploadChunkSize
     """.stripMargin

    case DisabledPrivacyStorage =>
      s"""
         |vendor: $DisabledPrivacyStorage.vendor
       """.stripMargin
  }
}
