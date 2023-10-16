package com.wavesenterprise.database.migration

import cats.implicits._
import com.wavesenterprise.database.BaseKey
import com.wavesenterprise.database.rocksdb._
import com.wavesenterprise.utils.ScorexLogging

import scala.util.control.NonFatal

abstract class BaseSchemaManager[CF <: ColumnFamily, RO <: BaseReadOnlyDB[CF], RW <: BaseReadWriteDB[CF]](storage: BaseRocksDBOperations[CF, RO, RW])
    extends ScorexLogging {

  import MainMigrationType.Version

  type ErrorOrVersion = Either[MigrationException, Version]

  def dataBaseName: String

  def lastVersion: Version

  def schemeVersion: BaseKey[Option[Int], CF]

  def migrateSchema(): Unit = {
    log.info(s"Starting $dataBaseName state schema migration process")
    if (stateNoneEmpty) {
      val dbVersion = getDbVersion.getOrElse(0)
      val result    = applyMigrations()
      result match {
        case Left(ex: MigrationException) =>
          log.error(ex.getMessage, ex.cause)
          throw ex
        case Right(version) if version == dbVersion =>
          log.info(s"${dataBaseName.capitalize} state  schema version is up-to-date, no need to apply migrations")
        case Right(version) =>
          log.info(s"Successfully migrated $dataBaseName state to '$version' schema version")
      }
    } else {
      updateSchemaVersion(lastVersion)
      log.info(s"{dataBaseName.capitalize} state is empty, no need to apply state migrations")
    }
  }

  private[database] def stateNoneEmpty: Boolean = {
    val iterator = storage.newIterator()
    iterator.seekToFirst()
    val hasNext = iterator.isValid
    iterator.close()
    hasNext
  }

  protected[database] def getDbVersion: Option[Version]

  private[database] def updateSchemaVersion(version: Version): Unit = {
    log.info(s"Setting $dataBaseName schema version to '$version'")
    updateSchema(version)
  }

  protected def updateSchema(version: Version): Unit

  def applyMigrations(migrationsToApply: Seq[Migration[CF, RW]]): ErrorOrVersion = {
    migrationsToApply.foldLeft[ErrorOrVersion](Right(migrationsToApply.head.version - 1)) {
      case (acc, migration) =>
        acc >> {
          log.info(s"Applying $dataBaseName migration version '${migration.version}'")
          try {
            applyMigration(migration)
            log.info(s"Successfully upgraded $dataBaseName state schema version to '${migration.version}'")
            Right(migration.version)
          } catch {
            case NonFatal(ex) => Left(MigrationException(dataBaseName, migration.version, ex))
          }
        }
    }
  }

  private def applyMigration(migration: Migration[CF, RW]): Unit = {
    storage.readWrite { rw =>
      migration.apply(rw)
      // updating schema version
      rw.put[Option[Int]](schemeVersion, Some(migration.version))
    }
  }

  protected[database] def applyMigrations(): ErrorOrVersion

}
