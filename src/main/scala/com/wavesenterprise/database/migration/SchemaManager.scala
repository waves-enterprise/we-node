package com.wavesenterprise.database.migration

import cats.implicits._
import com.wavesenterprise.database.Keys
import com.wavesenterprise.database.rocksdb._
import com.wavesenterprise.utils.ScorexLogging

import scala.util.control.NonFatal

class SchemaManager(storage: RocksDBStorage) extends ScorexLogging {

  import MigrationType.Version

  type ErrorOrVersion = Either[MigrationException, Version]

  def migrateSchema(): Unit = {
    log.info("Starting state schema migration process")
    if (stateNoneEmpty) {
      val dbVersion      = getDbVersion.getOrElse(0)
      val migrationsList = MigrationType.all.drop(dbVersion)

      if (migrationsList.nonEmpty) {
        val result = applyMigrations(migrationsList)
        result match {
          case Left(ex @ MigrationException(_, cause)) =>
            log.error(ex.getMessage, cause)
            throw ex
          case Right(version) =>
            log.info(s"Successfully migrated state to '$version' schema version")
        }
      } else {
        log.info("State schema version is up-to-date, no need to apply migrations")
      }
    } else {
      updateSchemaVersion(MigrationType.lastVersion)
      log.info("State is empty, no need to apply state migrations")
    }
  }

  private[database] def stateNoneEmpty: Boolean = {
    val iterator = storage.newIterator()
    iterator.seekToFirst()
    val hasNext = iterator.isValid
    iterator.close()
    hasNext
  }

  private[database] def getDbVersion: Option[Version] = {
    storage.get(Keys.schemaVersion)
  }

  private[database] def updateSchemaVersion(version: Version): Unit = {
    log.info(s"Setting schema version to '$version'")
    storage.put(Keys.schemaVersion, Some(version))
  }

  /**
    * Applies sequence of passed schema migrations one by one
    *
    * @return either exception that occurred during schema migration or last schema version that have applied successfully
    **/
  private[database] def applyMigrations(migrationsToApply: List[Migration]): ErrorOrVersion = {
    migrationsToApply.foldLeft[ErrorOrVersion](Right(migrationsToApply.head.version - 1)) {
      case (accum, migration) =>
        accum >> {
          log.info(s"Applying migration version '${migration.version}'")
          try {
            storage.readWrite { rw =>
              migration.apply(rw)
              // updating schema version
              rw.put[Option[Int]](Keys.schemaVersion, Some(migration.version))
            }
            log.info(s"Successfully upgraded state schema version to '${migration.version}'")
            Right(migration.version)
          } catch {
            case NonFatal(ex) => Left(MigrationException(migration.version, ex))
          }
        }
    }
  }
}
