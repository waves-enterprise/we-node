package com.wavesenterprise.database.migration

import com.wavesenterprise.database.rocksdb._
import com.wavesenterprise.database.{BaseKey, Keys}

class MainSchemaManager(storage: MainRocksDBStorage) extends BaseSchemaManager(storage) {
  import MainMigrationType.Version

  override def dataBaseName: String = "main"

  override def schemeVersion: BaseKey[Option[Version], MainDBColumnFamily] = Keys.schemaVersion

  override protected[database] def getDbVersion: Option[Version] = {
    storage.get(Keys.schemaVersion)
  }

  override protected def updateSchema(version: Version): Unit = storage.put(Keys.schemaVersion, Some(version))

  override def lastVersion: Version = MainMigrationType.lastVersion

  override protected[database] def applyMigrations(): ErrorOrVersion = {
    val dbVersion         = getDbVersion.getOrElse(0)
    val migrationsToApply = MainMigrationType.all.drop(dbVersion)
    if (migrationsToApply.isEmpty) Right(dbVersion)
    else
      applyMigrations(migrationsToApply)
  }
}
