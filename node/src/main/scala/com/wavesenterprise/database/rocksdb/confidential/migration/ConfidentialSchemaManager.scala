package com.wavesenterprise.database.rocksdb.confidential.migration

import com.wavesenterprise.database.BaseKey
import com.wavesenterprise.database.migration.BaseSchemaManager
import com.wavesenterprise.database.migration.MainMigrationType.Version
import com.wavesenterprise.database.rocksdb.ConfidentialDBColumnFamily
import com.wavesenterprise.database.rocksdb.confidential.{ConfidentialDBKeys, ConfidentialRocksDBStorage}

class ConfidentialSchemaManager(storage: ConfidentialRocksDBStorage) extends BaseSchemaManager(storage) {

  override def dataBaseName = "confidential"

  override def schemeVersion: BaseKey[Option[Version], ConfidentialDBColumnFamily] = ConfidentialDBKeys.schemaVersion

  override protected[database] def getDbVersion: Option[Version] = {
    storage.get(ConfidentialDBKeys.schemaVersion)
  }

  override protected def updateSchema(version: Version): Unit = storage.put(ConfidentialDBKeys.schemaVersion, Some(version))

  override protected[database] def applyMigrations(): ErrorOrVersion = {
    val dbVersion         = getDbVersion.getOrElse(0)
    val migrationsToApply = ConfidentialMigrationType.all.drop(dbVersion)
    if (migrationsToApply.isEmpty) Right(dbVersion)
    else
      applyMigrations(migrationsToApply)
  }

  override def lastVersion: Version = ConfidentialMigrationType.lastVersion
}
