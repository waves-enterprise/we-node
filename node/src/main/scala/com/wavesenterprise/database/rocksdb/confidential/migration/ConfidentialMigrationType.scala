package com.wavesenterprise.database.rocksdb.confidential.migration

import com.wavesenterprise.database.migration.Migration
import com.wavesenterprise.database.rocksdb.ConfidentialDBColumnFamily
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialReadWriteDB
import enumeratum.values.{IntEnum, IntEnumEntry}

import scala.collection.immutable

// noinspection ScalaStyle
object ConfidentialMigrationType extends IntEnum[ConfidentialMigrationEntry] {

  type Version = Int

  case object `1` extends ConfidentialMigrationEntry(1, _ => ())

  override val values: immutable.IndexedSeq[ConfidentialMigrationEntry]         = findValues.sortBy(_.version)
  val all: List[Migration[ConfidentialDBColumnFamily, ConfidentialReadWriteDB]] = values.toList
  val lastVersion: Version                                                      = all.last.version
}

import com.wavesenterprise.database.rocksdb.confidential.migration.ConfidentialMigrationType.Version

sealed abstract class ConfidentialMigrationEntry(val value: Version, val apply: ConfidentialReadWriteDB => Unit) extends IntEnumEntry
    with Migration[ConfidentialDBColumnFamily, ConfidentialReadWriteDB] {
  val version: Version = value
}
