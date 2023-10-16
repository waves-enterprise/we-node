package com.wavesenterprise.database.migration

import com.wavesenterprise.database.rocksdb.{MainDBColumnFamily, MainReadWriteDB}
import enumeratum.values.{IntEnum, IntEnumEntry}

import scala.collection.immutable

// noinspection ScalaStyle
object MainMigrationType extends IntEnum[MainMigrationEntry] {

  type Version = Int

  case object `1`  extends MainMigrationEntry(1, _ => ())
  case object `2`  extends MainMigrationEntry(2, MigrationV2.apply)
  case object `3`  extends MainMigrationEntry(3, MigrationV3.apply)
  case object `4`  extends MainMigrationEntry(4, MigrationV4.apply)
  case object `5`  extends MainMigrationEntry(5, MigrationV5.apply)
  case object `6`  extends MainMigrationEntry(6, MigrationV6.apply)
  case object `7`  extends MainMigrationEntry(7, MigrationV7.apply)
  case object `8`  extends MainMigrationEntry(8, MigrationV8.apply)
  case object `9`  extends MainMigrationEntry(9, MigrationV9.apply)
  case object `10` extends MainMigrationEntry(10, MigrationV10.apply)
  case object `11` extends MainMigrationEntry(11, MigrationV11.apply)

  override val values: immutable.IndexedSeq[MainMigrationEntry] = findValues.sortBy(_.version)
  val all: List[Migration[MainDBColumnFamily, MainReadWriteDB]] = values.toList
  val lastVersion: Version                                      = all.last.version
}

import com.wavesenterprise.database.migration.MainMigrationType.Version

sealed abstract class MainMigrationEntry(val value: Version, val apply: MainReadWriteDB => Unit) extends IntEnumEntry
    with Migration[MainDBColumnFamily, MainReadWriteDB] {
  val version: Version = value
}
