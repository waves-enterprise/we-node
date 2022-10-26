package com.wavesenterprise.database.migration

import com.wavesenterprise.database.rocksdb.RW
import enumeratum.values.{IntEnum, IntEnumEntry}

import scala.collection.immutable

// noinspection ScalaStyle
object MigrationType extends IntEnum[MigrationEntry] {

  type Version = Int

  case object `1` extends MigrationEntry(1, _ => ())
  case object `2` extends MigrationEntry(2, MigrationV2.apply)
  case object `3` extends MigrationEntry(3, MigrationV3.apply)
  case object `4` extends MigrationEntry(4, MigrationV4.apply)
  case object `5` extends MigrationEntry(5, MigrationV5.apply)
  case object `6` extends MigrationEntry(6, MigrationV6.apply)
  case object `7` extends MigrationEntry(7, MigrationV7.apply)
  case object `8` extends MigrationEntry(8, MigrationV8.apply)
  case object `9` extends MigrationEntry(9, MigrationV9.apply)

  override val values: immutable.IndexedSeq[MigrationEntry] = findValues.sortBy(_.version)
  val all: List[Migration]                                  = values.toList
  val lastVersion: Version                                  = all.last.version
}

import com.wavesenterprise.database.migration.MigrationType.Version

trait Migration {
  def version: Version
  def apply: RW => Unit
}

sealed abstract class MigrationEntry(val value: Version, val apply: RW => Unit) extends IntEnumEntry with Migration {
  val version: Version = value
}

final case class MigrationException(errorVersion: Int, cause: Throwable)
    extends Exception(s"Error during schema migration. Can't migrate to version '$errorVersion'", cause)
