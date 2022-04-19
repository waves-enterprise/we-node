package com.wavesenterprise.database.rocksdb

import enumeratum.{EnumEntry, _}

import scala.collection.immutable

sealed abstract class ColumnFamily(override val entryName: String) extends EnumEntry

object ColumnFamily extends Enum[ColumnFamily] {

  case object DefaultCF     extends ColumnFamily("default")
  case object AddressCF     extends ColumnFamily("address")
  case object BlockCF       extends ColumnFamily("block")
  case object TransactionCF extends ColumnFamily("transaction")
  case object PermissionCF  extends ColumnFamily("permission")
  case object ContractCF    extends ColumnFamily("contract")
  case object PrivacyCF     extends ColumnFamily("privacy")

  override def values: immutable.IndexedSeq[ColumnFamily] = findValues
}
