package com.wavesenterprise.database.rocksdb

import enumeratum.{EnumEntry, _}

import scala.collection.immutable

sealed abstract class ColumnFamily(override val entryName: String) extends EnumEntry

sealed abstract class MainDBColumnFamily(override val entryName: String) extends ColumnFamily(entryName)

object MainDBColumnFamily extends Enum[MainDBColumnFamily] {

  case object PresetCF      extends MainDBColumnFamily("default")
  case object AddressCF     extends MainDBColumnFamily("address")
  case object BlockCF       extends MainDBColumnFamily("block")
  case object TransactionCF extends MainDBColumnFamily("transaction")
  case object PermissionCF  extends MainDBColumnFamily("permission")
  case object ContractCF    extends MainDBColumnFamily("contract")
  case object PrivacyCF     extends MainDBColumnFamily("privacy")
  case object CertsCF       extends MainDBColumnFamily("certs")
  case object LeaseCF       extends MainDBColumnFamily("lease")

  override def values: immutable.IndexedSeq[MainDBColumnFamily] = findValues
}

sealed abstract class ConfidentialDBColumnFamily(override val entryName: String) extends ColumnFamily(entryName)

object ConfidentialDBColumnFamily extends Enum[ConfidentialDBColumnFamily] {

  case object PresetCF extends ConfidentialDBColumnFamily("default")

  case object ConfidentialDataSyncCF extends ConfidentialDBColumnFamily("confidential_data_sync")

  case object ConfidentialInputCF extends ConfidentialDBColumnFamily("confidential_input")

  case object ConfidentialOutputCF extends ConfidentialDBColumnFamily("confidential_output")

  case object ConfidentialStateCF extends ConfidentialDBColumnFamily("confidential_state")

  override def values: immutable.IndexedSeq[ConfidentialDBColumnFamily] = findValues
}
