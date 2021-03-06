package com.wavesenterprise.database

import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.acl.PermissionOp
import com.wavesenterprise.consensus.MinerBanlistEntry
import com.wavesenterprise.consensus.MinerBanlistEntry.CancelledWarning
import com.wavesenterprise.database.KeyHelpers._
import com.wavesenterprise.database.keys.{AddressCFKeys, ContractCFKeys, PermissionCFKeys, PrivacyCFKeys}
import com.wavesenterprise.database.rocksdb.RocksDBStorage
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId, PrivacyItemDescriptor}
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt

/**
  * Keys, related to waves enterprise
  */
object WEKeys {

  private val MinerBanHistoryPrefix: Short            = 0x1008
  private val LastMinerBanHistoryEntryIdPrefix: Short = 0x1024
  private val MinerBanHistoryEntryPrefix: Short       = 0x1025
  private[database] val SetSizePrefix: Short          = 0x1029
  private val MinerCancelledWarningsPrefix: Short     = 0x1034

  /**
    * Permissions artifacts
    */
  def permissions(addressId: BigInt): Key[Option[Seq[PermissionOp]]] = PermissionCFKeys.permissions(addressId)

  def miners: Key[Seq[BigInt]] = PermissionCFKeys.miners()

  def validators: Key[Seq[BigInt]] = PermissionCFKeys.validators()

  /**
    * PoA artifacts
    */
  def minerBanHistoryV1(addressId: BigInt): Key[Option[Seq[MinerBanlistEntry]]] =
    Key.opt("minerBanHistory", addr(MinerBanHistoryPrefix, addressId), readMinerBanlistEntries, writeMinerBanlistEntries)

  def minerCancelledWarnings(addressId: BigInt): Key[Option[Seq[CancelledWarning]]] =
    Key.opt("minerCancelledWarnings", addr(MinerCancelledWarningsPrefix, addressId), readMinerCancelledWarnings, writeMinerCancelledWarnings)

  def lastMinerBanHistoryV2EntryId(addressId: BigInt): Key[Option[BigInt]] =
    Key.opt("lastMinerBanHistoryV2EntryId", addr(LastMinerBanHistoryEntryIdPrefix, addressId), BigInt(_), _.toByteArray)

  def idToMinerBanHistoryV2Entry(entryId: BigInt, addressId: BigInt): Key[MinerBanlistEntry] = {
    Key(
      "idToMinerBanHistoryV2Entry",
      bytes(MinerBanHistoryEntryPrefix, entryId.toByteArray ++ addressId.toByteArray),
      MinerBanlistEntry.fromBytes(_).explicitGet(),
      _.bytes
    )
  }

  def participantPubKey(addressId: BigInt): Key[Option[PublicKeyAccount]] = PermissionCFKeys.participantPubKey(addressId)

  def networkParticipants(): Key[Seq[BigInt]] = PermissionCFKeys.networkParticipants()

  /**
    * Docker contracts artifacts
    */
  def contractIdsSet(storage: RocksDBStorage): RocksDBSet[ByteStr] = ContractCFKeys.contractIdsSet(storage)

  def contractHistory(contractId: ByteStr): Key[Seq[Int]] = ContractCFKeys.contractHistory(contractId)

  def contract(contractId: ByteStr)(height: Int): Key[Option[ContractInfo]] = ContractCFKeys.contract(contractId)(height)

  def contractKeys(contractId: ByteStr, storage: RocksDBStorage): RocksDBSet[String] = ContractCFKeys.contractKeys(contractId, storage)

  def contractDataHistory(contractId: ByteStr, key: String): Key[Seq[Int]] = ContractCFKeys.contractDataHistory(contractId, key)

  def contractData(contractId: ByteStr, key: String)(height: Int): Key[Option[DataEntry[_]]] = ContractCFKeys.contractData(contractId, key)(height)

  def executedTxIdFor(txId: ByteStr): Key[Option[ByteStr]] = ContractCFKeys.executedTxIdFor(txId)

  /**
    * Privacy artifacts
    */
  def policyOwners(storage: RocksDBStorage, policyId: ByteStr): RocksDBSet[Address] = PrivacyCFKeys.policyOwners(storage, policyId)

  def policyRecipients(storage: RocksDBStorage, policyId: ByteStr): RocksDBSet[Address] = PrivacyCFKeys.policyRecipients(storage, policyId)

  def policyDataHashes(storage: RocksDBStorage, policyId: ByteStr): RocksDBSet[PolicyDataHash] = PrivacyCFKeys.policyDataHashes(storage, policyId)

  def policyDataHashTxId(id: PolicyDataId): Key[Option[ByteStr]] = PrivacyCFKeys.policyDataHashTxId(id)

  val lastNonEmptyRoleAddressId: Key[Option[BigInt]] = AddressCFKeys.LastNonEmptyRoleAddressId

  def nonEmptyRoleAddressId(address: Address): Key[Option[BigInt]] = AddressCFKeys.nonEmptyRoleAddressId(address)

  def idToNonEmptyRoleAddress(id: BigInt): Key[Address] = AddressCFKeys.idToNonEmptyRoleAddress(id)

  def pendingPrivacyItemsSet(storage: RocksDBStorage): RocksDBSet[PolicyDataId] = PrivacyCFKeys.pendingPrivacyItemsSet(storage)

  def lostPrivacyItemsSet(storage: RocksDBStorage): RocksDBSet[PolicyDataId] = PrivacyCFKeys.lostPrivacyItemsSet(storage)

  def unconfirmedPrivacyIdsSet(storage: RocksDBStorage): RocksDBSet[PolicyDataId] = PrivacyCFKeys.unconfirmedPrivacyIdsSet(storage)

  def policyIdsSet(storage: RocksDBStorage): RocksDBSet[ByteStr] = PrivacyCFKeys.policyIdsSet(storage)

  def policyItemDescriptor(policyId: ByteStr, dataHash: PolicyDataHash): Key[Option[PrivacyItemDescriptor]] =
    PrivacyCFKeys.itemDescriptor(policyId, dataHash)
}
