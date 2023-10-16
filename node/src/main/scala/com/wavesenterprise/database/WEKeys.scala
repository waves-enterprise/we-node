package com.wavesenterprise.database

import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.acl.PermissionOp
import com.wavesenterprise.consensus.MinerBanlistEntry
import com.wavesenterprise.consensus.MinerBanlistEntry.CancelledWarning
import com.wavesenterprise.database.KeyHelpers._
import com.wavesenterprise.database.keys._
import com.wavesenterprise.database.rocksdb.MainRocksDBStorage
import com.wavesenterprise.database.RocksDBSet._
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId, PrivacyItemDescriptor}
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.pki.CrlData

import java.net.URL
import java.security.cert.Certificate

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
  def permissions(addressId: BigInt): MainDBKey[Option[Seq[PermissionOp]]] = PermissionCFKeys.permissions(addressId)

  def miners: MainDBKey[Seq[BigInt]] = PermissionCFKeys.miners()

  def validators: MainDBKey[Seq[BigInt]] = PermissionCFKeys.validators()

  val lastNonEmptyRoleAddressId: MainDBKey[Option[BigInt]] = AddressCFKeys.LastNonEmptyRoleAddressId

  def nonEmptyRoleAddressId(address: Address): MainDBKey[Option[BigInt]] = AddressCFKeys.nonEmptyRoleAddressId(address)

  def idToNonEmptyRoleAddress(id: BigInt): MainDBKey[Address] = AddressCFKeys.idToNonEmptyRoleAddress(id)

  /**
    * PoA artifacts
    */
  def minerBanHistoryV1(addressId: BigInt): MainDBKey[Option[Seq[MinerBanlistEntry]]] =
    MainDBKey.opt("minerBanHistory", addr(MinerBanHistoryPrefix, addressId), readMinerBanlistEntries, writeMinerBanlistEntries)

  def minerCancelledWarnings(addressId: BigInt): MainDBKey[Option[Seq[CancelledWarning]]] =
    MainDBKey.opt("minerCancelledWarnings", addr(MinerCancelledWarningsPrefix, addressId), readMinerCancelledWarnings, writeMinerCancelledWarnings)

  def lastMinerBanHistoryV2EntryId(addressId: BigInt): MainDBKey[Option[BigInt]] =
    MainDBKey.opt("lastMinerBanHistoryV2EntryId", addr(LastMinerBanHistoryEntryIdPrefix, addressId), BigInt(_), _.toByteArray)

  def idToMinerBanHistoryV2Entry(entryId: BigInt, addressId: BigInt): MainDBKey[MinerBanlistEntry] = {
    MainDBKey(
      "idToMinerBanHistoryV2Entry",
      bytes(MinerBanHistoryEntryPrefix, entryId.toByteArray ++ addressId.toByteArray),
      MinerBanlistEntry.fromBytes(_).explicitGet(),
      _.bytes
    )
  }

  def participantPubKey(addressId: BigInt): MainDBKey[Option[PublicKeyAccount]] = PermissionCFKeys.participantPubKey(addressId)

  def networkParticipants(): MainDBKey[Seq[BigInt]] = PermissionCFKeys.networkParticipants()

  /**
    * Docker contracts artifacts
    */
  def contractIdsSet(storage: MainRocksDBStorage): MainRocksDBSet[ByteStr] = ContractCFKeys.contractIdsSet(storage)

  def contractHistory(contractId: ByteStr): MainDBKey[Seq[Int]] = ContractCFKeys.contractHistory(contractId)

  def contract(contractId: ByteStr)(height: Int): MainDBKey[Option[ContractInfo]] = ContractCFKeys.contract(contractId)(height)

  def contractKeys(contractId: ByteStr, storage: MainRocksDBStorage): MainRocksDBSet[String] = ContractCFKeys.contractKeys(contractId, storage)

  def contractDataHistory(contractId: ByteStr, key: String): MainDBKey[Seq[Int]] = ContractCFKeys.contractDataHistory(contractId, key)

  def contractData(contractId: ByteStr, key: String)(height: Int): MainDBKey[Option[DataEntry[_]]] =
    ContractCFKeys.contractData(contractId, key)(height)

  def executedTxIdFor(txId: ByteStr): MainDBKey[Option[ByteStr]] = ContractCFKeys.executedTxIdFor(txId)

  def contractIdToStateId(contractId: ByteStr): MainDBKey[Option[BigInt]] = ContractCFKeys.contractIdToStateId(contractId)

  def stateIdToContractId(contractStateId: BigInt): MainDBKey[ByteStr] = ContractCFKeys.stateIdToContractId(contractStateId)

  def contractWestBalanceHistory(contractStateId: BigInt): MainDBKey[Seq[Int]] = ContractCFKeys.contractWestBalanceHistory(contractStateId)

  def contractWestBalance(contractStateId: BigInt)(height: Int): MainDBKey[Long] = ContractCFKeys.contractWestBalance(contractStateId)(height)

  def contractAssetList(contractStateId: BigInt): MainDBKey[Set[ByteStr]] = ContractCFKeys.contractAssetList(contractStateId)

  def contractAssetBalanceHistory(contractStateId: BigInt, assetId: ByteStr): MainDBKey[Seq[Int]] =
    ContractCFKeys.contractAssetBalanceHistory(contractStateId, assetId)

  def contractAssetBalance(contractStateId: BigInt, assetId: ByteStr)(height: Int): MainDBKey[Long] =
    ContractCFKeys.contractAssetBalance(contractStateId, assetId)(height)

  val lastContractStateId: MainDBKey[Option[BigInt]] = ContractCFKeys.LastContractStateId

  def changedContracts(height: Int): MainDBKey[Seq[BigInt]] = ContractCFKeys.changedContracts(height)

  /**
    * Privacy artifacts
    */
  def policyOwners(storage: MainRocksDBStorage, policyId: ByteStr): MainRocksDBSet[Address] = PrivacyCFKeys.policyOwners(storage, policyId)

  def policyRecipients(storage: MainRocksDBStorage, policyId: ByteStr): MainRocksDBSet[Address] = PrivacyCFKeys.policyRecipients(storage, policyId)

  def policyDataHashes(storage: MainRocksDBStorage, policyId: ByteStr): MainRocksDBSet[PolicyDataHash] =
    PrivacyCFKeys.policyDataHashes(storage, policyId)

  def policyDataHashTxId(id: PolicyDataId): MainDBKey[Option[ByteStr]] = PrivacyCFKeys.policyDataHashTxId(id)

  def pendingPrivacyItemsSet(storage: MainRocksDBStorage): MainRocksDBSet[PolicyDataId] = PrivacyCFKeys.pendingPrivacyItemsSet(storage)

  def lostPrivacyItemsSet(storage: MainRocksDBStorage): MainRocksDBSet[PolicyDataId] = PrivacyCFKeys.lostPrivacyItemsSet(storage)

  def unconfirmedPrivacyIdsSet(storage: MainRocksDBStorage): MainRocksDBSet[PolicyDataId] = PrivacyCFKeys.unconfirmedPrivacyIdsSet(storage)

  def policyIdsSet(storage: MainRocksDBStorage): MainRocksDBSet[ByteStr] = PrivacyCFKeys.policyIdsSet(storage)

  def policyItemDescriptor(policyId: ByteStr, dataHash: PolicyDataHash): MainDBKey[Option[PrivacyItemDescriptor]] =
    PrivacyCFKeys.itemDescriptor(policyId, dataHash)

  /**
    * Certificates artifacts
    */
  def certByDnHash(distinguishedNameHash: ByteStr): MainDBKey[Option[Certificate]] = CertificatesCFKeys.certByDnHash(distinguishedNameHash)

  def certDnHashByPublicKey(publicKey: PublicKeyAccount): MainDBKey[Option[ByteStr]] = CertificatesCFKeys.certDnHashByPublicKey(publicKey)

  def certDnHashByFingerprint(fingerprint: ByteStr): MainDBKey[Option[ByteStr]] = CertificatesCFKeys.certDnHashByFingerprint(fingerprint)

  def certDnHashesAtHeight(height: Int): MainDBKey[Set[ByteStr]] = CertificatesCFKeys.certDnHashesAtHeight(height)

  def crlIssuers(storage: MainRocksDBStorage): MainRocksDBSet[PublicKeyAccount] = CertificatesCFKeys.crlIssuers(storage)

  def crlUrlsByIssuerPublicKey(publicKey: PublicKeyAccount, storage: MainRocksDBStorage): MainRocksDBSet[URL] =
    CertificatesCFKeys.crlUrlsByIssuerPublicKey(publicKey, storage)

  def crlDataByHash(hash: ByteStr): MainDBKey[Option[CrlData]] = CertificatesCFKeys.crlDataByHash(hash)

  def crlHashByKey(crlKey: CrlKey): MainDBKey[Option[ByteStr]] = CertificatesCFKeys.crlHashByKey(crlKey)
}
