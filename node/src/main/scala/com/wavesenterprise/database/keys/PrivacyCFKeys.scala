package com.wavesenterprise.database.keys

import com.google.common.primitives.{Longs, Shorts}
import com.wavesenterprise.account.Address
import com.wavesenterprise.database.KeyHelpers.{bytes, hash}
import com.wavesenterprise.database.RocksDBSet._
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.PrivacyCF
import com.wavesenterprise.database.rocksdb.MainRocksDBStorage
import com.wavesenterprise.database._
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId, PrivacyItemDescriptor}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.EitherUtils.EitherExt

object PrivacyCFKeys {

  val PolicyOwnersPrefix: Short                        = 1
  val PolicyRecipientsPrefix: Short                    = 2
  val PolicyDataHashPrefix: Short                      = 3
  val UnconfirmedPolicyDataItemUploadTimePrefix: Short = 4
  val PolicyDataHashTxIdPrefix: Short                  = 5
  val PolicyPendingItemsPrefix: Short                  = 6
  val PolicyLostItemsPrefix: Short                     = 7
  val UnconfirmedPolicyDataIdsPrefix: Short            = 8
  val PolicyIdsPrefix: Short                           = 9
  val PolicyItemDescriptorPrefix: Short                = 10

  def policyOwners(storage: MainRocksDBStorage, policyId: ByteStr): MainRocksDBSet[Address] = {
    RocksDBSet.newMain(
      name = "policy-owners",
      columnFamily = PrivacyCF,
      prefix = hash(PolicyOwnersPrefix, policyId),
      storage = storage,
      itemEncoder = _.bytes.arr,
      itemDecoder = Address.fromBytes(_).explicitGet()
    )
  }

  def policyRecipients(storage: MainRocksDBStorage, policyId: ByteStr): MainRocksDBSet[Address] = {
    RocksDBSet.newMain(
      name = "policy-recipients",
      columnFamily = PrivacyCF,
      prefix = hash(PolicyRecipientsPrefix, policyId),
      storage = storage,
      itemEncoder = _.bytes.arr,
      itemDecoder = Address.fromBytes(_).explicitGet()
    )
  }

  def policyDataHashes(storage: MainRocksDBStorage, policyId: ByteStr): MainRocksDBSet[PolicyDataHash] = {
    RocksDBSet.newMain(
      name = "policy-hashes",
      columnFamily = PrivacyCF,
      prefix = hash(PolicyDataHashPrefix, policyId),
      storage = storage,
      itemEncoder = _.bytes.arr,
      itemDecoder = PolicyDataHash.deserialize
    )
  }

  def unconfirmedPolicyDataItemUploadTime(id: PolicyDataId): MainDBKey[Long] = {
    MainDBKey(
      "unconfirmed-policy-data-item-upload-time",
      PrivacyCF,
      bytes(UnconfirmedPolicyDataItemUploadTimePrefix, id.policyId.arr ++ id.dataHash.bytes.arr),
      Option(_).fold(0L)(Longs.fromByteArray),
      Longs.toByteArray
    )
  }

  def policyDataHashTxId(id: PolicyDataId): MainDBKey[Option[ByteStr]] = {
    MainDBKey.opt("policy-data-hash-tx-id", PrivacyCF, bytes(PolicyDataHashTxIdPrefix, id.policyId.arr ++ id.dataHash.bytes.arr), ByteStr(_), _.arr)
  }

  def pendingPrivacyItemsSet(storage: MainRocksDBStorage): MainRocksDBSet[PolicyDataId] =
    RocksDBSet.newMain(
      name = "pending-privacy-items",
      columnFamily = PrivacyCF,
      prefix = bytes(PolicyPendingItemsPrefix, Array.emptyByteArray),
      storage = storage,
      itemEncoder = writePrivacyDataId,
      itemDecoder = readPrivacyDataId
    )

  def lostPrivacyItemsSet(storage: MainRocksDBStorage): MainRocksDBSet[PolicyDataId] =
    RocksDBSet.newMain(
      name = "lost-privacy-items",
      columnFamily = PrivacyCF,
      prefix = bytes(PolicyLostItemsPrefix, Array.emptyByteArray),
      storage = storage,
      itemEncoder = writePrivacyDataId,
      itemDecoder = readPrivacyDataId
    )

  def unconfirmedPrivacyIdsSet(storage: MainRocksDBStorage): MainRocksDBSet[PolicyDataId] =
    RocksDBSet.newMain(
      name = "unconfirmed-privacy-items",
      columnFamily = PrivacyCF,
      prefix = bytes(UnconfirmedPolicyDataIdsPrefix, Array.emptyByteArray),
      storage = storage,
      itemEncoder = writePrivacyDataId,
      itemDecoder = readPrivacyDataId
    )

  def policyIdsSet(storage: MainRocksDBStorage): MainRocksDBSet[ByteStr] =
    RocksDBSet.newMain[ByteStr](
      name = "policy-ids",
      columnFamily = PrivacyCF,
      prefix = Shorts.toByteArray(PolicyIdsPrefix),
      storage = storage,
      itemEncoder = (_: ByteStr).arr,
      itemDecoder = ByteStr(_)
    )

  def itemDescriptor(policyId: ByteStr, dataHash: PolicyDataHash): MainDBKey[Option[PrivacyItemDescriptor]] =
    MainDBKey.opt(
      name = "item-descriptor",
      columnFamily = PrivacyCF,
      key = bytes(PolicyItemDescriptorPrefix, Array.concat(policyId.arr, dataHash.bytes.arr)),
      parser = PrivacyItemDescriptor.fromBytesUnsafe(_),
      encoder = _.bytes
    )
}
