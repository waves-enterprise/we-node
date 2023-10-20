package com.wavesenterprise.database.keys

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.acl.PermissionOp
import com.wavesenterprise.database.KeyHelpers.{addr, bytes}
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.PermissionCF
import com.wavesenterprise.database.{MainDBKey, readBigIntSeq, readPermissionSeq, writeBigIntSeq, writePermissions}

object PermissionCFKeys {

  val PermissionPrefix: Short            = 1
  val MinersKeysPrefix: Short            = 2
  val NetworkParticipantPrefix: Short    = 3
  val NetworkParticipantSeqPrefix: Short = 4
  val ContractValidatorKeysPrefix: Short = 5

  def permissions(addressId: BigInt): MainDBKey[Option[Seq[PermissionOp]]] =
    MainDBKey.opt("permissions", PermissionCF, addr(PermissionPrefix, addressId), readPermissionSeq, writePermissions)

  def miners(): MainDBKey[Seq[BigInt]] =
    MainDBKey("miners", PermissionCF, bytes(MinersKeysPrefix, Array.emptyByteArray), readBigIntSeq, writeBigIntSeq)

  def participantPubKey(addressId: BigInt): MainDBKey[Option[PublicKeyAccount]] =
    MainDBKey.opt("networkParticipantPubKey", PermissionCF, addr(NetworkParticipantPrefix, addressId), PublicKeyAccount.apply, _.publicKey.getEncoded)

  def networkParticipants(): MainDBKey[Seq[BigInt]] =
    MainDBKey("networkParticipantsSeq", PermissionCF, bytes(NetworkParticipantSeqPrefix, Array.emptyByteArray), readBigIntSeq, writeBigIntSeq)

  def validators(): MainDBKey[Seq[BigInt]] =
    MainDBKey("contract-validators", PermissionCF, bytes(ContractValidatorKeysPrefix, Array.emptyByteArray), readBigIntSeq, writeBigIntSeq)
}
