package com.wavesenterprise.database.keys

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.acl.PermissionOp
import com.wavesenterprise.database.KeyHelpers.{addr, bytes}
import com.wavesenterprise.database.rocksdb.ColumnFamily.PermissionCF
import com.wavesenterprise.database.{Key, readBigIntSeq, readPermissionSeq, writeBigIntSeq, writePermissions}

object PermissionCFKeys {

  val PermissionPrefix: Short            = 1
  val MinersKeysPrefix: Short            = 2
  val NetworkParticipantPrefix: Short    = 3
  val NetworkParticipantSeqPrefix: Short = 4
  val ContractValidatorKeysPrefix: Short = 5

  def permissions(addressId: BigInt): Key[Option[Seq[PermissionOp]]] =
    Key.opt("permissions", PermissionCF, addr(PermissionPrefix, addressId), readPermissionSeq, writePermissions)

  def miners(): Key[Seq[BigInt]] = Key("miners", PermissionCF, bytes(MinersKeysPrefix, Array.emptyByteArray), readBigIntSeq, writeBigIntSeq)

  def participantPubKey(addressId: BigInt): Key[Option[PublicKeyAccount]] =
    Key.opt("networkParticipantPubKey", PermissionCF, addr(NetworkParticipantPrefix, addressId), PublicKeyAccount.apply, _.publicKey.getEncoded)

  def networkParticipants(): Key[Seq[BigInt]] =
    Key("networkParticipantsSeq", PermissionCF, bytes(NetworkParticipantSeqPrefix, Array.emptyByteArray), readBigIntSeq, writeBigIntSeq)

  def validators(): Key[Seq[BigInt]] =
    Key("contract-validators", PermissionCF, bytes(ContractValidatorKeysPrefix, Array.emptyByteArray), readBigIntSeq, writeBigIntSeq)
}
