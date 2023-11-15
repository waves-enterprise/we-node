package com.wavesenterprise.database.migration

import com.google.common.io.ByteArrayDataOutput
import com.google.common.io.ByteStreams.newDataOutput
import com.google.common.primitives.{Ints, Shorts}
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.crypto
import com.wavesenterprise.database.KeyHelpers.hBytes
import com.wavesenterprise.database.keys.ContractCFKeys.{ContractIdsPrefix, ContractPrefix}
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.ContractCF
import com.wavesenterprise.database.rocksdb.{MainDBColumnFamily, MainReadWriteDB}
import com.wavesenterprise.database.{InternalRocksDBSet, MainDBKey, WEKeys}
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.serialization.{BinarySerializer, ModelsBinarySerializer}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.DatabaseUtils.ByteArrayDataOutputExt

object MigrationV11 {

  object KeysInfo {
    def legacyContractInfoKey(contractId: ByteStr)(height: Int): MainDBKey[Option[LegacyContractInfo]] =
      MainDBKey.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), parseLegacyContractInfo, writeLegacyContractInfo)

    def modernContractInfoKey(contractId: ByteStr)(height: Int): MainDBKey[Option[ModernContractInfo]] =
      MainDBKey.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), parseModernContractInfo, writeModernContractInfo)
  }

  private val ContractsIdSet = new InternalRocksDBSet[ByteStr, MainDBColumnFamily](
    name = "contract-ids",
    columnFamily = ContractCF,
    prefix = Shorts.toByteArray(ContractIdsPrefix),
    itemEncoder = (_: ByteStr).arr,
    itemDecoder = ByteStr(_),
    keyConstructors = MainDBKey
  )

  def apply(rw: MainReadWriteDB): Unit = {
    for {
      contractId <- ContractsIdSet.members(rw)
      contractHistory = rw.get(WEKeys.contractHistory(contractId))
      height          <- contractHistory
      oldContractInfo <- rw.get(KeysInfo.legacyContractInfoKey(contractId)(height)).toSeq
    } yield {
      val newContractInfo = ModernContractInfo(
        creator = oldContractInfo.creator,
        contractId = oldContractInfo.contractId,
        image = oldContractInfo.image,
        imageHash = oldContractInfo.imageHash,
        version = oldContractInfo.version,
        active = oldContractInfo.active,
        validationPolicy = oldContractInfo.validationPolicy,
        apiVersion = oldContractInfo.apiVersion,
        isConfidential = false,
        groupParticipants = Set(),
        groupOwners = Set()
      )
      rw.put(KeysInfo.modernContractInfoKey(contractId)(height), Some(newContractInfo))
    }
  }

  case class LegacyContractInfo(creator: PublicKeyAccount,
                                contractId: ByteStr,
                                image: String,
                                imageHash: String,
                                version: Int,
                                active: Boolean,
                                validationPolicy: ValidationPolicy,
                                apiVersion: ContractApiVersion)

  case class ModernContractInfo(creator: PublicKeyAccount,
                                contractId: ByteStr,
                                image: String,
                                imageHash: String,
                                version: Int,
                                active: Boolean,
                                validationPolicy: ValidationPolicy,
                                apiVersion: ContractApiVersion,
                                isConfidential: Boolean,
                                groupParticipants: Set[Address],
                                groupOwners: Set[Address])

  def writeLegacyContractInfo(contractInfo: LegacyContractInfo): Array[Byte] = {
    import contractInfo._
    val ndo = newDataOutput()
    ndo.writePublicKey(creator)
    ndo.writeBytes(contractId.arr)
    ndo.writeString(image)
    ndo.writeString(imageHash)
    ndo.writeInt(version)
    ndo.writeBoolean(active)
    ndo.write(contractInfo.validationPolicy.bytes)
    ndo.write(contractInfo.apiVersion.bytes)
    ndo.toByteArray
  }

  def parseLegacyContractInfo(bytes: Array[Byte]): LegacyContractInfo = {
    val (creatorBytes, creatorEnd)              = bytes.take(crypto.KeyLength)                                             -> crypto.KeyLength
    val (contractId, contractIdEnd)             = BinarySerializer.parseShortByteStr(bytes, creatorEnd)
    val (image, imageEnd)                       = BinarySerializer.parseShortString(bytes, contractIdEnd)
    val (imageHash, imageHashEnd)               = BinarySerializer.parseShortString(bytes, imageEnd)
    val (version, versionEnd)                   = Ints.fromByteArray(bytes.slice(imageHashEnd, imageHashEnd + Ints.BYTES)) -> (imageHashEnd + Ints.BYTES)
    val (active, activeEnd)                     = (bytes(versionEnd) == 1)                                                 -> (versionEnd + 1)
    val (validationPolicy, validationPolicyEnd) = ValidationPolicy.fromBytesUnsafe(bytes, activeEnd)
    val (apiVersion, _)                         = ContractApiVersion.fromBytesUnsafe(bytes, validationPolicyEnd)

    LegacyContractInfo(PublicKeyAccount(creatorBytes), contractId, image, imageHash, version, active, validationPolicy, apiVersion)
  }

  def writeModernContractInfo(contractInfo: ModernContractInfo): Array[Byte] = {
    def addressWriter(address: Address, output: ByteArrayDataOutput): Unit = {
      output.write(address.bytes.arr)
    }

    import contractInfo._
    val ndo = newDataOutput()
    ndo.writePublicKey(creator)
    ndo.writeBytes(contractId.arr)
    ndo.writeString(image)
    ndo.writeString(imageHash)
    ndo.writeInt(version)
    ndo.writeBoolean(active)
    ndo.write(contractInfo.validationPolicy.bytes)
    ndo.write(contractInfo.apiVersion.bytes)
    ndo.writeBoolean(isConfidential)
    BinarySerializer.writeShortIterable(contractInfo.groupParticipants, addressWriter, ndo)
    BinarySerializer.writeShortIterable(contractInfo.groupOwners, addressWriter, ndo)

    ndo.toByteArray
  }

  def parseModernContractInfo(bytes: Array[Byte]): ModernContractInfo = {

    val (creatorBytes, creatorEnd)                = bytes.take(crypto.KeyLength)                                             -> crypto.KeyLength
    val (contractId, contractIdEnd)               = BinarySerializer.parseShortByteStr(bytes, creatorEnd)
    val (image, imageEnd)                         = BinarySerializer.parseShortString(bytes, contractIdEnd)
    val (imageHash, imageHashEnd)                 = BinarySerializer.parseShortString(bytes, imageEnd)
    val (version, versionEnd)                     = Ints.fromByteArray(bytes.slice(imageHashEnd, imageHashEnd + Ints.BYTES)) -> (imageHashEnd + Ints.BYTES)
    val (active, activeEnd)                       = (bytes(versionEnd) == 1)                                                 -> (versionEnd + 1)
    val (validationPolicy, validationPolicyEnd)   = ValidationPolicy.fromBytesUnsafe(bytes, activeEnd)
    val (apiVersion, apiVersionEnd)               = ContractApiVersion.fromBytesUnsafe(bytes, validationPolicyEnd)
    val (isConfidential, isConfidentialEnd)       = (bytes(apiVersionEnd) == 1)                                              -> (apiVersionEnd + 1)
    val (groupParticipants, groupParticipantsEnd) = ModelsBinarySerializer.parseAddressesSet(bytes, isConfidentialEnd)
    val (groupOwners, _)                          = ModelsBinarySerializer.parseAddressesSet(bytes, groupParticipantsEnd)

    ModernContractInfo(PublicKeyAccount(creatorBytes),
                       contractId,
                       image,
                       imageHash,
                       version,
                       active,
                       validationPolicy,
                       apiVersion,
                       isConfidential,
                       groupParticipants,
                       groupOwners)
  }
}
