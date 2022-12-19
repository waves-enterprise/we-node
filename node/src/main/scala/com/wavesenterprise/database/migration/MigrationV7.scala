package com.wavesenterprise.database.migration

import com.google.common.io.ByteStreams.{newDataInput, newDataOutput}
import com.google.common.primitives.{Ints, Shorts}
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.crypto
import com.wavesenterprise.database.KeyHelpers.hBytes
import com.wavesenterprise.database.keys.ContractCFKeys.{ContractIdsPrefix, ContractPrefix}
import com.wavesenterprise.database.rocksdb.ColumnFamily.ContractCF
import com.wavesenterprise.database.rocksdb.RW
import com.wavesenterprise.database.{InternalRocksDBSet, Key, Keys, WEKeys}
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.docker.CreateContractTransaction
import com.wavesenterprise.utils.DatabaseUtils.{ByteArrayDataInputExt, ByteArrayDataOutputExt}

//noinspection UnstableApiUsage
object MigrationV7 {

  case class LegacyContractInfo(creator: PublicKeyAccount, contractId: ByteStr, image: String, imageHash: String, version: Int, active: Boolean)

  case class ModernContractInfo(creator: PublicKeyAccount,
                                contractId: ByteStr,
                                image: String,
                                imageHash: String,
                                version: Int,
                                active: Boolean,
                                validationPolicy: ValidationPolicy,
                                apiVersion: ContractApiVersion)

  private[migration] object KeysInfo {

    def legacyContractInfoKey(contractId: ByteStr)(height: Int): Key[Option[LegacyContractInfo]] =
      Key.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), parseLegacyContractInfo, writeLegacyContractInfo)

    def modernContractInfoKey(contractId: ByteStr)(height: Int): Key[Option[ModernContractInfo]] =
      Key.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), parseModernContractInfo, writeModernContractInfo)

    private def writeLegacyContractInfo(contractInfo: LegacyContractInfo): Array[Byte] = {
      import contractInfo._
      val ndo = newDataOutput()
      ndo.writePublicKey(creator)
      ndo.writeBytes(contractId.arr)
      ndo.writeString(image)
      ndo.writeString(imageHash)
      ndo.writeInt(version)
      ndo.writeBoolean(active)
      ndo.toByteArray
    }

    private def parseLegacyContractInfo(bytes: Array[Byte]): LegacyContractInfo = {
      val ndi        = newDataInput(bytes)
      val creator    = PublicKeyAccount(ndi.readBytes(crypto.KeyLength))
      val contractId = ndi.readBytes()
      val image      = ndi.readString()
      val imageHash  = ndi.readString()
      val version    = ndi.readInt()
      val active     = ndi.readBoolean()
      LegacyContractInfo(creator, ByteStr(contractId), image, imageHash, version, active)
    }

    def writeModernContractInfo(contractInfo: ModernContractInfo): Array[Byte] = {
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

    def parseModernContractInfo(bytes: Array[Byte]): ModernContractInfo = {
      val (creatorBytes, creatorEnd)              = bytes.take(crypto.KeyLength)                                             -> crypto.KeyLength
      val (contractId, contractIdEnd)             = BinarySerializer.parseShortByteStr(bytes, creatorEnd)
      val (image, imageEnd)                       = BinarySerializer.parseShortString(bytes, contractIdEnd)
      val (imageHash, imageHashEnd)               = BinarySerializer.parseShortString(bytes, imageEnd)
      val (version, versionEnd)                   = Ints.fromByteArray(bytes.slice(imageHashEnd, imageHashEnd + Ints.BYTES)) -> (imageHashEnd + Ints.BYTES)
      val (active, activeEnd)                     = (bytes(versionEnd) == 1)                                                 -> (versionEnd + 1)
      val (validationPolicy, validationPolicyEnd) = ValidationPolicy.fromBytesUnsafe(bytes, activeEnd)
      val (apiVersion, _)                         = ContractApiVersion.fromBytesUnsafe(bytes, validationPolicyEnd)

      ModernContractInfo(PublicKeyAccount(creatorBytes), contractId, image, imageHash, version, active, validationPolicy, apiVersion)
    }
  }

  private val ContractsIdSet = new InternalRocksDBSet[ByteStr](
    name = "contract-ids",
    columnFamily = ContractCF,
    prefix = Shorts.toByteArray(ContractIdsPrefix),
    itemEncoder = (_: ByteStr).arr,
    itemDecoder = ByteStr(_)
  )

  def apply(rw: RW): Unit = {
    for {
      contractId <- ContractsIdSet.members(rw)
      (_, createTx)   = findCreateContractTx(rw, contractId)
      contractHistory = rw.get(WEKeys.contractHistory(contractId))
      height          <- contractHistory
      oldContractInfo <- rw.get(KeysInfo.legacyContractInfoKey(contractId)(height)).toSeq
    } yield {
      val newContractInfo = ModernContractInfo(
        creator = createTx.sender,
        contractId = oldContractInfo.contractId,
        image = oldContractInfo.image,
        imageHash = oldContractInfo.imageHash,
        version = oldContractInfo.version,
        active = oldContractInfo.active,
        validationPolicy = ValidationPolicy.Default,
        apiVersion = ContractApiVersion.Initial
      )
      rw.put(KeysInfo.modernContractInfoKey(contractId)(height), Some(newContractInfo))
    }
  }

  private def findCreateContractTx(rw: RW, txId: ByteStr): (Int, CreateContractTransaction) = {
    rw.get(Keys.transactionInfo(txId))
      .collect {
        case (h: Int, tx: CreateContractTransaction) => h -> tx
      }
      .getOrElse(throw new RuntimeException(s"Create contract transaction '$txId' is not found"))
  }
}
