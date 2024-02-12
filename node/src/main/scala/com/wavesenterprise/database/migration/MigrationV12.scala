package com.wavesenterprise.database.migration

import com.google.common.io.ByteStreams.newDataOutput
import com.google.common.primitives.{Ints, Shorts}
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.crypto
import com.wavesenterprise.database.KeyHelpers.hBytes
import com.wavesenterprise.database.keys.ContractCFKeys.{ContractIdsPrefix, ContractPrefix}
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.ContractCF
import com.wavesenterprise.database.rocksdb.{MainDBColumnFamily, MainReadWriteDB}
import com.wavesenterprise.database.{InternalRocksDBSet, Keys, MainDBKey, WEKeys}
import com.wavesenterprise.docker.StoredContract.DockerContract
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.docker.{ContractApiVersion, ContractInfo}
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.docker.CreateContractTransaction
import com.wavesenterprise.utils.DatabaseUtils.ByteArrayDataOutputExt
import monix.eval.Coeval

//noinspection UnstableApiUsage
object MigrationV12 {

  case class LegacyContractInfo(creator: PublicKeyAccount,
                                contractId: ByteStr,
                                image: String,
                                imageHash: String,
                                version: Int,
                                active: Boolean,
                                validationPolicy: ValidationPolicy,
                                apiVersion: ContractApiVersion)

  type ModernContractInfo = ContractInfo

  private[migration] object KeysInfo {

    def legacyContractInfoKey(contractId: ByteStr)(height: Int): MainDBKey[Option[LegacyContractInfo]] =
      MainDBKey.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), parseLegacyContractInfo, writeLegacyContractInfo)

    def modernContractInfoKey(contractId: ByteStr)(height: Int): MainDBKey[Option[ModernContractInfo]] =
      MainDBKey.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), ContractInfo.fromBytes, ContractInfo.toBytes)

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
      (_, createTx)   = findCreateContractTx(rw, contractId)
      contractHistory = rw.get(WEKeys.contractHistory(contractId))
      height          <- contractHistory
      oldContractInfo <- rw.get(KeysInfo.legacyContractInfoKey(contractId)(height)).toSeq
    } yield {
      val newContractInfo = ContractInfo(
        creator = Coeval.pure(createTx.sender),
        contractId = oldContractInfo.contractId,
        storedContract = DockerContract(oldContractInfo.image, oldContractInfo.imageHash, oldContractInfo.apiVersion),
        version = oldContractInfo.version,
        active = oldContractInfo.active,
        validationPolicy = oldContractInfo.validationPolicy,
      )
      rw.put(KeysInfo.modernContractInfoKey(contractId)(height), Some(newContractInfo))
    }
  }

  private def findCreateContractTx(rw: MainReadWriteDB, txId: ByteStr): (Int, CreateContractTransaction) = {
    rw.get(Keys.transactionInfo(txId))
      .collect {
        case (h: Int, tx: CreateContractTransaction) => h -> tx
      }
      .getOrElse(throw new RuntimeException(s"Create contract transaction '$txId' is not found"))
  }
}
