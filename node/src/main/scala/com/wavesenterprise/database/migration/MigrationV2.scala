package com.wavesenterprise.database.migration

import com.google.common.io.ByteStreams.{newDataInput, newDataOutput}
import com.google.common.primitives.{Bytes, Ints, Shorts}
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.crypto
import com.wavesenterprise.database.KeyHelpers.hBytes
import com.wavesenterprise.database.address.AddressTransactions
import com.wavesenterprise.database.keys.ContractCFKeys.{ContractIdsPrefix, ContractPrefix}
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.ContractCF
import com.wavesenterprise.database.rocksdb.{MainDBColumnFamily, MainReadWriteDB}
import com.wavesenterprise.database.{InternalRocksDBSet, Keys, MainDBKey, WEKeys}
import com.wavesenterprise.serialization.Deser
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.AssetId
import com.wavesenterprise.transaction.assets.{BurnTransaction, IssueTransaction}
import com.wavesenterprise.transaction.docker.CreateContractTransaction
import com.wavesenterprise.utils.DatabaseUtils._

import java.nio.charset.StandardCharsets.UTF_8

//noinspection UnstableApiUsage
object MigrationV2 {

  case class AssetInfoV1(isReissuable: Boolean, volume: BigInt)

  case class AssetInfoV2(issuer: PublicKeyAccount,
                         height: Int,
                         timestamp: Long,
                         name: String,
                         description: String,
                         decimals: Byte,
                         reissuable: Boolean,
                         volume: BigInt,
                         wasBurnt: Boolean = false)

  def parseAssetInfoV2(data: Array[Byte]): AssetInfoV2 = {
    val ndi         = newDataInput(data)
    val issuer      = ndi.readPublicKey
    val height      = ndi.readInt()
    val timestamp   = ndi.readLong()
    val name        = ndi.readString()
    val description = ndi.readString()
    val decimals    = ndi.readByte()
    val reissuable  = ndi.readBoolean()
    val volume      = ndi.readBigInt()
    val wasBurnt    = ndi.readBoolean()
    AssetInfoV2(issuer, height, timestamp, name, description, decimals, reissuable, volume, wasBurnt)
  }

  def writeAssetInfoV2(assetInfo: AssetInfoV2): Array[Byte] = {
    val ndo = newDataOutput()
    ndo.writePublicKey(assetInfo.issuer)
    ndo.writeInt(assetInfo.height)
    ndo.writeLong(assetInfo.timestamp)
    ndo.writeString(assetInfo.name)
    ndo.writeString(assetInfo.description)
    ndo.writeByte(assetInfo.decimals)
    ndo.writeBoolean(assetInfo.reissuable)
    ndo.writeBigInt(assetInfo.volume)
    ndo.writeBoolean(assetInfo.wasBurnt)
    ndo.toByteArray
  }

  case class LegacyContractInfo(contractId: ByteStr, image: String, imageHash: String, version: Int, active: Boolean)

  case class ModernContractInfo(creator: PublicKeyAccount, contractId: ByteStr, image: String, imageHash: String, version: Int, active: Boolean)

  object KeysInfo {

    def assetInfoV1Key(assetId: ByteStr)(height: Int): MainDBKey[AssetInfoV1] = {
      MainDBKey("asset-info", hBytes(Keys.AssetInfoPrefix, height, assetId.arr), parseAssetInfoV1, writeAssetInfoV1)
    }

    def assetInfoV2Key(assetId: ByteStr)(height: Int): MainDBKey[AssetInfoV2] = {
      MainDBKey("asset-info", hBytes(Keys.AssetInfoPrefix, height, assetId.arr), parseAssetInfoV2, writeAssetInfoV2)
    }

    private def parseAssetInfoV1(bytes: Array[Byte]): AssetInfoV1 = {
      val ndi     = newDataInput(bytes)
      val reissue = ndi.readBoolean()
      val volume  = ndi.readBigInt()
      AssetInfoV1(reissue, volume)
    }

    private def writeAssetInfoV1(assetInfo: AssetInfoV1): Array[Byte] = {
      val ndo = newDataOutput()
      ndo.writeBoolean(assetInfo.isReissuable)
      ndo.writeBigInt(assetInfo.volume)
      ndo.toByteArray
    }

    def legacyContractInfoKey(contractId: ByteStr)(height: Int): MainDBKey[Option[LegacyContractInfo]] =
      MainDBKey.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), parseLegacyContractInfo, writeLegacyContractInfo)

    def modernContractInfoKey(contractId: ByteStr)(height: Int): MainDBKey[Option[ModernContractInfo]] =
      MainDBKey.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), parseModernContractInfo, writeModernContractInfo)

    private def writeLegacyContractInfo(contractInfo: LegacyContractInfo): Array[Byte] = {
      import contractInfo._
      Bytes.concat(
        Deser.serializeArray(contractId.arr),
        Deser.serializeArray(image.getBytes(UTF_8)),
        Deser.serializeArray(imageHash.getBytes(UTF_8)),
        Ints.toByteArray(version),
        Ints.toByteArray(if (active) 1 else 0)
      )
    }

    private def parseLegacyContractInfo(bytes: Array[Byte]): LegacyContractInfo = {
      val (contractId, imageOffset)  = Deser.parseArraySize(bytes, 0)
      val (image, imageHashOffset)   = Deser.parseArraySize(bytes, imageOffset)
      val (imageHash, versionOffset) = Deser.parseArraySize(bytes, imageHashOffset)
      val version                    = Ints.fromByteArray(bytes.slice(versionOffset, versionOffset + Integer.BYTES))
      val active                     = Ints.fromByteArray(bytes.slice(versionOffset + Integer.BYTES, versionOffset + 2 * Integer.BYTES)) == 1
      LegacyContractInfo(ByteStr(contractId), new String(image, UTF_8), new String(imageHash, UTF_8), version, active)
    }

    private def writeModernContractInfo(contractInfo: ModernContractInfo): Array[Byte] = {
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

    private def parseModernContractInfo(bytes: Array[Byte]): ModernContractInfo = {
      val ndi        = newDataInput(bytes)
      val creator    = PublicKeyAccount(ndi.readBytes(crypto.KeyLength))
      val contractId = ndi.readBytes()
      val image      = ndi.readString()
      val imageHash  = ndi.readString()
      val version    = ndi.readInt()
      val active     = ndi.readBoolean()
      ModernContractInfo(creator, ByteStr(contractId), image, imageHash, version, active)
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
    migrateAssets(rw)
    migrateContracts(rw)
  }

  private def migrateAssets(rw: MainReadWriteDB): Unit = {
    val lastAddressId = rw.get(Keys.lastAddressId).getOrElse(BigInt(0))
    val assetsBuilder = Set.newBuilder[AssetId]
    for (id <- BigInt(1) to lastAddressId) {
      assetsBuilder ++= rw.get(Keys.assetList(id))
    }
    val assets = assetsBuilder.result()

    for {
      assetId <- assets
      (issueHeight, issueTx) = findIssueTx(rw, assetId)
      assetHistory           = rw.get(Keys.assetInfoHistory(assetId))
      height <- assetHistory
      oldAssetInfo = rw.get(KeysInfo.assetInfoV1Key(assetId)(height))
      wasBurnt     = assetWasBurnt(rw, issueTx)
    } yield {
      val newAssetInfo = AssetInfoV2(
        issuer = issueTx.sender,
        height = issueHeight,
        timestamp = issueTx.timestamp,
        name = new String(issueTx.name, UTF_8),
        description = new String(issueTx.description, UTF_8),
        decimals = issueTx.decimals,
        reissuable = oldAssetInfo.isReissuable,
        volume = oldAssetInfo.volume,
        wasBurnt = wasBurnt
      )
      rw.put(KeysInfo.assetInfoV2Key(assetId)(height), newAssetInfo)
    }
  }

  def findIssueTx(rw: MainReadWriteDB, txId: ByteStr): (Int, IssueTransaction) = {
    rw.get(Keys.transactionInfo(txId))
      .collect {
        case (h: Int, tx: IssueTransaction) => h -> tx
      }
      .getOrElse(throw new RuntimeException(s"Issue transaction '$txId' is not found"))
  }

  private def assetWasBurnt(rw: MainReadWriteDB, tx: IssueTransaction): Boolean = {
    AddressTransactions(rw, tx.sender.toAddress, Set(BurnTransaction.typeId), Int.MaxValue, None)
      .getOrElse(Seq.empty)
      .exists {
        case (_, t: BurnTransaction) if t.assetId == tx.assetId() => true
        case _                                                    => false
      }
  }

  private def migrateContracts(rw: MainReadWriteDB): Unit = {
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
        active = oldContractInfo.active
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
