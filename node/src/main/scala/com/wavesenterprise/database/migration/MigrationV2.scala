package com.wavesenterprise.database.migration

import com.google.common.io.ByteStreams.{newDataInput, newDataOutput}
import com.google.common.primitives.{Bytes, Ints, Shorts}
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.crypto
import com.wavesenterprise.database.KeyHelpers.hBytes
import com.wavesenterprise.database.address.AddressTransactions
import com.wavesenterprise.database.keys.ContractCFKeys.{ContractIdsPrefix, ContractPrefix}
import com.wavesenterprise.database.rocksdb.ColumnFamily.ContractCF
import com.wavesenterprise.database.rocksdb.RW
import com.wavesenterprise.database.{InternalRocksDBSet, Key, Keys, WEKeys}
import com.wavesenterprise.serialization.Deser
import com.wavesenterprise.state.{AssetInfo, ByteStr}
import com.wavesenterprise.transaction.AssetId
import com.wavesenterprise.transaction.assets.{BurnTransaction, IssueTransaction}
import com.wavesenterprise.transaction.docker.CreateContractTransaction
import com.wavesenterprise.utils.DatabaseUtils._

import java.nio.charset.StandardCharsets.UTF_8

//noinspection UnstableApiUsage
object MigrationV2 {

  case class LegacyAssetInfo(isReissuable: Boolean, volume: BigInt)

  case class LegacyContractInfo(contractId: ByteStr, image: String, imageHash: String, version: Int, active: Boolean)

  case class ModernContractInfo(creator: PublicKeyAccount, contractId: ByteStr, image: String, imageHash: String, version: Int, active: Boolean)

  private[migration] object KeysInfo {

    def assetInfoKey(assetId: ByteStr)(height: Int): Key[LegacyAssetInfo] = {
      Key("asset-info", hBytes(Keys.AssetInfoPrefix, height, assetId.arr), parseAssetInfo, writeAssetInfo)
    }

    private def parseAssetInfo(bytes: Array[Byte]): LegacyAssetInfo = {
      val ndi     = newDataInput(bytes)
      val reissue = ndi.readBoolean()
      val volume  = ndi.readBigInt()
      LegacyAssetInfo(reissue, volume)
    }

    private def writeAssetInfo(ai: LegacyAssetInfo): Array[Byte] = {
      val ndo = newDataOutput()
      ndo.writeBoolean(ai.isReissuable)
      ndo.writeBigInt(ai.volume)
      ndo.toByteArray
    }

    def legacyContractInfoKey(contractId: ByteStr)(height: Int): Key[Option[LegacyContractInfo]] =
      Key.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), parseLegacyContractInfo, writeLegacyContractInfo)

    def modernContractInfoKey(contractId: ByteStr)(height: Int): Key[Option[ModernContractInfo]] =
      Key.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), parseModernContractInfo, writeModernContractInfo)

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

  private val ContractsIdSet = new InternalRocksDBSet[ByteStr](
    name = "contract-ids",
    columnFamily = ContractCF,
    prefix = Shorts.toByteArray(ContractIdsPrefix),
    itemEncoder = (_: ByteStr).arr,
    itemDecoder = ByteStr(_)
  )

  def apply(rw: RW): Unit = {
    migrateAssets(rw)
    migrateContracts(rw)
  }

  private def migrateAssets(rw: RW): Unit = {
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
      oldAssetInfo = rw.get(KeysInfo.assetInfoKey(assetId)(height))
      wasBurnt     = assetWasBurnt(rw, issueTx)
    } yield {
      val newAssetInfo = AssetInfo(
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
      rw.put(Keys.assetInfo(assetId)(height), newAssetInfo)
    }
  }

  private def findIssueTx(rw: RW, txId: ByteStr): (Int, IssueTransaction) = {
    rw.get(Keys.transactionInfo(txId))
      .collect {
        case (h: Int, tx: IssueTransaction) => h -> tx
      }
      .getOrElse(throw new RuntimeException(s"Issue transaction '$txId' is not found"))
  }

  private def assetWasBurnt(rw: RW, tx: IssueTransaction): Boolean = {
    AddressTransactions(rw, tx.sender.toAddress, Set(BurnTransaction.typeId), Int.MaxValue, None)
      .getOrElse(Seq.empty)
      .exists {
        case (_, t: BurnTransaction) if t.assetId == tx.assetId() => true
        case _                                                    => false
      }
  }

  private def migrateContracts(rw: RW): Unit = {
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

  private def findCreateContractTx(rw: RW, txId: ByteStr): (Int, CreateContractTransaction) = {
    rw.get(Keys.transactionInfo(txId))
      .collect {
        case (h: Int, tx: CreateContractTransaction) => h -> tx
      }
      .getOrElse(throw new RuntimeException(s"Create contract transaction '$txId' is not found"))
  }
}
