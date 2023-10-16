package com.wavesenterprise

import com.google.common.io.ByteArrayDataInput
import com.google.common.io.ByteStreams.{newDataInput, newDataOutput}
import com.google.common.primitives.{Ints, Shorts}
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.PermissionOp
import com.wavesenterprise.block._
import com.wavesenterprise.consensus.MinerBanlistEntry
import com.wavesenterprise.consensus.MinerBanlistEntry.{CancelledWarning, Warning}
import com.wavesenterprise.database.rocksdb.{ConfidentialDBColumnFamily, MainDBColumnFamily}
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId}
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.{Transaction, TransactionParsers}
import com.wavesenterprise.utils.DatabaseUtils._
import com.wavesenterprise.utils.EitherUtils.EitherExt

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.security.cert.{Certificate, CertificateFactory}

//noinspection UnstableApiUsage
package object database {
  type MainDBKey[V]         = BaseKey[V, MainDBColumnFamily]
  type ConfidentialDBKey[V] = BaseKey[V, ConfidentialDBColumnFamily]

  def writeAddressesSet(addresses: Set[Address]): Array[Byte] = {
    val addressesCount = addresses.size
    addresses.foldLeft(ByteBuffer.allocate(Address.AddressLength * addressesCount))(_ put _.bytes.arr).array()
  }

  def readAddressesSet(data: Array[Byte]): Set[Address] = {
    if (data == null) {
      Set.empty
    } else {
      val addressesCount = data.length / Address.AddressLength
      Range(0, addressesCount).map { i =>
        val startIdx     = i * Address.AddressLength
        val addressBytes = data.slice(startIdx, startIdx + Address.AddressLength)
        Address.fromBytes(addressBytes).explicitGet()
      }.toSet
    }
  }

  def writeByteStrsSet(byteStrs: Set[PolicyDataHash]): Array[Byte] = {
    val strsCount = byteStrs.size
    byteStrs.foldLeft(ByteBuffer.allocate(PolicyDataHash.DataHashLength * strsCount))(_ put _.bytes.arr).array()
  }

  def readPolicyDataSet(data: Array[Byte]): Set[PolicyDataHash] = {
    if (data == null) {
      Set.empty
    } else {
      val dataHashesCount = data.length / PolicyDataHash.DataHashLength
      Range(0, dataHashesCount).map { i =>
        val startIdx      = i * PolicyDataHash.DataHashLength
        val dataHashBytes = data.slice(startIdx, startIdx + PolicyDataHash.DataHashLength)
        PolicyDataHash.deserialize(dataHashBytes)
      }.toSet
    }
  }

  def writePrivacyDataId(pdi: PolicyDataId): Array[Byte] = {
    val policyIdBytes       = pdi.policyId.arr
    val policyDataHashBytes = pdi.dataHash.bytes.arr
    ByteBuffer
      .allocate(policyIdBytes.length + policyDataHashBytes.length + 2 * Ints.BYTES)
      .putInt(policyIdBytes.length)
      .put(policyIdBytes)
      .putInt(policyDataHashBytes.length)
      .put(policyDataHashBytes)
      .array()
  }

  def readPrivacyDataId(data: Array[Byte]): PolicyDataId = {
    val buff           = ByteBuffer.wrap(data)
    val policyIdLength = buff.getInt()
    val policyIdArr    = new Array[Byte](policyIdLength)
    buff.get(policyIdArr)
    val dataHashLength = buff.getInt()
    val dataHashArr    = new Array[Byte](dataHashLength)
    buff.get(dataHashArr)
    PolicyDataId(ByteStr(policyIdArr), PolicyDataHash.deserialize(dataHashArr))
  }

  def readSet[T](deserialize: Array[Byte] => T)(data: Array[Byte]): Set[T] = {
    if (data == null || data.isEmpty) {
      Set.empty
    } else {
      val buff = ByteBuffer.wrap(data)
      val set  = Set.newBuilder[T]
      while (buff.hasRemaining) {
        val size = buff.getInt
        val arr  = new Array[Byte](size)
        buff.get(arr)
        set += deserialize(arr)
      }
      set.result()
    }
  }

  def readSet[T](deserialize: Array[Byte] => T, itemSize: Int)(data: Array[Byte]): Set[T] = {
    if (data == null || data.isEmpty) {
      Set.empty
    } else {
      val input = newDataInput(data)
      val size  = input.readInt()
      (1 to size).view.map { _ =>
        deserialize(input.readBytes(itemSize))
      }.toSet
    }
  }

  def writeSet[T](serialize: T => Array[Byte])(set: Set[T]): Array[Byte] = {
    var totalSize = 0
    val bytes = set.map { e =>
      val arr = serialize(e)
      totalSize += (Ints.BYTES + arr.length)
      (arr.length, arr)
    }
    val buff = ByteBuffer.allocate(totalSize)
    for ((size, arr) <- bytes) {
      buff
        .putInt(size)
        .put(arr)
    }
    buff.array()
  }

  def writeSet[T](serialize: T => Array[Byte], itemSize: Int)(set: Set[T]): Array[Byte] = {
    val ndo = newDataOutput(Ints.BYTES + set.size * itemSize)
    ndo.writeInt(set.size)
    set.foreach { item =>
      val bytes = serialize(item)
      require(bytes.length == itemSize, s"The size of the serialized item must be equal to '$itemSize', actual '${bytes.length}'")
      ndo.write(bytes)
    }
    ndo.toByteArray
  }

  def writeIntSeq(values: Seq[Int]): Array[Byte] = {
    values.foldLeft(ByteBuffer.allocate(Ints.BYTES * values.length))(_ putInt _).array()
  }

  def readIntSeq(data: Array[Byte]): Seq[Int] = Option(data).fold(Seq.empty[Int]) { d =>
    val in = ByteBuffer.wrap(data)
    Seq.fill(d.length / Ints.BYTES)(in.getInt)
  }

  def readTxIds(data: Array[Byte]): Seq[ByteStr] = Option(data).fold(Seq.empty[ByteStr]) { d =>
    val b   = ByteBuffer.wrap(d)
    val ids = Seq.newBuilder[ByteStr]

    while (b.remaining() > 0) {
      val buffer = b.get() match {
        case crypto.DigestSize      => new Array[Byte](crypto.DigestSize)
        case crypto.SignatureLength => new Array[Byte](crypto.SignatureLength)
      }
      b.get(buffer)
      ids += ByteStr(buffer)
    }

    ids.result()
  }

  def writeTxIds(ids: Seq[ByteStr]): Array[Byte] =
    ids
      .foldLeft(ByteBuffer.allocate(ids.map(_.arr.length + 1).sum)) {
        case (b, id) =>
          b.put(id.arr.length match {
            case crypto.DigestSize      => crypto.DigestSize.toByte
            case crypto.SignatureLength => crypto.SignatureLength.toByte
          })
            .put(id.arr)
      }
      .array()

  def readStrings(data: Array[Byte]): Vector[String] = Option(data).fold(Vector.empty[String]) { _ =>
    var i = 0
    val s = Vector.newBuilder[String]

    while (i < data.length) {
      val len = Shorts.fromByteArray(data.drop(i))
      s += new String(data, i + 2, len, UTF_8)
      i += (2 + len)
    }
    s.result()
  }

  def writeStrings(strings: Seq[String]): Array[Byte] =
    strings
      .foldLeft(ByteBuffer.allocate(strings.map(_.getBytes(UTF_8).length + 2).sum)) {
        case (b, s) =>
          val bytes = s.getBytes(UTF_8)
          b.putShort(bytes.length.toShort).put(bytes)
      }
      .array()

  def writeBigIntSeq(values: Seq[BigInt]): Array[Byte] = {
    require(values.length <= Short.MaxValue, s"BigInt sequence is too long")
    val ndo = newDataOutput()
    ndo.writeShort(values.size)
    for (v <- values) {
      ndo.writeBigInt(v)
    }
    ndo.toByteArray
  }

  def readBigIntSeq(data: Array[Byte]): Seq[BigInt] = Option(data).fold(Seq.empty[BigInt]) { d =>
    val ndi    = newDataInput(d)
    val length = ndi.readShort()
    for (_ <- 0 until length) yield ndi.readBigInt()
  }

  def writeLeaseBalance(lb: LeaseBalance): Array[Byte] = {
    val ndo = newDataOutput()
    ndo.writeLong(lb.in)
    ndo.writeLong(lb.out)
    ndo.toByteArray
  }

  def readLeaseBalance(data: Array[Byte]): LeaseBalance = Option(data).fold(LeaseBalance.empty) { d =>
    val ndi = newDataInput(d)
    LeaseBalance(ndi.readLong(), ndi.readLong())
  }

  def readVolumeAndFee(data: Array[Byte]): VolumeAndFee = Option(data).fold(VolumeAndFee.empty) { d =>
    val ndi = newDataInput(d)
    VolumeAndFee(ndi.readLong(), ndi.readLong())
  }

  def writeVolumeAndFee(vf: VolumeAndFee): Array[Byte] = {
    val ndo = newDataOutput()
    ndo.writeLong(vf.volume)
    ndo.writeLong(vf.fee)
    ndo.toByteArray
  }

  def readTransactionInfo(data: Array[Byte]): (Int, Transaction) =
    (Ints.fromByteArray(data), TransactionParsers.parseBytes(data.drop(4)).get)

  def writeTransactionInfo(txInfo: (Int, Transaction)): Array[Byte] = {
    val (h, tx) = txInfo
    val txBytes = tx.bytes()
    ByteBuffer.allocate(4 + txBytes.length).putInt(h).put(txBytes).array()
  }

  def readTransactionIds(data: Array[Byte]): Seq[(Int, ByteStr)] = Option(data).fold(Seq.empty[(Int, ByteStr)]) { d =>
    val b   = ByteBuffer.wrap(d)
    val ids = Seq.newBuilder[(Int, ByteStr)]
    while (b.hasRemaining) {
      ids += b.get.toInt -> {
        val buf = new Array[Byte](b.get)
        b.get(buf)
        ByteStr(buf)
      }
    }
    ids.result()
  }

  def writeTransactionIds(ids: Seq[(Int, ByteStr)]): Array[Byte] = {
    val size   = ids.foldLeft(0) { case (prev, (_, id)) => prev + 2 + id.arr.length }
    val buffer = ByteBuffer.allocate(size)
    for ((typeId, id) <- ids) {
      buffer.put(typeId.toByte).put(id.arr.length.toByte).put(id.arr)
    }
    buffer.array()
  }

  def readFeatureMap(data: Array[Byte]): Map[Short, Int] = Option(data).fold(Map.empty[Short, Int]) { _ =>
    val b        = ByteBuffer.wrap(data)
    val features = Map.newBuilder[Short, Int]
    while (b.hasRemaining) {
      features += b.getShort -> b.getInt
    }

    features.result()
  }

  def writeFeatureMap(features: Map[Short, Int]): Array[Byte] = {
    val b = ByteBuffer.allocate(features.size * 6)
    for ((featureId, height) <- features)
      b.putShort(featureId).putInt(height)

    b.array()
  }

  def readSponsorship(data: Array[Byte]): SponsorshipValue = {
    val ndi = newDataInput(data)
    SponsorshipValue(ndi.readBoolean())
  }

  def writeSponsorship(ai: SponsorshipValue): Array[Byte] = {
    val ndo = newDataOutput()
    ndo.writeBoolean(ai.isEnabled)
    ndo.toByteArray
  }

  def readAssetInfo(data: Array[Byte]): AssetInfo = {
    val ndi = newDataInput(data)

    def readAssetHolder(input: ByteArrayDataInput): AssetHolder = {
      input.readByte() match {
        case Account.binaryHeader =>
          val addressBytes = new Array[Byte](Address.AddressLength)
          input.readFully(addressBytes)
          val address = Address.fromBytes(addressBytes).fold(err => throw new RuntimeException(err.message), identity)
          Account(address)
        case Contract.binaryHeader =>
          val contractIdBytes = new Array[Byte](com.wavesenterprise.crypto.DigestSize)
          input.readFully(contractIdBytes)
          Contract(ContractId(ByteStr(contractIdBytes)))
      }
    }

    val issuer      = readAssetHolder(ndi)
    val height      = ndi.readInt()
    val timestamp   = ndi.readLong()
    val name        = ndi.readString()
    val description = ndi.readString()
    val decimals    = ndi.readByte()
    val reissuable  = ndi.readBoolean()
    val volume      = ndi.readBigInt()
    AssetInfo(issuer, height, timestamp, name, description, decimals, reissuable, volume)
  }

  def writeAssetInfo(assetInfo: AssetInfo): Array[Byte] = {
    val ndo = newDataOutput()

    ndo.write(assetInfo.issuer.toBytes)
    ndo.writeInt(assetInfo.height)
    ndo.writeLong(assetInfo.timestamp)
    ndo.writeString(assetInfo.name)
    ndo.writeString(assetInfo.description)
    ndo.writeByte(assetInfo.decimals)
    ndo.writeBoolean(assetInfo.reissuable)
    ndo.writeBigInt(assetInfo.volume)
    ndo.toByteArray
  }

  def readPermissionSeq(data: Array[Byte]): Seq[PermissionOp] = {
    val in   = newDataInput(data)
    val size = in.readInt()
    (1 to size).flatMap { _ =>
      PermissionOp.fromBytes(in.readBytes(PermissionOp.serializedSize)).toSeq
    }
  }

  def writePermissions(perms: Seq[PermissionOp]): Array[Byte] = {
    val size = perms.length
    val out  = newDataOutput(Ints.BYTES + size * PermissionOp.serializedSize)
    out.writeInt(size)
    perms.foreach { permOp =>
      out.write(permOp.bytes)
    }
    out.toByteArray
  }

  def readMinerBanlistEntries(data: Array[Byte]): Seq[MinerBanlistEntry] = {
    val in   = newDataInput(data)
    val size = in.readInt()
    (1 to size).toArray.flatMap { _ =>
      // ignore the length of entry
      val _ = in.readInt()
      Seq(MinerBanlistEntry.fromDataInput(in).explicitGet())
    }
  }

  def readMinerCancelledWarnings(data: Array[Byte]): Seq[CancelledWarning] = {
    val in   = newDataInput(data)
    val size = in.readInt()

    (1 to size).toArray.map { _ =>
      val cancelTs  = in.readLong()
      val warningTs = in.readLong()
      CancelledWarning(cancelTs, Warning(warningTs))
    }
  }

  def writeMinerBanlistEntries(banHistory: Seq[MinerBanlistEntry]): Array[Byte] = {
    val size: Int = banHistory.length
    val out       = newDataOutput()
    out.writeInt(size)
    banHistory.foreach { entry =>
      val entryBytes = entry.bytes
      out.writeInt(entryBytes.length)
      out.write(entryBytes)
    }
    out.toByteArray
  }

  def writeMinerCancelledWarnings(cancelledWarnings: Seq[CancelledWarning]): Array[Byte] = {
    val size = cancelledWarnings.size
    val out  = newDataOutput()
    out.writeInt(size)
    cancelledWarnings.foreach { cancelledWarning =>
      out.writeLong(cancelledWarning.cancellationTimestamp)
      out.writeLong(cancelledWarning.warning.timestamp)
    }
    out.toByteArray
  }

  def readContractInfo(data: Array[Byte]): ContractInfo = {
    ContractInfo.fromBytes(data)
  }

  def writeContractInfo(ci: ContractInfo): Array[Byte] = {
    ContractInfo.toBytes(ci)
  }

  def writeBlockHeaderAndSize(data: (BlockHeader, Int)): Array[Byte] = {
    val (bh, size) = data
    val ndo        = newDataOutput()
    ndo.writeInt(size)
    ndo.writeHeader(bh)
    ndo.toByteArray
  }

  def readBlockHeaderAndSize(bs: Array[Byte]): (BlockHeader, Int) = {
    val ndi         = newDataInput(bs)
    val size        = ndi.readInt()
    val blockHeader = ndi.readHeader
    (blockHeader, size)
  }

  def readCert(data: Array[Byte]): Certificate =
    CertificateFactory
      .getInstance("X.509")
      .generateCertificate(new ByteArrayInputStream(data))
}
