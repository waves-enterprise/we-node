package com.wavesenterprise.network.message

import com.google.common.base.Charsets
import com.google.common.io.ByteArrayDataOutput
import com.google.common.io.ByteStreams.newDataOutput
import com.google.common.primitives.{Bytes, Ints, Longs, Shorts}
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.block.{Block, MicroBlock}
import com.wavesenterprise.consensus.Vote
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.internals.EncryptedForSingle
import com.wavesenterprise.crypto.{DigestSize, KeyLength, PublicKey, SignatureLength}
import com.wavesenterprise.docker.ContractExecutionMessage
import com.wavesenterprise.mining.Miner.MaxTransactionsPerMicroblock
import com.wavesenterprise.network._
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.privacy.{PolicyDataHash, PrivacyDataType}
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.TransactionParsers
import com.wavesenterprise.utils.pki.CrlData
import enumeratum.values.{ByteEnum, ByteEnumEntry}
import org.apache.commons.io.FileUtils
import scorex.crypto.signatures.Signature

import java.net.URL
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import scala.collection.immutable
import scala.reflect.ClassTag
import scala.util.Try

sealed abstract class MessageSpec[Content <: AnyRef](implicit contentCt: ClassTag[Content]) extends ByteEnumEntry {
  val contentClass: Class[_]    = contentCt.runtimeClass
  def messageCode: MessageCode  = value
  final val messageName: String = """Spec\$$""".r.replaceAllIn(getClass.getSimpleName, "")

  def maxLength: Int

  def deserializeData(bytes: Array[Byte]): Try[Content]

  def serializeData(data: Content): Array[Byte]

  override def toString: String = s"MessageSpec ($messageCode: $messageName)"
}

//noinspection UnstableApiUsage
object MessageSpec extends ByteEnum[MessageSpec[_ <: AnyRef]] {

  val MaxCertChainLength: Int = {
    val maxCertChainsInStore = 128
    val maxChainLength       = 8
    val maxCertLength        = FileUtils.ONE_KB.toInt

    maxCertChainsInStore * maxChainLength * maxCertLength
  }

  object GetPeersV2Spec extends MessageSpec[GetPeersV2.type] {
    override val value: Byte = 3

    override val maxLength: Int = 0

    override def deserializeData(bytes: Array[Byte]): Try[GetPeersV2.type] =
      Try {
        require(bytes.isEmpty, "Non-empty data for GetPeers")
        GetPeersV2
      }

    override def serializeData(data: GetPeersV2.type): Array[Byte] = Array()
  }

  object PeersV2Spec extends MessageSpec[KnownPeersV2] {

    private val oneAddressSize: Int = Shorts.BYTES + "some-mediocre-hostname-string".getBytes.length + java.lang.Integer.BYTES
    override val maxLength: Int     = 1000 * oneAddressSize
    override val value              = 4

    override def deserializeData(bytes: Array[Byte]): Try[KnownPeersV2] = Try {
      val peersCountBytes = util.Arrays.copyOfRange(bytes, 0, Shorts.BYTES)
      val peersCount      = Shorts.fromByteArray(peersCountBytes)

      val (_, peerHostnames) = (0 until peersCount).foldLeft((Shorts.BYTES, List.empty[PeerHostname])) {
        case ((bytePosition, result), _) =>
          val hostnameSizeBytes = util.Arrays.copyOfRange(bytes, bytePosition, bytePosition + Shorts.BYTES)
          val hostnameSize      = Shorts.fromByteArray(hostnameSizeBytes)

          val hostnameStartIndex = bytePosition + Shorts.BYTES
          val hostnameEndIndex   = hostnameStartIndex + hostnameSize
          val hostnameBytes      = util.Arrays.copyOfRange(bytes, hostnameStartIndex, hostnameEndIndex)
          val hostname           = new String(hostnameBytes, Charsets.UTF_8)

          val portStartIndex = hostnameStartIndex + hostnameSize
          val portEndIndex   = portStartIndex + Ints.BYTES
          val portBytes      = util.Arrays.copyOfRange(bytes, portStartIndex, portEndIndex)
          val port           = Ints.fromByteArray(portBytes)

          val peerHostname = PeerHostname(hostname, port)
          (portEndIndex, peerHostname :: result)
      }

      KnownPeersV2(peerHostnames)
    }

    override def serializeData(peers: KnownPeersV2): Array[Byte] = {
      val peersSize = peers.peers.size
      require(peersSize <= Short.MaxValue, "Too many peers to serialize!")

      val peersSizeBytes = Shorts.toByteArray(peersSize.toShort)

      peers.peers.foldLeft(peersSizeBytes) {
        case (result, peer) =>
          val hostnameBytes = peer.hostname.getBytes(Charsets.UTF_8)
          require(hostnameBytes.length <= Short.MaxValue, "Hostname is too long!")

          val byteBufferSize =
            // for hostname string length
            java.lang.Short.BYTES +
              // for hostname
              hostnameBytes.length +
              // for port
              java.lang.Integer.BYTES

          val bb = java.nio.ByteBuffer.allocate(byteBufferSize)
          bb.putShort(hostnameBytes.length.toShort)
          bb.put(hostnameBytes)
          bb.put(Ints.toByteArray(peer.port))

          Bytes.concat(result, bb.array())
      }
    }
  }

  trait SignaturesSeqSpec[A <: AnyRef] extends MessageSpec[A] {

    private val DataLength = 4

    def wrap(signatures: Seq[Array[Byte]]): A

    def unwrap(v: A): Seq[Array[Byte]]

    override val maxLength: Int = DataLength + (200 * SignatureLength)

    override def deserializeData(bytes: Array[Byte]): Try[A] = Try {
      val lengthBytes = bytes.take(DataLength)
      val length      = Ints.fromByteArray(lengthBytes)

      assert(bytes.length == DataLength + (length * SignatureLength), "Data does not match length")

      wrap((0 until length).map { i =>
        val position = DataLength + (i * SignatureLength)
        bytes.slice(position, position + SignatureLength)
      })
    }

    override def serializeData(v: A): Array[Byte] = {
      val signatures  = unwrap(v)
      val length      = signatures.size
      val lengthBytes = Ints.toByteArray(length)

      // WRITE SIGNATURES
      signatures.foldLeft(lengthBytes) { case (bs, header) => Bytes.concat(bs, header) }
    }
  }

  object GetSignaturesSpec extends SignaturesSeqSpec[GetNewSignatures] {
    override def wrap(signatures: Seq[Array[Byte]]): GetNewSignatures = GetNewSignatures(signatures.map(ByteStr(_)))

    override def unwrap(v: GetNewSignatures): Seq[Array[MessageCode]] = v.knownSignatures.map(_.arr)

    override val value: Byte = 20
  }

  object SignaturesSpec extends SignaturesSeqSpec[Signatures] {
    override def wrap(signatures: Seq[Array[Byte]]): Signatures = Signatures(signatures.map(ByteStr(_)))

    override def unwrap(v: Signatures): Seq[Array[MessageCode]] = v.signatures.map(_.arr)

    override val value: Byte = 21
  }

  /**
    * Used to transfer historical blocks in the [[com.wavesenterprise.network.HistoryReplier]] if there is no SeparateBlockAndTxMessages
    * protocol feature.
    *
    * Used to transfer the key block in all cases.
    */
  object BlockSpec extends MessageSpec[Block] {
    override val value: Byte = 23

    override val maxLength: Int = 271 + TransactionSpec.maxLength * Block.MaxTransactionsPerBlockVer3

    override def serializeData(block: Block): Array[Byte] = block.bytes()

    override def deserializeData(bytes: Array[Byte]): Try[Block] = Block.parseBytes(bytes)
  }

  object ScoreSpec extends MessageSpec[BigInt] {
    override val value: Byte = 24

    override val maxLength: Int = 64 // allows representing scores as high as 6.6E153

    override def serializeData(score: BigInt): Array[Byte] = {
      val scoreBytes = score.toByteArray
      val bb         = ByteBuffer.allocate(scoreBytes.length)
      bb.put(scoreBytes)
      bb.array()
    }

    override def deserializeData(bytes: Array[Byte]): Try[BigInt] = Try {
      BigInt(1, bytes)
    }
  }

  object TransactionSpec extends MessageSpec[TransactionWithSize] {
    override val value: Byte = 25

    override val maxLength: Int = 150 * 1024

    override def deserializeData(bytes: Array[Byte]): Try[TransactionWithSize] = {
      TransactionParsers.parseBytes(bytes).map { tx =>
        TransactionWithSize(bytes.length, tx)
      }
    }

    override def serializeData(txWithSize: TransactionWithSize): Array[Byte] = txWithSize.tx.bytes()
  }

  object MicroBlockInventoryV1Spec extends MessageSpec[MicroBlockInventoryV1] {
    override val value: Byte = 26

    override def deserializeData(bytes: Array[Byte]): Try[MicroBlockInventoryV1] = {
      Try {
        val buffer = ByteBuffer.wrap(bytes)

        val publicKeyBytes = new Array[Byte](KeyLength)
        buffer.get(publicKeyBytes)

        val totalBlockSig = new Array[Byte](SignatureLength)
        buffer.get(totalBlockSig)

        val prevBlockSig = new Array[Byte](SignatureLength)
        buffer.get(prevBlockSig)

        val signatureBytes = new Array[Byte](SignatureLength)
        buffer.get(signatureBytes)

        MicroBlockInventoryV1(
          sender = PublicKeyAccount(publicKeyBytes),
          totalBlockSig = ByteStr(totalBlockSig),
          prevBlockSig = ByteStr(prevBlockSig),
          signature = ByteStr(signatureBytes)
        )
      }
    }

    override def serializeData(inv: MicroBlockInventoryV1): Array[Byte] =
      Array.concat(inv.sender.publicKey.getEncoded, inv.totalBlockSig.arr, inv.prevBlockSig.arr, inv.signature.arr)

    override val maxLength: Int = 300
  }

  object MicroBlockRequestSpec extends MessageSpec[MicroBlockRequest] {
    override val value: Byte = 27

    override def deserializeData(bytes: Array[Byte]): Try[MicroBlockRequest] =
      Try(MicroBlockRequest(ByteStr(bytes)))

    override def serializeData(req: MicroBlockRequest): Array[Byte] = req.totalBlockSig.arr

    override val maxLength: Int = 500
  }

  object MicroBlockResponseV1Spec extends MessageSpec[MicroBlockResponseV1] {
    override val value: Byte = 28

    override def deserializeData(bytes: Array[Byte]): Try[MicroBlockResponseV1] =
      MicroBlock.parseBytes(bytes).map(MicroBlockResponseV1)

    override def serializeData(resp: MicroBlockResponseV1): Array[Byte] = resp.microblock.bytes()

    override val maxLength: Int = 271 + TransactionSpec.maxLength * MaxTransactionsPerMicroblock

  }

  object MicroBlockResponseV2Spec extends MessageSpec[MicroBlockResponseV2] {

    override val value: Byte = 47

    override def deserializeData(bytes: Array[MessageCode]): Try[MicroBlockResponseV2] =
      for {
        (microBlockBytes, microBlockBytesEnd) <- Try(BinarySerializer.parseBigByteArray(bytes))
        microBlock                            <- MicroBlock.parseBytes(microBlockBytes)
        (certChainBytes, certChainBytesEnd)   <- Try(BinarySerializer.parseBigByteArray(bytes, microBlockBytesEnd))
        (certChain, _)                        <- Try(CertChainStore.fromBytesUnsafe(certChainBytes))
        (crlHashes, _)                        <- Try(BinarySerializer.parseShortList(bytes, BinarySerializer.parseShortByteStr, certChainBytesEnd))
      } yield MicroBlockResponseV2(microBlock, certChain, crlHashes.toSet)

    override def serializeData(data: MicroBlockResponseV2): Array[MessageCode] = {
      val microBlockBytes = data.microblock.bytes.value()
      val certChainBytes  = data.certChainStore.bytes
      val output          = newDataOutput(Ints.BYTES + microBlockBytes.length + certChainBytes.length)
      BinarySerializer.writeBigByteArray(microBlockBytes, output)
      BinarySerializer.writeBigByteArray(certChainBytes, output)
      BinarySerializer.writeShortIterable(data.crlHashes, BinarySerializer.writeShortByteStr, output)
      output.toByteArray
    }

    override def maxLength: Int = MicroBlockResponseV1Spec.maxLength + MaxCertChainLength
  }

  object NetworkContractExecutionMessageSpec extends MessageSpec[NetworkContractExecutionMessage] {

    override val value: Byte = 29

    override def maxLength: Int = ContractExecutionMessage.MaxLengthInBytes

    override def deserializeData(bytes: Array[Byte]): Try[NetworkContractExecutionMessage] =
      ContractExecutionMessage.parseBytes(bytes).map(NetworkContractExecutionMessage)

    override def serializeData(data: NetworkContractExecutionMessage): Array[Byte] = data.message.bytes()
  }

  object PrivateDataRequestSpec extends MessageSpec[PrivateDataRequest] {
    override val value: Byte = 30

    override def maxLength: Int = DigestSize + PolicyDataHash.DataHashLength + SignatureLength

    override def deserializeData(bytes: Array[Byte]): Try[PrivateDataRequest] = {
      Try {
        val buf = ByteBuffer.wrap(bytes)

        val policyId = new Array[Byte](DigestSize)
        buf.get(policyId)

        val dataHash = new Array[Byte](PolicyDataHash.DataHashLength)
        buf.get(dataHash)

        val policyDataHash = PolicyDataHash.deserialize(dataHash)
        PrivateDataRequest(ByteStr(policyId), policyDataHash)
      }
    }

    override def serializeData(request: PrivateDataRequest): Array[Byte] = {
      Bytes.concat(request.policyId.arr, request.dataHash.bytes.arr)
    }
  }

  object PrivateDataResponseSpec extends MessageSpec[PrivateDataResponse] {
    override val value: Byte = 31

    override def maxLength: Int =
      com.wavesenterprise.crypto.DigestSize + PolicyDataHash.DataHashLength + com.wavesenterprise.crypto.SignatureLength + 20 * 1024 * 1024 // 20 Mb

    private val HAS_DATA: Byte = 1
    private val NO_DATA: Byte  = 0

    def deserializeData(bytes: Array[Byte], dataEncrypted: Boolean): Try[PrivateDataResponse] = Try {
      val buf = ByteBuffer.wrap(bytes)

      val policyId = new Array[Byte](com.wavesenterprise.crypto.DigestSize)
      buf.get(policyId)

      val dataHash = new Array[Byte](PolicyDataHash.DataHashLength)
      buf.get(dataHash)

      val policyDataHash = PolicyDataHash.deserialize(dataHash)
      buf.get() match {
        case NO_DATA =>
          NoDataResponse(ByteStr(policyId), policyDataHash)
        case HAS_DATA if dataEncrypted =>
          val wrappedKey = new Array[Byte](com.wavesenterprise.crypto.WrappedStructureLength)
          buf.get(wrappedKey)

          val encryptedDataLength = new Array[Byte](Ints.BYTES)
          buf.get(encryptedDataLength)

          val encryptedData = new Array[Byte](Ints.fromByteArray(encryptedDataLength))
          buf.get(encryptedData)
          GotEncryptedDataResponse(ByteStr(policyId), policyDataHash, EncryptedForSingle(encryptedData, wrappedKey))
        case HAS_DATA =>
          val dataLength = buf.getInt
          val data       = new Array[Byte](dataLength)
          buf.get(data)
          GotDataResponse(ByteStr(policyId), policyDataHash, ByteStr(data))
      }
    }

    override def deserializeData(bytes: Array[Byte]): Try[PrivateDataResponse] =
      throw new UnsupportedOperationException("Unexpected call of PrivateDataResponseSpec#deserializeData(bytes: Array[Byte])")

    override def serializeData(response: PrivateDataResponse): Array[Byte] = {
      response match {
        case GotDataResponse(policyId, dataHash, data) =>
          Bytes.concat(
            policyId.arr,
            dataHash.bytes.arr,
            Array(HAS_DATA),
            Ints.toByteArray(data.arr.length),
            data.arr
          )

        case GotEncryptedDataResponse(policyId, dataHash, encryptedData) =>
          Bytes.concat(
            policyId.arr,
            dataHash.bytes.arr,
            Array(HAS_DATA),
            encryptedData.wrappedStructure,
            Ints.toByteArray(encryptedData.encryptedData.length),
            encryptedData.encryptedData
          )

        case NoDataResponse(policyId, dataHash) =>
          Bytes.concat(
            policyId.arr,
            dataHash.bytes.arr,
            Array(NO_DATA)
          )
      }
    }
  }

  object ContractValidatorResultsSpec extends MessageSpec[ContractValidatorResults] {

    override val value: Byte = 32

    override def maxLength: Int =
      com.wavesenterprise.crypto.KeyLength + 2 * com.wavesenterprise.crypto.DigestSize + 2 * com.wavesenterprise.crypto.SignatureLength

    override def deserializeData(bytes: Array[Byte]): Try[ContractValidatorResults] = Try {
      val buf = ByteBuffer.wrap(bytes)

      val senderBytes = new Array[Byte](com.wavesenterprise.crypto.KeyLength)
      buf.get(senderBytes)

      val txIdBytes = new Array[Byte](com.wavesenterprise.crypto.DigestSize)
      buf.get(txIdBytes)

      val keyBlockIdBytes = new Array[Byte](com.wavesenterprise.crypto.SignatureLength)
      buf.get(keyBlockIdBytes)

      val resultsHashBytes = new Array[Byte](com.wavesenterprise.crypto.DigestSize)
      buf.get(resultsHashBytes)

      val signatureBytes = new Array[Byte](com.wavesenterprise.crypto.SignatureLength)
      buf.get(signatureBytes)

      ContractValidatorResults(PublicKeyAccount(senderBytes),
                               ByteStr(txIdBytes),
                               ByteStr(keyBlockIdBytes),
                               ByteStr(resultsHashBytes),
                               ByteStr(signatureBytes))
    }

    override def serializeData(data: ContractValidatorResults): Array[MessageCode] = {
      val buf = ByteBuffer.allocate(maxLength)
      buf.put(data.sender.publicKey.getEncoded).put(data.txId.arr).put(data.keyBlockId.arr).put(data.resultsHash.arr).put(data.signature.arr).array()
    }
  }

  object VoteMessageSpec extends MessageSpec[VoteMessage] {
    override val value: Byte = 33

    override def maxLength: Int = KeyLength + SignatureLength * 2

    override def deserializeData(bytes: Array[Byte]): Try[VoteMessage] = Vote.parseBytes(bytes).map(VoteMessage)

    override def serializeData(voteMessage: VoteMessage): Array[Byte] = voteMessage.vote.bytes()
  }

  object GetBlocksSpec extends MessageSpec[GetBlocks] {
    override val value: Byte = 34

    override val maxLength: Int = 200 * SignatureLength

    override def serializeData(getBlocks: GetBlocks): Array[Byte] = getBlocks.signatures.flatMap(_.arr).toArray

    override def deserializeData(bytes: Array[Byte]): Try[GetBlocks] = Try {
      require(bytes.length % SignatureLength == 0, "Data length is not multiple of the signature length")
      GetBlocks(bytes.grouped(SignatureLength).map(ByteStr(_)).toList)
    }
  }

  object SnapshotNotificationSpec extends MessageSpec[SnapshotNotification] {
    override val value: Byte = 35

    override def maxLength: Int = KeyLength + java.lang.Long.BYTES

    override def deserializeData(bytes: Array[Byte]): Try[SnapshotNotification] = Try {
      val buf = ByteBuffer.wrap(bytes)

      val senderBytes = new Array[Byte](com.wavesenterprise.crypto.KeyLength)
      buf.get(senderBytes)

      val size = buf.getLong
      SnapshotNotification(PublicKeyAccount(senderBytes), size)
    }

    override def serializeData(notification: SnapshotNotification): Array[Byte] = {
      val buf = ByteBuffer.allocate(maxLength)
      buf.put(notification.sender.publicKey.getEncoded).putLong(notification.size).array()
    }
  }

  object SnapshotRequestSpec extends MessageSpec[SnapshotRequest] {
    override val value: Byte = 36

    override def maxLength: Int = KeyLength + java.lang.Long.BYTES

    override def deserializeData(bytes: Array[Byte]): Try[SnapshotRequest] = Try {
      val buf = ByteBuffer.wrap(bytes)

      val senderBytes = new Array[Byte](com.wavesenterprise.crypto.KeyLength)
      buf.get(senderBytes)

      val offset = buf.getLong
      SnapshotRequest(PublicKeyAccount(senderBytes), offset)
    }

    override def serializeData(request: SnapshotRequest): Array[Byte] = {
      val buf = ByteBuffer.allocate(maxLength)
      buf.put(request.sender.publicKey.getEncoded).putLong(request.offset).array()
    }
  }

  object GenesisSnapshotRequestSpec extends MessageSpec[GenesisSnapshotRequest] {
    override val value: Byte = 37

    override def maxLength: Int = KeyLength + 2 * SignatureLength + java.lang.Long.BYTES

    override def deserializeData(bytes: Array[Byte]): Try[GenesisSnapshotRequest] = Try {
      val buf = ByteBuffer.wrap(bytes)

      val senderBytes = new Array[Byte](KeyLength)
      buf.get(senderBytes)

      val genesisBytes = new Array[Byte](SignatureLength)
      buf.get(genesisBytes)

      val offset = buf.getLong

      val signatureBytes = new Array[Byte](SignatureLength)
      buf.get(signatureBytes)

      GenesisSnapshotRequest(PublicKeyAccount(senderBytes), ByteStr(genesisBytes), offset, ByteStr(signatureBytes))
    }

    override def serializeData(request: GenesisSnapshotRequest): Array[Byte] = {
      val buf = ByteBuffer.allocate(maxLength)
      buf.put(request.sender.publicKey.getEncoded).put(request.genesis.arr).putLong(request.offset).put(request.signature.arr).array()
    }
  }

  object GenesisSnapshotErrorSpec extends MessageSpec[GenesisSnapshotError] {
    override val value: Byte = 38

    override def maxLength: Int = java.lang.Integer.MAX_VALUE

    override def deserializeData(bytes: Array[Byte]): Try[GenesisSnapshotError] = Try {
      val buf = ByteBuffer.wrap(bytes)

      val length       = buf.getInt
      val messageBytes = new Array[Byte](length)
      buf.get(messageBytes)

      GenesisSnapshotError(new String(messageBytes, UTF_8))
    }

    override def serializeData(error: GenesisSnapshotError): Array[Byte] = {
      val messageBytes = error.message.getBytes(UTF_8)
      val buf          = ByteBuffer.allocate(java.lang.Integer.BYTES + messageBytes.length)
      buf.putInt(messageBytes.length).put(messageBytes).array()
    }
  }

  object PrivacyInventoryV1Spec extends MessageSpec[PrivacyInventoryV1] {
    override val value: Byte = 39

    override def deserializeData(bytes: Array[Byte]): Try[PrivacyInventoryV1] =
      Try {
        val buf = ByteBuffer.wrap(bytes)

        val publicKeyBytes = new Array[Byte](KeyLength)
        buf.get(publicKeyBytes)

        val policyIdBytes = new Array[Byte](DigestSize)
        buf.get(policyIdBytes)

        val dataHashBytes = new Array[Byte](PolicyDataHash.DataHashLength)
        buf.get(dataHashBytes)

        val signatureBytes = new Array[Byte](SignatureLength)
        buf.get(signatureBytes)

        PrivacyInventoryV1(
          sender = PublicKeyAccount(publicKeyBytes),
          policyId = ByteStr(policyIdBytes),
          dataHash = PolicyDataHash.deserialize(dataHashBytes),
          signature = ByteStr(signatureBytes)
        )
      }

    override def serializeData(inventory: PrivacyInventoryV1): Array[Byte] =
      Array.concat(
        inventory.sender.publicKey.getEncoded,
        inventory.policyId.arr,
        inventory.dataHash.bytes.arr,
        inventory.signature.arr
      )

    override val maxLength: Int = KeyLength + DigestSize + PolicyDataHash.DataHashLength + SignatureLength
  }

  object PrivacyInventoryRequestSpec extends MessageSpec[PrivacyInventoryRequest] {
    override val value: Byte = 40

    override def deserializeData(bytes: Array[Byte]): Try[PrivacyInventoryRequest] =
      Try {
        val buf = ByteBuffer.wrap(bytes)

        val policyIdBytes = new Array[Byte](DigestSize)
        buf.get(policyIdBytes)

        val dataHashBytes = new Array[Byte](PolicyDataHash.DataHashLength)
        buf.get(dataHashBytes)

        PrivacyInventoryRequest(
          policyId = ByteStr(policyIdBytes),
          dataHash = PolicyDataHash.deserialize(dataHashBytes)
        )
      }

    override def serializeData(inventory: PrivacyInventoryRequest): Array[Byte] =
      Array.concat(
        inventory.policyId.arr,
        inventory.dataHash.bytes.arr
      )

    override val maxLength: Int = DigestSize + PolicyDataHash.DataHashLength
  }

  object MissingBlockSpec extends MessageSpec[MissingBlock] {
    override val value: Byte = 41

    override val maxLength: Int = SignatureLength

    override def deserializeData(bytes: Array[Byte]): Try[MissingBlock] = Try {
      require(bytes.length == maxLength, "Data does not match length")
      MissingBlock(ByteStr(bytes))
    }

    override def serializeData(data: MissingBlock): Array[Byte] = data.signature.arr
  }

  object RawAttributesSpec extends MessageSpec[RawAttributes] {
    override val value: Byte = 42

    override val maxLength: Int = crypto.KeyLength + crypto.SignatureLength + Shorts.BYTES + FileUtils.ONE_KB.toInt

    override def deserializeData(bytes: Array[Byte]): Try[RawAttributes] = Try {
      val buffer = ByteBuffer.wrap(bytes)

      val senderBytes = new Array[Byte](crypto.KeyLength)
      buffer.get(senderBytes)

      val bodyLength = buffer.getShort
      val bodyBytes  = new Array[Byte](bodyLength)
      buffer.get(bodyBytes)

      val signatureBytes = new Array[Byte](crypto.SignatureLength)
      buffer.get(signatureBytes)
      val signature: Signature = Signature(signatureBytes)

      RawAttributes(ByteStr(bodyBytes), PublicKeyAccount(senderBytes), ByteStr(signature))
    }

    override def serializeData(value: RawAttributes): Array[Byte] = {
      val senderBytes    = value.sender.publicKey.getEncoded
      val bodyBytes      = value.bodyBytes.arr
      val signatureBytes = value.signature.arr

      // noinspection UnstableApiUsage
      val ndo = newDataOutput(senderBytes.length + Shorts.BYTES + bodyBytes.length + signatureBytes.length)

      ndo.write(senderBytes)

      ndo.writeShort(bodyBytes.length.ensuring(_.isValidShort))
      ndo.write(bodyBytes)

      ndo.write(signatureBytes)

      ndo.toByteArray
    }
  }

  object MicroBlockInventoryV2Spec extends MessageSpec[MicroBlockInventoryV2] {
    override val value: Byte = 43

    override def deserializeData(bytes: Array[Byte]): Try[MicroBlockInventoryV2] =
      Try {
        val buffer = ByteBuffer.wrap(bytes)

        val publicKeyBytes = new Array[Byte](KeyLength)
        buffer.get(publicKeyBytes)

        val totalBlockSig = new Array[Byte](SignatureLength)
        buffer.get(totalBlockSig)

        val prevBlockSig = new Array[Byte](SignatureLength)
        buffer.get(prevBlockSig)

        val keyBlockSig = new Array[Byte](SignatureLength)
        buffer.get(keyBlockSig)

        val signatureBytes = new Array[Byte](SignatureLength)
        buffer.get(signatureBytes)

        MicroBlockInventoryV2(
          sender = PublicKeyAccount(publicKeyBytes),
          totalBlockSig = ByteStr(totalBlockSig),
          prevBlockSig = ByteStr(prevBlockSig),
          keyBlockSig = ByteStr(keyBlockSig),
          signature = ByteStr(signatureBytes)
        )
      }

    override def serializeData(inv: MicroBlockInventoryV2): Array[Byte] =
      Array.concat(inv.sender.publicKey.getEncoded, inv.totalBlockSig.arr, inv.prevBlockSig.arr, inv.keyBlockSig.arr, inv.signature.arr)

    override val maxLength: Int = KeyLength + SignatureLength * 4
  }

  object PrivacyInventoryV2Spec extends MessageSpec[PrivacyInventoryV2] {
    override val value: Byte = 44

    override def deserializeData(bytes: Array[Byte]): Try[PrivacyInventoryV2] =
      Try {
        val buf = ByteBuffer.wrap(bytes)

        val publicKeyBytes = new Array[Byte](KeyLength)
        buf.get(publicKeyBytes)

        val policyIdBytes = new Array[Byte](DigestSize)
        buf.get(policyIdBytes)

        val dataHashBytes = new Array[Byte](PolicyDataHash.DataHashLength)
        buf.get(dataHashBytes)

        val dataType = PrivacyDataType.withValue(buf.get())

        val signatureBytes = new Array[Byte](SignatureLength)
        buf.get(signatureBytes)

        PrivacyInventoryV2(
          sender = PublicKeyAccount(publicKeyBytes),
          policyId = ByteStr(policyIdBytes),
          dataHash = PolicyDataHash.deserialize(dataHashBytes),
          dataType = dataType,
          signature = ByteStr(signatureBytes)
        )
      }

    override def serializeData(inventory: PrivacyInventoryV2): Array[Byte] =
      Array.concat(
        inventory.sender.publicKey.getEncoded,
        inventory.policyId.arr,
        inventory.dataHash.bytes.arr,
        Array(inventory.dataType.value),
        inventory.signature.arr
      )

    override val maxLength: Int = KeyLength + DigestSize + PolicyDataHash.DataHashLength + SignatureLength + 1
  }

  /**
    * Used to transfer historical blocks in the [[com.wavesenterprise.network.HistoryReplier]] if the SeparateBlockAndTxMessages
    * protocol feature is active.
    */
  object HistoryBlockSpec extends MessageSpec[HistoryBlock] {
    override val value: Byte = 45

    override val maxLength: Int = 271 + TransactionSpec.maxLength * Block.MaxTransactionsPerBlockVer3 + MaxCertChainLength

    override def serializeData(historyBlock: HistoryBlock): Array[Byte] = {
      val blockBytes     = historyBlock.block.bytes()
      val certChainBytes = historyBlock.certChain.bytes

      val output = newDataOutput(Ints.BYTES + blockBytes.length + Ints.BYTES + certChainBytes.length)
      BinarySerializer.writeBigByteArray(blockBytes, output)
      BinarySerializer.writeBigByteArray(certChainBytes, output)
      output.toByteArray
    }

    override def deserializeData(bytes: Array[Byte]): Try[HistoryBlock] =
      for {
        (blockBytes, blockBytesEnd) <- Try(BinarySerializer.parseBigByteArray(bytes))
        (certChainBytes, _)         <- Try(BinarySerializer.parseBigByteArray(bytes, blockBytesEnd))
        (certChain, _)              <- Try(CertChainStore.fromBytesUnsafe(certChainBytes))
        block                       <- Block.parseBytes(blockBytes)
      } yield {
        HistoryBlock(block, certChain)
      }
  }

  object BroadcastedTransactionSpec extends MessageSpec[BroadcastedTransaction] {
    override val value: Byte = 46

    override val maxLength: Int = 150 * 1024 + MaxCertChainLength

    override def serializeData(broadcastedTx: BroadcastedTransaction): Array[Byte] = {
      val txBytes        = broadcastedTx.tx.bytes()
      val certChainBytes = broadcastedTx.certChain.bytes

      val output = newDataOutput(Ints.BYTES + txBytes.length + Ints.BYTES + certChainBytes.length)
      BinarySerializer.writeBigByteArray(txBytes, output)
      BinarySerializer.writeBigByteArray(certChainBytes, output)
      output.toByteArray
    }

    override def deserializeData(bytes: Array[Byte]): Try[BroadcastedTransaction] =
      for {
        (txBytes, txBytesEnd) <- Try(BinarySerializer.parseBigByteArray(bytes))
        (certChainBytes, _)   <- Try(BinarySerializer.parseBigByteArray(bytes, txBytesEnd))
        (certChain, _)        <- Try(CertChainStore.fromBytesUnsafe(certChainBytes))
        tx                    <- TransactionParsers.parseBytes(txBytes)
        txWithSize = TransactionWithSize(txBytes.length, tx)
      } yield {
        BroadcastedTransaction(txWithSize, certChain)
      }
  }

  object MissingCrlsRequestSpec extends MessageSpec[MissingCrlDataRequest] {
    override val value: Byte    = 48
    override val maxLength: Int = KeyLength + 256 * FileUtils.ONE_KB.toInt

    private[message] def bigIntWriter(value: BigInt, out: ByteArrayDataOutput): Unit = {
      BinarySerializer.writeShortByteArray(value.toByteArray, out)
    }

    private[message] def bigIntReader(bytes: Array[Byte], offset: Int): (BigInt, Int) = {
      val (bigIntBytes, bigIntEnd) = BinarySerializer.parseShortByteArray(bytes, offset)
      BigInt(bigIntBytes) -> bigIntEnd
    }

    override def serializeData(data: MissingCrlDataRequest): Array[Byte] = {
      val output      = newDataOutput()
      val issuerBytes = data.issuer.publicKey.getEncoded
      output.write(issuerBytes)
      BinarySerializer.writeShortString(data.cdp.toString, output)
      BinarySerializer.writeShortIterable(data.missingIds, bigIntWriter, output)
      output.toByteArray
    }

    override def deserializeData(bytes: Array[Byte]): Try[MissingCrlDataRequest] = {
      for {
        (issuerBytes, issuerEnd) <- Try(bytes.take(KeyLength) -> KeyLength)
        (cdpString, cdpEnd)      <- Try(BinarySerializer.parseShortString(bytes, issuerEnd))
        (missingIds, _)          <- Try(BinarySerializer.parseShortList(bytes, bigIntReader, cdpEnd))
        issuer                   <- Try(PublicKeyAccount(PublicKey(issuerBytes)))
        cdp                      <- Try(new URL(cdpString))
      } yield MissingCrlDataRequest(issuer, cdp, missingIds.toSet)
    }
  }

  object CrlDataByHashesRequestSpec extends MessageSpec[CrlDataByHashesRequest] {
    override val value: Byte    = 49
    override val maxLength: Int = 20 * 1024 // sha1 hash length is 20 bytes

    override def serializeData(data: CrlDataByHashesRequest): Array[Byte] = {
      val output = newDataOutput()
      BinarySerializer.writeShortIterable(data.crlHashes, BinarySerializer.writeShortByteStr, output)
      output.toByteArray
    }

    override def deserializeData(bytes: Array[Byte]): Try[CrlDataByHashesRequest] = {
      Try(BinarySerializer.parseShortList(bytes, BinarySerializer.parseShortByteStr, 0)).map {
        case (crlHashes, _) => CrlDataByHashesRequest(crlHashes.toSet)
      }
    }
  }

  object CrlDataByTimestampRangeRequestSpec extends MessageSpec[CrlDataByTimestampRangeRequest] {
    override val value: Byte = 50

    override val maxLength: Int = 2 * Longs.BYTES

    override def deserializeData(bytes: Array[Byte]): Try[CrlDataByTimestampRangeRequest] = {
      for {
        fromTimestamp <- Try(Longs.fromByteArray(bytes.take(Longs.BYTES)))
        toTimestamp   <- Try(Longs.fromByteArray(bytes.slice(Longs.BYTES, Longs.BYTES * 2)))
        request       <- Try(CrlDataByTimestampRangeRequest(fromTimestamp, toTimestamp))
      } yield request
    }

    override def serializeData(data: CrlDataByTimestampRangeRequest): Array[Byte] = {
      Longs.toByteArray(data.from) ++ Longs.toByteArray(data.to)
    }
  }

  object CrlDataResponseSpec extends MessageSpec[CrlDataResponse] {
    override val value: Byte    = 51
    override val maxLength: Int = 1 + 5 * FileUtils.ONE_MB.toInt

    override def serializeData(data: CrlDataResponse): Array[Byte] = {
      val output = newDataOutput()
      output.writeByte(data.id.value)
      BinarySerializer.writeShortIterable(data.foundCrlData, CrlData.writeCrlData, output)
      output.toByteArray
    }

    override def deserializeData(bytes: Array[Byte]): Try[CrlDataResponse] =
      for {
        idByte         <- Try(bytes.head)
        id             <- CrlMessageContextId.fromByte(idByte)
        (crlHashes, _) <- Try(BinarySerializer.parseShortList(bytes, CrlData.parseCrlData, 1))
      } yield CrlDataResponse(id, crlHashes.toSet)
  }

// Virtual, only for logs
  object HandshakeSpec {
    val messageCode: Byte = 101
  }

  object PeerIdentitySpec {
    val messageCode: Byte = 102
  }

  type Spec = MessageSpec[_ <: AnyRef]

  override val values: immutable.IndexedSeq[MessageSpec[_ <: AnyRef]] = findValues

  val specsByCodes: Map[Byte, Spec]       = values.map(s => s.messageCode -> s).toMap
  val specsByClasses: Map[Class[_], Spec] = values.map(s => s.contentClass -> s).toMap
}
