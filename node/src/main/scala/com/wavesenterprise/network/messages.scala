package com.wavesenterprise.network

import cats.Show
import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.block.{Block, MicroBlock}
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.consensus.Vote
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.crypto.internals.{EncryptedForSingle, SaltBytes}
import com.wavesenterprise.crypto.{KeyLength, SignatureLength}
import com.wavesenterprise.docker.ContractExecutionMessage
import com.wavesenterprise.network.ContractValidatorResultsV2.toByteArray
import com.wavesenterprise.network.contracts.{ConfidentialDataId, ConfidentialDataType}
import com.wavesenterprise.network.message.MessageSpec.{BlockSpec, MicroBlockResponseV1Spec, MicroBlockResponseV2Spec, TransactionSpec}
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId, PrivacyDataType}
import com.wavesenterprise.settings.NodeMode
import com.wavesenterprise.state.contracts.confidential.ConfidentialDataUnit
import com.wavesenterprise.state.{ByteStr, ContractId, DataEntry}
import com.wavesenterprise.transaction.docker.{ContractTransactionValidation, ReadDescriptor, ReadingsHash}
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.{Signed, Transaction}
import com.wavesenterprise.utils.pki.CrlData
import enumeratum.values.{ByteEnum, ByteEnumEntry}
import monix.eval.Coeval

import java.net.URL
import java.nio.ByteBuffer
import scala.collection.immutable
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Success, Try}

sealed trait Message

case object GetPeersV2 extends Message

case class PeerHostname(hostname: String, port: Int)
case class KnownPeersV2(peers: Seq[PeerHostname]) extends Message

/**
  * Request new block signatures generated after known signatures.
  * @param knownSignatures existing signatures from newer to old
  */
case class GetNewSignatures(knownSignatures: Seq[ByteStr]) extends Message {
  override def toString: String = s"GetSignatures(${formatSignatures(knownSignatures)})"
}

case class Signatures(signatures: Seq[ByteStr]) extends Message {
  override def toString: String = s"Signatures(${formatSignatures(signatures)})"
}

case class GetBlocks(signatures: Seq[ByteStr]) extends Message {
  override def toString: String = s"GetBlocks(${formatSignatures(signatures)})"
}

case class LocalScoreChanged(newLocalScore: BigInt) extends Message

case class RawBytes(code: Byte, data: Array[Byte]) extends Message

object RawBytes {
  def from(tx: Transaction): RawBytes = RawBytes(TransactionSpec.messageCode, tx.bytes())
  def from(block: Block): RawBytes    = RawBytes(BlockSpec.messageCode, BlockSpec.serializeData(block))

  def from(mb: MicroBlock): RawBytes =
    RawBytes(MicroBlockResponseV1Spec.messageCode, MicroBlockResponseV1Spec.serializeData(MicroBlockResponseV1(mb)))
  def from(mb: MicroBlock, certChainStore: CertChainStore, crlHashes: Set[ByteStr]): RawBytes =
    RawBytes(MicroBlockResponseV2Spec.messageCode, MicroBlockResponseV2Spec.serializeData(MicroBlockResponseV2(mb, certChainStore, crlHashes)))
}

trait BlockWrapper {
  def block: Block
}

case class HistoryBlock(block: Block, certChain: CertChainStore) extends Message with BlockWrapper

case class BroadcastedTransaction(transaction: TransactionWithSize, certChain: CertChainStore) extends Message with TxWithSize {
  override val size: Int       = transaction.size
  override val tx: Transaction = transaction.tx
}

case class BlockForged(block: Block) extends Message

case class MissingBlock(signature: ByteStr) extends Message

case class MicroBlockRequest(totalBlockSig: ByteStr) extends Message

sealed trait MicroBlockResponse extends Message {
  def microblock: MicroBlock
  def certChainStore: CertChainStore
  def crlHashes: Set[ByteStr]
}

case class MicroBlockResponseV1(microblock: MicroBlock) extends MicroBlockResponse {
  override val certChainStore: CertChainStore = CertChainStore.empty
  override val crlHashes: Set[ByteStr]        = Set.empty
}
case class MicroBlockResponseV2(microblock: MicroBlock, certChainStore: CertChainStore, crlHashes: Set[ByteStr]) extends MicroBlockResponse

sealed trait MicroBlockInventory extends Message with Signed {
  def sender: PublicKeyAccount
  def totalBlockSig: ByteStr
  def prevBlockSig: ByteStr
  def signature: ByteStr
}

case class MicroBlockInventoryV1(sender: PublicKeyAccount, totalBlockSig: ByteStr, prevBlockSig: ByteStr, signature: ByteStr)
    extends MicroBlockInventory {

  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce {
    crypto.verify(signature.arr, Array.concat(sender.toAddress.bytes.arr, totalBlockSig.arr, prevBlockSig.arr), sender.publicKey)
  }
}

object MicroBlockInventoryV1 {

  def apply(sender: PrivateKeyAccount, totalBlockSig: ByteStr, prevBlockSig: ByteStr): MicroBlockInventoryV1 = {
    val signature = crypto.sign(sender, Array.concat(sender.toAddress.bytes.arr, totalBlockSig.arr, prevBlockSig.arr))
    new MicroBlockInventoryV1(sender, totalBlockSig, prevBlockSig, ByteStr(signature))
  }
}

case class MicroBlockInventoryV2(sender: PublicKeyAccount, totalBlockSig: ByteStr, prevBlockSig: ByteStr, keyBlockSig: ByteStr, signature: ByteStr)
    extends MicroBlockInventory {

  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce {
    crypto.verify(signature.arr, Array.concat(sender.toAddress.bytes.arr, totalBlockSig.arr, keyBlockSig.arr, prevBlockSig.arr), sender.publicKey)
  }
}

object MicroBlockInventoryV2 {

  def apply(keyBlockSig: ByteStr, sender: PrivateKeyAccount, totalBlockSig: ByteStr, prevBlockSig: ByteStr): MicroBlockInventoryV2 = {
    val signature = crypto.sign(sender, Array.concat(sender.toAddress.bytes.arr, totalBlockSig.arr, keyBlockSig.arr, prevBlockSig.arr))
    new MicroBlockInventoryV2(sender, totalBlockSig, prevBlockSig, keyBlockSig, ByteStr(signature))
  }
}

case class NetworkContractExecutionMessage(message: ContractExecutionMessage) extends Message

sealed trait PrivacyInventory extends Message with Signed {
  def sender: PublicKeyAccount
  def policyId: ByteStr
  def dataHash: PolicyDataHash
  def dataType: PrivacyDataType
  def signature: ByteStr
  def dataId: PolicyDataId = PolicyDataId(policyId, dataHash)
  def id: Coeval[ByteStr]
}

case class PrivacyInventoryV1(sender: PublicKeyAccount, policyId: ByteStr, dataHash: PolicyDataHash, signature: ByteStr) extends PrivacyInventory {
  override def dataType: PrivacyDataType = PrivacyDataType.Default

  def bodyBytes: Array[Byte] = Array.concat(sender.toAddress.bytes.arr, policyId.arr, dataHash.bytes.arr)

  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce {
    crypto.verify(signature.arr, bodyBytes, sender.publicKey)
  }

  override def id: Coeval[ByteStr] = Coeval.evalOnce {
    ByteStr(crypto.fastHash(bodyBytes))
  }
}

object PrivacyInventoryV1 {

  def apply(sender: PrivateKeyAccount, policyId: ByteStr, dataHash: PolicyDataHash): PrivacyInventoryV1 = {
    val signature = crypto.sign(sender, Array.concat(sender.toAddress.bytes.arr, policyId.arr, dataHash.bytes.arr))
    new PrivacyInventoryV1(sender, policyId, dataHash, ByteStr(signature))
  }
}

case class PrivacyInventoryV2(sender: PublicKeyAccount, policyId: ByteStr, dataHash: PolicyDataHash, dataType: PrivacyDataType, signature: ByteStr)
    extends PrivacyInventory {

  def bodyBytes: Array[Byte] = Array.concat(sender.toAddress.bytes.arr, policyId.arr, dataHash.bytes.arr, Array(dataType.value))

  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce {
    crypto.verify(signature.arr, bodyBytes, sender.publicKey)
  }

  override def id: Coeval[ByteStr] = Coeval.evalOnce {
    ByteStr(crypto.fastHash(bodyBytes))
  }
}

object PrivacyInventoryV2 {

  def apply(sender: PrivateKeyAccount, policyId: ByteStr, dataHash: PolicyDataHash, dataType: PrivacyDataType): PrivacyInventoryV2 = {
    val signature = crypto.sign(sender, Array.concat(sender.toAddress.bytes.arr, policyId.arr, dataHash.bytes.arr, Array(dataType.value)))
    new PrivacyInventoryV2(sender, policyId, dataHash, dataType, ByteStr(signature))
  }
}

case class PrivacyInventoryRequest(policyId: ByteStr, dataHash: PolicyDataHash) extends Message

case class PrivateDataRequest(policyId: ByteStr, dataHash: PolicyDataHash) extends Message

sealed trait PrivateDataResponse extends Message with Product with Serializable {
  def policyId: ByteStr
  def dataHash: PolicyDataHash
}
//todo EncryptedDataWithWrappedKey maybe change to ByteStr based
sealed trait NonEmptyResponse                                                                                       extends PrivateDataResponse
case class GotEncryptedDataResponse(policyId: ByteStr, dataHash: PolicyDataHash, encryptedData: EncryptedForSingle) extends NonEmptyResponse
case class GotDataResponse(policyId: ByteStr, dataHash: PolicyDataHash, data: ByteStr)                              extends NonEmptyResponse
case class NoDataResponse(policyId: ByteStr, dataHash: PolicyDataHash)                                              extends PrivateDataResponse

sealed trait ContractValidatorResults extends Message with Product with Serializable with Signed {
  def sender: PublicKeyAccount
  def txId: ByteStr
  def keyBlockId: ByteStr
  def resultsHash: ByteStr
  def signature: ByteStr
}

case class ContractValidatorResultsV1(
    sender: PublicKeyAccount,
    txId: ByteStr,
    keyBlockId: ByteStr,
    resultsHash: ByteStr,
    signature: ByteStr
) extends ContractValidatorResults {

  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce(crypto.verify(signature.arr, resultsHash.arr, sender.publicKey))

  // Do not compare signature to remove duplicates from one validator
  override def equals(other: Any): Boolean = {
    other match {
      case that: ContractValidatorResultsV1 =>
        eq(that) || (that canEqual this) &&
          sender == that.sender &&
          txId == that.txId &&
          keyBlockId == that.keyBlockId &&
          resultsHash == that.resultsHash
      case _ => false
    }
  }

  override def hashCode(): Int = MurmurHash3.orderedHash(Seq(sender, txId, keyBlockId, resultsHash))
}

//noinspection UnstableApiUsage
object ContractValidatorResultsV1 {

  def apply(sender: PrivateKeyAccount, txId: ByteStr, keyBlockId: ByteStr, resultsHash: ByteStr): ContractValidatorResultsV1 = {
    val signature = crypto.sign(sender, resultsHash.arr)
    new ContractValidatorResultsV1(sender, txId, keyBlockId, resultsHash, ByteStr(signature))
  }

  def apply(sender: PrivateKeyAccount,
            txId: ByteStr,
            keyBlockId: ByteStr,
            results: Seq[DataEntry[_]],
            assetOps: Seq[ContractAssetOperation]): ContractValidatorResultsV1 = {
    val resultsHash = ContractTransactionValidation.resultsHash(results, assetOps)
    apply(sender, txId, keyBlockId, resultsHash)
  }
}

case class ContractValidatorResultsV2(
    sender: PublicKeyAccount,
    txId: ByteStr,
    keyBlockId: ByteStr,
    readings: Seq[ReadDescriptor],
    readingsHash: Option[ReadingsHash],
    outputCommitment: Commitment,
    resultsHash: ByteStr,
    signature: ByteStr
) extends ContractValidatorResults {

  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce(crypto.verify(
    signature.arr,
    readings.flatMap(toByteArray).toArray ++ readingsHash.map(_.hash.arr).getOrElse(
      Array.emptyByteArray) ++ outputCommitment.hash.arr ++ resultsHash.arr,
    sender.publicKey
  ))

  // Do not compare signature to remove duplicates from one validator
  override def equals(other: Any): Boolean = {
    other match {
      case that: ContractValidatorResultsV2 =>
        eq(that) || (that canEqual this) &&
          sender == that.sender &&
          txId == that.txId &&
          keyBlockId == that.keyBlockId &&
          readings == that.readings &&
          readingsHash == that.readingsHash &&
          outputCommitment == that.outputCommitment &&
          resultsHash == that.resultsHash
      case _ => false
    }
  }

  override def hashCode(): Int = MurmurHash3.orderedHash(Seq(sender, txId, keyBlockId, readings, readingsHash, outputCommitment, resultsHash))

  override def toString: String = {
    s"ContractValidatorResultsV2[sender=${sender.toAddress}, txId=$txId, resultsHash =${resultsHash}]"
  }
}

//noinspection UnstableApiUsage
object ContractValidatorResultsV2 {

  def apply(sender: PrivateKeyAccount,
            txId: ByteStr,
            keyBlockId: ByteStr,
            readings: Seq[ReadDescriptor],
            readingsHash: Option[ReadingsHash],
            outputCommitment: Commitment,
            resultsHash: ByteStr): ContractValidatorResultsV2 = {
    val signature =
      crypto.sign(
        sender,
        readings.flatMap(toByteArray).toArray ++ readingsHash.map(_.hash.arr).getOrElse(
          Array.emptyByteArray) ++ outputCommitment.hash.arr ++ resultsHash.arr
      )
    new ContractValidatorResultsV2(sender, txId, keyBlockId, readings, readingsHash, outputCommitment, resultsHash, ByteStr(signature))
  }

  def apply(sender: PrivateKeyAccount,
            txId: ByteStr,
            keyBlockId: ByteStr,
            readings: Seq[ReadDescriptor],
            readingsHash: Option[ReadingsHash],
            outputCommitment: Commitment,
            results: Seq[DataEntry[_]],
            assetOps: Seq[ContractAssetOperation]): ContractValidatorResultsV2 = {
    val resultsHash = ContractTransactionValidation.resultsHash(results, assetOps)
    apply(sender, txId, keyBlockId, readings, readingsHash, outputCommitment, resultsHash)
  }

  def toByteArray(readDescriptor: ReadDescriptor): Array[Byte] = {
    val output = newDataOutput()
    ReadDescriptor.writeBytes(readDescriptor, output)
    output.toByteArray
  }
}

case class VoteMessage(vote: Vote) extends Message

case class SnapshotNotification(sender: PublicKeyAccount, size: Long) extends Message

case class SnapshotRequest(sender: PublicKeyAccount, offset: Long) extends Message

case class GenesisSnapshotRequest(sender: PublicKeyAccount, genesis: ByteStr, offset: Long, signature: ByteStr) extends Message {
  lazy val bytesWOSig: Array[Byte] = {
    val buffer = ByteBuffer.allocate(KeyLength + SignatureLength + java.lang.Long.BYTES)
    buffer.put(sender.publicKey.getEncoded).put(genesis.arr).putLong(offset).array()
  }
}

case class GenesisSnapshotError(message: String) extends Message

object GenesisSnapshotError {
  def apply(error: ApiError): GenesisSnapshotError = new GenesisSnapshotError(error.message)
}

case class NodeAttributes(nodeMode: NodeMode, p2pTlsEnabled: Boolean, sender: PublicKeyAccount) {
  val bytes: Coeval[ByteStr] = Coeval.evalOnce {
    ByteStr(
      Array(
        nodeMode.value,
        (if (p2pTlsEnabled) 1 else 0).toByte
      )
    )
  }
}

object NodeAttributes {
  implicit val show: Show[NodeAttributes] = Show.show { attributes =>
    s"Node attributes [mode: ${attributes.nodeMode}, p2pTlsEnabled: ${attributes.p2pTlsEnabled}] from address '${attributes.sender.toAddress}'"
  }

  def fromBytes(bytes: Array[Byte], sender: PublicKeyAccount): Either[String, NodeAttributes] = {
    val buf = ByteBuffer.wrap(bytes)
    for {
      nodeMode      <- NodeMode.withValue(buf.get())
      p2pTlsEnabled <- parseP2PTlsEnabled(buf)
    } yield NodeAttributes(nodeMode, p2pTlsEnabled, sender)
  }

  private def parseP2PTlsEnabled(buf: ByteBuffer): Either[String, Boolean] = {
    if (buf.remaining() == 0) {
      Right(false)
    } else {
      buf.get() match {
        case 0     => Right(false)
        case 1     => Right(true)
        case other => Left(s"Unexpected byte for p2pTlsEnabled field: '$other'")
      }
    }
  }
}

case class RawAttributes(bodyBytes: ByteStr, sender: PublicKeyAccount, signature: ByteStr) extends Message {
  require(bodyBytes.arr.nonEmpty, "Empty body bytes")

  def signatureIsValid: Boolean = crypto.verify(signature.arr, bodyBytes.arr, sender.publicKey)

  def toNodeAttributes: Either[String, NodeAttributes] = NodeAttributes.fromBytes(bodyBytes.arr, sender)
}

object RawAttributes {
  implicit val show: Show[RawAttributes] = Show.show { attributes =>
    s"Raw node attributes [bytes length: ${attributes.bodyBytes.arr.length}] from address '${attributes.sender.toAddress}'"
  }

  def createAndSign(ownerKey: PrivateKeyAccount, nodeAttributes: NodeAttributes): RawAttributes = {
    val unsigned  = RawAttributes(nodeAttributes.bytes(), ownerKey, ByteStr.empty)
    val signature = crypto.sign(ownerKey, unsigned.bodyBytes.arr)
    unsigned.copy(signature = ByteStr(signature))
  }
}

case class MissingCrlDataRequest(issuer: PublicKeyAccount, cdp: URL, missingIds: Set[BigInt]) extends Message
case class CrlDataByHashesRequest(crlHashes: Set[ByteStr])                                    extends Message
case class CrlDataByTimestampRangeRequest(from: Long, to: Long) extends Message {
  require(from >= 0 && to >= 0, "Timestamp must be positive")
  require(to >= from, "'to' must be after 'from'")
}

case class CrlDataResponse(id: CrlMessageContextId, foundCrlData: Set[CrlData]) extends Message

sealed abstract class CrlMessageContextId(val value: Byte) extends ByteEnumEntry

object CrlMessageContextId extends ByteEnum[CrlMessageContextId] {
  case object CrlSyncManagerId   extends CrlMessageContextId(0)
  case object MicroblockLoaderId extends CrlMessageContextId(1)
  case object BlockLoaderId      extends CrlMessageContextId(2)

  override val values: immutable.IndexedSeq[CrlMessageContextId] = findValues

  def fromByte(value: Byte): Try[CrlMessageContextId] = value match {
    case 0           => Success(CrlSyncManagerId)
    case 1           => Success(MicroblockLoaderId)
    case 2           => Success(BlockLoaderId)
    case unsupported => Failure(new IllegalArgumentException(s"Unsupported CrlMessageContextId '$unsupported'"))
  }
}

// todo: review if we need salt(commitment's) here
case class ConfidentialDataRequest(contractId: ContractId, commitment: Commitment, dataType: ConfidentialDataType) extends Message {
  override def toString: String = {
    s"ConfidentialDataRequest { contractId: '$contractId', commitment: '${commitment.hash}', type: '$dataType'}"
  }
}

sealed trait ConfidentialDataResponse extends Message {
  def contractId: ContractId
  def commitment: Commitment
  def dataType: ConfidentialDataType
}

object ConfidentialDataResponse {
  case class NoDataResponse(request: ConfidentialDataRequest) extends ConfidentialDataResponse {
    override def contractId: ContractId = request.contractId

    override def commitment: Commitment = request.commitment

    override def dataType: ConfidentialDataType = request.dataType
  }

  case class HasDataResponse(contractId: ContractId,
                             txId: ByteStr,
                             commitmentKey: SaltBytes,
                             commitment: Commitment,
                             dataType: ConfidentialDataType,
                             data: ByteStr) extends ConfidentialDataResponse

  object HasDataResponse {

    def apply(dataEntry: ConfidentialDataUnit, dataType: ConfidentialDataType, data: Array[Byte]): HasDataResponse = {
      HasDataResponse(
        dataEntry.contractId,
        dataEntry.txId,
        dataEntry.commitmentKey,
        dataEntry.commitment,
        dataType,
        ByteStr(data)
      )
    }

  }
}

case class ConfidentialInventoryRequest(contractId: ContractId, commitment: Commitment, dataType: ConfidentialDataType) extends Message {
  override def toString: String = s"ConfidentialInventoryRequest(contractId: $contractId, commitment: ${commitment.hash}, dataType: $dataType"
}

case class ConfidentialInventory(sender: PublicKeyAccount,
                                 contractId: ContractId,
                                 commitment: Commitment,
                                 dataType: ConfidentialDataType,
                                 signature: ByteStr) extends Message with Signed {

  val bodyBytes: Coeval[Array[Byte]] = Coeval(Array.concat(
    sender.toAddress.bytes.arr,
    contractId.byteStr.arr,
    commitment.hash.arr,
    Array(dataType.value)
  ))

  val id: Coeval[ByteStr] = Coeval(ByteStr(crypto.fastHash(bodyBytes.value())))

  override val signatureValid: Coeval[Boolean] = Coeval(crypto.verify(signature.arr, bodyBytes.value(), sender.publicKey))

  def dataId: ConfidentialDataId = ConfidentialDataId(contractId, commitment, dataType)

  override def toString: String = s"ConfidentialInventory(sender: $sender, contractId: $contractId, commitment: $commitment, dataType: $dataType)"
}

object ConfidentialInventory {

  def apply(sender: PrivateKeyAccount, contractId: ContractId, commitment: Commitment, dataType: ConfidentialDataType): ConfidentialInventory = {
    val signature = crypto.sign(sender, Array.concat(sender.toAddress.bytes.arr, contractId.byteStr.arr, commitment.hash.arr, Array(dataType.value)))
    ConfidentialInventory(sender, contractId, commitment, dataType, ByteStr(signature))
  }

}
