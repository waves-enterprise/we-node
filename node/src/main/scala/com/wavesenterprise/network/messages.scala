package com.wavesenterprise.network

import cats.Show
import cats.implicits._
import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.block.{Block, MicroBlock}
import com.wavesenterprise.consensus.Vote
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.internals.EncryptedForSingle
import com.wavesenterprise.crypto.{KeyLength, SignatureLength}
import com.wavesenterprise.docker.ContractExecutionMessage
import com.wavesenterprise.network.message.MessageSpec.{BlockSpec, MicroBlockResponseV1Spec, MicroBlockResponseV2Spec, TransactionSpec}
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId, PrivacyDataType}
import com.wavesenterprise.settings.NodeMode
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.{Signed, Transaction}
import monix.eval.Coeval

import java.nio.ByteBuffer
import scala.util.hashing.MurmurHash3

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
  def from(mb: MicroBlock, certChainStore: CertChainStore): RawBytes =
    RawBytes(MicroBlockResponseV2Spec.messageCode, MicroBlockResponseV2Spec.serializeData(MicroBlockResponseV2(mb, certChainStore)))
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
}

case class MicroBlockResponseV1(microblock: MicroBlock) extends MicroBlockResponse {
  override val certChainStore: CertChainStore = CertChainStore.empty
}
case class MicroBlockResponseV2(microblock: MicroBlock, certChainStore: CertChainStore) extends MicroBlockResponse

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

case class ContractValidatorResults(sender: PublicKeyAccount, txId: ByteStr, keyBlockId: ByteStr, resultsHash: ByteStr, signature: ByteStr)
    extends Message
    with Signed {

  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce(crypto.verify(signature.arr, resultsHash.arr, sender.publicKey))

  // Do not compare signature to remove duplicates from one validator
  override def equals(other: Any): Boolean = {
    other match {
      case that: ContractValidatorResults =>
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
object ContractValidatorResults {

  def apply(sender: PrivateKeyAccount, txId: ByteStr, keyBlockId: ByteStr, resultsHash: ByteStr): ContractValidatorResults = {
    val signature = crypto.sign(sender, resultsHash.arr)
    new ContractValidatorResults(sender, txId, keyBlockId, resultsHash, ByteStr(signature))
  }

  def apply(sender: PrivateKeyAccount,
            txId: ByteStr,
            keyBlockId: ByteStr,
            results: Seq[DataEntry[_]],
            assetOps: Seq[ContractAssetOperation]): ContractValidatorResults = {
    val resultsHash = ContractValidatorResults.resultsHash(results, assetOps)
    apply(sender, txId, keyBlockId, resultsHash)
  }

  def resultsHash(results: Seq[DataEntry[_]], assetOps: Seq[ContractAssetOperation] = Seq()): ByteStr = {
    val output = newDataOutput()
    results.sorted.foreach(ContractTransactionEntryOps.writeBytes(_, output))
    assetOps.foreach(_.writeContractAssetOperationBytes(output)) // contract is responsible for sanity of operations' order
    ByteStr(crypto.fastHash(output.toByteArray))
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
