package com.wavesenterprise.docker

import java.nio.charset.StandardCharsets
import com.google.common.io.ByteStreams.newDataOutput
import com.google.common.primitives.{Bytes, Ints, Longs}
import com.wavesenterprise.account.{Address, PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.crypto
import com.wavesenterprise.protobuf.service.util.ContractExecutionResponse.{Status => PbStatus}
import com.wavesenterprise.protobuf.service.util.{ContractExecutionResponse => PbContractExecutionMessage}
import com.wavesenterprise.serialization.Deser
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.crypto.KeyLength
import com.wavesenterprise.utils.Base58
import enumeratum.EnumEntry
import enumeratum.EnumEntry.Camelcase
import play.api.libs.json.{JsObject, Json, OWrites}

import java.util
import scala.collection.immutable
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Success, Try}

/**
  * Contract execution statuses
  */
sealed trait ContractExecutionStatus extends EnumEntry with Camelcase {
  def toProto: PbStatus
}

object ContractExecutionStatus extends enumeratum.Enum[ContractExecutionStatus] {
  case object Success extends ContractExecutionStatus {
    override def toProto: PbStatus = PbStatus.SUCCESS
  }
  case object Error extends ContractExecutionStatus {
    override def toProto: PbStatus = PbStatus.ERROR
  }
  case object Failure extends ContractExecutionStatus {
    override def toProto: PbStatus = PbStatus.FAILURE
  }

  override def values: immutable.IndexedSeq[ContractExecutionStatus] = findValues
}

/**
  * Message about contract execution
  */
case class ContractExecutionMessage private (rawSenderPubKey: Array[Byte],
                                             txId: ByteStr,
                                             status: ContractExecutionStatus,
                                             code: Option[Int],
                                             message: String,
                                             timestamp: Long,
                                             signature: ByteStr) {
  def bodyBytes(): Array[Byte] = {
    val output = newDataOutput()
    output.write(rawSenderPubKey)
    Deser.writeArray(txId.arr, output)
    Deser.writeArray(status.toString.getBytes(StandardCharsets.UTF_8), output)
    output.write(Deser.serializeOption(code)(Ints.toByteArray))
    Deser.writeArray(message.getBytes(StandardCharsets.UTF_8), output)
    output.writeLong(timestamp)
    output.toByteArray
  }

  def bytes(): Array[Byte] = Bytes.concat(bodyBytes(), signature.arr)

  def json(): JsObject = Json.obj(
    "sender"          -> Address.fromPublicKey(rawSenderPubKey).address,
    "senderPublicKey" -> Base58.encode(rawSenderPubKey),
    "txId"            -> txId.base58,
    "status"          -> status.toString,
    "code"            -> code,
    "message"         -> message,
    "timestamp"       -> timestamp,
    "signature"       -> signature.base58
  )

  def toProto: PbContractExecutionMessage =
    PbContractExecutionMessage(
      txId.base58,
      status.toProto,
      code,
      message,
      timestamp,
      signature.base58,
      PublicKeyAccount(rawSenderPubKey).toAddress.address,
      Base58.encode(rawSenderPubKey)
    )

  override def equals(other: Any): Boolean = other match {
    case that: ContractExecutionMessage =>
      eq(that) || (that canEqual this) &&
        util.Arrays.equals(rawSenderPubKey, that.rawSenderPubKey) &&
        txId == that.txId &&
        status == that.status &&
        code == that.code &&
        message == that.message &&
        timestamp == that.timestamp &&
        signature == that.signature
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq[Any](rawSenderPubKey.toSeq, txId, status, code, message, timestamp, signature)
    MurmurHash3.orderedHash(state)
  }

}

object ContractExecutionMessage {

  implicit val Writes: OWrites[ContractExecutionMessage] = (message: ContractExecutionMessage) => message.json()

  val MessageMaxLength: Int = 1000     // in symbols
  val MaxLengthInBytes: Int = 5 * 1024 // 5KB in bytes

  private def trimMessage(message: String) = if (message.length > MessageMaxLength) message.substring(0, MessageMaxLength) else message

  def apply(sender: PrivateKeyAccount,
            txId: ByteStr,
            status: ContractExecutionStatus,
            code: Option[Int],
            message: String,
            timestamp: Long): ContractExecutionMessage = {
    val rawSenderPubKey = sender.publicKey.getEncoded
    val trimmedMessage  = trimMessage(message)
    val unsigned        = ContractExecutionMessage(rawSenderPubKey, txId, status, code, trimmedMessage, timestamp, ByteStr.empty)
    val signature       = ByteStr(crypto.sign(sender, unsigned.bodyBytes()))
    applyInner(rawSenderPubKey, txId, status, code, trimmedMessage, timestamp, signature)
  }

  private def applyInner(rawSenderPubKey: Array[Byte],
                         txId: ByteStr,
                         status: ContractExecutionStatus,
                         code: Option[Int],
                         message: String,
                         timestamp: Long,
                         signature: ByteStr): ContractExecutionMessage = {
    val trimmedMessage = trimMessage(message)
    val signed         = ContractExecutionMessage(rawSenderPubKey, txId, status, code, trimmedMessage, timestamp, signature)
    val bytes          = signed.bytes()
    require(bytes.length <= MaxLengthInBytes, s"Message size is too big, actual size is ${bytes.length} bytes, max length is $MaxLengthInBytes bytes")
    signed
  }

  def parseBytes(bytes: Array[Byte]): Try[ContractExecutionMessage] =
    Try {
      val rawSenderPublicKey              = bytes.slice(0, KeyLength)
      val (txIdBytes, statusOffset)       = Deser.parseArraySize(bytes, KeyLength)
      val (statusBytes, codeOffset)       = Deser.parseArraySize(bytes, statusOffset)
      val status                          = ContractExecutionStatus.withName(new String(statusBytes, StandardCharsets.UTF_8))
      val (code, messageOffset)           = Deser.parseOption(bytes, codeOffset)(Ints.fromByteArray)
      val (messageBytes, timestampOffset) = Deser.parseArraySize(bytes, messageOffset)
      val message                         = new String(messageBytes, StandardCharsets.UTF_8)
      val timestamp                       = Longs.fromByteArray(bytes.slice(timestampOffset, timestampOffset + java.lang.Long.BYTES))
      val signatureBytes                  = bytes.drop(timestampOffset + java.lang.Long.BYTES)
      ContractExecutionMessage(rawSenderPublicKey, ByteStr(txIdBytes), status, code, message, timestamp, ByteStr(signatureBytes))
    } transform (Success(_), e => Failure(new RuntimeException("Can't construct contract execution message", e)))
}
