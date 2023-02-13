package com.wavesenterprise.privacy

import java.nio.ByteBuffer

import com.google.common.primitives.{Ints, Longs}
import com.wavesenterprise.api.http.PolicyItem

import scala.util.Try

case class PolicyMetaData(
    policyId: String,
    hash: String,
    sender: String,
    filename: String,
    size: Int,
    timestamp: Long,
    author: String,
    comment: String,
    loId: Option[Long],
    isLarge: Boolean = false
) {
  def bytes(): Array[Byte] = {
    val policyIdBytes = policyId.getBytes("UTF-8")
    val hashBytes     = hash.getBytes("UTF-8")
    val senderBytes   = sender.getBytes("UTF-8")
    val filenameBytes = filename.getBytes("UTF-8")
    val authorBytes   = author.getBytes("UTF-8")
    val commentBytes  = comment.getBytes("UTF-8")

    ByteBuffer
      .allocate(
        Ints.BYTES * 7 + Longs.BYTES + 1 + policyIdBytes.length + hashBytes.length + senderBytes.length + filenameBytes.length + authorBytes.length + commentBytes.length)
      .putInt(policyIdBytes.length)
      .put(policyIdBytes)
      .putInt(hashBytes.length)
      .put(hashBytes)
      .putInt(senderBytes.length)
      .put(senderBytes)
      .put(PolicyMetaData.policyDataType)
      .putInt(filenameBytes.length)
      .put(filenameBytes)
      .putInt(size)
      .putLong(timestamp)
      .putInt(authorBytes.length)
      .put(authorBytes)
      .putInt(commentBytes.length)
      .put(commentBytes)
      .array()
  }

  def toMap: Map[String, String] =
    Map(
      "policyid"  -> policyId,
      "hash"      -> hash,
      "sender"    -> sender,
      "filename"  -> filename,
      "size"      -> size.toString,
      "timestamp" -> timestamp.toString,
      "author"    -> author,
      "comment"   -> comment,
      "isLarge"   -> isLarge.toString
    )
}

object PolicyMetaData {
  type PostgresFields = (String, String, String, String, Int, Long, String, String, Option[Long])

  def postgresExtractor(): PartialFunction[PolicyMetaData, PostgresFields] = {
    case PolicyMetaData(policyId, hash, sender, filename, size, timestamp, author, comment, loId, _) =>
      (policyId, hash, sender, filename, size, timestamp, author, comment, loId)
  }

  def tupled: ((String, String, String, String, Int, Long, String, String, Option[Long])) => PolicyMetaData =
    ((
         policyId: String,
         hash: String,
         sender: String,
         filename: String,
         size: Int,
         timestamp: Long,
         author: String,
         comment: String,
         loId: Option[Long]
     ) => apply(policyId, hash, sender, filename, size, timestamp, author, comment, loId, loId.isDefined)).tupled

  val policyDataType: Byte = 1.toByte

  def apply(policyId: String, hash: String, size: Int): PolicyMetaData =
    new PolicyMetaData(policyId, hash, "", "", size, -1L, "", "", None)

  def fromPolicyItem(item: PolicyItem, privacyDataType: PrivacyDataType): PolicyMetaData = {
    PolicyMetaData(
      item.policyId,
      item.hash,
      item.sender,
      item.info.filename,
      item.info.size,
      item.info.timestamp,
      item.info.author,
      item.info.comment,
      None,
      privacyDataType == PrivacyDataType.Large
    )
  }

  def fromBytes(bytes: Array[Byte]): Try[PolicyMetaData] = Try {
    val buf = ByteBuffer.wrap(bytes)

    val policyIdLength = buf.getInt()
    val policyId       = new Array[Byte](policyIdLength)
    buf.get(policyId)

    val hashBytesLength = buf.getInt()
    val hashBytes       = new Array[Byte](hashBytesLength)
    buf.get(hashBytes)

    val senderBytesLength = buf.getInt()
    val senderBytes       = new Array[Byte](senderBytesLength)
    buf.get(senderBytes)

    if (buf.get() != PolicyMetaData.policyDataType) throw new RuntimeException("Unknown policy data type")

    val filenameLength = buf.getInt()
    val filenameBytes  = new Array[Byte](filenameLength)
    buf.get(filenameBytes)

    val size      = buf.getInt()
    val timestamp = buf.getLong()

    val authorLength = buf.getInt()
    val author       = new Array[Byte](authorLength)
    buf.get(author)

    val commentLength = buf.getInt()
    val comment       = new Array[Byte](commentLength)
    buf.get(comment)

    PolicyMetaData(
      policyId = new String(policyId, "UTF-8"),
      hash = new String(hashBytes, "UTF-8"),
      sender = new String(senderBytes, "UTF-8"),
      filename = new String(filenameBytes, "UTF-8"),
      size = size,
      timestamp = timestamp,
      author = new String(author, "UTF-8"),
      comment = new String(comment, "UTF-8"),
      loId = None
    )
  }

  def fromMap(map: Map[String, String]): Try[PolicyMetaData] = Try {
    PolicyMetaData(
      policyId = map("policyid"),
      hash = map("hash"),
      sender = map("sender"),
      filename = map("filename"),
      size = map("size").toInt,
      timestamp = map("timestamp").toLong,
      author = map("author"),
      comment = map("comment"),
      isLarge = map.get("isLarge").exists(_.toBoolean),
      loId = None
    )
  }
}
