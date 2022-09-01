package com.wavesenterprise.block

import com.google.common.io.ByteArrayDataInput
import com.google.common.io.ByteStreams.{newDataInput, newDataOutput}
import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.MicroBlock.ModifiedTotalSignVersions
import com.wavesenterprise.consensus.Vote
import com.wavesenterprise.crypto
import com.wavesenterprise.mining.Miner.{MaxTransactionsPerMicroblock, MaxVotesPerMicroblock}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.crypto.{SignatureLength, KeyLength}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction._
import com.wavesenterprise.utils.DatabaseUtils._
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Coeval

import scala.util.{Failure, Try}

sealed trait MicroBlock extends Signed {
  def version: Byte
  def sender: PublicKeyAccount
  def timestamp: Long
  def prevLiquidBlockSig: BlockId
  def totalLiquidBlockSig: BlockId
  def signature: ByteStr

  def bytes: Coeval[Array[Byte]]

  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce {
    val bytesToVerify = {
      if (ModifiedTotalSignVersions.contains(version))
        withTotalSignature(ByteStr.empty).bytes()
      else
        bytes()
    }.dropRight(SignatureLength)

    crypto.verify(signature.arr, bytesToVerify, sender.publicKey)
  }

  def withTotalSignature(signature: ByteStr): MicroBlock
}

object MicroBlock {

  // Versions with modification that excludes the total signature (of the liquid block) from the signature of the
  // micro-block in order to optimize performance.
  val ModifiedTotalSignVersions = Set(TxMicroBlock.ModifiedTotalSignVersion, VoteMicroBlock.ModifiedTotalSignVersion)

  def parseBytes(bytes: Array[Byte]): Try[MicroBlock] = {
    val in      = newDataInput(bytes)
    val version = in.readVersion
    version match {
      case TxMicroBlock.DefaultVersion             => TxMicroBlock.parseBytes(version, in)
      case TxMicroBlock.ModifiedTotalSignVersion   => TxMicroBlock.parseBytes(version, in)
      case VoteMicroBlock.DefaultVersion           => VoteMicroBlock.parseBytes(version, in)
      case VoteMicroBlock.ModifiedTotalSignVersion => VoteMicroBlock.parseBytes(version, in)
    }
  }

  private[block] def validate(version: Byte, generator: PrivateKeyAccount, prevResBlockSig: BlockId, totalResBlockSig: BlockId) = {
    for {
      _ <- Either.cond(prevResBlockSig.arr.length == SignatureLength, (), GenericError(s"Incorrect prevResBlockSig: ${prevResBlockSig.arr.length}"))
      _ <- Either.cond(
        ModifiedTotalSignVersions.contains(version) || totalResBlockSig.arr.length == SignatureLength,
        (),
        GenericError(s"Incorrect totalLiquidBlockSig length: '${totalResBlockSig.arr.length}', expected '$SignatureLength'")
      )
      _ <- Either.cond(
        generator.publicKey.getEncoded.length == KeyLength,
        (),
        GenericError(s"Incorrect generator public key length: '${generator.publicKey.getEncoded.length}', expected '$KeyLength'")
      )
    } yield ()
  }
}

case class TxMicroBlock(version: Byte,
                        sender: PublicKeyAccount,
                        timestamp: Long,
                        transactionData: Seq[Transaction],
                        prevLiquidBlockSig: BlockId,
                        totalLiquidBlockSig: BlockId,
                        signature: ByteStr)
    extends MicroBlock {

  override val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    val out = newDataOutput()
    out.writeVersion(version)
    out.writeTimestamp(timestamp)
    out.writeByteStr(prevLiquidBlockSig)
    out.writeByteStr(totalLiquidBlockSig)
    out.writeMicroBlockTransactions(transactionData)
    out.writePublicKey(sender)
    out.writeByteStr(signature)
    out.toByteArray
  }

  override def toString: String =
    s"TxMicroBlock($totalLiquidBlockSig -> $prevLiquidBlockSig, version=$version, sender=$sender, ts=$timestamp, txs=${transactionData.size}, signature=$signature)"

  override def withTotalSignature(signature: AssetId): MicroBlock = copy(totalLiquidBlockSig = signature)
}

object TxMicroBlock extends ScorexLogging {

  val DefaultVersion: Byte           = 3
  val ModifiedTotalSignVersion: Byte = 5

  def buildAndSign(generator: PrivateKeyAccount,
                   timestamp: Long,
                   transactionData: Seq[Transaction],
                   prevResBlockSig: BlockId,
                   totalResBlockSig: BlockId,
                   version: Byte = DefaultVersion): Either[ValidationError, TxMicroBlock] =
    for {
      _ <- MicroBlock.validate(version, generator, prevResBlockSig, totalResBlockSig)
      preliminaryTotalSig = if (version == ModifiedTotalSignVersion) ByteStr.empty else totalResBlockSig
      nonSigned <- create(version, generator, timestamp, transactionData, prevResBlockSig, preliminaryTotalSig, ByteStr.empty)
    } yield {
      val signature = crypto.sign(generator, nonSigned.bytes())
      nonSigned.copy(signature = ByteStr(signature), totalLiquidBlockSig = totalResBlockSig)
    }

  private def create(version: Byte,
                     generator: PublicKeyAccount,
                     timestamp: Long,
                     transactionData: Seq[Transaction],
                     prevResBlockSig: BlockId,
                     totalResBlockSig: BlockId,
                     signature: ByteStr): Either[ValidationError, TxMicroBlock] = {
    if (transactionData.isEmpty)
      Left(GenericError("cannot create MicroBlock with empty transactions"))
    else if (transactionData.size > MaxTransactionsPerMicroblock)
      Left(GenericError(s"too many txs in MicroBlock: allowed: $MaxTransactionsPerMicroblock, actual: ${transactionData.size}"))
    else
      Right(new TxMicroBlock(version, generator, timestamp, transactionData, prevResBlockSig, totalResBlockSig, signature))
  }

  private[block] def parseBytes(version: Byte, in: ByteArrayDataInput): Try[MicroBlock] =
    Try {
      val timestamp        = in.readTimestamp
      val prevResBlockSig  = in.readSignature
      val totalResBlockSig = in.readSignature
      val txs              = in.readMicroBlockTransactions
      val generatorKey     = in.readPublicKey
      val signature        = in.readSignature

      create(version, generatorKey, timestamp, txs, prevResBlockSig, totalResBlockSig, signature).explicitGet()
    }.recoverWith {
      case t =>
        log.error("Error when parsing TxMicroBlock", t)
        Failure(t)
    }
}

case class VoteMicroBlock(version: Byte,
                          sender: PublicKeyAccount,
                          timestamp: Long,
                          votes: Seq[Vote],
                          prevLiquidBlockSig: BlockId,
                          totalLiquidBlockSig: BlockId,
                          signature: ByteStr)
    extends MicroBlock {

  val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    val out = newDataOutput()
    out.writeVersion(version)
    out.writeTimestamp(timestamp)
    out.writeByteStr(prevLiquidBlockSig)
    out.writeByteStr(totalLiquidBlockSig)
    out.writeVotes(votes)
    out.writePublicKey(sender)
    out.writeByteStr(signature)
    out.toByteArray
  }

  override def toString: String =
    s"VoteMicroBlock($totalLiquidBlockSig -> $prevLiquidBlockSig, version=$version, sender=$sender, ts=$timestamp, votes=${votes.size}, signature=$signature)"

  override def withTotalSignature(signature: AssetId): MicroBlock = copy(totalLiquidBlockSig = signature)
}

object VoteMicroBlock extends ScorexLogging {

  val DefaultVersion: Byte           = 4
  val ModifiedTotalSignVersion: Byte = 6

  def buildAndSign(generator: PrivateKeyAccount,
                   timestamp: Long,
                   votes: Seq[Vote],
                   prevResBlockSig: BlockId,
                   totalResBlockSig: BlockId,
                   version: Byte = DefaultVersion): Either[ValidationError, VoteMicroBlock] =
    for {
      _ <- MicroBlock.validate(version, generator, prevResBlockSig, totalResBlockSig)
      preliminaryTotalSig = if (version == ModifiedTotalSignVersion) ByteStr.empty else totalResBlockSig
      nonSigned <- create(version, generator, timestamp, votes, prevResBlockSig, preliminaryTotalSig, ByteStr.empty)
    } yield {
      val signature = crypto.sign(generator, nonSigned.bytes())
      nonSigned.copy(signature = ByteStr(signature), totalLiquidBlockSig = totalResBlockSig)
    }

  private def create(version: Byte,
                     generator: PublicKeyAccount,
                     timestamp: Long,
                     votes: Seq[Vote],
                     prevResBlockSig: BlockId,
                     totalResBlockSig: BlockId,
                     signature: ByteStr): Either[ValidationError, VoteMicroBlock] = {
    if (votes.isEmpty)
      Left(GenericError("cannot create MicroBlock with empty votes"))
    else if (votes.size > MaxVotesPerMicroblock)
      Left(GenericError(s"too many votes in MicroBlock: allowed: $MaxVotesPerMicroblock, actual: ${votes.size}"))
    else
      Right(new VoteMicroBlock(version, generator, timestamp, votes, prevResBlockSig, totalResBlockSig, signature))
  }

  private[block] def parseBytes(version: Byte, in: ByteArrayDataInput): Try[MicroBlock] =
    Try {
      val timestamp        = in.readTimestamp
      val prevResBlockSig  = in.readSignature
      val totalResBlockSig = in.readSignature
      val votes            = in.readVotes()
      val genPK            = in.readPublicKey
      val signature        = in.readSignature

      create(version, genPK, timestamp, votes, prevResBlockSig, totalResBlockSig, signature).explicitGet()
    }.recoverWith {
      case t =>
        log.error("Error when parsing VoteMicroBlock", t)
        Failure(t)
    }
}
