package com.wavesenterprise.consensus

import com.google.common.io.ByteStreams.{newDataInput, newDataOutput}
import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.block.BlockByteArrayDataInputExt
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.{DigestSize, KeyLength}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{Signed, ValidationError}
import com.wavesenterprise.utils.DatabaseUtils._
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Coeval
import play.api.libs.json.{JsObject, Json}

import scala.util.{Failure, Try}

case class Vote(sender: PublicKeyAccount, blockVotingHash: ByteStr, signature: ByteStr) extends Signed {
  val bodyBytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    //noinspection UnstableApiUsage
    val out = newDataOutput()
    out.writePublicKey(sender)
    out.writeByteStr(blockVotingHash)
    out.toByteArray
  }

  val bytes: Coeval[Array[Byte]] = Coeval.evalOnce(bodyBytes() ++ signature.arr)

  val id: Coeval[ByteStr] = Coeval.evalOnce(ByteStr(crypto.fastHash(bodyBytes())))

  val json: Coeval[JsObject] = Coeval.evalOnce {
    Json.obj(
      "sender"          -> sender.address,
      "blockVotingHash" -> blockVotingHash.base58,
      "signature"       -> signature.base58
    )
  }

  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce {
    crypto.verify(signature.arr, bodyBytes(), sender.publicKey)
  }
}

object Vote extends ScorexLogging {

  def buildAndSign(generator: PrivateKeyAccount, blockVotingHash: ByteStr): Either[ValidationError, Vote] = {
    for {
      _ <- Either.cond(
        generator.publicKey.getEncoded.length == KeyLength,
        (),
        GenericError(s"Invalid generator.publicKey length: '${generator.publicKey.getEncoded.length}', expected '$KeyLength'")
      )
      _ <- Either.cond(
        blockVotingHash.arr.length == DigestSize,
        (),
        GenericError(s"Invalid blockVotingHash length: '${blockVotingHash.arr.length}', expected '$DigestSize'")
      )
    } yield {
      val notSigned = Vote(generator, blockVotingHash, ByteStr.empty)
      val signature = crypto.sign(generator, notSigned.bytes())
      notSigned.copy(signature = ByteStr(signature))
    }
  }

  def parseBytes(bytes: Array[Byte]): Try[Vote] =
    Try {
      //noinspection UnstableApiUsage
      val in              = newDataInput(bytes)
      val sender          = in.readPublicKey
      val blockVotingHash = in.readHash
      val signature       = in.readSignature

      Vote(sender, blockVotingHash, signature)
    }.recoverWith {
      case t: Throwable =>
        log.error("Error when parsing vote", t)
        Failure(t)
    }
}
