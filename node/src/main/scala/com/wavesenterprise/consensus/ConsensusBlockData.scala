package com.wavesenterprise.consensus

import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.GenericError
import play.api.libs.json.{JsObject, Json}

sealed trait ConsensusBlockData extends Product with Serializable { self =>
  def name: String
  def json: JsObject

  def asPoSMaybe(): Either[GenericError, PoSLikeConsensusBlockData] = self match {
    case pos: PoSLikeConsensusBlockData => Right(pos)
    case _: PoALikeConsensusBlockData   => Left(GenericError("Expected PoS consensus block data, but got PoA instead"))
    case _: CftLikeConsensusBlockData   => Left(GenericError("Expected PoS consensus block data, but got CFT instead"))
  }

  def asPoAMaybe(): Either[GenericError, PoALikeConsensusBlockData] = self match {
    case poa: PoALikeConsensusBlockData => Right(poa)
    case _: PoSLikeConsensusBlockData   => Left(GenericError("Expected PoA consensus block data, but got PoS instead"))
    case _: CftLikeConsensusBlockData   => Left(GenericError("Expected PoA consensus block data, but got CFT instead"))
  }

  def asCftMaybe(): Either[GenericError, CftLikeConsensusBlockData] = self match {
    case cft: CftLikeConsensusBlockData => Right(cft)
    case _: PoSLikeConsensusBlockData   => Left(GenericError("Expected CFT consensus block data, but got PoS instead"))
    case _: PoALikeConsensusBlockData   => Left(GenericError("Expected CFT consensus block data, but got PoA instead"))
  }
}

case class PoSLikeConsensusBlockData(baseTarget: Long, generationSignature: ByteStr) extends ConsensusBlockData { self =>
  override def name: String = "pos-consensus"

  override def json: JsObject =
    Json.obj(
      name -> Json.obj(
        "base-target"          -> self.baseTarget,
        "generation-signature" -> self.generationSignature.base58
      ))
}

object PoSLikeConsensusBlockData {
  val typeByte: Byte = 1
}

case class PoALikeConsensusBlockData(overallSkippedRounds: Long) extends ConsensusBlockData { self =>
  override def name: String = "poa-consensus"

  override def json: JsObject =
    Json.obj(
      name -> Json.obj(
        "overall-skipped-rounds" -> overallSkippedRounds
      ))
}

object PoALikeConsensusBlockData {
  val typeByte: Byte = 2
}

case class CftLikeConsensusBlockData(votes: Seq[Vote], overallSkippedRounds: Long) extends ConsensusBlockData { self =>
  override def name: String = "cft-consensus"

  override def json: JsObject =
    Json.obj(
      name -> Json.obj(
        "votes"                  -> votes.map(_.json()),
        "overall-skipped-rounds" -> overallSkippedRounds
      )
    )

  def isFinalized: Boolean    = votes.nonEmpty
  def isNotFinalized: Boolean = !isFinalized
}

object CftLikeConsensusBlockData {
  val typeByte: Byte = 3
}
