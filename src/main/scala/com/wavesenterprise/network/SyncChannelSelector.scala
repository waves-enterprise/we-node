package com.wavesenterprise.network

import cats.Eq
import io.netty.channel.Channel
import monix.eval.Coeval
import monix.reactive.Observable
import play.api.libs.json.{JsObject, Json, OWrites}

/**
  * Selects the channel from peers for blockchain synchronization.
  */
trait SyncChannelSelector {
  def syncChannelUpdateEvents: Observable[SyncChannelSelector.SyncChannelUpdateEvent]
  def statsReporter: Coeval[SyncChannelSelector.Stats]
}

object SyncChannelSelector {

  sealed trait ChannelInfo { def channel: Channel }

  case class ScoredChannelInfo(channel: Channel, score: BigInt) extends ChannelInfo {
    override def toString: String = s"ScoredChannelInfo('${id(channel)}', score: '$score')"
  }

  case class SimpleChannelInfo(channel: Channel) extends ChannelInfo {
    override def toString: String = s"SimpleChannelInfo('${id(channel)}')"
  }

  case class SyncChannelUpdateEvent(currentChannelOpt: Option[ChannelInfo])

  object SyncChannelUpdateEvent {
    implicit val eq: Eq[SyncChannelUpdateEvent] = { (x, y) =>
      x.currentChannelOpt == y.currentChannelOpt
    }
  }

  sealed trait Stats { def json: JsObject }

  case class ScoringStats(bestChannelIdOpt: Option[String], localScore: BigInt, scoresCacheSize: Long) extends Stats {
    implicit val writes: OWrites[ScoringStats] = Json.writes[ScoringStats]
    override def json: JsObject                = Json.toJsObject(this)
  }

  case class SimpleStats(currentChannelIdOpt: Option[String]) extends Stats {
    implicit val writes: OWrites[SimpleStats] = Json.writes[SimpleStats]
    override def json: JsObject               = Json.toJsObject(this)
  }
}
