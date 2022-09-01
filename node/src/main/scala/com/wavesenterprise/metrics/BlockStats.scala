package com.wavesenterprise.metrics

import com.wavesenterprise.block.{Block, MicroBlock, TxMicroBlock, VoteMicroBlock}
import com.wavesenterprise.network.{Attributes, MicroBlockInventory}
import com.wavesenterprise.state.ByteStr
import io.netty.channel.Channel
import org.influxdb.dto.Point

object BlockStats {

  private val StringIdLength = 6

  trait Named {
    private[BlockStats] val name: String = {
      val className = getClass.getName
      className.slice(className.lastIndexOf('$', className.length - 2) + 1, className.length - 1)
    }
  }

  private sealed abstract class Event extends Named

  private object Event {
    case object Inv      extends Event
    case object Received extends Event
    case object Applied  extends Event
    case object Declined extends Event
    case object Mined    extends Event
  }

  private sealed abstract class Type extends Named

  private object Type {
    case object Block extends Type
    case object Micro extends Type
  }

  sealed abstract class Source extends Named

  object Source {
    case object Broadcast extends Source
    case object Extension extends Source
  }

  def received(b: Block, source: Source, ch: Channel): Unit = write(
    block(b, source)
      .addField("from", nodeName(ch))
      .addField("prop-time", System.currentTimeMillis() - b.timestamp)
      .addField("bt", b.consensusData.asPoSMaybe().map(_.baseTarget).getOrElse(0L))
      .addField("skipped-rounds", b.consensusData.asPoAMaybe().map(_.overallSkippedRounds).getOrElse(0L)),
    Event.Received,
    Seq.empty
  )

  def applied(b: Block, source: Source, newHeight: Int): Unit = write(
    block(b, source)
      .addField("txs", b.transactionData.size)
      .addField("height", newHeight),
    Event.Applied,
    Seq.empty
  )

  def declined(b: Block, source: Source): Unit = write(
    block(b, source),
    Event.Declined,
    Seq.empty
  )

  def mined(b: Block, baseHeight: Int): Unit = write(
    block(b, Source.Broadcast)
      .tag("parent-id", id(b.reference))
      .addField("txs", b.transactionData.size)
      .addField("bt", b.consensusData.asPoSMaybe().map(_.baseTarget).getOrElse(0L))
      .addField("skipped-rounds", b.consensusData.asPoAMaybe().map(_.overallSkippedRounds).getOrElse(0L))
      .addField("height", baseHeight),
    Event.Mined,
    Seq.empty
  )

  def inventory(m: MicroBlockInventory, ch: Channel): Unit = write(
    measurement(Type.Micro)
      .tag("id", id(m.totalBlockSig))
      .tag("parent-id", id(m.prevBlockSig))
      .addField("from", nodeName(ch)),
    Event.Inv,
    Seq.empty
  )

  def received(m: MicroBlock, ch: Channel): Unit = write(
    micro(m)
      .tag("parent-id", id(m.prevLiquidBlockSig))
      .addField("from", nodeName(ch)),
    Event.Received,
    Seq.empty
  )

  def applied(m: MicroBlock): Unit = {
    val point = micro(m)

    val enrichedPoint = m match {
      case txMicro: TxMicroBlock     => point.addField("txs", txMicro.transactionData.size)
      case voteMicro: VoteMicroBlock => point.addField("votes", voteMicro.votes.size)
    }

    write(enrichedPoint, Event.Applied, Seq.empty)
  }

  def declined(m: MicroBlock): Unit = write(
    micro(m),
    Event.Declined,
    Seq.empty
  )

  def mined(m: MicroBlock): Unit = {
    val point = micro(m).tag("parent-id", id(m.prevLiquidBlockSig))

    val enrichedPoint = m match {
      case txMicro: TxMicroBlock     => point.addField("txs", txMicro.transactionData.size)
      case voteMicro: VoteMicroBlock => point.addField("votes", voteMicro.votes.size)
    }

    write(enrichedPoint, Event.Mined, Seq.empty)
  }

  private def block(b: Block, source: Source): Point.Builder =
    measurement(Type.Block)
      .tag("id", id(b.uniqueId))
      .tag("source", source.name)

  private def micro(m: MicroBlock): Point.Builder =
    measurement(Type.Micro)
      .tag("id", id(m.totalLiquidBlockSig))

  private def measurement(t: Type): Point.Builder = Point.measurement("block").tag("type", t.toString)

  private def nodeName(ch: Channel): String = {
    Option(ch.attr(Attributes.NodeNameAttributeKey).get()).getOrElse("")
  }

  private def id(x: ByteStr): String = x.toString.take(StringIdLength)

  private def write(init: Point.Builder, event: Event, addFields: Seq[(String, String)]): Unit = {
    Metrics.write(MetricsType.Block, addFields.foldLeft(init.tag("event", event.name)) { case (r, (k, v)) => r.addField(k, v) })
  }
}
