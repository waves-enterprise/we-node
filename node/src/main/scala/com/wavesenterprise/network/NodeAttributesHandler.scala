package com.wavesenterprise.network

import cats.implicits._
import com.wavesenterprise.network.Attributes.{NodeModeAttribute, TlsAttribute}
import com.wavesenterprise.network.peers.ActivePeerConnections
import monix.execution.Scheduler
import monix.execution.cancelables.SerialCancelable
import monix.reactive.OverflowStrategy
import scorex.util.ScorexLogging

class NodeAttributesHandler(
    incomingMessages: ChannelObservable[RawAttributes],
    activePeersConnections: ActivePeerConnections
)(implicit scheduler: Scheduler)
    extends AutoCloseable
    with ScorexLogging {

  private[this] val process = SerialCancelable()

  def run(): Unit = {
    process := incomingMessages
      .asyncBoundary(OverflowStrategy.Default)
      .foreach {
        case (channel, rawAttributes) =>
          val attributesSignatureIsValid = rawAttributes.signatureIsValid
          val maybeNodeAttributes        = rawAttributes.toNodeAttributes
          val attributesSenderIsActive = activePeersConnections
            .peerConnection(channel)
            .exists(_.peerInfo.nodeOwnerAddress == rawAttributes.sender.toAddress)

          if (attributesSignatureIsValid && attributesSenderIsActive && maybeNodeAttributes.isRight) {
            maybeNodeAttributes.foreach { nodeAttributes =>
              channel.attr(NodeModeAttribute).set(nodeAttributes.nodeMode)
              if (nodeAttributes.p2pTlsEnabled) {
                channel.attr(TlsAttribute).set(Unit)
              }
              log.debug(show"$nodeAttributes have been applied, channel '${id(channel)}'")
            }
          } else {
            val reasons = Seq(
              if (attributesSignatureIsValid) None else Some("attributes signature is invalid"),
              if (attributesSenderIsActive) None else Some("node owner not found among active peers"),
              if (maybeNodeAttributes.isRight) None else Some(maybeNodeAttributes.left.get.toLowerCase)
            ).flatten.mkString(", ")

            log.warn(show"$rawAttributes have been rejected, channel '${id(channel)}', reasons: $reasons")
          }
      }
  }

  override def close(): Unit = process.cancel()
}
