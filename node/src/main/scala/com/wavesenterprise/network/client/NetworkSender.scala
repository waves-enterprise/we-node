package com.wavesenterprise.network.client

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels.ClosedChannelException
import com.wavesenterprise.network.TrafficLogger
import com.wavesenterprise.network.handshake.SignedHandshakeV3
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.Channel
import monix.eval.Coeval

import scala.concurrent.{ExecutionContext, Future, Promise}

class NetworkSender(trafficLoggerSettings: TrafficLogger.Settings, handshakeMaker: Coeval[SignedHandshakeV3])(implicit ec: ExecutionContext)
    extends ScorexLogging {

  private[this] val MessagesBatchSize = 100

  private[this] val client = new NetworkClient(trafficLoggerSettings, handshakeMaker)

  def connect(address: InetSocketAddress): Future[Channel] =
    client.connect(address)

  def send(channel: Channel, messages: Any*): Future[Unit] = {
    def doWrite(messages: Seq[Any]): Future[Unit] =
      if (messages.isEmpty)
        Future.successful(())
      else if (!channel.isWritable)
        Future.failed(new ClosedChannelException)
      else {
        val (send, keep) = messages.splitAt(MessagesBatchSize)
        val futures = send.toVector.map { msg =>
          val result = Promise[Unit]()
          channel.write(msg).addListener { f: io.netty.util.concurrent.Future[Void] =>
            if (!f.isSuccess) {
              val cause = Option(f.cause()).getOrElse(new IOException("Can't send a message to the channel"))
              log.error(s"Can't send a message to the channel: $msg", cause)
              result.failure(cause)
            } else {
              result.success(())
            }
          }
          result.future
        }

        channel.flush()
        Future.sequence(futures).flatMap(_ => doWrite(keep))
      }

    doWrite(messages)
  }

  def close(): Unit = client.shutdown()
}
