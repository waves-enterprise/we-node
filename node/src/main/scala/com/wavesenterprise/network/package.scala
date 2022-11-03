package com.wavesenterprise

import java.net.{InetSocketAddress, SocketAddress, URI}
import java.util.concurrent.Callable
import cats.Eq
import cats.syntax.either._
import com.wavesenterprise.block.Block
import com.wavesenterprise.network.message.MessageSpec.TransactionSpec
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.group.{ChannelGroup, ChannelGroupException, ChannelGroupFuture, ChannelMatcher}
import io.netty.channel.local.LocalAddress
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{Channel, ChannelFuture, ChannelHandlerContext}
import io.netty.util.NetUtil.toSocketAddressString
import io.netty.util.concurrent.{EventExecutorGroup, ScheduledFuture}
import monix.catnap.MVar
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler
import monix.reactive.{Observable, OverflowStrategy}

import scala.concurrent.duration._
import scala.util.Try
import scala.collection.JavaConverters._

package object network extends ScorexLogging {

  def taskFromChannelFuture[F <: ChannelFuture](channelFuture: F): Task[Unit] =
    Task.async[Unit] { callBack =>
      channelFuture.addListener { completedFuture: F =>
        if (completedFuture.isSuccess) {
          callBack.onSuccess(())
        } else {
          callBack.onError(new RuntimeException(s"Failed to write to channel '${id(channelFuture.channel)}'", completedFuture.cause))
        }
      }
    }

  def taskFromChannelGroupFuture[F <: ChannelGroupFuture](channelGroupFuture: F): Task[Set[Channel]] =
    Task.async[Set[Channel]] { callBack =>
      channelGroupFuture.addListener { result: F =>
        val successChannels = result.asScala.collect {
          case channelResult if channelResult.isSuccess => channelResult.channel
        }.toSet

        if (!result.isSuccess) {
          result.cause match {
            case groupEx: ChannelGroupException =>
              log.debug(s"Failed to write to group '${result.group.name}'", groupEx)
              groupEx.forEach { entry =>
                log.debug(s"Failed to write to channel '${id(entry.getKey)}'", entry.getValue)
              }
            case ex =>
              log.debug(s"Failed to write to group '${result.group.name}'", ex)
          }
        }

        callBack.onSuccess(successChannels)
      }
    }

  def inetSocketAddress(addr: String, defaultPort: Int): InetSocketAddress = {
    val uri = new URI(s"node://$addr")
    if (uri.getPort < 0) new InetSocketAddress(addr, defaultPort)
    else new InetSocketAddress(uri.getHost, uri.getPort)
  }

  implicit class EventExecutorGroupExt(val e: EventExecutorGroup) extends AnyVal {
    def scheduleWithFixedDelay(initialDelay: FiniteDuration, delay: FiniteDuration)(f: => Unit): ScheduledFuture[_] =
      e.scheduleWithFixedDelay((() => f): Runnable, initialDelay.toNanos, delay.toNanos, NANOSECONDS)

    def schedule[A](delay: FiniteDuration)(f: => A): ScheduledFuture[A] =
      e.schedule((() => f): Callable[A], delay.length, delay.unit)
  }

  //noinspection ScalaStyle
  private def formatAddress(sa: SocketAddress): String = sa match {
    case null                   => ""
    case l: LocalAddress        => s" $l"
    case isa: InetSocketAddress => s" ${toSocketAddressString(isa)}"
    case x                      => s" $x" // For EmbeddedSocketAddress
  }

  def id(ctx: ChannelHandlerContext): String = id(ctx.channel())

  def id(chan: Channel, prefix: String = ""): String = s"[$prefix${chan.id().asShortText()}${formatAddress(chan.remoteAddress())}]"

  def formatBlocks(blocks: Seq[Block]): String = formatSignatures(blocks.view.map(_.uniqueId))

  def formatSignatures(signatures: Seq[ByteStr]): String =
    if (signatures.isEmpty) "[Empty]"
    else if (signatures.size == 1) s"[${signatures.head.trim}]"
    else s"(total=${signatures.size}) [${signatures.head.trim} -- ${signatures.last.trim}]"

  implicit val channelEq: Eq[Channel] = Eq.fromUniversalEquals

  implicit class ChannelHandlerContextExt(val ctx: ChannelHandlerContext) extends AnyVal {
    def remoteAddressOpt: Option[InetSocketAddress] = ctx.channel.remoteAddressOpt
  }

  implicit class ChannelExt(val channel: Channel) extends AnyVal {
    def remoteAddressOpt: Option[InetSocketAddress] = channel match {
      case nioSC: NioSocketChannel => Option(nioSC.remoteAddress())
      case unknown =>
        log.debug(s"Doesn't know how to get a remoteAddress from '${id(channel)}', $unknown")
        None
    }

    def remoteAddressEither: Either[Throwable, InetSocketAddress] = channel match {
      case nio: NioSocketChannel =>
        Either.fromTry(Try(nio.remoteAddress()))

      case unknownChannel =>
        Left(new IllegalStateException(s"Unknown channel type '$unknownChannel'"))
    }
  }

  def closeChannel(channel: Channel, reason: String): Unit = {
    log.debug(s"Closing connection with '${id(channel)}' because of: $reason")
    channel.close()
  }

  implicit class ChannelGroupExt(val channelGroup: ChannelGroup) extends AnyVal {
    def broadcast(message: AnyRef, except: Option[Channel] = None): Unit = broadcast(message, except.toSet)

    def broadcast(message: AnyRef, except: Set[Channel]): ChannelGroupFuture = {
      logBroadcast(message, except)
      channelGroup.writeAndFlush(message, { channel: Channel =>
        !except.contains(channel)
      })
    }

    def broadcastMany(messages: Seq[AnyRef], except: Set[Channel] = Set.empty): Unit = {
      val channelMatcher: ChannelMatcher = { channel: Channel =>
        !except.contains(channel)
      }
      messages.foreach { message =>
        logBroadcast(message, except)
        channelGroup.write(message, channelMatcher)
      }

      channelGroup.flush(channelMatcher)
    }

    def broadcastTx(tx: Transaction, except: Option[Channel] = None): Unit = channelGroup.broadcast(RawBytes.from(tx), except)

    def broadcastTx(txs: Seq[Transaction]): Unit = channelGroup.broadcastMany(txs.map(RawBytes.from))

    private def logBroadcast(message: AnyRef, except: Set[Channel]): Unit = {
      val MessageCode = TransactionSpec.messageCode
      message match {
        case RawBytes(MessageCode, _) =>
        case _ =>
          val exceptMsg = if (except.isEmpty) "" else s" (except ${except.map(id(_)).mkString(", ")})"
          log.trace(s"Broadcasting $message to ${channelGroup.size()} channels$exceptMsg")
      }
    }
  }

  type ChannelObservable[A] = Observable[(Channel, A)]

  def lastObserved[A](o: Observable[A])(implicit s: Scheduler): Coeval[Option[A]] = {
    @volatile var last = Option.empty[A]
    o.foreach(a => last = Some(a))
    Coeval(last)
  }

  def newItems[A](o: Observable[A])(implicit s: Scheduler): Coeval[Seq[A]] = {
    @volatile var collected = Seq.empty[A]
    o.foreach(a => collected = collected :+ a)
    Coeval {
      val r = collected
      collected = Seq.empty
      r
    }
  }

  def sendRequestAndAwait[T, R](channel: Channel, obs: Observable[T])(requestPrep: Task[Message])(awaitLogic: Observable[T] => Task[R]): Task[R] = {
    for {
      subscribeTrigger <- MVar.empty[Task, Unit]()
      awaitResponse    <- awaitLogic(obs.asyncBoundary(OverflowStrategy.Default).doAfterSubscribe(subscribeTrigger.put(Unit))).start
      _                <- subscribeTrigger.take
      request          <- requestPrep
      _                <- taskFromChannelFuture(channel.writeAndFlush(request))
      response         <- awaitResponse.join
    } yield response
  }
}
