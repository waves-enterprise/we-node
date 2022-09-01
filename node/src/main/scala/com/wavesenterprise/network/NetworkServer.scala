package com.wavesenterprise.network

import com.google.common.cache.CacheBuilder
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.api.http.service.PeersIdentityService
import com.wavesenterprise.network.MessageObserver.IncomingMessages
import com.wavesenterprise.network.handshake.{HandshakeDecoder, HandshakeFreeMessagesDecoder, HandshakeHandler, HandshakeTimeoutHandler}
import com.wavesenterprise.network.netty.handler.stream.ChunkedWriteHandler
import com.wavesenterprise.network.peers._
import com.wavesenterprise.privacy.InitialParticipantsDiscoverResult
import com.wavesenterprise.settings._
import com.wavesenterprise.state.{ByteStr, NG}
import com.wavesenterprise.transaction._
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.{ApplicationInfo, Schedulers}
import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import io.netty.handler.flow.FlowControlHandler
import io.netty.util.concurrent.DefaultThreadFactory
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

class NetworkServer(val networkSettings: NetworkSettings,
                    applicationInfo: ApplicationInfo,
                    lastBlockInfos: Observable[LastBlockInfo],
                    blockchain: BlockchainUpdater with NG,
                    historyReplier: HistoryReplier,
                    peerDatabase: PeerDatabase,
                    peersIdentityService: PeersIdentityService,
                    ownerKey: PrivateKeyAccount,
                    schedulers: Schedulers,
                    initialParticipantsDiscoverResult: InitialParticipantsDiscoverResult,
                    connectionAcceptor: PeerConnectionAcceptor)
    extends ScorexLogging
    with ChannelInfoProvider {

  import NetworkServer._

  private val serverStarted     = new AtomicBoolean(false)
  private val shutdownInitiated = new AtomicBoolean(false)

  private val bossGroup   = new NioEventLoopGroup(1, new DefaultThreadFactory("nio-boss-group", true))
  private val workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("nio-worker-group", true))

  /* Sharable handlers start */
  private val trafficWatcher = new TrafficWatcher
  private val trafficLogger  = new TrafficLogger(networkSettings.trafficLogger)
  private val messageCodec   = new MessageCodec()

  /* There are two error handlers by design. WriteErrorHandler adds a future listener to make sure writes to network
       succeed. It is added to the head of pipeline (it's the closest of the two to actual network), because some writes
       are initiated from the middle of the pipeline (e.g. extension requests). FatalErrorHandler, on the other hand,
       reacts to inbound exceptions (the ones thrown during channelRead). It is added to the tail of pipeline to handle
       exceptions bubbling up from all the handlers below. When a fatal exception is caught (like OutOfMemory), the
       application is terminated. */
  private val writeErrorHandler = new WriteErrorHandler
  private val fatalErrorHandler = new FatalErrorHandler

  private val (messageObserver, networkMessages)            = MessageObserver(networkSettings.txBufferSize, schedulers.messageObserverScheduler)
  private val (channelClosedHandler, closedChannelsSubject) = ChannelClosedHandler(schedulers.channelProcessingScheduler)
  private val discardingHandler                             = new DiscardingHandler(networkSettings.mode, lastBlockInfos.map(_.ready), schedulers.discardingHandlerScheduler)
  private val idleConnectionDetector                        = new IdleConnectionDetector(networkSettings.breakIdleConnectionsTimeout)
  private val peerIdentityProcessingHandler                 = new PeerIdentityProcessingHandler(peersIdentityService)
  private val lengthFieldPrepender                          = new LengthFieldPrepender(4)
  /* Sharable handlers end */

  /* LRU cache that exist to prevent incoming txs duplicates */
  private val receivedTxsCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(networkSettings.receivedTxsCacheTimeout.length, networkSettings.receivedTxsCacheTimeout.unit)
    .build[ByteStr, Object]()

  private var serverChannel: ChannelFuture = _

  val messages: IncomingMessages          = networkMessages
  val closedChannels: Observable[Channel] = closedChannelsSubject

  val incomingConnections: ConcurrentSubject[PeerConnection, PeerConnection] =
    ConcurrentSubject.publish[PeerConnection](monix.execution.Scheduler.global)

  protected val nodeAttributesSender = new NodeAttributesSender(ownerKey, networkSettings)

  private def createPeerSynchronizer(): ChannelHandlerAdapter = {
    if (networkSettings.enablePeersExchange) {
      new PeerSynchronizer(peerDatabase, networkSettings.peersRequestInterval)
    } else PeerSynchronizer.Disabled
  }

  private def createLengthFieldBasedFrameDecoder(): LengthFieldBasedFrameDecoder = {
    new LengthFieldBasedFrameDecoder(100 * 1024 * 1024, 0, 4, 0, 4)
  }

  def start(): Future[InetSocketAddress] = {
    val promise = Promise[InetSocketAddress]
    if (serverStarted.compareAndSet(false, true)) {
      serverChannel = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .option(ChannelOption.SO_REUSEADDR, java.lang.Boolean.TRUE)
        .childOption(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
        .childOption(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
        .childHandler(initInboundPipeline())
        .bind(networkSettings.bindSocketAddress)
      serverChannel.addListener { bindFuture: io.netty.util.concurrent.Future[_] =>
        val socketAddress = serverChannel.channel().localAddress().asInstanceOf[InetSocketAddress]
        if (bindFuture.isSuccess) {
          promise.success(socketAddress)
        } else {
          val message = s"Can't start server listening on '${networkSettings.bindAddress}'. Check port is available"
          promise.failure(new IllegalStateException(message))
        }
      }
    } else {
      promise.failure(new IllegalStateException("Network server already started"))
    }
    promise.future
  }

  protected def buildInboundHandlers(channel: SocketChannel, connectPromise: Promise[PeerConnection]): IndexedSeq[ChannelHandler] =
    IndexedSeq(
      idleConnectionDetector,
      new HandshakeFreeMessagesDecoder(),
      new PeerIdentityResponseEncoder,
      peerIdentityProcessingHandler,
      new HandshakeTimeoutHandler(networkSettings.handshakeTimeout),
      buildServerHandshakeHandler(connectPromise),
      lengthFieldPrepender,
      createLengthFieldBasedFrameDecoder(),
      new FlowControlHandler(),
      new MetaMessageCodec(receivedTxsCache),
      channelClosedHandler,
      trafficWatcher,
      discardingHandler,
      new ChunkedWriteHandler(),
      messageCodec,
      trafficLogger,
      writeErrorHandler,
      createPeerSynchronizer(),
      nodeAttributesSender,
      historyReplier,
      messageObserver,
      fatalErrorHandler
    )

  private def initInboundPipeline(): ChannelInitializer[SocketChannel] = { (channel: SocketChannel) =>
    val connectPromise = Promise[PeerConnection]
    connectPromise.future.foreach(incomingConnections.onNext)(ExecutionContext.global)
    val handlers = buildInboundHandlers(channel, connectPromise)
    channel.pipeline.addLast(handlers: _*)
  }

  def connect(remoteAddress: InetSocketAddress, closeCallback: CloseCallback = NoActionCloseCallback): Future[PeerConnection] = {
    val connectPromise = Promise[PeerConnection]
    val channelFuture = new Bootstrap()
      .remoteAddress(remoteAddress)
      .option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
      .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, networkSettings.connectionTimeout.toMillis.toInt: Integer)
      .group(workerGroup)
      .channel(classOf[NioSocketChannel])
      .handler(initOutboundPipeline(connectPromise))
      .connect()
    channelFuture
      .addListener { f: io.netty.util.concurrent.Future[_] =>
        if (f.isSuccess) {
          val channel = channelFuture.channel
          log.trace(s"Peer '$remoteAddress' connection established by channel '${id(channel)}'\n${channelInfo(channel)}")
        } else {
          connectPromise.failure(f.cause)
        }
      }
    channelFuture.channel.closeFuture
      .addListener { f: io.netty.util.concurrent.Future[_] =>
        closeCallback(remoteAddress)
        onOutgoingChannelClosed(channelFuture.channel(), f)
      }
    log.trace(s"${id(channelFuture.channel())} Connecting to '$remoteAddress'")
    connectPromise.future
  }

  protected def buildOutboundHandlers(channel: SocketChannel, connectPromise: Promise[PeerConnection]): IndexedSeq[ChannelHandler] =
    IndexedSeq(
      idleConnectionDetector,
      new HandshakeDecoder(),
      new HandshakeTimeoutHandler(networkSettings.handshakeTimeout),
      buildClientHandshakeHandler(connectPromise),
      lengthFieldPrepender,
      createLengthFieldBasedFrameDecoder(),
      new FlowControlHandler(),
      new MetaMessageCodec(receivedTxsCache),
      channelClosedHandler,
      trafficWatcher,
      discardingHandler,
      new ChunkedWriteHandler(),
      messageCodec,
      trafficLogger,
      writeErrorHandler,
      createPeerSynchronizer(),
      nodeAttributesSender,
      historyReplier,
      messageObserver,
      fatalErrorHandler
    )

  protected def buildServerHandshakeHandler(connectPromise: Promise[PeerConnection]): HandshakeHandler =
    new HandshakeHandler.Server(blockchain, applicationInfo, ownerKey, connectPromise, initialParticipantsDiscoverResult, connectionAcceptor)

  protected def buildClientHandshakeHandler(connectPromise: Promise[PeerConnection]): HandshakeHandler =
    new HandshakeHandler.Client(blockchain, applicationInfo, ownerKey, connectPromise, initialParticipantsDiscoverResult, connectionAcceptor)

  private def initOutboundPipeline(connectPromise: Promise[PeerConnection]): ChannelInitializer[SocketChannel] = { (channel: SocketChannel) =>
    val handlers = buildOutboundHandlers(channel, connectPromise)
    channel.pipeline.addLast(handlers: _*)
  }

  private def onOutgoingChannelClosed(channel: Channel, f: io.netty.util.concurrent.Future[_]): Unit = {
    if (f.isSuccess) {
      log.trace(s"${id(channel)} Outgoing channel closed")
    } else {
      val errorCause = Option(f.cause()).map(_.getMessage).getOrElse("<no message>")
      log.debug(s"Outgoing channel '${id(channel)}' closed with error: '$errorCause'")
    }
  }

  def shutdown(): Unit = {
    if (shutdownInitiated.compareAndSet(false, true)) {
      messageObserver.shutdown()
      connectionAcceptor.close()
      workerGroup.shutdownGracefully().await(ShutdownTimeout.toMillis)
      bossGroup.shutdownGracefully().await(ShutdownTimeout.toMillis)
      Option(serverChannel).foreach(_.channel().close().await())
      log.debug("Closed Nio groups")
      channelClosedHandler.shutdown()
    }
  }
}

object NetworkServer extends ScorexLogging {

  type CloseCallback = InetSocketAddress => Unit

  val NoActionCloseCallback: CloseCallback = (_: InetSocketAddress) => ()

  private val ShutdownTimeout     = 30.seconds
  val MetaMessageCodecHandlerName = "MetaMessageCodec#0"
}
