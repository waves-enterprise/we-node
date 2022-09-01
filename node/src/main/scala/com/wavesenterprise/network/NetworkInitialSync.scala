package com.wavesenterprise.network

import cats.implicits._
import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.network.peers._
import com.wavesenterprise.privacy.InitialParticipantsDiscoverResult
import com.wavesenterprise.privacy.InitialParticipantsDiscoverResult.Participants
import com.wavesenterprise.settings.{NetworkSettings, WESettings}
import com.wavesenterprise.state.NG
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.transaction.ValidationError.ParticipantNotRegistered
import com.wavesenterprise.utils.ScorexLogging
import io.netty.bootstrap.Bootstrap
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{Channel, ChannelFuture, ChannelHandler, ChannelInitializer, ChannelOption}
import io.netty.util.concurrent.GlobalEventExecutor
import monix.eval.Task

import java.net.InetSocketAddress
import scala.concurrent.Promise

class NetworkInitialSync(ownerKey: PrivateKeyAccount, settings: WESettings, blockchain: BlockchainUpdater with NG, peerDatabase: PeerDatabase)
    extends ScorexLogging {

  protected val networkClient = new InitialSyncNetworkClient(ownerKey, settings.network)

  def initialSyncPerform(): Task[InitialParticipantsDiscoverResult] = Task.defer {
    val nodeAddress = ownerKey.toAddress
    if (blockchain.participantPubKey(nodeAddress).isDefined) {
      log.info(s"Initial sync not required: Node owner address '${ownerKey.address}' is present in state")
      Task.now(InitialParticipantsDiscoverResult.NotNeeded)
    } else if (settings.network.knownPeers.isEmpty) {
      Task.raiseError(
        new RuntimeException(
          "Can't execute InitialSync: 'node.network.known-peers' is empty in the configuration and owner-address is not found in the state"))
    } else {
      executeInitialSync(ownerKey)
    }
  }

  private def executeInitialSync(ownerKey: PrivateKeyAccount): Task[InitialParticipantsDiscoverResult] = Task.defer {
    log.info(s"Starting network initial sync. Owner key: '${ownerKey.address}'")
    val peers: List[InetSocketAddress] = peerDatabase.knownPeers.keys.toList
    log.info(s"Peers count '${peers.size}'")

    Task.parTraverse(peers) { remoteAddress =>
      val (connectTask, responseTask) = networkClient.connect(remoteAddress)
      connectTask
        .flatMap(_ => responseTask)
        .redeem(ex => {
          log.error(s"Initial sync from '$remoteAddress' failed with exception", ex)
          Left(ex)
        }, response => Right(response))
    } flatMap { results =>
      val (errors, responses) = results.separate
      handleResponses(responses, errors.size)
    }
  }

  private def handleResponses(responses: List[PeerIdentityResponse], errorsCount: Int): Task[InitialParticipantsDiscoverResult] = Task.defer {
    responses match {
      case Nil =>
        log.error("Got '0' initial sync responses, all peers have failed to respond.")
        Task.raiseError(new RuntimeException("InitialSync failed with errors, could not resume application startup"))
      case _ =>
        log.info(s"Got '${responses.length}' identity responses!")
        val (failures, successes) = responses.map {
          case SuccessPeerIdentityResponse(pk, certificates) => Right(PublicKeyAccount(pk) -> certificates)
          case MessagePeerIdentityResponse(message) =>
            log.warn(s"Initial sync from some peer failed with error: '$message'")
            Left(message)
        }.separate
        if (failures.size == responses.size) {
          log.error("Got '0' success identity responses from all peers.")
          val errorMessage = failures
            .find(_.toLowerCase.contains(ParticipantNotRegistered.participantNotRegisteredClassName))
            .map { _ =>
              "Your node is probably not registered in the network. " +
                settings.blockchain.custom.addressSchemeCharacter.toString match {
                case "V" => "To register your node please refer to 'https://support.wavesenterprise.com/servicedesk/customer/user/'"
                case _   => "To register your node please contact your connection-manager."
              }
            }
            .getOrElse("Identity failed with errors, could not resume application startup")
          Task.raiseError(new RuntimeException(errorMessage))
        } else {
          val result = successes
            .filter { case (pka, _) => blockchain.participantPubKey(pka.toAddress).isEmpty }
            .map { case (pka, certs) => (pka.toAddress, pka -> certs) }
          Task.apply(Participants(result.toMap))
        }
    }
  }

  def shutdown(): Unit = {
    networkClient.shutdown()
  }
}

class InitialSyncNetworkClient(ownerKey: PrivateKeyAccount, val networkSettings: NetworkSettings) extends ScorexLogging {

  private val workerGroup  = new NioEventLoopGroup()
  private val channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

  protected def buildHandlers(channel: SocketChannel, syncResult: Promise[PeerIdentityResponse]): IndexedSeq[ChannelHandler] =
    IndexedSeq(
      new PeerIdentityRequestEncoder,
      new PeerIdentityResponseDecoder(syncResult),
      new PeerIdentityClientHandler(ownerKey, syncResult)
    )

  def connect(remoteAddress: InetSocketAddress): (Task[Channel], Task[PeerIdentityResponse]) = {
    val connection = Promise[Channel]
    val syncResult = Promise[PeerIdentityResponse]

    val bootstrap = new Bootstrap()
      .group(workerGroup)
      .channel(classOf[NioSocketChannel])
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, networkSettings.connectionTimeout.toMillis.toInt: Integer)
      .option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
      .handler(
        new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            val handlers = buildHandlers(ch, syncResult)
            ch.pipeline.addLast(handlers: _*)
          }
        }
      )

    log.info(s"Connecting to '$remoteAddress'")
    val channelFuture = bootstrap.connect(remoteAddress)
    channelFuture.addListener((future: io.netty.util.concurrent.Future[Void]) => {
      if (future.isCancelled) {
        connection.failure(new RuntimeException(s"Connection to '$remoteAddress' is cancelled"))
      } else if (future.isSuccess) {
        log.info(s"Connected to '$remoteAddress'")
        connection.success(channelFuture.channel())
      } else {
        connection.failure(new RuntimeException(s"Connection exception to '$remoteAddress'", future.cause()))
      }
    })

    val channel = channelFuture.channel()
    channelGroup.add(channel)
    channel.closeFuture().addListener { chf: ChannelFuture =>
      log.info(s"Connection to '$remoteAddress' closed")
      channelGroup.remove(chf.channel())
      if (!syncResult.isCompleted) {
        syncResult.failure(new RuntimeException(s"Connection to '$remoteAddress' has been closed before receiving response"))
      }
    }

    (Task.fromFuture(connection.future), Task.fromFuture(syncResult.future))
  }

  def shutdown(): Unit =
    try {
      channelGroup.close().await()
      log.trace("Closed all channels")
    } finally {
      workerGroup.shutdownGracefully()
    }
}
