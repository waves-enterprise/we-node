package com.wavesenterprise.network.handshake

import cats.implicits.toShow
import cats.syntax.either._
import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.network.Attributes.ChannelAttrOps
import com.wavesenterprise.network.peers.{PeerConnection, PeerConnectionAcceptor, PeerInfo}
import com.wavesenterprise.network.{Attributes, closeChannel, id}
import com.wavesenterprise.privacy.InitialParticipantsDiscoverResult
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.{Validation, WrongHandshake}
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.{ApplicationInfo, NodeVersion, crypto}
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.socket.SocketChannel

import scala.Ordering.Implicits.infixOrderingOps
import scala.concurrent.Promise

abstract class HandshakeHandler(
    blockchain: Blockchain,
    applicationInstanceInfo: ApplicationInfo,
    ownerKey: PrivateKeyAccount,
    connectPromise: Promise[PeerConnection],
    initialParticipantsDiscoverResult: InitialParticipantsDiscoverResult,
    connectionAcceptor: PeerConnectionAcceptor
) extends ChannelInboundHandlerAdapter
    with ScorexLogging {

  import HandshakeHandler._

  private lazy val sessionKey = PrivateKeyAccount(crypto.generateSessionKeyPair())

  private def checkHandshake(signedHandshake: SignedHandshake): Validation[Unit] = {
    val pubKeyFromInitSync   = initialParticipantsDiscoverResult.participantPubKey(signedHandshake.nodeOwnerAddress)
    val pubKeyFromBlockchain = blockchain.participantPubKey(signedHandshake.nodeOwnerAddress)

    checkHandshake(signedHandshake, pubKeyFromInitSync, pubKeyFromBlockchain)
  }

  protected def checkHandshake(signedHandshake: SignedHandshake,
                               pubKeyFromInitSync: Option[PublicKeyAccount],
                               pubKeyFromBlockchain: Option[PublicKeyAccount]): Either[ValidationError, Unit] = {
    checkPubKey(signedHandshake, pubKeyFromBlockchain)
      .leftFlatMap { _ =>
        checkPubKey(signedHandshake, pubKeyFromInitSync)
      }

  }

  private def checkPeerVersion(ctx: ChannelHandlerContext, remoteHandshake: SignedHandshake): Either[String, Unit] = {
    if (remoteHandshake.nodeVersion < NodeVersion.MinInventoryBasedPrivacyProtocolSupport) {
      log.warn(
        s"Private data exchange protocol is incompatible with peer '${id(ctx)}' (${remoteHandshake.nodeOwnerAddress}), " +
          s"peer's node version '${remoteHandshake.nodeVersion}' < ${NodeVersion.MinInventoryBasedPrivacyProtocolSupport.asFlatString}")
    }

    Right(())
  }

  private def checkHandshakeInfo(remoteHandshake: SignedHandshake): Either[String, Unit] = {
    if (remoteHandshake.nodeOwnerAddress.stringRepr == ownerKey.toAddress.stringRepr) {
      Left(s"Shouldn't connect to node with same owner address '${ownerKey.toAddress.stringRepr}'")
    } else if (remoteHandshake.chainId != applicationInstanceInfo.chainId) {
      Left(s"Remote chain id '${remoteHandshake.chainId}' does not match local '${applicationInstanceInfo.chainId}'")
    } else if (!versionIsSupported(remoteHandshake.nodeVersion)) {
      Left(s"Remote node version '${remoteHandshake.nodeVersion.show}' is not supported")
    } else if (remoteHandshake.consensusType.toUpperCase != applicationInstanceInfo.consensusType.toUpperCase) {
      Left(s"Remote application consensus type '${remoteHandshake.consensusType}' doesn't match local '${applicationInstanceInfo.consensusType}'")
    } else {
      Right(())
    }
  }

  private def checkPubKey(signedHandshake: SignedHandshake, pubKeyOpt: Option[PublicKeyAccount]): Validation[Unit] = {
    for {
      senderPubKey <- pubKeyOpt.toRight(
        WrongHandshake(s"Wrong remote handshake! Sender address '${signedHandshake.nodeOwnerAddress}', is not permitted to join the cluster."))
      _ <- Either.cond(signedHandshake.isSignatureValid(senderPubKey), (), WrongHandshake("Wrong remote handshake! Invalid signature."))
    } yield ()
  }

  private def isAddressResolvable(channel: Channel): Boolean = channel match {
    case sc: SocketChannel   => Option(sc.remoteAddress()).flatMap(a => Option(a.getAddress)).isDefined
    case ec: EmbeddedChannel => Option(ec.remoteAddress()).isDefined
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
    case HandshakeTimeoutExpired =>
      handshakeFailed(ctx, "Timeout expired while waiting for handshake")
    case remoteHandshake: SignedHandshake =>
      handleHandshakeMessage(ctx, remoteHandshake)
    case _ => super.channelRead(ctx, msg)
  }

  private def handleHandshakeMessage(ctx: ChannelHandlerContext, remoteHandshake: SignedHandshake): Unit = {
    validateHandshake(ctx, remoteHandshake)
      .leftMap { handshakeError =>
        handshakeFailed(ctx, handshakeError)
      }
      .flatMap { _ =>
        acceptConnection(ctx, remoteHandshake)
      }
      .foreach { _ =>
        handshakeAccepted(ctx, remoteHandshake)
      }
  }

  private def validateHandshake(ctx: ChannelHandlerContext, remoteHandshake: SignedHandshake): Either[String, Unit] = {
    for {
      _ <- checkHandshakeInfo(remoteHandshake)
      _ <- Either.cond(isAddressResolvable(ctx.channel()), (), s"Unknown remote address for channel '${id(ctx)}'")
      _ <- checkHandshake(remoteHandshake)
        .leftMap {
          case error: WrongHandshake => error.err
          case _                     => "Unknown handshake validation error"
        }
      _ <- checkPeerVersion(ctx, remoteHandshake)
    } yield ()
  }

  private def handshakeFailed(ctx: ChannelHandlerContext, reason: String): Unit = {
    connectPromise.failure(new HandshakeException(reason))
    closeChannel(ctx.channel, reason)
  }

  private def acceptConnection(ctx: ChannelHandlerContext, remoteHandshake: SignedHandshake): Either[Unit, PeerConnection] = {
    val peerInfo       = PeerInfo.fromHandshake(remoteHandshake, ctx.channel.remoteAddress)
    val peerConnection = connectionAcceptor.newConnection(ctx.channel(), peerInfo, sessionKey)

    val result = connectionAcceptor.accept(peerConnection)
    result.foreach(connectPromise.success)

    result.leftMap { closeReason =>
      closeChannel(ctx.channel(), closeReason)
      connectPromise.failure(new IllegalStateException(closeReason))
    }
  }

  private def handshakeAccepted(ctx: ChannelHandlerContext, remoteHandshake: SignedHandshake): Unit = {
    log.info(s"Accepted handshake '$remoteHandshake' on channel '${id(ctx)}'")
    removeHandshakeHandlers(ctx, this)
    setChannelAttributes(ctx, remoteHandshake)
    onAccepted(ctx)
    super.channelRead(ctx, remoteHandshake)
  }

  protected def onAccepted(ctx: ChannelHandlerContext): Unit = ()

  private def setChannelAttributes(ctx: ChannelHandlerContext, remoteHandshake: SignedHandshake): Unit = {
    // set node name attribute
    ctx.channel.attr(Attributes.NodeNameAttributeKey).set(remoteHandshake.nodeName)

    remoteHandshake.nodeVersion.features.flatMap(_.triggerAttribute).foreach { featureAttribute =>
      ctx.channel().setAttrWithLogging(featureAttribute, ())
    }
  }

  /**
    * That's where we create SignedHandshake, which is unique for every session
    */
  protected def sendLocalHandshake(ctx: ChannelHandlerContext): Unit = {
    val signedHandshake = SignedHandshakeV3.createAndSign(applicationInstanceInfo, PublicKeyAccount(sessionKey.publicKey), ownerKey)
    log.debug(s"Sending a handshake '$signedHandshake' to the channel '${id(ctx)}'")
    ctx.writeAndFlush(signedHandshake.encode(ctx.alloc.buffer))
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, throwable: Throwable) {
    log.debug("Handshake error:", throwable)
    connectPromise.failure(throwable)
    ctx.close()
  }
}

object HandshakeHandler extends ScorexLogging {

  val minSupportedVersion: NodeVersion = NodeVersion(1, 5, 0)

  //TODO: will be fixed in future
  def versionIsSupported(remoteVersion: NodeVersion): Boolean =
    remoteVersion >= minSupportedVersion

  def removeHandshakeHandlers(ctx: ChannelHandlerContext, thisHandler: ChannelHandler): Unit = {
    ctx.pipeline.remove(classOf[HandshakeTimeoutHandler])
    ctx.pipeline.remove(thisHandler)
  }

  class Server(blockchain: Blockchain,
               applicationInstanceInfo: ApplicationInfo,
               ownerKey: PrivateKeyAccount,
               connectPromise: Promise[PeerConnection],
               initialParticipantsDiscoverResult: InitialParticipantsDiscoverResult,
               connectionAcceptor: PeerConnectionAcceptor)
      extends HandshakeHandler(
        blockchain,
        applicationInstanceInfo,
        ownerKey,
        connectPromise,
        initialParticipantsDiscoverResult,
        connectionAcceptor
      ) {
    override protected def onAccepted(ctx: ChannelHandlerContext): Unit = {
      sendLocalHandshake(ctx)
      super.onAccepted(ctx)
    }
  }

  class Client(blockchain: Blockchain,
               applicationInstanceInfo: ApplicationInfo,
               ownerKey: PrivateKeyAccount,
               connectPromise: Promise[PeerConnection],
               initialParticipantsDiscoverResult: InitialParticipantsDiscoverResult,
               connectionAcceptor: PeerConnectionAcceptor)
      extends HandshakeHandler(
        blockchain,
        applicationInstanceInfo,
        ownerKey,
        connectPromise,
        initialParticipantsDiscoverResult,
        connectionAcceptor
      ) {
    override protected def channelActive(ctx: ChannelHandlerContext): Unit = {
      sendLocalHandshake(ctx)
      super.channelActive(ctx)
    }
  }

}

class HandshakeException(message: String) extends RuntimeException(message)
