package com.wavesenterprise

import cats.Show
import cats.effect.ExitCode
import cats.implicits._
import ch.qos.logback.classic.{Level, LoggerContext}
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import com.wavesenterprise.Application.readOwnerPasswordMode
import com.wavesenterprise.account.{AddressSchemeHelper, PrivateKeyAccount}
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.database.rocksdb.{DefaultReadOnlyParams, RocksDBStorage}
import com.wavesenterprise.database.snapshot.PackedSnapshot.PackedSnapshotFile
import com.wavesenterprise.database.snapshot.{PackedSnapshot, SnapshotDataStreamHandler}
import com.wavesenterprise.network.NetworkServer.MetaMessageCodecHandlerName
import com.wavesenterprise.network.netty.handler.stream.StreamReadProgressListener
import com.wavesenterprise.network.peers._
import com.wavesenterprise.network.{
  GenesisSnapshotError,
  GenesisSnapshotRequest,
  MessageCodec,
  MetaMessageCodec,
  SnapshotNotification,
  SnapshotRequest,
  id
}
import com.wavesenterprise.settings.{CryptoSettings, NetworkSettings, WESettings}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.utils.TaskUtils._
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.wallet.Wallet.WalletWithKeystore
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import io.netty.util.concurrent.DefaultThreadFactory
import monix.eval.{Task, TaskApp}
import org.apache.commons.io.FileUtils
import org.rocksdb.RocksDBException
import org.slf4j.LoggerFactory
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigReader, ConfigSource}

import java.io.File
import java.net.InetSocketAddress
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

object SnapshotStarterApp extends TaskApp with ScorexLogging {

  override def run(args: List[String]): Task[ExitCode] = {
    for {
      config             <- loadConfig(args.headOption)
      starterAppSettings <- loadStarterAppSettings(config)
      _                  <- setAddressSchemeByte(config)
      cryptoSettings     <- loadCryptoSettings(config)
      _                  <- initCrypto(cryptoSettings)
      weSettings         <- loadWeSettings(config)
      dataDirectory     = weSettings.dataDirectory
      snapshotDirectory = weSettings.snapshotDirectory
      _ <- ensureDataEmptiness(dataDirectory)
      _ <- processSnapshot(dataDirectory, snapshotDirectory, weSettings, starterAppSettings)
    } yield ExitCode.Success
  }

  private def processSnapshot(dataDirectory: String,
                              snapshotDirectory: String,
                              weSettings: WESettings,
                              starterAppSettings: SnapshotStarterSettings): Task[Unit] = {
    (for {
      peers           <- loadPeers(weSettings)
      wallet          <- buildWallet(weSettings)
      maybePassword   <- loadPassword
      ownerPrivateKey <- loadPrivateKey(maybePassword, weSettings, wallet)
      networkClient   <- buildNetworkClient(ownerPrivateKey, weSettings)
      _               <- loadSnapshotLoop(peers, networkClient, snapshotDirectory, starterAppSettings, ownerPrivateKey, starterAppSettings.maxRetries)
      _               <- unpackSnapshot(snapshotDirectory, dataDirectory)
    } yield ()).doOnFinish {
      case Some(_) =>
        Task(log.info(s"Error occurred during snapshot processing. Cleaning up Data directory '$dataDirectory'...")) >>
          Task(FileUtils.cleanDirectory(new File(dataDirectory)))
      case _ => Task.unit
    }
  }

  case class SnapshotStarterSettings(
      loggingLevel: String,
      requestTimeout: FiniteDuration,
      peerSelectionDelay: FiniteDuration,
      peerLoopRetryDelay: FiniteDuration,
      maxRetries: Int
  )

  object SnapshotStarterSettings {
    val configPath: String = "snapshot-starter-app"

    implicit val configReader: ConfigReader[SnapshotStarterSettings] = deriveReader

    implicit val toPrintable: Show[SnapshotStarterSettings] = { s =>
      import s._
      s"""
         |requestTimeout: $requestTimeout
         |peerSelectionDelay: $peerSelectionDelay
         |peerLoopRetryDelay: $peerLoopRetryDelay
         |maxRetries: $maxRetries""".stripMargin
    }
  }

  sealed trait AppException extends NoStackTrace

  object AppException {
    case class ConfigLoadingError(cause: Throwable) extends RuntimeException("Failed to load configuration", cause) with AppException

    case class SnapshotStarterSettingsLoadingError(cause: Throwable)
        extends RuntimeException("Failed to load app configuration", cause)
        with AppException

    case class CryptoSettingsLoadingError(cause: Throwable) extends RuntimeException("Failed to load crypto settings", cause) with AppException

    case class WeSettingsLoadingError(cause: Throwable) extends RuntimeException("Failed to load WE node settings", cause) with AppException

    case class CryptoInitializationError(cause: Throwable) extends RuntimeException("Crypto initialization failed", cause) with AppException

    case class PeersLoadingError(cause: Throwable) extends RuntimeException("Failed to load peers", cause) with AppException

    case object NonEmptyStateException
        extends IllegalStateException("Data directory contains non-empty state. To download snapshot, you need to clear the data.")
        with AppException

    case class InvalidOwnerPasswordError(cause: Throwable) extends RuntimeException("Invalid node owner password", cause) with AppException

    case class PrivateKeyLoadingError(cause: Throwable) extends RuntimeException("Failed to load owner private key", cause) with AppException

    case class SnapshotUnpackingError(cause: Throwable) extends RuntimeException("Failed to unpack snapshot", cause) with AppException

    case class SnapshotLoadingError(message: String) extends RuntimeException(message) with AppException
  }

  import AppException._

  def loadConfig(maybeConfigPath: Option[String]): Task[Config] =
    Task {
      val (config, _) = readConfigOrTerminate(maybeConfigPath)
      config
    } onErrorRecoverWith {
      case ex => Task.raiseError(ConfigLoadingError(ex))
    }

  def loadStarterAppSettings(config: Config): Task[SnapshotStarterSettings] =
    Task {
      ConfigSource.fromConfig(config).at(SnapshotStarterSettings.configPath).loadOrThrow[SnapshotStarterSettings]
    } flatTap { settings =>
      Task {
        val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
        lc.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(Level.valueOf(settings.loggingLevel))
      }
    } onErrorRecoverWith {
      case ex => Task.raiseError(SnapshotStarterSettingsLoadingError(ex))
    } flatTap { settings =>
      Task(log.info(s"Snapshot starter app settings: ${show"$settings"}"))
    }

  def loadCryptoSettings(config: Config): Task[CryptoSettings] =
    Task {
      ConfigSource.fromConfig(config).at(WESettings.configPath).loadOrThrow[CryptoSettings]
    } onErrorRecoverWith {
      case ex => Task.raiseError(CryptoSettingsLoadingError(ex))
    }

  def loadWeSettings(config: Config): Task[WESettings] =
    Task {
      ConfigSource.fromConfig(config).at(WESettings.configPath).loadOrThrow[WESettings]
    } onErrorRecoverWith {
      case ex => Task.raiseError(WeSettingsLoadingError(ex))
    }

  def initCrypto(settings: CryptoSettings): Task[Unit] =
    deferCryptoEitherToTask {
      CryptoInitializer.init(settings)
    } onErrorRecoverWith {
      case ex => Task.raiseError(CryptoInitializationError(ex))
    }

  def buildWallet(weSettings: WESettings): Task[WalletWithKeystore] =
    Task(Wallet(weSettings.wallet))

  def loadPeers(weSettings: WESettings): Task[PeerDatabase] =
    Task {
      PeerDatabaseImpl(weSettings.network)
    } onErrorRecoverWith {
      case ex => Task.raiseError(PeersLoadingError(ex))
    }

  def loadPassword: Task[Option[Array[Char]]] =
    Task {
      val ownerPasswordMode = readOwnerPasswordMode()
      Application.readPassword(ownerPasswordMode)
    } onErrorRecoverWith {
      case ex => Task.raiseError(InvalidOwnerPasswordError(ex))
    }

  def setAddressSchemeByte(config: Config): Task[Unit] =
    Task(AddressSchemeHelper.setAddressSchemaByte(config))

  def loadPrivateKey(maybePassword: Option[Array[Char]], weSettings: WESettings, wallet: WalletWithKeystore): Task[PrivateKeyAccount] =
    deferEitherToTask {
      wallet.privateKeyAccount(weSettings.ownerAddress, maybePassword)
    } onErrorRecoverWith {
      case ex => Task.raiseError(PrivateKeyLoadingError(ex))
    }

  def ensureDataEmptiness(dataDirectory: String): Task[Unit] = {
    Task(RocksDBStorage.openDB(dataDirectory, migrateScheme = false, params = DefaultReadOnlyParams))
      .bracket { db =>
        Task(db.newIterator()).bracket { iterator =>
          Task {
            iterator.seekToFirst()
            iterator.isValid
          }
        } { iterator =>
          Task(iterator.close())
        }
      } { db =>
        Task(db.close())
      }
      .flatMap {
        case true  => Task.raiseError(NonEmptyStateException)
        case false => Task.unit
      }
      .onErrorRecoverWith {
        case ex: RocksDBException if ex.getStatus.getState.contains("No such file or directory") => Task.unit
      }
  }

  def buildNetworkClient(ownerKey: PrivateKeyAccount, weSettings: WESettings): Task[NetworkClient] =
    Task {
      val genesisSignature = weSettings.blockchain.custom.genesis.signature.getOrElse(ByteStr.empty)
      new NetworkClient(ownerKey, genesisSignature, weSettings.network)
    }

  def loadSnapshotLoop(peers: PeerDatabase,
                       client: NetworkClient,
                       snapshotDirectory: String,
                       settings: SnapshotStarterSettings,
                       ownerKey: PrivateKeyAccount,
                       retriesCount: Int,
                       excludePeers: Set[InetSocketAddress] = Set.empty): Task[Unit] =
    Task.defer {
      if (retriesCount > 0) {
        peers.randomPeer(excludePeers) match {
          case Some(address) =>
            connectAndLoadSnapshot(client, snapshotDirectory, settings.requestTimeout, ownerKey, address)
              .onErrorHandleWith { ex =>
                Task {
                  log.error(s"Failed to load snapshot from peer '$address', retrying with another peer after '${settings.peerSelectionDelay}'", ex)
                } *> Task
                  .defer(loadSnapshotLoop(peers, client, snapshotDirectory, settings, ownerKey, retriesCount, excludePeers + address))
                  .delayExecution(settings.peerSelectionDelay)
              }
          case None =>
            Task(log.warn(s"Peers are over, retrying loop after '${settings.peerLoopRetryDelay}'")) *>
              Task
                .defer(loadSnapshotLoop(peers, client, snapshotDirectory, settings, ownerKey, retriesCount - 1))
                .delayExecution(settings.peerLoopRetryDelay)
        }
      } else {
        Task.raiseError(SnapshotLoadingError("Unable to load snapshot from peers. Retries have been exceeded"))
      }
    }

  def connectAndLoadSnapshot(client: NetworkClient,
                             snapshotDirectory: String,
                             requestTimeout: FiniteDuration,
                             ownerKey: PrivateKeyAccount,
                             selectedAddress: InetSocketAddress): Task[Unit] =
    Task(log.info(s"Connecting with peer '$selectedAddress'")) *>
      client.connect(selectedAddress, requestTimeout) <*
      Task(log.info(s"Requesting snapshot")) >>=
      (loadSnapshot(ownerKey, requestTimeout, snapshotDirectory, _, _)).tupled

  def loadSnapshot(ownerKey: PrivateKeyAccount,
                   requestTimeout: FiniteDuration,
                   snapshotDirectory: String,
                   channel: Channel,
                   notification: SnapshotNotification): Task[Unit] =
    Task.defer {
      val snapshotDir  = Paths.get(snapshotDirectory)
      val snapshotFile = snapshotDir.resolve(PackedSnapshotFile)

      Files.createDirectories(snapshotDir)
      if (Files.notExists(snapshotFile)) Files.createFile(snapshotFile)

      Task(FileChannel.open(snapshotFile, StandardOpenOption.APPEND)).bracket { fileChannel =>
        val offset   = fileChannel.size()
        val listener = new StreamReadProgressListener(s"${id(channel)} snapshot loading", offset, Some(notification.size))
        val handler  = new SnapshotDataStreamHandler(channel, fileChannel, listener)

        channel.pipeline().addBefore(MetaMessageCodecHandlerName, SnapshotDataStreamHandler.Name, handler)
        val request = SnapshotRequest(ownerKey, offset)
        channel.writeAndFlush(request)
        log.trace(s"Sent snapshot request '$request' to channel '${id(channel)}'")

        (Task.fromFuture(handler.start).timeout(requestTimeout) *>
          Task(log.info("Started snapshot loading")) *>
          Task.fromFuture(handler.completion)).guarantee(Task(handler.dispose()))
      } { fileChannel =>
        Task(fileChannel.close())
      }
    }

  def unpackSnapshot(snapshotDirectory: String, dataDirectory: String): Task[Unit] =
    Task(log.info("Unpacking snapshot")) *>
      (PackedSnapshot.unpackSnapshot(snapshotDirectory, dataDirectory) >>= eitherToTask)
        .onErrorRecoverWith { case ex => Task.raiseError(SnapshotUnpackingError(ex)) } *>
      Task(log.info("Snapshot unpacking successfully completed"))

  class NetworkClient(
      ownerKey: PrivateKeyAccount,
      genesisSignature: ByteStr,
      val networkSettings: NetworkSettings
  ) {

    private val workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("nio-worker-group", true))

    private val messageCodecCache = CacheBuilder
      .newBuilder()
      .expireAfterWrite(networkSettings.receivedTxsCacheTimeout.length, networkSettings.receivedTxsCacheTimeout.unit)
      .build[ByteStr, Object]()

    def connect(remoteAddress: InetSocketAddress, timeout: FiniteDuration): Task[(Channel, SnapshotNotification)] = {
      val connectPromise  = Promise[Channel]
      val responsePromise = Promise[SnapshotNotification]
      val channelFuture = new Bootstrap()
        .remoteAddress(remoteAddress)
        .option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
        .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, networkSettings.connectionTimeout.toMillis.toInt: Integer)
        .group(workerGroup)
        .channel(classOf[NioSocketChannel])
        .handler {
          new ChannelInitializer[SocketChannel] {
            override def initChannel(channel: SocketChannel): Unit = {
              channel
                .pipeline()
                .addLast(
                  new LengthFieldPrepender(4),
                  new LengthFieldBasedFrameDecoder(100 * 1024 * 1024, 0, 4, 0, 4),
                  new MetaMessageCodec(messageCodecCache),
                  new MessageCodec(),
                  new InitialClientHandler(responsePromise, ownerKey, genesisSignature)
                )
            }
          }
        }
        .connect()

      log.trace(s"Connecting to '$remoteAddress' by channel '${id(channelFuture.channel())}'")

      channelFuture
        .addListener { future: io.netty.util.concurrent.Future[_] =>
          if (future.isSuccess) {
            log.info(s"Peer '$remoteAddress' connection established by channel '${id(channelFuture.channel())}'")
            connectPromise.success(channelFuture.channel())
          } else {
            connectPromise.failure(future.cause)
          }
        }

      (Task.deferFuture(connectPromise.future) -> Task.deferFuture(responsePromise.future)).tupled.timeout(timeout)
    }
  }

  class InitialClientHandler(responsePromise: Promise[SnapshotNotification], ownerKey: PrivateKeyAccount, genesisSignature: ByteStr)
      extends ChannelInboundHandlerAdapter
      with ScorexLogging {

    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      val request = createRequest()
      log.trace(s"Sending genesis snapshot request '$request' to '${id(ctx)}'")
      ctx
        .writeAndFlush(request)
        .addListener { f: io.netty.util.concurrent.Future[Void] =>
          if (f.isSuccess) {
            log.trace(s"Successfully sent genesis snapshot request '$request' to '${id(ctx)}")
          } else {
            responsePromise.failure(new RuntimeException(s"Couldn't send genesis snapshot request to '${id(ctx)}'"))
            ctx.close()
          }
        }
    }

    private def createRequest(): GenesisSnapshotRequest = {
      val reqWithoutSignature = GenesisSnapshotRequest(ownerKey, genesisSignature, 0L, ByteStr.empty)
      val signature           = crypto.sign(ownerKey, reqWithoutSignature.bytesWOSig)
      reqWithoutSignature.copy(signature = ByteStr(signature.array))
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
      msg match {
        case response: SnapshotNotification =>
          log.trace(s"Received snapshot notification response: '$response'")
          responsePromise.success(response)
        case error: GenesisSnapshotError =>
          responsePromise.failure(new RuntimeException(s"Received genesis snapshot error: '${error.message}'"))
          ctx.close()
        case _ =>
          responsePromise.failure(new RuntimeException("Unknown response. Closing channel"))
          ctx.close()
      }
    }
  }
}
