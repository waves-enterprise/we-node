package com.wavesenterprise

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.{Http, ServerBuilder}
import akka.stream.Materializer
import cats.implicits.showInterpolator
import ch.qos.logback.classic.{Level, LoggerContext}
import com.typesafe.config._
import com.wavesenterprise.Application.readPassword
import com.wavesenterprise.OwnerPasswordMode.{EmptyPassword, PasswordSpecified, PromptForPassword}
import com.wavesenterprise.account.{Address, AddressSchemeHelper, PrivateKeyAccount}
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.actor.RootActorSystem
import com.wavesenterprise.anchoring.{Anchoring, EnabledAnchoring}
import com.wavesenterprise.api.grpc.CompositeGrpcService
import com.wavesenterprise.api.grpc.service._
import com.wavesenterprise.api.http.acl.PermissionApiRoute
import com.wavesenterprise.api.http.alias.AliasApiRoute
import com.wavesenterprise.api.http.assets.AssetsApiRoute
import com.wavesenterprise.api.http.consensus.ConsensusApiRoute
import com.wavesenterprise.api.http.docker.ContractsApiRoute
import com.wavesenterprise.api.http.leasing.LeaseApiRoute
import com.wavesenterprise.api.http.service._
import com.wavesenterprise.api.http.{CryptoApiRoute, _}
import com.wavesenterprise.consensus.{BlockVotesHandler, Consensus}
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.database.rocksdb.RocksDBStorage
import com.wavesenterprise.database.snapshot.SnapshotComponents
import com.wavesenterprise.docker.validator.ContractValidatorResultsHandler
import com.wavesenterprise.docker.{ContractExecutionComponents, ContractExecutionMessagesCache}
import com.wavesenterprise.features.api.ActivationApiRoute
import com.wavesenterprise.history.BlockchainFactory
import com.wavesenterprise.http.{DebugApiRoute, HealthChecker, NodeApiRoute}
import com.wavesenterprise.metrics.Metrics
import com.wavesenterprise.metrics.Metrics.MetricsSettings
import com.wavesenterprise.mining.{Miner, TransactionsAccumulatorProvider}
import com.wavesenterprise.network._
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerConnectionAcceptor, PeerDatabaseImpl}
import com.wavesenterprise.privacy._
import com.wavesenterprise.protobuf.service.address.AddressPublicServicePowerApiHandler
import com.wavesenterprise.protobuf.service.alias.AliasPublicServicePowerApiHandler
import com.wavesenterprise.protobuf.service.messagebroker.BlockchainEventsServicePowerApiHandler
import com.wavesenterprise.protobuf.service.privacy.{PrivacyEventsServicePowerApiHandler, PrivacyPublicServicePowerApiHandler}
import com.wavesenterprise.protobuf.service.transaction.TransactionPublicServicePowerApiHandler
import com.wavesenterprise.protobuf.service.util.{
  ContractStatusServicePowerApiHandler,
  NodeInfoServicePowerApiHandler,
  UtilPublicServicePowerApiHandler
}
import com.wavesenterprise.settings.privacy.PrivacyStorageVendor
import com.wavesenterprise.settings.{NodeMode, _}
import com.wavesenterprise.state.SignatureValidator
import com.wavesenterprise.state.appender.{BaseAppender, BlockAppender, MicroBlockAppender}
import com.wavesenterprise.transaction.validation.FeeCalculator
import com.wavesenterprise.utils.NTPUtils.NTPExt
import com.wavesenterprise.utils.{NTP, ScorexLogging, SystemInformationReporter, forceStopApplication}
import com.wavesenterprise.utx.{UtxPool, UtxPoolImpl}
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.wallet.Wallet.WalletWithKeystore
import kamon.Kamon
import kamon.influxdb.InfluxDBReporter
import kamon.system.SystemMetrics
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler._
import monix.execution.schedulers.{CanBlock, SchedulerService}
import monix.execution.{Scheduler, UncaughtExceptionReporter}
import org.slf4j.LoggerFactory
import org.slf4j.bridge.SLF4JBridgeHandler
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}

import java.io.File
import java.security.Security
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ShutdownMode extends Enumeration {
  type Mode = Value
  val FULL_SHUTDOWN, STOP_NODE_BUT_LEAVE_API = Value
}

class Application(val ownerPasswordMode: OwnerPasswordMode,
                  val actorSystem: ActorSystem,
                  val settings: WESettings,
                  val metricsSettings: MetricsSettings,
                  config: Config,
                  time: NTP,
                  schedulers: AppSchedulers)
    extends ScorexLogging {

  private val storage = RocksDBStorage.openDB(settings.dataDirectory)

  private val configRoot: ConfigObject = config.root()
  private val ScoreThrottleDuration    = 1.second

  private val (bcState, blockchainUpdater) = BlockchainFactory(settings, storage, time, schedulers)

  private val permissionValidator: PermissionValidator = blockchainUpdater.permissionValidator

  private lazy val enableHttp2: Config = ConfigFactory.parseString("akka.http.server.preview.enable-http2 = on")

  private lazy val upnp = new UPnP(settings.network.upnp) // don't initialize unless enabled

  alarmAboutFileExistence(settings.network.file)
  alarmAboutFileExistence(settings.wallet.file)

  private val wallet: WalletWithKeystore = try {
    Wallet(settings.wallet).validated()
  } catch {
    case e: IllegalArgumentException =>
      log.error(s"Illegal wallet state '${settings.wallet.file.get.getAbsolutePath}'")
      throw e
    case e: IllegalStateException =>
      log.error(s"Failed to open wallet file '${settings.wallet.file.get.getAbsolutePath}")
      throw e
  }

  private val peerDatabase = PeerDatabaseImpl(settings.network)

  private var maybeBlockLoader: Option[BlockLoader]                                 = None
  private var maybeMiner: Option[Miner]                                             = None
  private var maybeMicroBlockLoader: Option[MicroBlockLoader]                       = None
  private var maybeBlockAppender: Option[BlockAppender]                             = None
  private var maybeMicroBlockAppender: Option[MicroBlockAppender]                   = None
  private var maybeUtx: Option[UtxPool]                                             = None
  private var maybeNetwork: Option[P2PNetwork]                                      = None
  private var maybeAnchoring: Option[Anchoring]                                     = None
  private var maybeNodeAttributesHandler: Option[NodeAttributesHandler]             = None
  private var maybePrivacyComponents: Option[PrivacyComponents]                     = None
  private var maybeContractExecutionComponents: Option[ContractExecutionComponents] = None
  private var maybeUtxPoolSynchronizer: Option[UtxPoolSynchronizer]                 = None
  private var maybeTxBroadcaster: Option[TxBroadcaster]                             = None
  private var maybeGrpcActorSystem: Option[ActorSystem]                             = None
  private var maybeSnapshotComponents: Option[SnapshotComponents]                   = None
  private var maybeHealthChecker: Option[HealthChecker]                             = None

  def shutdown(shutdownMode: ShutdownMode.Mode = ShutdownMode.FULL_SHUTDOWN): Unit = {
    internalShutdown(shutdownMode)
  }

  /**
    * Node owner key is necessary for the node to function
    * It is used for Privacy, if it is enabled on the network, to sign SignedHandshakes to pass blockchain auth on other nodes
    * It is also used as the only key for Miner
    */
  private val ownerKey: PrivateKeyAccount = {
    wallet.containsAlias(settings.ownerAddress.toString) match {
      case Left(error) =>
        log.error(s"Failed while attempting to access node keystore: ${error.message}")
        forceStopApplication()
      case Right(aliasExist) =>
        if (!aliasExist) {
          log.error(s"Node owner address '${settings.ownerAddress}' does not exist in the keystore")
          forceStopApplication()
        } else log.trace("Node owner address is present in the keystore")
    }

    val passwordOpt = readPassword(ownerPasswordMode)

    wallet.privateKeyAccount(settings.ownerAddress, passwordOpt) match {
      case Right(pka) => pka
      case Left(ex) =>
        log.error(s"Failed to get privateKey from wallet for owner address ${settings.ownerAddress}: $ex")
        forceStopApplication()
        throw new IllegalArgumentException(s"Unable to get owner private key from wallet: $ex")
    }
  }

  private val nodeOwnerAddress: Address = ownerKey.toAddress

  //noinspection ScalaStyle
  def run(): Unit = {

    if (settings.license.file.exists())
      log.warn(
        s"License file '${settings.license.file}' is found, however it is not mandatory anymore " +
          s"since you're running an opensource version of WE Node")

    checkGenesis(settings, blockchainUpdater)

    val initialSyncTimeout = 2.minutes
    val initialSyncScheduler: SchedulerService = {
      val currentKnownPeersCount                         = peerDatabase.knownPeers.size
      val initialSyncReporter: UncaughtExceptionReporter = log.error("Error in InitialSync", _)
      val poolSize                                       = Math.max(1, currentKnownPeersCount)
      fixedPool("initialSyncPool", poolSize = poolSize, reporter = initialSyncReporter)
    }

    val networkInitialSync = new NetworkInitialSync(ownerKey, settings, blockchainUpdater, peerDatabase)

    val initialSyncResult: InitialParticipantsDiscoverResult = networkInitialSync
      .initialSyncPerform()
      .onErrorRecoverWith {
        case NonFatal(ex) =>
          log.error(s"Initial sync failed, shutting down", ex)
          Task.now(forceStopApplication()).map(_ => InitialParticipantsDiscoverResult.NotNeeded)
      }
      .doOnFinish { _ =>
        Task.apply {
          networkInitialSync.shutdown()
        }
      }
      .runSyncUnsafe(initialSyncTimeout)(initialSyncScheduler, CanBlock.permit)

    AppSchedulers.shutdownAndWait(initialSyncScheduler, "InitialSyncScheduler")

    val activePeerConnections = new ActivePeerConnections()
    val utxStorage =
      new UtxPoolImpl(
        time,
        blockchainUpdater,
        settings.blockchain,
        settings.utx,
        permissionValidator,
        schedulers.utxPoolSyncScheduler,
        settings.consensualSnapshot
      )(schedulers.utxPoolBackgroundScheduler)

    maybeUtx = Some(utxStorage)

    val txBroadcaster = TxBroadcaster(
      settings = settings,
      utx = utxStorage,
      activePeerConnections = activePeerConnections,
      maxSimultaneousConnections = settings.network.maxSimultaneousConnections
    )(schedulers.transactionBroadcastScheduler)

    maybeTxBroadcaster = Some(txBroadcaster)

    val knownInvalidBlocks = new InvalidBlockStorageImpl(settings.synchronization.invalidBlocksStorage)

    val consensusSettings = settings.blockchain.consensus
    val consensus         = Consensus(settings.blockchain, blockchainUpdater)

    val dockerMiningEnabled = settings.miner.enable && settings.dockerEngine.enable

    val contractExecutionMessagesCache = new ContractExecutionMessagesCache(
      settings.dockerEngine.contractExecutionMessagesCache,
      dockerMiningEnabled,
      activePeerConnections,
      utxStorage,
      schedulers.contractExecutionMessagesScheduler
    )

    val microBlockLoaderStorage = new MicroBlockLoaderStorage(settings.synchronization.microBlockSynchronizer)

    val historyReplier = new HistoryReplier(blockchainUpdater, microBlockLoaderStorage, settings.synchronization)(schedulers.historyRepliesScheduler)

    val peersIdentityService = new PeersIdentityService(ownerKey.toAddress, blockchainUpdater)

    val connectionAcceptor =
      new PeerConnectionAcceptor(activePeerConnections, settings.network.maxSimultaneousConnections, blockchainUpdater, time, dockerMiningEnabled)
    val networkServer = NetworkServer(
      networkSettings = settings.network,
      applicationInfo = settings.applicationInfo(),
      lastBlockInfos = blockchainUpdater.lastBlockInfo,
      blockchain = blockchainUpdater,
      historyReplier = historyReplier,
      peerDatabase = peerDatabase,
      peersIdentityService = peersIdentityService,
      ownerKey = ownerKey,
      schedulers = schedulers,
      initialParticipantsDiscoverResult = initialSyncResult,
      connectionAcceptor = connectionAcceptor
    )

    val p2PNetwork = P2PNetwork(settings.network, networkServer, peerDatabase)
    p2PNetwork.start()
    maybeNetwork = Some(p2PNetwork)

    val incomingMessages = networkServer.messages

    val nodeAttributesHandler = new NodeAttributesHandler(
      incomingMessages = incomingMessages.nodeAttributes,
      activePeersConnections = activePeerConnections
    )(schedulers.channelProcessingScheduler)

    maybeNodeAttributesHandler = Some(nodeAttributesHandler)
    nodeAttributesHandler.run()

    implicit val actorSys: ActorSystem = actorSystem

    val privacyComponents = PrivacyComponents(
      settings = settings,
      state = blockchainUpdater,
      incomingMessages = incomingMessages,
      activePeerConnections = activePeerConnections,
      scheduler = schedulers.policyScheduler,
      time = time,
      owner = ownerKey,
      shutdownFunction = () => shutdown()
    )(actorSys)

    maybePrivacyComponents = Some(privacyComponents)
    privacyComponents.run()

    val privacyEnabled = settings.privacy.storage.vendor != PrivacyStorageVendor.Unknown

    val feeCalculator =
      FeeCalculator(blockchainUpdater, settings.blockchain.custom.functionality, settings.blockchain.fees)
    val privacyApiService =
      new PrivacyApiService(
        state = blockchainUpdater,
        wallet = wallet,
        owner = ownerKey,
        storage = privacyComponents.storage,
        policyDataSynchronizer = privacyComponents.synchronizer,
        feeCalculator = feeCalculator,
        time = time,
        txBroadcaster = txBroadcaster
      )(schedulers.policyScheduler)

    val transactionsAccumulatorProvider =
      new TransactionsAccumulatorProvider(blockchainUpdater, bcState, settings, permissionValidator, time, ownerKey)

    var contractsApiService: ContractsApiService = null
    if (settings.api.rest.enable || settings.api.grpc.enable) {
      contractsApiService = new ContractsApiService(blockchainUpdater, contractExecutionMessagesCache)
    }

    lazy val addressApiService = new AddressApiService(blockchainUpdater, wallet)

    if (settings.api.grpc.enable || dockerMiningEnabled) {
      val grpcActorSystem: ActorSystem = ActorSystem("gRPC", enableHttp2.withFallback(settings.api.grpc.akkaHttpSettings))

      maybeGrpcActorSystem = Some(grpcActorSystem)

      maybeContractExecutionComponents = {
        if (dockerMiningEnabled) {
          Some(
            ContractExecutionComponents(
              settings = settings,
              schedulers = schedulers,
              contractExecutionMessagesCache = contractExecutionMessagesCache,
              blockchainUpdater = blockchainUpdater,
              nodeOwnerAccount = ownerKey,
              utx = utxStorage,
              permissionValidator = permissionValidator,
              time = time,
              wallet = wallet,
              privacyApiService = privacyApiService,
              activePeerConnections = activePeerConnections,
              scheduler = schedulers.apiComputationsScheduler
            )(grpcActorSystem))
        } else None
      }

      def dockerPartialHandlers: Seq[PartialFunction[HttpRequest, Future[HttpResponse]]] =
        maybeContractExecutionComponents.map(_.partialHandlers).getOrElse(Seq.empty)

      def privacyPartialHandlers: Seq[PartialFunction[HttpRequest, Future[HttpResponse]]] =
        if (settings.api.grpc.enable && privacyEnabled) {
          Seq(
            PrivacyEventsServicePowerApiHandler.partial(
              new PrivacyEventsServiceImpl(
                settings.api.auth,
                nodeOwnerAddress,
                time,
                privacyEnabled,
                settings.api.grpc.services.privacyEvents,
                privacyComponents.storage,
                bcState,
                blockchainUpdater,
                schedulers.apiComputationsScheduler
              ))(system = grpcActorSystem),
            PrivacyPublicServicePowerApiHandler.partial(
              new PrivacyServiceImpl(
                privacyApiService,
                privacyEnabled,
                settings.api.auth,
                settings.privacy.service,
                nodeOwnerAddress,
                time,
                settings.network.mode
              )(Scheduler(grpcActorSystem.getDispatcher), Materializer.matFromSystem(grpcActorSystem)))(system = grpcActorSystem)
          )
        } else Nil

      def publicServicesPartialHandlers: Seq[PartialFunction[HttpRequest, Future[HttpResponse]]] =
        if (settings.api.grpc.enable) {
          val grpcScheduler = Scheduler(grpcActorSystem.getDispatcher)
          Seq(
            BlockchainEventsServicePowerApiHandler.partial(
              new BlockchainEventsServiceImpl(
                settings.api.auth,
                nodeOwnerAddress,
                time,
                settings.api.grpc.services.blockchainEvents,
                settings.miner.maxBlockSizeInBytes,
                blockchainUpdater,
                bcState,
                schedulers.apiComputationsScheduler
              ))(system = grpcActorSystem),
            NodeInfoServicePowerApiHandler
              .partial(new NodeInfoServiceImpl(time, settings, blockchainUpdater, ownerKey)(grpcScheduler))(system = grpcActorSystem),
            ContractStatusServicePowerApiHandler.partial(
              new ContractStatusServiceImpl(settings.api.grpc.services.contractStatusEvents,
                                            contractsApiService,
                                            settings.api.auth,
                                            nodeOwnerAddress,
                                            time,
                                            schedulers.apiComputationsScheduler))(system = grpcActorSystem),
            TransactionPublicServicePowerApiHandler.partial(
              new TransactionServiceImpl(blockchainUpdater,
                                         feeCalculator,
                                         txBroadcaster,
                                         privacyComponents.storage,
                                         settings.api.auth,
                                         nodeOwnerAddress,
                                         time,
                                         settings.network.mode)(grpcScheduler))(system = grpcActorSystem),
            AddressPublicServicePowerApiHandler.partial(
              new AddressServiceImpl(settings.api.auth, nodeOwnerAddress, time, addressApiService, blockchainUpdater)(grpcScheduler)
            )(system = grpcActorSystem),
            AliasPublicServicePowerApiHandler.partial(
              new AliasServiceImpl(settings.api.auth, nodeOwnerAddress, time, blockchainUpdater)(grpcScheduler)
            )(system = grpcActorSystem),
            UtilPublicServicePowerApiHandler.partial(
              new com.wavesenterprise.api.grpc.service.UtilServiceImpl(settings.api.auth, nodeOwnerAddress, time)(grpcScheduler)
            )(system = grpcActorSystem)
          )
        } else Nil

      val compositePartialHandlers = dockerPartialHandlers ++ privacyPartialHandlers ++ publicServicesPartialHandlers

      val serviceHandler: HttpRequest => Future[HttpResponse] =
        CompositeGrpcService(metricsSettings.httpRequestsCache, compositePartialHandlers: _*)(grpcActorSystem.getDispatcher).enrichedCompositeHandler
      val grpcBindingFuture =
        getServerBuilder(settings.api.grpc.bindAddress, settings.api.grpc.port)(grpcActorSystem)
          .bind(serviceHandler)

      grpcServerBinding = Await.result(grpcBindingFuture, 20.seconds)
      log.info(s"gRPC server bound to: ${grpcServerBinding.localAddress}")
    }

    val signatureValidator = new SignatureValidator()(schedulers.signaturesValidationScheduler)

    val microBlockLoader = new MicroBlockLoader(
      ng = blockchainUpdater,
      settings = settings.synchronization.microBlockSynchronizer,
      incomingMicroBlockInventoryEvents = incomingMessages.microblockInvs,
      incomingMicroBlockEvents = incomingMessages.microblockResponses,
      signatureValidator = signatureValidator,
      activePeerConnections = activePeerConnections,
      storage = microBlockLoaderStorage
    )(schedulers.microBlockLoaderScheduler)

    maybeMicroBlockLoader = Some(microBlockLoader)

    val baseAppender = new BaseAppender(
      blockchainUpdater = blockchainUpdater,
      utxStorage = utxStorage,
      consensus = consensus,
      time = time,
      microBlockLoader = microBlockLoader,
      keyBlockAppendingSettings = settings.synchronization.keyBlockAppending
    )(schedulers.appenderScheduler)

    val executableTransactionsValidatorOpt = maybeContractExecutionComponents.map(_.createTransactionValidator(transactionsAccumulatorProvider))

    val microBlockAppender = new MicroBlockAppender(
      blockchainUpdater = blockchainUpdater,
      utxStorage = utxStorage,
      signatureValidator = signatureValidator,
      consensus = consensus,
      microBlockLoader = microBlockLoader,
      privacyMicroBlockHandler = privacyComponents.microBlockHandler,
      executableTransactionsValidatorOpt = executableTransactionsValidatorOpt
    )(schedulers.appenderScheduler)

    maybeMicroBlockAppender = Some(microBlockAppender)

    contractExecutionMessagesCache.subscribe(incomingMessages.contractsExecutions)

    val contractValidatorResultsHandler =
      new ContractValidatorResultsHandler(
        activePeerConnections,
        utxStorage,
        maybeContractExecutionComponents.map(_.contractValidatorResultsStore)
      )(schedulers.validatorResultsHandlerScheduler)
    contractValidatorResultsHandler.subscribe(incomingMessages.contractValidatorResults)

    val lastBlockScoreEvents = blockchainUpdater.lastBlockInfo.map(_.score)

    val scoreObserver = new ScoringSyncChannelSelector(
      scoreTtl = settings.synchronization.scoreTtl,
      scoreThrottleDuration = ScoreThrottleDuration,
      initLocalScore = blockchainUpdater.score,
      lastBlockScoreEvents = lastBlockScoreEvents,
      remoteScoreEvents = incomingMessages.blockchainScores,
      channelCloseEvents = networkServer.closedChannels,
      activePeerConnections = activePeerConnections
    )(schedulers.syncChannelSelectorScheduler)

    val lastBlockIdsReporter = Coeval(blockchainUpdater.lastBlockIds(settings.synchronization.maxRollback))

    val blockLoader = new BlockLoader(
      syncTimeOut = settings.synchronization.synchronizationTimeout,
      extensionBatchSize = settings.synchronization.extensionBatchSize,
      lastBlockIdsReporter = lastBlockIdsReporter,
      invalidBlocks = knownInvalidBlocks,
      incomingBlockEvents = incomingMessages.blocks,
      incomingSignatureEvents = incomingMessages.signatures,
      incomingMissingBlockEvents = incomingMessages.missingBlocks,
      syncChannelUpdateEvents = scoreObserver.syncChannelUpdateEvents,
      channelCloseEvents = networkServer.closedChannels
    )(schedulers.blockLoaderScheduler)

    maybeBlockLoader = Some(blockLoader)

    lazy val votesHandler = {
      settings.blockchain.consensus match {
        case cftSettings: ConsensusSettings.CftSettings =>
          new BlockVotesHandler(cftSettings, activePeerConnections, networkServer.messages.blockVotes)(schedulers.blockVotesHandlerScheduler)
        case _ =>
          forceStopApplication()
          throw new IllegalArgumentException("Expected CFT settings for creating block votes handler")
      }
    }

    val miner = {
      if (settings.miner.enable && settings.network.mode == NodeMode.Default)
        Miner(
          appender = baseAppender,
          microBlockAppender = microBlockAppender,
          activePeerConnections = activePeerConnections,
          transactionsAccumulatorProvider = transactionsAccumulatorProvider,
          blockchainUpdater = blockchainUpdater,
          settings = settings,
          timeService = time,
          utx = utxStorage,
          ownerKey = ownerKey,
          consensus = consensus,
          contractExecutionComponentsOpt = maybeContractExecutionComponents,
          executableTransactionsValidatorOpt = executableTransactionsValidatorOpt,
          loaderStateReporter = blockLoader.stateReporter.map(_.loaderState),
          policyDataSynchronizer = privacyComponents.synchronizer,
          votesHandler = votesHandler
        )(schedulers.minerScheduler)
      else
        Miner.Disabled
    }

    maybeMiner = Some(miner)

    val blockAppender = new BlockAppender(
      baseAppender = baseAppender,
      blockchainUpdater = blockchainUpdater,
      invalidBlocks = knownInvalidBlocks,
      miner = miner,
      consensus = consensus,
      signatureValidator = signatureValidator,
      blockLoader = blockLoader,
      permissionValidator = permissionValidator,
      activePeerConnections = activePeerConnections,
      executableTransactionsValidatorOpt = executableTransactionsValidatorOpt,
      contractValidatorResultsStoreOpt = maybeContractExecutionComponents.map(_.contractValidatorResultsStore)
    )(schedulers.appenderScheduler)

    maybeBlockAppender = Some(blockAppender)

    if (settings.network.mode == NodeMode.Default) {
      val utxPoolSynchronizer = new UtxPoolSynchronizer(
        settings = settings.synchronization.utxSynchronizer,
        incomingTransactions = incomingMessages.transactions,
        txBroadcaster = txBroadcaster
      )(schedulers.utxPoolSyncScheduler)

      maybeUtxPoolSynchronizer = Some(utxPoolSynchronizer)
    }

    miner.scheduleNextMining()

    maybeSnapshotComponents = SnapshotComponents(
      settings = settings,
      nodeOwner = ownerKey,
      connections = activePeerConnections,
      notifications = incomingMessages.snapshotNotifications,
      requests = incomingMessages.snapshotRequests,
      genesisRequests = incomingMessages.genesisSnapshotRequests,
      blockchain = blockchainUpdater,
      peersIdentityService = peersIdentityService,
      state = bcState,
      time = time,
      freezeApp = () => shutdown(ShutdownMode.STOP_NODE_BUT_LEAVE_API)
    )(schedulers.consensualSnapshotScheduler)
    val snapshotApiRoutes = maybeSnapshotComponents.map(_.routes).getOrElse(Seq.empty)

    for (addr <- settings.network.maybeDeclaredSocketAddress if settings.network.upnp.enable) {
      upnp.addPort(addr.getPort)
    }

    val anchoringCreationResult =
      Anchoring.createAnchoring(ownerKey, settings.anchoring, blockchainUpdater, time, txBroadcaster)(actorSys, schedulers.anchoringScheduler)
    anchoringCreationResult match {
      case Left(ex) =>
        log.error("Failed to start anchoring", ex)
        shutdown()
      case Right(anchoring) =>
        maybeAnchoring = Some(anchoring)
        maybeAnchoring.foreach(_.run())
    }

    val initRestAPI                    = settings.api.rest.enable || maybeContractExecutionComponents.isDefined
    val dockerApiRoutes: Seq[ApiRoute] = maybeContractExecutionComponents.map(_.contractsRoutes).getOrElse(Seq.empty[ApiRoute])
    val restAPIRoutes: Seq[ApiRoute] = {
      if (settings.api.rest.enable) {
        val peersApiService =
          new PeersApiService(
            blockchainUpdater,
            peerDatabase,
            p2PNetwork.connect,
            activePeerConnections,
            time
          )
        val cryptoService             = new CryptoApiService(wallet, schedulers.cryptoServiceScheduler)
        val anchoringApiService       = new AnchoringApiService(anchoringCreationResult.right.get.configuration)
        val permissionApiService      = new PermissionApiService(blockchainUpdater)
        val maybeAnchoringAuthService = maybeAnchoring.collect { case enabled: EnabledAnchoring => enabled.tokenProvider }
        val roundDuration = consensusSettings match {
          case ConsensusSettings.PoSSettings      => settings.blockchain.custom.genesis.toPlainSettingsUnsafe.averageBlockDelay
          case poa: ConsensusSettings.PoASettings => poa.roundDuration
          case cft: ConsensusSettings.CftSettings => cft.roundDuration
        }
        val healthChecker = HealthChecker(
          settings.healthCheck,
          blockchainUpdater,
          roundDuration,
          settings.miner.maxBlockSizeInBytes,
          storage,
          settings.dataDirectory,
          privacyComponents.storage,
          maybeAnchoringAuthService,
          maybeContractExecutionComponents.map(_.dockerEngine.docker),
          nodeIsFrozen
        )(schedulers.healthCheckScheduler)

        maybeHealthChecker = Some(healthChecker)

        val maybeContractAuthTokenService = maybeContractExecutionComponents.map(_.contractAuthTokenService)
        Seq(
          new NodeApiRoute(settings, blockchainUpdater, time, ownerKey, healthChecker),
          new BlocksApiRoute(settings.api, time, blockchainUpdater, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new TransactionsApiRoute(
            settings.api,
            feeCalculator,
            wallet,
            blockchainUpdater,
            utxStorage,
            time,
            maybeContractAuthTokenService,
            nodeOwnerAddress,
            privacyComponents.storage,
            txBroadcaster,
            settings.network.mode,
            schedulers.apiComputationsScheduler
          )(ExecutionContext.global),
          new ConsensusApiRoute(settings.api,
                                time,
                                blockchainUpdater,
                                settings.blockchain.custom.functionality,
                                consensusSettings,
                                nodeOwnerAddress,
                                schedulers.apiComputationsScheduler),
          new UtilsApiRoute(time, settings.api, wallet, time, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new PeersApiRoute(peersApiService, settings.api, time, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new AddressApiRoute(
            addressApiService,
            settings.api,
            time,
            blockchainUpdater,
            utxStorage,
            settings.blockchain.custom.functionality,
            maybeContractAuthTokenService,
            nodeOwnerAddress,
            schedulers.apiComputationsScheduler
          ),
          new DebugApiRoute(
            settings,
            time,
            feeCalculator,
            blockchainUpdater,
            wallet,
            blockchainUpdater,
            permissionValidator,
            blockId => Task(blockchainUpdater.removeAfter(blockId)).executeOn(schedulers.appenderScheduler),
            activePeerConnections,
            utxStorage,
            txBroadcaster,
            miner,
            historyReplier,
            blockLoader.stateReporter,
            microBlockLoaderStorage.cacheSizesReporter,
            scoreObserver.statsReporter,
            configRoot,
            nodeOwnerAddress,
            maybeContractAuthTokenService,
            () => shutdown(ShutdownMode.STOP_NODE_BUT_LEAVE_API)
          ),
          new PermissionApiRoute(settings.api, utxStorage, time, permissionApiService, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new AssetsApiRoute(settings.api, utxStorage, blockchainUpdater, time, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new ActivationApiRoute(settings.api,
                                 time,
                                 settings.blockchain.custom.functionality,
                                 settings.features,
                                 blockchainUpdater,
                                 nodeOwnerAddress,
                                 schedulers.apiComputationsScheduler),
          new LeaseApiRoute(settings.api, wallet, blockchainUpdater, utxStorage, time, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new AliasApiRoute(settings.api, utxStorage, time, blockchainUpdater, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new ContractsApiRoute(contractsApiService, settings.api, time, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new PrivacyApiRoute(
            privacyApiService,
            settings.api,
            settings.privacy.service,
            time,
            privacyEnabled,
            maybeContractAuthTokenService,
            nodeOwnerAddress,
            settings.network.mode,
            schedulers.apiComputationsScheduler
          ),
          new CryptoApiRoute(cryptoService, settings.api, time, maybeContractAuthTokenService, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new AnchoringApiRoute(anchoringApiService, settings.api, time, nodeOwnerAddress)
        )
      } else {
        Seq.empty
      }
    }

    if (initRestAPI) {
      val allRoutes: Seq[ApiRoute] = dockerApiRoutes ++ restAPIRoutes ++ snapshotApiRoutes
      val combinedRoute            = CompositeHttpService(allRoutes, settings.api, metricsSettings.httpRequestsCache).enrichedCompositeRoute
      val httpBindingFuture =
        getServerBuilder(settings.api.rest.bindAddress, settings.api.rest.port).bind(combinedRoute)
      serverBinding = Await.result(httpBindingFuture, 20.seconds)
      log.info(s"REST API was bound on ${settings.api.rest.bindAddress}:${settings.api.rest.port}")
    }

    // on unexpected shutdown
    sys.addShutdownHook {
      log.debug("Entering JAVA SYSTEM shutdown hook")
      Await.ready(Kamon.stopAllReporters(), 20.seconds)
      Metrics.shutdown()
      shutdown()
    }
  }

  private def getServerBuilder(
      bindAddress: String,
      port: Int
  )(implicit s: ActorSystem): ServerBuilder =
    Http().newServerAt(bindAddress, port)

  private val shutdownInProgress                 = new AtomicBoolean(false)
  private val nodeIsFrozen                       = new AtomicBoolean(false)
  @volatile var serverBinding: ServerBinding     = _
  @volatile var grpcServerBinding: ServerBinding = _

  private def internalShutdown(shutdownMode: ShutdownMode.Mode): Unit = {
    if (shutdownInProgress.compareAndSet(false, true)) {
      maybeSnapshotComponents.foreach(_.close(shutdownMode))

      maybeAnchoring.foreach(_.stop())
      maybeAnchoring = None

      maybeUtx.foreach(_.close())
      maybeUtx = None

      maybeNodeAttributesHandler.foreach(_.close())
      maybeNodeAttributesHandler = None

      maybePrivacyComponents.foreach(_.close())
      maybePrivacyComponents = None

      for (addr <- settings.network.maybeDeclaredSocketAddress if settings.network.upnp.enable) {
        upnp.deletePort(addr.getPort)
      }

      log.debug("Closing peer database")
      peerDatabase.close()

      log.debug("Closing miner")
      maybeMiner.foreach(_.close)
      maybeMiner = None

      log.debug("Closing UTX poll synchronizer")
      maybeUtxPoolSynchronizer.foreach(_.close())
      maybeUtxPoolSynchronizer = None

      log.debug("Closing blocks appenders")
      maybeBlockLoader.foreach(_.close())
      maybeMicroBlockLoader.foreach(_.close())
      maybeBlockAppender.foreach(_.close())
      maybeMicroBlockAppender.foreach(_.close())
      maybeBlockLoader = None
      maybeMicroBlockLoader = None
      maybeBlockAppender = None
      maybeMicroBlockAppender = None
      blockchainUpdater.shutdown()

      log.info("Stopping network services")
      maybeNetwork.foreach(_.shutdown())
      maybeNetwork = None

      maybeTxBroadcaster.foreach(_.close())
      maybeTxBroadcaster = None

      maybeContractExecutionComponents.foreach(_.close())
      maybeContractExecutionComponents = None

      Metrics.shutdown()

      schedulers.shutdown(shutdownMode)

      time.close()

      maybeHealthChecker.foreach { healthChecker =>
        log.info("Shutting down Health Checker")
        healthChecker.close()
        log.info("Health Checker has been stopped")
      }

      if (shutdownMode == ShutdownMode.FULL_SHUTDOWN) {
        if (settings.api.rest.enable) {
          log.info("Shutting down REST API")
          Try(Await.ready(serverBinding.terminate(2.minutes), 2.minutes)).failed.map(e => log.error("Failed to terminate REST API", e))
          log.info("REST API stopped")
        }

        if (settings.api.grpc.enable) {
          log.info("Shutting down gRPC API")
          Try(Await.ready(grpcServerBinding.terminate(2.minutes), 2.minutes)).failed.map(e => log.error("Failed to terminate gRPC API", e))
          log.info("gRPC API stopped")
        }

        maybeGrpcActorSystem.foreach { grpcActorSystem =>
          log.info("Shutting down gRPC actor system")
          Try(Await.result(grpcActorSystem.terminate(), 2 minutes)) match {
            case Success(_) => log.debug("gRPC actor system shutdown successful")
            case Failure(e) => log.error("Failed to terminate gRPC actor system", e)
          }
        }
        maybeGrpcActorSystem = None

        log.debug("Shutting down actor system")
        Try(Await.result(actorSystem.terminate(), 2.minutes)).failed.map(e => log.error("Failed to terminate actor system", e))
        log.debug("Node's actor system shutdown successful")
      } else if (shutdownMode == ShutdownMode.STOP_NODE_BUT_LEAVE_API) {
        nodeIsFrozen.compareAndSet(false, true)
      }

      log.info("Closing storage")
      storage.close()

      shutdownInProgress.compareAndSet(true, false)
      log.info("Shutdown complete")
    }
  }

  private def alarmAboutFileExistence(fileOpt: Option[File]): Unit = fileOpt match {
    case None => Unit
    case Some(file) =>
      if (!file.exists()) log.warn(s"File '${file.getAbsolutePath}' does not exist!")
  }
}

object Application extends ScorexLogging {

  final val ownerPasswordEmptyEnvKey: String = "WE_NODE_OWNER_PASSWORD_EMPTY"
  final val ownerPasswordEnvKey: String      = "WE_NODE_OWNER_PASSWORD"
  final val logLevelEnvKey: String           = "LOG_LEVEL"

  private def setLoggingLevel(config: Config): Unit = {
    val rootLoggerLevel = Option(System.getenv(logLevelEnvKey)) match {
      case Some(level) => level
      case None        => config.getString(s"${WESettings.configPath}.logging-level")
    }
    val lc                                        = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    implicit val levelReader: ConfigReader[Level] = (cur: ConfigCursor) => cur.asString.map(s => Level.valueOf(s.toUpperCase))

    (ConfigSource.fromConfig(config).at(s"${WESettings.configPath}.loggers").load[Map[String, Level]].getOrElse(Map.empty[String, Level]) +
      // Do not log "java.lang.ClassNotFoundException: org.osgi.framework.BundleReference" error on Docker Client init
      (org.slf4j.Logger.ROOT_LOGGER_NAME                                 -> Level.valueOf(rootLoggerLevel)) +
      ("org.glassfish.jersey.internal.util.ReflectionHelper"             -> Level.INFO) +
      ("org.glassfish.jersey.client.ClientExecutorProvidersConfigurator" -> Level.INFO))
      .foreach {
        case (logger, level) => lc.getLogger(logger).setLevel(level)
      }
  }

  def main(args: Array[String]): Unit = {

    // prevents java from caching successful name resolutions, which is needed e.g. for proper NTP server rotation
    // http://stackoverflow.com/a/17219327
    System.setProperty("sun.net.inetaddr.ttl", "0")
    System.setProperty("sun.net.inetaddr.negative.ttl", "0")
    Security.setProperty("networkaddress.cache.ttl", "0")
    Security.setProperty("networkaddress.cache.negative.ttl", "0")

    // specify aspectj to use it's build-in infrastructure
    // http://www.eclipse.org/aspectj/doc/released/pdguide/trace.html
    System.setProperty("org.aspectj.tracing.factory", "default")

    // j.u.l should log messages using the projects' conventions
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()

    val (config, configPath) = readConfigOrTerminate(args.headOption)

    // DO NOT LOG BEFORE THIS LINE, THIS PROPERTY IS USED IN logback.xml
    System.setProperty("node.directory", config.getString("node.directory"))
    setLoggingLevel(config)

    ResourceAvailability.logResources()
    log.info("Starting node...")

    log.info(s"Using configuration file $configPath")
    sys.addShutdownHook {
      SystemInformationReporter.report(config)
    }

    val ownerPasswordMode = readOwnerPasswordMode

    val cryptoSettings = ConfigSource.fromConfig(config).at(WESettings.configPath).loadOrThrow[CryptoSettings]
    CryptoInitializer.init(cryptoSettings).left.foreach { error =>
      log.error(s"Startup failure. ${error}")
      System.exit(1)
    }

    // Initialize global var with actual address scheme
    AddressSchemeHelper.setAddressSchemaByte(config)

    val settings = ConfigSource.fromConfig(config).at(WESettings.configPath).loadOrThrow[WESettings]
    log.info(s"WavesEnterprise settings: ${show"$settings"}")

    if (config.getBoolean("kamon.enable")) {
      log.info("Aggregated metrics are enabled")
      Kamon.reconfigure(config)
      Kamon.addReporter(new InfluxDBReporter())
      SystemMetrics.startCollecting()
    }

    val schedulers = new AppSchedulers
    val time       = NTP(settings.blockchain.consensus.consensusType, settings.ntp)(schedulers.ntpTimeScheduler)

    val metricsSettings = ConfigSource.fromConfig(config.getConfig(Metrics.configPath)).loadOrThrow[Metrics.MetricsSettings]
    log.info(s"Metrics settings: ${show"$metricsSettings"}")
    val isMetricsStarted = Metrics.start(metricsSettings, time)

    RootActorSystem.start("wavesenterprise", config) { actorSystem =>
      import actorSystem.dispatcher
      isMetricsStarted.foreach {
        case true =>
          Metrics.sendInitialMetrics(settings)
        case _ =>
      }

      log.info(s"${VersionConstants.AgentName} Blockchain Id: ${settings.blockchain.custom.addressSchemeCharacter}")

      new Application(ownerPasswordMode, actorSystem, settings, metricsSettings, config, time, schedulers).run()
    }
  }

  def readOwnerPasswordMode(): OwnerPasswordMode = {
    val positiveCases   = Set("true", "yes")
    val passwordIsEmpty = Option(System.getenv(ownerPasswordEmptyEnvKey)).exists(positiveCases.contains)
    if (passwordIsEmpty) {
      EmptyPassword
    } else {
      Option(System.getenv(ownerPasswordEnvKey)) match {
        case Some(secret) =>
          PasswordSpecified(secret)
        case _ =>
          PromptForPassword
      }
    }
  }

  def readPassword(mode: OwnerPasswordMode): Option[Array[Char]] = {
    mode match {
      case EmptyPassword =>
        None
      case PasswordSpecified(secret) =>
        Some(secret.toCharArray)
      case PromptForPassword =>
        com.wavesenterprise.utils.Console.readPasswordFromConsole("Enter the password for private key: ") match {
          case Right(password) => Option(password).filter(_.nonEmpty)
          case Left(value) =>
            log.error(
              s"Node owner's password has not been received because: '$value'. " +
                s"Please, specify '${Application.ownerPasswordEmptyEnvKey}' or '${Application.ownerPasswordEnvKey}' environment keys instead")
            forceStopApplication()
            None
        }
    }
  }
}

sealed trait OwnerPasswordMode

object OwnerPasswordMode {
  case object EmptyPassword                    extends OwnerPasswordMode
  case class PasswordSpecified(secret: String) extends OwnerPasswordMode
  case object PromptForPassword                extends OwnerPasswordMode
}
