package com.wavesenterprise

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.{Http, ServerBuilder}
import akka.stream.Materializer
import cats.implicits.showInterpolator
import cats.syntax.option._
import ch.qos.logback.classic.{Level, LoggerContext}
import com.github.ghik.silencer.silent
import com.typesafe.config._
import com.wavesenterprise.Application.readPassword
import com.wavesenterprise.OwnerPasswordMode.{EmptyPassword, PasswordSpecified, PromptForPassword}
import com.wavesenterprise.account.{Address, AddressSchemeHelper, PrivateKeyAccount}
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.actor.RootActorSystem
import com.wavesenterprise.anchoring.{Anchoring, EnabledAnchoring}
import com.wavesenterprise.api.grpc.CompositeGrpcService
import com.wavesenterprise.api.grpc.service._
import com.wavesenterprise.api.http._
import com.wavesenterprise.api.http.acl.PermissionApiRoute
import com.wavesenterprise.api.http.alias.AliasApiRoute
import com.wavesenterprise.api.http.assets.AssetsApiRoute
import com.wavesenterprise.api.http.consensus.ConsensusApiRoute
import com.wavesenterprise.api.http.docker.{ContractsApiRoute, InternalContractsApiRoute}
import com.wavesenterprise.api.http.leasing.LeaseApiRoute
import com.wavesenterprise.api.http.service._
import com.wavesenterprise.api.http.snapshot.EnabledSnapshotApiRoute
import com.wavesenterprise.block.Block
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.consensus.{BlockVotesHandler, Consensus}
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.database.rocksdb.{RocksDBStorage, RocksDBWriter}
import com.wavesenterprise.database.snapshot.{SnapshotComponents, SnapshotStatusHolder}
import com.wavesenterprise.docker.grpc.GrpcContractExecutor
import com.wavesenterprise.docker.validator.{ContractValidatorResultsHandler, ContractValidatorResultsStore, ExecutableTransactionsValidator}
import com.wavesenterprise.docker._
import com.wavesenterprise.features.api.ActivationApiRoute
import com.wavesenterprise.history.BlockchainFactory
import com.wavesenterprise.http.{DebugApiRoute, HealthChecker, NodeApiRoute}
import com.wavesenterprise.metrics.Metrics
import com.wavesenterprise.metrics.Metrics.{HttpRequestsCacheSettings, MetricsSettings}
import com.wavesenterprise.mining.{Miner, MinerDebugInfo, TransactionsAccumulatorProvider}
import com.wavesenterprise.network.MessageObserver.IncomingMessages
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerConnectionAcceptor, PeerDatabaseImpl}
import com.wavesenterprise.network.{TxBroadcaster, _}
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
import com.wavesenterprise.settings.SynchronizationSettings.MicroblockSynchronizerSettings
import com.wavesenterprise.settings.dockerengine.DockerEngineSettings
import com.wavesenterprise.settings.privacy.PrivacyStorageVendor
import com.wavesenterprise.settings.{NodeMode, _}
import com.wavesenterprise.state.appender.{BaseAppender, BlockAppender, MicroBlockAppender}
import com.wavesenterprise.state.{Blockchain, MiningConstraintsHolder, NG, SignatureValidator}
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.transaction.validation.FeeCalculator
import com.wavesenterprise.utils.NTPUtils.NTPExt
import com.wavesenterprise.utils.{NTP, ScorexLogging, SystemInformationReporter, Time, forceStopApplication}
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
                  val cryptoSettings: CryptoSettings,
                  val metricsSettings: MetricsSettings,
                  config: Config,
                  time: NTP,
                  schedulers: AppSchedulers,
                  customSwaggerRoute: Option[Route])
    extends ScorexLogging {

  protected val storage: RocksDBStorage = RocksDBStorage.openDB(settings.dataDirectory)

  protected val configRoot: ConfigObject = config.root()
  private val ScoreThrottleDuration      = 1.second

  protected val (persistentStorage, blockchainUpdater) = buildBlockchain()

  protected val permissionValidator: PermissionValidator = blockchainUpdater.permissionValidator

  protected lazy val enableHttp2: Config = ConfigFactory.parseString("akka.http.server.preview.enable-http2 = on")

  private lazy val enableTlsSessionInfoHeader: Config = ConfigFactory.parseString("akka.http.server.parsing.tls-session-info-header = on")

  private lazy val upnp = new UPnP(settings.network.upnp) // don't initialize unless enabled

  alarmAboutFileExistence(settings.network.file)
  alarmAboutFileExistence(settings.wallet.file)

  protected val wallet: WalletWithKeystore = try {
    Wallet(settings.wallet).validated()
  } catch {
    case e: IllegalArgumentException =>
      log.error(s"Illegal wallet state '${settings.wallet.file.get.getAbsolutePath}'")
      throw e
    case e: IllegalStateException =>
      log.error(s"Failed to open wallet file '${settings.wallet.file.get.getAbsolutePath}")
      throw e
  }

  protected val peerDatabase = PeerDatabaseImpl(settings.network)

  private var maybeBlockLoader: Option[BlockLoader]                     = None
  private var maybeMiner: Option[Miner]                                 = None
  private var maybeMicroBlockLoader: Option[MicroBlockLoader]           = None
  private var maybeBlockAppender: Option[BlockAppender]                 = None
  private var maybeMicroBlockAppender: Option[MicroBlockAppender]       = None
  private var maybeUtx: Option[UtxPool]                                 = None
  private var maybeNetwork: Option[P2PNetwork]                          = None
  private var maybeAnchoring: Option[Anchoring]                         = None
  private var maybeNodeAttributesHandler: Option[NodeAttributesHandler] = None
  private var maybePrivacyComponents: Option[PrivacyComponents]         = None
  private var maybeUtxPoolSynchronizer: Option[UtxPoolSynchronizer]     = None
  private var maybeGrpcActorSystem: Option[ActorSystem]                 = None
  private var maybeSnapshotComponents: Option[SnapshotComponents]       = None
  private var maybeHealthChecker: Option[HealthChecker]                 = None
  private var maybeCrlSyncManager: Option[AutoCloseable]                = None

  protected var maybeTxBroadcaster: Option[TxBroadcaster]                             = None
  protected var maybeContractExecutionComponents: Option[ContractExecutionComponents] = None
  protected val predefinedRoutes: Seq[ApiRoute]                                       = Seq.empty

  protected def buildNodeApiRoute(healthChecker: HealthChecker): NodeApiRoute =
    new NodeApiRoute(settings, cryptoSettings, blockchainUpdater, time, ownerKey, healthChecker)

  protected def buildTxApiRoute(feeCalculator: FeeCalculator,
                                utxStorage: UtxPool,
                                maybeContractAuthTokenService: Option[ContractAuthTokenService],
                                storage: PolicyStorage,
                                txBroadcaster: TxBroadcaster): TransactionsApiRoute =
    new TransactionsApiRoute(
      settings.api,
      feeCalculator,
      wallet,
      blockchainUpdater,
      utxStorage,
      time,
      maybeContractAuthTokenService,
      nodeOwnerAddress,
      storage,
      txBroadcaster,
      settings.network.mode,
      schedulers.apiComputationsScheduler,
      cryptoSettings
    )(ExecutionContext.global)

  protected def buildPrivacyApiRoute(privacyApiService: PrivacyApiService,
                                     privacyEnabled: Boolean,
                                     maybeContractAuthTokenService: Option[ContractAuthTokenService])(implicit mat: Materializer): PrivacyApiRoute =
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
    )

  protected def buildUtilsApiRoute(): UtilsApiRoute =
    new UtilsApiRoute(time, settings.api, wallet, time, nodeOwnerAddress, schedulers.apiComputationsScheduler)

  protected def buildPeersApiRoute(peersApiService: PeersApiService): PeersApiRoute =
    new PeersApiRoute(peersApiService, settings.api, time, nodeOwnerAddress, schedulers.apiComputationsScheduler)

  protected def buildDebugApiRoute(feeCalculator: FeeCalculator,
                                   utx: UtxPool,
                                   txBroadcaster: TxBroadcaster,
                                   miner: Miner with MinerDebugInfo,
                                   historyReplier: HistoryReplier,
                                   extLoaderStateReporter: Coeval[BlockLoader.State],
                                   mbsCacheSizesReporter: Coeval[MicroBlockLoaderStorage.CacheSizes],
                                   syncChannelSelectorReporter: Coeval[SyncChannelSelector.Stats],
                                   maybeContractAuthTokenService: Option[ContractAuthTokenService]): DebugApiRoute =
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
      utx,
      txBroadcaster,
      miner,
      historyReplier,
      extLoaderStateReporter,
      mbsCacheSizesReporter,
      syncChannelSelectorReporter,
      configRoot,
      nodeOwnerAddress,
      maybeContractAuthTokenService,
      schedulers.apiComputationsScheduler,
      () => shutdown(ShutdownMode.STOP_NODE_BUT_LEAVE_API)
    )

  def shutdown(shutdownMode: ShutdownMode.Mode = ShutdownMode.FULL_SHUTDOWN): Unit = {
    internalShutdown(shutdownMode)
  }

  /**
    * Node owner key is necessary for the node to function
    * It is used for Privacy, if it is enabled on the network, to sign SignedHandshakes to pass blockchain auth on other nodes
    * It is also used as the only key for Miner
    */
  protected val ownerKey: PrivateKeyAccount = {
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

  protected val nodeOwnerAddress: Address = ownerKey.toAddress

  protected val networkInitialSync = new NetworkInitialSync(ownerKey, settings, blockchainUpdater, peerDatabase)

  protected val activePeerConnections = new ActivePeerConnections()

  protected def loadCerts(settings: WESettings, rocksDBWriter: RocksDBWriter): CertChainStore = CertChainStore.empty

  protected def loadCrls(settings: WESettings, rocksDBWriter: RocksDBWriter): Unit = ()

  protected def buildCrlSyncManager(incomingMessages: IncomingMessages): Option[AutoCloseable] = None

  protected def predefinedPublicServices(actorSystem: ActorSystem, scheduler: Scheduler): Seq[PartialFunction[HttpRequest, Future[HttpResponse]]] =
    Seq.empty

  private def buildGrpcAkkaConfig(): Config = enableHttp2.withFallback(enableTlsSessionInfoHeader)

  //noinspection ScalaStyle
  def run(): Unit = {

    val certs = loadCerts(settings, persistentStorage)
    loadCrls(settings, persistentStorage)
    checkGenesis(settings, blockchainUpdater, certs)

    val initialSyncTimeout = 2.minutes
    val initialSyncScheduler: SchedulerService = {
      val currentKnownPeersCount                         = peerDatabase.knownPeers.size
      val initialSyncReporter: UncaughtExceptionReporter = log.error("Error in InitialSync", _)
      val poolSize                                       = Math.max(1, currentKnownPeersCount)
      fixedPool("initialSyncPool", poolSize = poolSize, reporter = initialSyncReporter)
    }

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

    val utx = buildUtx
    maybeUtx = Some(utx)

    val consensusSettings = settings.blockchain.consensus
    val consensus         = Consensus(settings.blockchain, blockchainUpdater, time)

    val txBroadcaster = TxBroadcaster(
      settings = settings,
      blockchainUpdater,
      consensus,
      utx = utx,
      activePeerConnections = activePeerConnections,
      maxSimultaneousConnections = settings.network.maxSimultaneousConnections,
    )(schedulers.transactionBroadcastScheduler)

    maybeTxBroadcaster = Some(txBroadcaster)

    val knownInvalidBlocks = new InvalidBlockStorageImpl(settings.synchronization.invalidBlocksStorage)

    val dockerMiningEnabled = settings.miner.enable && settings.dockerEngine.enable

    val contractExecutionMessagesCache = new ContractExecutionMessagesCache(
      settings.dockerEngine.contractExecutionMessagesCache,
      dockerMiningEnabled,
      activePeerConnections,
      utx,
      schedulers.contractExecutionMessagesScheduler
    )

    val microBlockLoaderStorage = new MicroBlockLoaderStorage(settings.synchronization.microBlockSynchronizer)

    val historyReplier = buildHistoryReplier(microBlockLoaderStorage)

    val peersIdentityService = buildPeerIdentityService()

    val connectionAcceptor =
      new PeerConnectionAcceptor(activePeerConnections, settings.network.maxSimultaneousConnections, blockchainUpdater, time, dockerMiningEnabled)
    val networkServer = buildNetworkServer(initialSyncResult, historyReplier, peersIdentityService, connectionAcceptor)

    val p2PNetwork = P2PNetwork(settings.network, networkServer, peerDatabase)
    p2PNetwork.start()
    maybeNetwork = Some(p2PNetwork)

    val incomingMessages = networkServer.messages

    maybeCrlSyncManager = buildCrlSyncManager(incomingMessages)

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

    val transactionsAccumulatorProvider = buildTransactionAccumulatorProvider

    var contractsApiService: ContractsApiService = null
    if (settings.api.rest.enable || settings.api.grpc.enable) {
      contractsApiService = new ContractsApiService(blockchainUpdater, contractExecutionMessagesCache)
    }

    lazy val addressApiService = new AddressApiService(blockchainUpdater, wallet)

    if (settings.api.grpc.enable || dockerMiningEnabled) {

      val grpcAkkaConfig = buildGrpcAkkaConfig()

      val grpcActorSystem: ActorSystem = ActorSystem("gRPC", grpcAkkaConfig.withFallback(settings.api.grpc.akkaHttpSettings))

      maybeGrpcActorSystem = Some(grpcActorSystem)

      maybeContractExecutionComponents = {
        if (dockerMiningEnabled) {
          val dockerEngineSettings     = settings.dockerEngine
          val contractAuthTokenService = new ContractAuthTokenService()

          val contractReusedContainers = new ContractReusedContainers(
            dockerEngineSettings.removeContainerAfter
          )(schedulers.dockerExecutorScheduler)

          buildContractExecutionComponents(
            wallet,
            utx,
            privacyApiService,
            contractExecutionMessagesCache,
            contractAuthTokenService,
            contractReusedContainers,
            activePeerConnections,
            dockerEngineSettings
          ).some
        } else None
      }

      def dockerPartialHandlers: Seq[PartialFunction[HttpRequest, Future[HttpResponse]]] =
        maybeContractExecutionComponents.map(_.partialHandlers).getOrElse(Seq.empty)

      def privacyPartialHandlers: Seq[PartialFunction[HttpRequest, Future[HttpResponse]]] =
        if (settings.api.grpc.enable && privacyEnabled) {
          Seq(
            PrivacyEventsServicePowerApiHandler.partial(
              buildPrivacyEventsService(privacyComponents, privacyEnabled)
            )(system = grpcActorSystem),
            PrivacyPublicServicePowerApiHandler.partial(
              buildPrivacyService(privacyEnabled, privacyApiService, grpcActorSystem)
            )(system = grpcActorSystem)
          )
        } else Nil

      def publicServicesPartialHandlers: Seq[PartialFunction[HttpRequest, Future[HttpResponse]]] =
        if (settings.api.grpc.enable) {
          val grpcScheduler = Scheduler(grpcActorSystem.getDispatcher)
          predefinedPublicServices(grpcActorSystem, grpcScheduler) ++
            Seq(
              BlockchainEventsServicePowerApiHandler.partial(
                new BlockchainEventsServiceImpl(
                  settings.api.auth,
                  nodeOwnerAddress,
                  time,
                  settings.api.grpc.services.blockchainEvents,
                  settings.miner.maxBlockSizeInBytes,
                  blockchainUpdater,
                  persistentStorage,
                  schedulers.apiComputationsScheduler
                ))(system = grpcActorSystem),
              NodeInfoServicePowerApiHandler
                .partial(new NodeInfoServiceImpl(time, settings, cryptoSettings, blockchainUpdater, ownerKey)(grpcScheduler))(
                  system = grpcActorSystem),
              ContractStatusServicePowerApiHandler.partial(
                new ContractStatusServiceImpl(settings.api.grpc.services.contractStatusEvents,
                                              contractsApiService,
                                              settings.api.auth,
                                              nodeOwnerAddress,
                                              time,
                                              schedulers.apiComputationsScheduler))(system = grpcActorSystem),
              TransactionPublicServicePowerApiHandler.partial(
                buildTransactionService(txBroadcaster, privacyComponents, feeCalculator, grpcScheduler)
              )(system = grpcActorSystem),
              AddressPublicServicePowerApiHandler.partial(
                new AddressServiceImpl(settings.api.auth, nodeOwnerAddress, time, addressApiService, blockchainUpdater)(grpcScheduler)
              )(system = grpcActorSystem),
              AliasPublicServicePowerApiHandler.partial(
                new AliasServiceImpl(settings.api.auth, nodeOwnerAddress, time, blockchainUpdater)(grpcScheduler)
              )(system = grpcActorSystem),
              UtilPublicServicePowerApiHandler.partial(
                new UtilServiceImpl(settings.api.auth, nodeOwnerAddress, time)(grpcScheduler)
              )(system = grpcActorSystem)
            )
        } else Nil

      val compositePartialHandlers = dockerPartialHandlers ++ privacyPartialHandlers ++ publicServicesPartialHandlers

      val serviceHandler: HttpRequest => Future[HttpResponse] = {
        buildCompositeGrpcService(metricsSettings.httpRequestsCache, compositePartialHandlers: _*)(grpcActorSystem.getDispatcher).enrichedCompositeHandler
      }

      val grpcBindingFuture =
        getServerBuilder(GrpcApi, settings.api.grpc.bindAddress, settings.api.grpc.port)(grpcActorSystem)
          .bind(serviceHandler)

      grpcServerBinding = Await.result(grpcBindingFuture, 20.seconds)
      log.info(s"gRPC server bound to: ${grpcServerBinding.localAddress}")
    }

    val signatureValidator = new SignatureValidator()(schedulers.signaturesValidationScheduler)

    val microBlockLoader = buildMicroBlockLoader(
      ng = blockchainUpdater,
      settings = settings.synchronization.microBlockSynchronizer,
      incomingMicroBlockInventoryEvents = incomingMessages.microblockInvs,
      incomingMicroBlockEvents = incomingMessages.microblockResponses,
      crlDataResponses = incomingMessages.crlDataResponses,
      signatureValidator = signatureValidator,
      activePeerConnections = activePeerConnections,
      storage = microBlockLoaderStorage
    )

    maybeMicroBlockLoader = Some(microBlockLoader)

    val baseAppender = buildBaseAppender(utx, consensus, microBlockLoader)

    val executableTransactionsValidatorOpt = maybeContractExecutionComponents.map(_.createTransactionValidator(transactionsAccumulatorProvider))

    val microBlockAppender = new MicroBlockAppender(
      blockchainUpdater = blockchainUpdater,
      utxStorage = utx,
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
        utx,
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
        buildMiner(
          utx = utx,
          consensus = consensus,
          privacyComponents = privacyComponents,
          transactionsAccumulatorProvider = transactionsAccumulatorProvider,
          baseAppender = baseAppender,
          executableTransactionsValidatorOpt = executableTransactionsValidatorOpt,
          microBlockAppender = microBlockAppender,
          blockLoader = blockLoader,
          votesHandler = votesHandler
        )
      else
        Miner.Disabled
    }

    maybeMiner = Some(miner)

    val blockAppender = buildBlockAppender(
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
      contractValidatorResultsStoreOpt = maybeContractExecutionComponents.map(_.contractValidatorResultsStore),
      crlDataResponses = incomingMessages.crlDataResponses,
      scheduler = schedulers.appenderScheduler
    )

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
      state = persistentStorage,
      time = time,
      buildSnapshotApiRoute = buildSnapshotApiRoute,
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
          buildNodeApiRoute(healthChecker),
          new BlocksApiRoute(settings.api, time, blockchainUpdater, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          buildTxApiRoute(
            feeCalculator,
            utx,
            maybeContractAuthTokenService,
            privacyComponents.storage,
            txBroadcaster
          ),
          new ConsensusApiRoute(settings.api,
                                time,
                                blockchainUpdater,
                                settings.blockchain.custom.functionality,
                                consensusSettings,
                                nodeOwnerAddress,
                                schedulers.apiComputationsScheduler),
          buildUtilsApiRoute(),
          buildPeersApiRoute(peersApiService),
          buildAddressApiRoute(utx, addressApiService, maybeContractAuthTokenService),
          buildDebugApiRoute(
            feeCalculator,
            utx,
            txBroadcaster,
            miner,
            historyReplier,
            blockLoader.stateReporter,
            microBlockLoaderStorage.cacheSizesReporter,
            scoreObserver.statsReporter,
            maybeContractAuthTokenService
          ),
          new PermissionApiRoute(settings.api, utx, time, permissionApiService, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new AssetsApiRoute(settings.api, utx, blockchainUpdater, time, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new ActivationApiRoute(settings.api,
                                 time,
                                 settings.blockchain.custom.functionality,
                                 settings.features,
                                 blockchainUpdater,
                                 nodeOwnerAddress,
                                 schedulers.apiComputationsScheduler),
          new LeaseApiRoute(settings.api, wallet, blockchainUpdater, utx, time, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new AliasApiRoute(settings.api, utx, time, blockchainUpdater, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          new ContractsApiRoute(contractsApiService, settings.api, time, nodeOwnerAddress, schedulers.apiComputationsScheduler),
          buildPrivacyApiRoute(
            privacyApiService,
            privacyEnabled,
            maybeContractAuthTokenService,
          ),
          new AnchoringApiRoute(anchoringApiService, settings.api, time, nodeOwnerAddress),
          buildCryptoApiRoute(maybeContractAuthTokenService)
        )
      } else {
        Seq.empty
      }
    }

    if (initRestAPI) {
      val allRoutes: Seq[ApiRoute] = predefinedRoutes ++ dockerApiRoutes ++ restAPIRoutes ++ snapshotApiRoutes
      val combinedRoute =
        buildCompositeHttpService(allRoutes, settings.api, metricsSettings.httpRequestsCache, customSwaggerRoute).enrichedCompositeRoute

      val httpBindingFuture =
        getServerBuilder(RestApi, settings.api.rest.bindAddress, settings.api.rest.port).bind(combinedRoute)

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

  protected def buildCryptoApiRoute(maybeContractAuthTokenService: Option[ContractAuthTokenService]): CryptoApiRoute = {
    val cryptoService = new CryptoApiService(wallet, schedulers.cryptoServiceScheduler)
    new CryptoApiRoute(cryptoService, settings.api, time, maybeContractAuthTokenService, nodeOwnerAddress, schedulers.apiComputationsScheduler)
  }

  protected def buildAddressApiRoute(utx: UtxPool,
                                     addressApiService: AddressApiService,
                                     maybeContractAuthTokenService: Option[ContractAuthTokenService]): AddressApiRoute =
    new AddressApiRoute(
      addressApiService = addressApiService,
      settings = settings.api,
      time = time,
      blockchain = blockchainUpdater,
      utx = utx,
      functionalitySettings = settings.blockchain.custom.functionality,
      contractAuthTokenService = maybeContractAuthTokenService,
      nodeOwner = nodeOwnerAddress,
      scheduler = schedulers.apiComputationsScheduler
    )

  protected def buildTransactionService(txBroadcaster: TxBroadcaster,
                                        privacyComponents: PrivacyComponents,
                                        feeCalculator: FeeCalculator,
                                        grpcScheduler: Scheduler) =
    new TransactionServiceImpl(blockchainUpdater,
                               feeCalculator,
                               txBroadcaster,
                               privacyComponents.storage,
                               settings.api.auth,
                               nodeOwnerAddress,
                               time,
                               settings.network.mode)(grpcScheduler)

  protected def buildPrivacyService(privacyEnabled: Boolean, privacyApiService: PrivacyApiService, grpcActorSystem: ActorSystem): PrivacyServiceImpl =
    new PrivacyServiceImpl(
      privacyService = privacyApiService,
      privacyEnabled = privacyEnabled,
      authSettings = settings.api.auth,
      privacyServiceSettings = settings.privacy.service,
      nodeOwner = nodeOwnerAddress,
      time = time,
      nodeMode = settings.network.mode
    )(Scheduler(grpcActorSystem.getDispatcher), Materializer.matFromSystem(grpcActorSystem))

  protected def buildPrivacyEventsService(privacyComponents: PrivacyComponents, privacyEnabled: Boolean): PrivacyEventsServiceImpl =
    new PrivacyEventsServiceImpl(
      authSettings = settings.api.auth,
      nodeOwner = nodeOwnerAddress,
      time = time,
      privacyEnabled = privacyEnabled,
      settings = settings.api.grpc.services.privacyEvents,
      privacyStorage = privacyComponents.storage,
      state = persistentStorage,
      blockchainUpdater = blockchainUpdater,
      scheduler = schedulers.apiComputationsScheduler
    )

  protected def buildBaseAppender(utx: UtxPool, consensus: Consensus, microBlockLoader: MicroBlockLoader): BaseAppender =
    new BaseAppender(
      blockchainUpdater = blockchainUpdater,
      utxStorage = utx,
      consensus = consensus,
      time = time,
      microBlockLoader = microBlockLoader,
      keyBlockAppendingSettings = settings.synchronization.keyBlockAppending
    )(schedulers.appenderScheduler)

  protected def buildBlockAppender(baseAppender: BaseAppender,
                                   blockchainUpdater: BlockchainUpdater with Blockchain,
                                   invalidBlocks: InvalidBlockStorage,
                                   miner: Miner,
                                   executableTransactionsValidatorOpt: Option[ExecutableTransactionsValidator],
                                   contractValidatorResultsStoreOpt: Option[ContractValidatorResultsStore],
                                   consensus: Consensus,
                                   signatureValidator: SignatureValidator,
                                   blockLoader: BlockLoader,
                                   permissionValidator: PermissionValidator,
                                   activePeerConnections: ActivePeerConnections,
                                   crlDataResponses: ChannelObservable[CrlDataResponse],
                                   scheduler: Scheduler): BlockAppender =
    new BlockAppender(
      baseAppender = baseAppender,
      blockchainUpdater = blockchainUpdater,
      invalidBlocks = invalidBlocks,
      miner = miner,
      consensus = consensus,
      signatureValidator = signatureValidator,
      blockLoader = blockLoader,
      permissionValidator = permissionValidator,
      activePeerConnections = activePeerConnections,
      executableTransactionsValidatorOpt = executableTransactionsValidatorOpt,
      contractValidatorResultsStoreOpt = contractValidatorResultsStoreOpt
    )(scheduler)

  protected def buildMicroBlockLoader(ng: NG,
                                      settings: MicroblockSynchronizerSettings,
                                      activePeerConnections: ActivePeerConnections,
                                      incomingMicroBlockInventoryEvents: ChannelObservable[MicroBlockInventory],
                                      incomingMicroBlockEvents: ChannelObservable[MicroBlockResponse],
                                      crlDataResponses: ChannelObservable[CrlDataResponse],
                                      signatureValidator: SignatureValidator,
                                      storage: MicroBlockLoaderStorage): MicroBlockLoader =
    new MicroBlockLoader(
      ng = blockchainUpdater,
      settings = settings,
      incomingMicroBlockInventoryEvents = incomingMicroBlockInventoryEvents,
      incomingMicroBlockEvents = incomingMicroBlockEvents,
      signatureValidator = signatureValidator,
      activePeerConnections = activePeerConnections,
      storage = storage,
    )(schedulers.microBlockLoaderScheduler)

  protected def buildHistoryReplier(microBlockLoaderStorage: MicroBlockLoaderStorage): HistoryReplier =
    new HistoryReplier(blockchainUpdater, microBlockLoaderStorage, settings.synchronization)(schedulers.historyRepliesScheduler)

  protected def buildMiner(utx: UtxPool,
                           consensus: Consensus,
                           privacyComponents: PrivacyComponents,
                           transactionsAccumulatorProvider: TransactionsAccumulatorProvider,
                           baseAppender: BaseAppender,
                           executableTransactionsValidatorOpt: Option[ExecutableTransactionsValidator],
                           microBlockAppender: MicroBlockAppender,
                           blockLoader: BlockLoader,
                           votesHandler: => BlockVotesHandler): Miner with MinerDebugInfo = {
    Miner(
      appender = baseAppender,
      microBlockAppender = microBlockAppender,
      activePeerConnections = activePeerConnections,
      transactionsAccumulatorProvider = transactionsAccumulatorProvider,
      blockchainUpdater = blockchainUpdater,
      settings = settings,
      timeService = time,
      utx = utx,
      ownerKey = ownerKey,
      consensus = consensus,
      contractExecutionComponentsOpt = maybeContractExecutionComponents,
      executableTransactionsValidatorOpt = executableTransactionsValidatorOpt,
      loaderStateReporter = blockLoader.stateReporter.map(_.loaderState),
      policyDataSynchronizer = privacyComponents.synchronizer,
      votesHandler = votesHandler
    )(schedulers.minerScheduler)
  }

  protected def buildBlockchain(): (RocksDBWriter, BlockchainUpdater with PrivacyState with NG with MiningConstraintsHolder) =
    BlockchainFactory(settings, storage, time, schedulers)

  protected def buildPeerIdentityService(): PeersIdentityService =
    new PeersIdentityService(ownerKey.toAddress, blockchainUpdater)

  protected def buildTransactionAccumulatorProvider() =
    new TransactionsAccumulatorProvider(blockchainUpdater, persistentStorage, settings, permissionValidator, time, ownerKey)

  protected def buildUtx(): UtxPool =
    new UtxPoolImpl(
      time = time,
      blockchain = blockchainUpdater,
      blockchainSettings = settings.blockchain,
      utxSettings = settings.utx,
      permissionValidator = permissionValidator,
      utxPoolSyncScheduler = schedulers.utxPoolSyncScheduler,
      snapshotSettings = settings.consensualSnapshot,
      txBroadcaster = maybeTxBroadcaster.get
    )(schedulers.utxPoolBackgroundScheduler)

  protected def buildPrivacyServiceImpl(privacyApiService: PrivacyApiService,
                                        contractAuthTokenService: ContractAuthTokenService,
                                        dockerExecutorScheduler: Scheduler): grpc.service.PrivacyServiceImpl = {
    new grpc.service.PrivacyServiceImpl(privacyApiService, contractAuthTokenService, dockerExecutorScheduler)
  }

  protected def buildInternalContractsApiRoute(contractsApiService: ContractsApiService,
                                               settings: ApiSettings,
                                               time: Time,
                                               contractAuthTokenServiceParam: ContractAuthTokenService,
                                               externalNodeOwner: Address,
                                               scheduler: SchedulerService) =
    new InternalContractsApiRoute(contractsApiService, settings, time, contractAuthTokenServiceParam, externalNodeOwner, scheduler)

  protected def buildContractExecutionComponents(
      wallet: Wallet,
      utx: UtxPool,
      privacyApiService: PrivacyApiService,
      contractExecutionMessagesCache: ContractExecutionMessagesCache,
      contractAuthTokenService: ContractAuthTokenService,
      contractReusedContainers: ContractReusedContainers,
      activePeerConnections: ActivePeerConnections,
      dockerEngineSettings: DockerEngineSettings,
  )(implicit grpcActorSystem: ActorSystem): ContractExecutionComponents = {
    import schedulers.dockerExecutorScheduler

    val dockerEngine = (for {
      dockerClient <- DockerClientBuilder.createDockerClient(dockerEngineSettings)
      dockerEngine <- DockerEngine(dockerClient, dockerEngineSettings)
    } yield dockerEngine).fold(ex => throw ex, identity)

    val localDockerHostResolver = new LocalDockerHostResolver(dockerEngine.docker)

    val legacyContractExecutor = LegacyContractExecutor(
      dockerEngine,
      dockerEngineSettings,
      contractAuthTokenService,
      contractReusedContainers,
      dockerExecutorScheduler,
      settings.api.rest.port,
      localDockerHostResolver
    )

    val grpcContractExecutor = GrpcContractExecutor(
      dockerEngine,
      dockerEngineSettings,
      contractAuthTokenService,
      contractReusedContainers,
      dockerExecutorScheduler,
      localDockerHostResolver
    )

    val privacyServiceImpl = buildPrivacyServiceImpl(privacyApiService, contractAuthTokenService, dockerExecutorScheduler)

    ContractExecutionComponents(
      settings,
      dockerExecutorScheduler,
      contractExecutionMessagesCache,
      blockchainUpdater,
      nodeOwnerAccount = ownerKey,
      utx,
      time,
      wallet,
      privacyServiceImpl,
      activePeerConnections,
      schedulerService = schedulers.apiComputationsScheduler,
      legacyContractExecutor,
      grpcContractExecutor,
      dockerEngine,
      contractAuthTokenService,
      contractReusedContainers,
      buildInternalContractsApiRoute
    )
  }

  protected def buildSnapshotApiRoute(statusHolder: SnapshotStatusHolder,
                                      snapshotGenesis: Task[Option[Block]],
                                      snapshotSwapTask: Boolean => Task[Either[ApiError, Unit]],
                                      settings: ApiSettings,
                                      time: Time,
                                      nodeOwner: Address,
                                      freezeApp: () => Unit,
                                      scheduler: Scheduler) =
    new EnabledSnapshotApiRoute(
      statusHolder,
      snapshotGenesis,
      snapshotSwapTask,
      settings,
      time,
      nodeOwner,
      freezeApp
    )(scheduler)

  protected def buildNetworkServer(initialSyncResult: InitialParticipantsDiscoverResult,
                                   historyReplier: HistoryReplier,
                                   peersIdentityService: PeersIdentityService,
                                   connectionAcceptor: PeerConnectionAcceptor): NetworkServer =
    new NetworkServer(
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

  protected def buildCompositeHttpService(routes: Seq[ApiRoute],
                                          settings: ApiSettings,
                                          httpRequestsCacheSettings: HttpRequestsCacheSettings,
                                          swaggerRoute: Option[Route]): CompositeHttpService =
    CompositeHttpService(routes, settings, httpRequestsCacheSettings, swaggerRoute)

  protected def buildCompositeGrpcService(httpRequestsCacheSettings: HttpRequestsCacheSettings,
                                          handlers: PartialFunction[HttpRequest, Future[HttpResponse]]*)(ec: ExecutionContext) =
    CompositeGrpcService(httpRequestsCacheSettings, handlers: _*)(ec)

  protected def getServerBuilder(
      apiType: ApiType,
      bindAddress: String,
      port: Int
  )(implicit s: ActorSystem): ServerBuilder =
    Http().newServerAt(bindAddress, port)

  protected val shutdownInProgress               = new AtomicBoolean(false)
  private val nodeIsFrozen                       = new AtomicBoolean(false)
  @volatile var serverBinding: ServerBinding     = _
  @volatile var grpcServerBinding: ServerBinding = _

  protected def shutdownActions(shutdownMode: ShutdownMode.Mode): Unit = {
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

    log.debug("Closing crl synchronization manager")
    maybeCrlSyncManager.foreach(_.close())
    maybeCrlSyncManager = None

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
  }

  private def internalShutdown(shutdownMode: ShutdownMode.Mode): Unit = {
    if (shutdownInProgress.compareAndSet(false, true)) {
      shutdownActions(shutdownMode)
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
    val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

    @silent("is never used")
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

    val ownerPasswordMode = readOwnerPasswordMode()

    implicit val cryptoSettings: CryptoSettings = ConfigSource.fromConfig(config).at(WESettings.configPath).loadOrThrow[CryptoSettings]
    CryptoInitializer.init(cryptoSettings).left.foreach { error =>
      log.error(s"Startup failure. $error")
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

      new Application(ownerPasswordMode, actorSystem, settings, cryptoSettings, metricsSettings, config, time, schedulers, None).run()
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
