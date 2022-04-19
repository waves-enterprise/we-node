package com.wavesenterprise.docker

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import com.wavesenterprise.Schedulers
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.api.http.ApiRoute
import com.wavesenterprise.api.http.docker.InternalContractsApiRoute
import com.wavesenterprise.api.http.service.{AddressApiService, ContractsApiService, PermissionApiService, PrivacyApiService}
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.docker.grpc.service._
import com.wavesenterprise.docker.grpc.{GrpcContractExecutor, NodeGrpcApiSettings}
import com.wavesenterprise.docker.validator.{ContractValidatorResultsStore, ExecutableTransactionsValidator}
import com.wavesenterprise.mining.{TransactionsAccumulator, TransactionsAccumulatorProvider}
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.protobuf.service.contract._
import com.wavesenterprise.settings.WESettings
import com.wavesenterprise.state.reader.DelegatingBlockchain
import com.wavesenterprise.state.{Blockchain, ByteStr, MiningConstraintsHolder, NG}
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.utils.{NTP, ScorexLogging}
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wallet.Wallet
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService

import scala.concurrent.Future

class ContractExecutionComponents(val settings: WESettings,
                                  val nodeOwnerAccount: PrivateKeyAccount,
                                  val dockerEngine: DockerEngine,
                                  val time: NTP,
                                  val utx: UtxPool,
                                  val blockchain: Blockchain with NG with MiningConstraintsHolder,
                                  val activePeerConnections: ActivePeerConnections,
                                  val contractReusedContainers: ContractReusedContainers,
                                  val legacyContractExecutor: LegacyContractExecutor,
                                  val grpcContractExecutor: GrpcContractExecutor,
                                  val contractExecutionMessagesCache: ContractExecutionMessagesCache,
                                  val contractValidatorResultsStore: ContractValidatorResultsStore,
                                  val contractAuthTokenService: ContractAuthTokenService,
                                  val dockerExecutorScheduler: Scheduler,
                                  val contractsRoutes: Seq[ApiRoute],
                                  val partialHandlers: Seq[PartialFunction[HttpRequest, Future[HttpResponse]]],
                                  private val delegatingState: DelegatingBlockchain)
    extends AutoCloseable
    with ScorexLogging {

  def setDelegatingState(blockchain: Blockchain): Unit =
    delegatingState.setState(blockchain)

  def createMinerExecutor(transactionsAccumulator: TransactionsAccumulator, keyBlockId: BlockId): MinerTransactionsExecutor = {
    new MinerTransactionsExecutor(
      messagesCache = contractExecutionMessagesCache,
      transactionsAccumulator = transactionsAccumulator,
      nodeOwnerAccount = nodeOwnerAccount,
      utx = utx,
      blockchain = blockchain,
      time = time,
      legacyContractExecutor = legacyContractExecutor,
      grpcContractExecutor = grpcContractExecutor,
      contractValidatorResultsStore = contractValidatorResultsStore,
      keyBlockId = keyBlockId,
      parallelism = settings.dockerEngine.contractsParallelism.value
    )(dockerExecutorScheduler)
  }

  def createTransactionValidator(transactionsAccumulatorProvider: TransactionsAccumulatorProvider): ExecutableTransactionsValidator = {
    new ExecutableTransactionsValidator(
      ownerKey = nodeOwnerAccount,
      transactionsAccumulatorProvider = transactionsAccumulatorProvider,
      contractExecutionComponentsOpt = Some(this),
      utx = utx,
      blockchain = blockchain,
      time = time,
      pullingBufferSize = settings.miner.pullingBufferSize,
      utxCheckDelay = settings.miner.utxCheckDelay
    )(dockerExecutorScheduler)
  }

  def createValidatorExecutor(transactionsAccumulator: TransactionsAccumulator, keyBlockId: ByteStr): ValidatorTransactionsExecutor = {
    new ValidatorTransactionsExecutor(
      nodeOwnerAccount = nodeOwnerAccount,
      transactionsAccumulator = transactionsAccumulator,
      messagesCache = contractExecutionMessagesCache,
      utx = utx,
      blockchain = blockchain,
      time = time,
      activePeerConnections = activePeerConnections,
      legacyContractExecutor = legacyContractExecutor,
      grpcContractExecutor = grpcContractExecutor,
      keyBlockId = keyBlockId,
      parallelism = settings.dockerEngine.contractsParallelism.value
    )(dockerExecutorScheduler)
  }

  override def close(): Unit = {
    log.info("Stopping running docker containers with contracts")
    contractReusedContainers.close()
    dockerEngine.stopAllRunningContainers()
    log.info("Stopping contract execution messages cache")
    contractExecutionMessagesCache.close()
  }
}

object ContractExecutionComponents extends ScorexLogging {

  def apply(
      settings: WESettings,
      schedulers: Schedulers,
      contractExecutionMessagesCache: ContractExecutionMessagesCache,
      blockchainUpdater: BlockchainUpdater with NG with MiningConstraintsHolder,
      nodeOwnerAccount: PrivateKeyAccount,
      utx: UtxPool,
      permissionValidator: PermissionValidator,
      time: NTP,
      wallet: Wallet,
      privacyApiService: PrivacyApiService,
      activePeerConnections: ActivePeerConnections,
      scheduler: SchedulerService
  )(implicit grpcSystem: ActorSystem): ContractExecutionComponents = {
    val nodeOwner = nodeOwnerAccount.toAddress

    val dockerEngineSettings    = settings.dockerEngine
    val apiSettings             = settings.api
    val dockerExecutorScheduler = schedulers.dockerExecutorScheduler

    val dockerEngine             = DockerEngine(dockerEngineSettings).fold(ex => throw ex, identity)
    val contractAuthTokenService = new ContractAuthTokenService()
    val contractReusedContainers = new ContractReusedContainers(dockerEngineSettings.removeContainerAfter)(dockerExecutorScheduler)

    val localDockerHostResolver = new LocalDockerHostResolver(dockerEngine.docker)

    val nodeRestApiSettings = NodeRestApiSettings
      .createApiSettings(
        localDockerHostResolver,
        dockerEngineSettings,
        apiSettings.rest.port
      )
      .fold(ex => throw ex, identity)
    val legacyContractExecutor =
      new LegacyContractExecutor(dockerEngine,
                                 dockerEngineSettings,
                                 nodeRestApiSettings,
                                 contractAuthTokenService,
                                 contractReusedContainers,
                                 dockerExecutorScheduler)
    val nodeGrpcApiSettings = NodeGrpcApiSettings
      .createApiSettings(localDockerHostResolver, dockerEngineSettings)
      .fold(ex => throw ex, identity)
    val grpcContractExecutor =
      new GrpcContractExecutor(dockerEngine,
                               dockerEngineSettings,
                               nodeGrpcApiSettings,
                               contractAuthTokenService,
                               contractReusedContainers,
                               dockerExecutorScheduler)
    val contractValidatorResultsStore = new ContractValidatorResultsStore()

    val delegatingState     = new DelegatingBlockchain(blockchainUpdater)
    val contractsApiService = new ContractsApiService(delegatingState, contractExecutionMessagesCache)
    val internalContractsApiRoute =
      new InternalContractsApiRoute(contractsApiService, apiSettings, time, contractAuthTokenService, nodeOwner, scheduler)

    val partialHandlers =
      Seq(
        AddressServicePowerApiHandler.partial(
          new AddressServiceImpl(new AddressApiService(delegatingState, wallet), contractAuthTokenService, dockerExecutorScheduler)),
        ContractServicePowerApiHandler.partial(
          new ContractServiceImpl(grpcContractExecutor, contractsApiService, contractAuthTokenService, dockerExecutorScheduler)),
        CryptoServicePowerApiHandler.partial(new CryptoServiceImpl(wallet, contractAuthTokenService, schedulers.cryptoServiceScheduler)),
        PermissionServicePowerApiHandler.partial(
          new PermissionServiceImpl(time, new PermissionApiService(blockchainUpdater), contractAuthTokenService, dockerExecutorScheduler)),
        PrivacyServicePowerApiHandler.partial(new PrivacyServiceImpl(privacyApiService, contractAuthTokenService, dockerExecutorScheduler)),
        UtilServicePowerApiHandler.partial(new UtilServiceImpl(time, contractAuthTokenService, dockerExecutorScheduler)),
        TransactionServicePowerApiHandler.partial(new TransactionServiceImpl(delegatingState, contractAuthTokenService, dockerExecutorScheduler))
      )

    new ContractExecutionComponents(
      settings,
      nodeOwnerAccount,
      dockerEngine,
      time,
      utx,
      blockchainUpdater,
      activePeerConnections,
      contractReusedContainers,
      legacyContractExecutor,
      grpcContractExecutor,
      contractExecutionMessagesCache,
      contractValidatorResultsStore,
      contractAuthTokenService,
      dockerExecutorScheduler,
      Seq(internalContractsApiRoute),
      partialHandlers,
      delegatingState
    )
  }
}
