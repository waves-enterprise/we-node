package com.wavesenterprise.docker.grpc

import akka.stream.QueueOfferResult
import akka.stream.QueueOfferResult.{Dropped, Enqueued, Failure, QueueClosed}
import akka.stream.scaladsl.SourceQueueWithComplete
import com.wavesenterprise.docker.ContractExecutionError.{FatalErrorCode, RecoverableErrorCode}
import com.wavesenterprise.docker.ContractExecutor.{ContainerKey, ContractTxClaimContent}
import com.wavesenterprise.docker._
import com.wavesenterprise.metrics.docker.{ContractConnected, ContractExecutionMetrics, ExecContractTx}
import com.wavesenterprise.protobuf.service.contract.ContractTransactionResponse
import com.wavesenterprise.settings.dockerengine.DockerEngineSettings
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.docker.{CallContractTransaction, CreateContractTransaction, ExecutableTransaction}
import monix.eval.Task
import monix.execution.Scheduler
import play.api.libs.json.{JsPath, JsValue, Json, Reads}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.Promise

class GrpcContractExecutor(val dockerEngine: DockerEngine,
                           val dockerEngineSettings: DockerEngineSettings,
                           val nodeApiSettings: NodeGrpcApiSettings,
                           val contractAuthTokenService: ContractAuthTokenService,
                           val contractReusedContainers: ContractReusedContainers,
                           val scheduler: Scheduler)
    extends ContractExecutor {

  import GrpcContractExecutor._

  private[this] val executionIdGenerator = new AtomicLong()

  private val connections = new ConcurrentHashMap[ConnectionId, ConnectionValue]()
  private val executions  = new ConcurrentHashMap[String, ExecutionValue]()

  def addConnection(connectionId: ConnectionId,
                    connection: SourceQueueWithComplete[ContractTransactionResponse]): Either[ContractExecutionException, Unit] = {
    for {
      connectionValue <- getConnectionValue(connectionId)
      promise = connectionValue.connectionPromise
      _       = log.debug(s"New connection with id '${connectionId.value}' from container '${connectionValue.containerId}'")
      _       = scheduler.executeAsync(() => promise.success(connection))
    } yield ()
  }

  def commitExecutionResults(executionId: String, txId: ByteStr, results: List[DataEntry[_]]): Either[ContractExecutionException, Unit] = {
    commitExecution(executionId, txId, ContractExecutionSuccess(results))
  }

  def commitExecutionError(executionId: String, txId: ByteStr, message: String, code: Int): Either[ContractExecutionException, Unit] = {
    commitExecution(executionId, txId, ContractExecutionError(code, message))
  }

  private def commitExecution(executionId: String, txId: ByteStr, execution: ContractExecution): Either[ContractExecutionException, Unit] = {
    for {
      executionValue <- Option(executions.get(executionId))
        .toRight(new ContractExecutionException(s"Transaction execution is not found for executionId '$executionId' and txId '$txId'"))
      promise = executionValue.executionPromise
      _ <- Either.cond(txId == executionValue.txId, (), new ContractExecutionException(s"Execution result is not for txId '${executionValue.txId}'"))
      _ = log.trace(s"New execution '$execution' for transaction with id '$txId'")
      _ = executions.remove(executionId)
    } yield {
      scheduler.executeAsync { () =>
        execution match {
          case ContractExecutionError(errorCode, message) =>
            errorCode match {
              case RecoverableErrorCode =>
                promise.failure(new ContractExecutionException(message, Some(RecoverableErrorCode)))
              case FatalErrorCode =>
                promise.success(execution)
              case unknownCode =>
                promise.success(ContractExecutionError(unknownCode, s"$message. Unknown contract execution error code '$unknownCode'"))
            }
          case _ => promise.success(execution)
        }
      }
    }
  }

  private def createContainerEnvParams(connectionId: ConnectionId): List[String] = {
    List(
      s"CONNECTION_ID=${connectionId.value}",
      s"CONNECTION_TOKEN=${contractAuthTokenService.create(ConnectionClaimContent(connectionId), dockerEngineSettings.contractAuthExpiresIn)}",
      s"NODE=${nodeApiSettings.node}",
      s"NODE_PORT=${nodeApiSettings.grpcApiPort}"
    )
  }

  override protected def startContainer(contract: ContractInfo, metrics: ContractExecutionMetrics): Task[String] = Task.defer {
    val containerKey = ContainerKey(contract)
    val connectionId = ConnectionId(containerKey)
    val envParams    = createContainerEnvParams(connectionId)

    deferEither(dockerEngine.createAndStartContainer(contract, metrics, envParams))
  }

  override protected def waitConnection(containerId: String, contract: ContractInfo, metrics: ContractExecutionMetrics): Task[Unit] = Task.defer {
    val containerKey      = ContainerKey(contract)
    val connectionId      = ConnectionId(containerKey)
    val connectionPromise = Promise[SourceQueueWithComplete[ContractTransactionResponse]]()
    connections.put(connectionId, ConnectionValue(connectionPromise, containerKey, containerId))

    val connectionTask = Task
      .fromFuture(connectionPromise.future)
      .timeoutTo(dockerEngineSettings.executionLimits.startupTimeout, handleConnectionTimeout(contract, containerId))
      .void
    metrics.measureTask(ContractConnected, connectionTask)
  }

  private def handleConnectionTimeout(contract: ContractInfo, containerId: String): Task[SourceQueueWithComplete[ContractTransactionResponse]] =
    Task.raiseError(
      new ContractExecutionException(s"Container '$containerId' startup timeout for image '${contract.image}', imageId '${contract.imageHash}'"))

  override protected def executeCall(containerId: String,
                                     contract: ContractInfo,
                                     tx: CallContractTransaction,
                                     metrics: ContractExecutionMetrics): Task[ContractExecution] = {
    executeTx(contract, tx, metrics)
  }

  override protected def executeCreate(containerId: String,
                                       contract: ContractInfo,
                                       tx: CreateContractTransaction,
                                       metrics: ContractExecutionMetrics): Task[ContractExecution] = {
    executeTx(contract, tx, metrics)
  }

  private def executeTx(contract: ContractInfo, tx: ExecutableTransaction, metrics: ContractExecutionMetrics): Task[ContractExecution] = Task.defer {
    val connectionId = ConnectionId(ContainerKey(contract))

    val resultTask = for {
      connectionValue <- Task.fromEither(getConnectionValue(connectionId))
      connection      <- Task.fromFuture(connectionValue.connectionPromise.future)
      executionPromise    = Promise[ContractExecution]()
      executionId         = executionIdGenerator.incrementAndGet().toString
      _                   = executions.put(executionId, ExecutionValue(executionPromise, tx.id()))
      claimContent        = ContractTxClaimContent(tx.id(), contract.contractId, executionId)
      authToken           = contractAuthTokenService.create(claimContent, dockerEngineSettings.contractAuthExpiresIn)
      contractTransaction = ProtoObjectsMapper.mapToProto(tx)
      contractTxResponse  = ContractTransactionResponse(Some(contractTransaction), authToken)
      _                   = log.trace(s"Executing transaction with id '${tx.id()}' using container '${connectionValue.containerId}''")
      offerResult <- Task.deferFuture(connection.offer(contractTxResponse))
      _           <- handleOfferResult(offerResult, tx)
      result      <- Task.fromFuture(executionPromise.future)
    } yield result

    metrics.measureTask(ExecContractTx, resultTask)
  }

  private def handleOfferResult(result: QueueOfferResult, tx: ExecutableTransaction): Task[Unit] = {
    result match {
      case Enqueued =>
        log.trace(s"Executable transaction '${tx.id()}' is successfully enqueued by stream")
        Task.unit
      case Dropped     => Task.raiseError(new ContractExecutionException(s"Executable transaction '${tx.id()}' is dropped by stream"))
      case Failure(e)  => Task.raiseError(new ContractExecutionException(s"Stream failed to enqueue executable transaction '${tx.id()}'", e))
      case QueueClosed => Task.raiseError(new ContractExecutionException(s"Stream is closed (completed during transaction '${tx.id()}' enqueue)"))
    }
  }

  def removeConnection(connectionId: ConnectionId): Unit =
    getConnectionValue(connectionId).foreach {
      case ConnectionValue(_, containerKey, _) =>
        contractReusedContainers.invalidate(containerKey)
    }

  override protected def invalidateContainer(containerKey: ContainerKey, containerId: String): Unit = {
    val connectionId = ConnectionId(containerKey)
    removeConnectionId(connectionId)
    super.invalidateContainer(containerKey, containerId)
  }

  private def removeConnectionId(connectionId: ConnectionId): Unit = {
    connections.remove(connectionId)
  }

  private def getConnectionValue(connectionId: ConnectionId): Either[ContractExecutionException, ConnectionValue] =
    Option(connections.get(connectionId)).toRight(new ContractExecutionException(s"Unknown connection id '${connectionId.value}'"))
}

object GrpcContractExecutor {

  case class ConnectionId(value: String) extends AnyVal

  object ConnectionId {
    def apply(containerKey: ContainerKey): ConnectionId = new ConnectionId(Integer.toHexString(containerKey.hashCode()))
  }

  case class ConnectionValue(connectionPromise: Promise[SourceQueueWithComplete[ContractTransactionResponse]],
                             containerKey: ContainerKey,
                             containerId: String)

  case class ExecutionValue(executionPromise: Promise[ContractExecution], txId: ByteStr)

  case class ConnectionClaimContent(connectionId: ConnectionId) extends ClaimContent {
    override def toJson: JsValue = Json.obj("connectionId" -> connectionId.value)
  }

  object ConnectionClaimContent {
    implicit val reads: Reads[ConnectionClaimContent] =
      (JsPath \ "connectionId").read[String].map(ConnectionId(_)).map(new ConnectionClaimContent(_))
  }
}
