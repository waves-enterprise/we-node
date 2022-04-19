package com.wavesenterprise.docker

import cats.implicits._
import com.wavesenterprise.docker.ContractExecutor.{ContractSuccessCode, ContractTxClaimContent}
import com.wavesenterprise.metrics.docker.{ContractExecutionMetrics, ExecContractTx, ParseContractResults}
import com.wavesenterprise.settings.dockerengine.DockerEngineSettings
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.docker.{CallContractTransaction, CreateContractTransaction, ExecutableTransaction}
import monix.eval.Task
import monix.execution.Scheduler
import play.api.libs.json.Json

class LegacyContractExecutor(val dockerEngine: DockerEngine,
                             val dockerEngineSettings: DockerEngineSettings,
                             val nodeApiSettings: NodeRestApiSettings,
                             val contractAuthTokenService: ContractAuthTokenService,
                             val contractReusedContainers: ContractReusedContainers,
                             val scheduler: Scheduler)
    extends ContractExecutor {

  private val containerEnvParams =
    List(
      s"NODE=${nodeApiSettings.node}",
      s"NODE_PORT=${nodeApiSettings.restApiPort}",
      s"NODE_API=${nodeApiSettings.nodeRestAPI}"
    )

  protected override def startContainer(contract: ContractInfo, metrics: ContractExecutionMetrics): Task[String] = deferEither {
    dockerEngine.createAndStartContainer(contract, metrics, containerEnvParams)
  }

  override protected def executeCreate(containerId: String,
                                       contract: ContractInfo,
                                       tx: CreateContractTransaction,
                                       metrics: ContractExecutionMetrics): Task[ContractExecution] = Task.defer {
    val task = deferEither(dockerEngine.executeRunScript(containerId, createEnvParams("CREATE", tx), metrics))
    metrics.measureTask(ExecContractTx, task).flatMap(toContractExecution(_, metrics))
  }

  override protected def executeCall(containerId: String,
                                     contract: ContractInfo,
                                     tx: CallContractTransaction,
                                     metrics: ContractExecutionMetrics): Task[ContractExecution] = Task.defer {
    val task = deferEither(dockerEngine.executeRunScript(containerId, createEnvParams("CALL", tx), metrics))
    metrics.measureTask(ExecContractTx, task).flatMap(toContractExecution(_, metrics))
  }

  private def createEnvParams(command: String, tx: ExecutableTransaction): Map[String, String] = {
    Map(
      "TX"        -> tx.json().toString(),
      "COMMAND"   -> command,
      "API_TOKEN" -> contractAuthTokenService.create(ContractTxClaimContent(tx.id(), tx.contractId), dockerEngineSettings.contractAuthExpiresIn)
    )
  }

  private def toContractExecution(result: (Int, String), metrics: ContractExecutionMetrics): Task[ContractExecution] = Task.defer {
    result match {
      case (ContractSuccessCode, string: String) =>
        if (string.isEmpty) {
          Task.now(ContractExecutionSuccess(List.empty))
        } else {
          metrics.measureTask(ParseContractResults, deferEither(parseContractResult(string)))
        }
      case (code: Int, message: String) =>
        Task.now(ContractExecutionError(code, message))
    }
  }

  private def parseContractResult(string: String): Either[ContractExecutionException, ContractExecution] = {
    (for {
      parsed <- Either.catchNonFatal(Json.parse(string)).leftMap(_ -> s"Can't parse and validate contract execution result '$string' as JSON")
      success <- Either
        .catchNonFatal(ContractExecutionSuccess(parsed.as[List[DataEntry[_]]]))
        .leftMap(_ -> s"Can't parse contract execution result '$string' as results array")
    } yield success).leftMap(ContractExecutionException.apply)
  }
}
