package com.wavesenterprise

import com.wavesenterprise.docker.{ContractExecution, ContractInfo}
import com.wavesenterprise.metrics.docker.ContractExecutionMetrics
import com.wavesenterprise.state.contracts.confidential.ConfidentialInput
import com.wavesenterprise.transaction.docker.{CallContractTransaction, CreateContractTransaction, ExecutableTransaction}
import monix.eval.Task

import scala.concurrent.Future

trait ContractExecutor {

  def executeTransaction(
      contract: ContractInfo,
      tx: ExecutableTransaction,
      maybeConfidentialInput: Option[ConfidentialInput],
      metrics: ContractExecutionMetrics
  ): Task[ContractExecution]

  def inspectOrPullContract(contract: ContractInfo, metrics: ContractExecutionMetrics): Future[Unit]

  protected def executeCall(containerId: String,
                            contract: ContractInfo,
                            tx: CallContractTransaction,
                            maybeConfidentialInput: Option[ConfidentialInput],
                            metrics: ContractExecutionMetrics): Task[ContractExecution]

  protected def executeCreate(containerId: String,
                              contract: ContractInfo,
                              tx: CreateContractTransaction,
                              metrics: ContractExecutionMetrics): Task[ContractExecution]
}
