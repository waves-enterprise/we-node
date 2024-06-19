package com.wavesenterprise

import com.wavesenterprise.docker.{ContractExecution, ContractInfo}
import com.wavesenterprise.metrics.docker.ContractExecutionMetrics
import com.wavesenterprise.state.contracts.confidential.ConfidentialInput
import com.wavesenterprise.transaction.docker.ExecutableTransaction
import monix.eval.Task

trait ContractExecutor {

  def executeTransaction(
      contract: ContractInfo,
      tx: ExecutableTransaction,
      maybeConfidentialInput: Option[ConfidentialInput],
      metrics: ContractExecutionMetrics
  ): Task[ContractExecution]
}
