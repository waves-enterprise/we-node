package com.wavesenterprise.wasm

import com.google.common.io.ByteArrayDataOutput
import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.database.rocksdb.confidential.PersistentConfidentialState
import com.wavesenterprise.docker._
import com.wavesenterprise.metrics.docker.{ContractExecutionMetrics, ExecContractTx}
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.settings.wasm.WASMSettings
import com.wavesenterprise.state.contracts.confidential.ConfidentialInput
import com.wavesenterprise.state.{Blockchain, ContractId, DataEntry}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps.toBytes
import com.wavesenterprise.transaction.docker.{CallContractTransactionV7, CreateContractTransaction, ExecutableTransaction, UpdateContractTransaction}
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.wasm.WASMContractExecutor.{FuncNotFoundException, wasmExecutorInstance}
import com.wavesenterprise.wasm.WASMServiceImpl.WEVMExecutionException
import com.wavesenterprise.wasm.core.WASMExecutor
import com.wavesenterprise.{ContractExecutor, getWasmContract}
import monix.eval.Task

class WASMContractExecutor(
    blockchain: Blockchain,
    settings: WASMSettings,
    persistentConfidentialState: PersistentConfidentialState
) extends ContractExecutor with ScorexLogging {

  log.info(s"Created WASM contract executor with settings $settings")

  private val executor = wasmExecutorInstance

  private def getParamsFromTx(tx: ExecutableTransaction, maybeConfidentialInput: Option[ConfidentialInput]): List[DataEntry[_]] = {
    maybeConfidentialInput.fold(tx.params) { confidentialInput =>
      tx match {
        case callV7: CallContractTransactionV7 if callV7.inputCommitmentOpt.nonEmpty => confidentialInput.entries
        case _                                                                       => tx.params
      }
    }
  }

  private def dataEntryWrite(value: DataEntry[_], output: ByteArrayDataOutput): Unit = {
    output.write(toBytes(value))
  }

  private def getArgs(params: List[DataEntry[_]]): Array[Byte] = {
    val ndo = newDataOutput()
    BinarySerializer.writeShortIterable(params, dataEntryWrite, ndo)
    ndo.toByteArray
  }

  private def executionError(code: Int, message: String) = {
    val errStr = if (message == "") "" else s" and message $message"
    ContractExecutionError(code, s"contract failed with error code $code$errStr")
  }

  private def runContract(
      contract: ContractInfo,
      tx: ExecutableTransaction,
      funcName: String,
      maybeConfidentialInput: Option[ConfidentialInput],
      metrics: ContractExecutionMetrics
  ): Task[ContractExecution] = {
    val cid      = ContractId(contract.contractId)
    val bytecode = getWasmContract(contract).bytecode
    val service  = new WASMServiceImpl(cid, tx, blockchain, persistentConfidentialState)

    val task = metrics.measureTask(
      ExecContractTx,
      Task.eval {
        (executor.runContract(
           cid.byteStr.arr,
           bytecode,
           funcName,
           getArgs(getParamsFromTx(tx, maybeConfidentialInput)),
           settings.fuelLimit,
           service
         ),
         "")
      }
    )

    task.onErrorRecover {
      case WEVMExecutionException(errCode, msg) =>
        log.debug(s"tx ${tx.id.value()} failed with code $errCode: $msg")
        (errCode, msg)
      case err =>
        log.error(s"unhandled error in WASMExecutor: ${err.getMessage}")
        (2, s"${err.getMessage}")
    }.map {
      case (0, _)         => service.getContractExecution
      case (errCode, msg) => executionError(errCode, msg)
    }
  }

  private def validateContract(contract: ContractInfo): Task[ContractExecution] = {
    val bytecode       = getWasmContract(contract).bytecode
    val validationCode = executor.validateBytecode(bytecode)

    Task.pure {
      if (validationCode != 0) executionError(validationCode, s"validation code is $validationCode")
      else ContractExecutionSuccessV2(Map.empty, Map.empty)
    }
  }

  override def executeTransaction(
      contract: ContractInfo,
      tx: ExecutableTransaction,
      maybeConfidentialInput: Option[ConfidentialInput],
      metrics: ContractExecutionMetrics
  ): Task[ContractExecution] = {
    tx match {
      case create: CreateContractTransaction => runContract(contract, create, "_constructor", maybeConfidentialInput, metrics)
      case call: CallContractTransactionV7 =>
        runContract(contract, call, call.callFunc.getOrElse(throw FuncNotFoundException(tx)), maybeConfidentialInput, metrics)
      case update: UpdateContractTransaction => validateContract(contract)
      case _                                 => throw new IllegalArgumentException(s"illegal tx executed: ${tx.json.value()}")
    }
  }
}

object WASMContractExecutor {

  val wasmExecutorInstance = new WASMExecutor()

  case class FuncNotFoundException(tx: ExecutableTransaction) extends Throwable
}
