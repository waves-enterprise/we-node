package com.wavesenterprise.wasm

import com.google.common.io.ByteArrayDataOutput
import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.docker._
import com.wavesenterprise.metrics.docker.{ContractExecutionMetrics, ExecContractTx}
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.settings.wasm.WASMSettings
import com.wavesenterprise.state.contracts.confidential.ConfidentialInput
import com.wavesenterprise.state.{Blockchain, ContractId, DataEntry}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps.toBytes
import com.wavesenterprise.transaction.docker.{
  CallContractTransaction,
  CallContractTransactionV7,
  CreateContractTransaction,
  ExecutableTransaction,
  UpdateContractTransactionV6
}
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.wasm.WASMContractExecutor.{FuncNotFoundException, wasmExecutorInstance}
import com.wavesenterprise.wasm.WASMServiceImpl.WEVMExecutionException
import com.wavesenterprise.wasm.core.WASMExecutor
import com.wavesenterprise.{ContractExecutor, getWasmContract}
import monix.eval.Task

import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class WASMContractExecutor(
    blockchain: Blockchain,
    settings: WASMSettings
) extends ContractExecutor with ScorexLogging {

  private val executor = wasmExecutorInstance

  private def injectConfidentialInput(tx: ExecutableTransaction, maybeConfidentialInput: Option[ConfidentialInput]): ExecutableTransaction = {
    maybeConfidentialInput.fold(tx) { confidentialInput =>
      tx match {
        case callV7: CallContractTransactionV7 => callV7.copy(params = confidentialInput.entries)
        case _                                 => tx
      }
    }
  }

  private def getFunc(tx: ExecutableTransaction): String = {
    tx match {
      case _: CreateContractTransaction   => "_constructor"
      case _: UpdateContractTransactionV6 => "update"
      case call: CallContractTransactionV7 =>
        call.callFunc.getOrElse(throw FuncNotFoundException(tx))
      case _ => throw new IllegalArgumentException(s"illegal tx executed: ${tx.json.value()}")
    }
  }

  def dataEntryWrite(value: DataEntry[_], output: ByteArrayDataOutput): Unit = {
    output.write(toBytes(value))
  }

  def getArgs(params: List[DataEntry[_]]): Array[Byte] = {
    val ndo = newDataOutput()
    BinarySerializer.writeShortIterable(params, dataEntryWrite, ndo)
    ndo.toByteArray
  }
  private def executionError(code: Int) = ContractExecutionError(code, s"contract failed with error code $code")

  override def executeTransaction(
      contract: ContractInfo,
      tx: ExecutableTransaction,
      maybeConfidentialInput: Option[ConfidentialInput],
      metrics: ContractExecutionMetrics
  ): Task[ContractExecution] = {
    val bytecode       = getWasmContract(contract).bytecode
    val validationCode = executor.validateBytecode(bytecode)
    val cid            = ContractId(contract.contractId)
    val service        = new WASMServiceImpl(cid, tx, blockchain)

    if (getFunc(tx) == "update") {
      Task.pure {
        if (validationCode != 0) executionError(validationCode)
        else ContractExecutionSuccessV2(Map.empty, Map.empty)
      }
    } else {
      val task = metrics.measureTask(
        ExecContractTx,
        Task.eval {
          executor.runContract(
            cid.byteStr.arr,
            bytecode,
            getFunc(tx),
            getArgs(injectConfidentialInput(tx, maybeConfidentialInput).params),
            service
          )
        } timeoutTo (
          settings.timeout,
          Task.raiseError[Int](new ContractExecutionException(s"Contract '${contract.contractId}' execution timeout'"))
        )
      )

      task.onErrorRecover {
        case WEVMExecutionException(errCode, msg) =>
          log.debug(s"tx ${tx.id.value()} failed with code $errCode: $msg")
          errCode
        case err =>
          log.error(s"unhandled error in WASMExecutor: ${err.getMessage}")
          2
      }.map {
        case 0       => service.getContractExecution
        case errCode => executionError(errCode)
      }
    }
  }

  override def inspectOrPullContract(
      contract: ContractInfo,
      metrics: ContractExecutionMetrics
  ): Future[Unit] = Future.unit

  override protected def executeCall(
      containerId: String,
      contract: ContractInfo,
      tx: CallContractTransaction,
      maybeConfidentialInput: Option[ConfidentialInput],
      metrics: ContractExecutionMetrics
  ): Task[ContractExecution] = {
    executeTransaction(contract, tx, maybeConfidentialInput, metrics)
  }

  override protected def executeCreate(
      containerId: String,
      contract: ContractInfo,
      tx: CreateContractTransaction,
      metrics: ContractExecutionMetrics
  ): Task[ContractExecution] = {
    executeTransaction(contract, tx, None, metrics)
  }
}

object WASMContractExecutor {

  val wasmExecutorInstance = new WASMExecutor()

  case class FuncNotFoundException(tx: ExecutableTransaction) extends Throwable
}
