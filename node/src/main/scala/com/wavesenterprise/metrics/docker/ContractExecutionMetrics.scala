package com.wavesenterprise.metrics.docker

import java.util.concurrent.TimeUnit

import com.wavesenterprise.metrics.{Metrics, MetricsType}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.docker.ExecutableTransaction
import monix.eval.Task
import org.influxdb.dto.Point

/**
  * Metrics measurements types used for docker contracts
  */
sealed trait MeasurementType {
  def name: String
}

case object PullImage extends MeasurementType {
  override def name: String = "pull_image"
}

case object CreateContainer extends MeasurementType {
  override def name: String = "create_container"
}

case object StartContainer extends MeasurementType {
  override def name: String = "start_container"
}

case object RunInContainer extends MeasurementType {
  override def name: String = "run_in_container"
}

case object ContractConnected extends MeasurementType {
  override def name: String = "contract_connected"
}

case object ParseContractResults extends MeasurementType {
  override def name: String = "parse_contract_results"
}

case object ExecContractTx extends MeasurementType {
  override def name: String = "exec_contract_tx"
}

case object UpdateContractTx extends MeasurementType {
  override def name: String = "update_contract_tx"
}

case object CreateExecutedTx extends MeasurementType {
  override def name: String = "create_executed_tx"
}

private case object MineContractTx extends MeasurementType {
  override def name: String = "mine_contract_tx"
}

case object ProcessContractTx extends MeasurementType {
  override def name: String = "process_contract_tx"
}

case object MvccConflict extends MeasurementType {
  override def name: String = "mvcc_conflict"
}

object ContractExecutionMetrics {

  private val MetricsPrefix = "contract_execution"

  def apply(tx: ExecutableTransaction): ContractExecutionMetrics = ContractExecutionMetrics(tx.contractId, tx.id(), tx.txType)
}

case class ContractExecutionMetrics(contractId: ByteStr, txId: ByteStr, txType: Long) {

  import ContractExecutionMetrics._

  private val executionStart = System.currentTimeMillis()

  def measure[A](measurementType: MeasurementType, f: => A): A = {
    val start = System.currentTimeMillis()
    val res   = f
    val end   = System.currentTimeMillis()
    write(measurementType, Some(end - start))
    res
  }

  def measureEither[R, L](measurementType: MeasurementType, either: => Either[L, R]): Either[L, R] = {
    val start = System.currentTimeMillis()
    either.map { result =>
      val end = System.currentTimeMillis()
      write(measurementType, Some(end - start))
      result
    }
  }

  def measureTask[A](measurementType: MeasurementType, task: Task[A]): Task[A] = {
    Task
      .defer {
        val start = System.currentTimeMillis()
        task.map { result =>
          {
            val end = System.currentTimeMillis()
            write(measurementType, Some(end - start))
            result
          }
        }
      }
  }

  def markContractTxMined(): Unit = write(MineContractTx, Some(System.currentTimeMillis() - executionStart))

  def markMvccConflict(): Unit = write(MvccConflict)

  private def write(measurementType: MeasurementType, maybeExecTime: Option[Long] = None): Unit = {
    val point = Point
      .measurement(s"${MetricsPrefix}_${measurementType.name}")
      .tag("contract_id", contractId.toString)
      .tag("tx_type", txType.toString)
      .tag("tx_id", txId.toString)
      .time(executionStart, TimeUnit.MICROSECONDS)

    Metrics.writeWithoutTime(
      MetricsType.Contract,
      maybeExecTime.fold(point)(execTime => point.addField("exec_time", execTime))
    )
  }
}
