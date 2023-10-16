package com.wavesenterprise.database.rocksdb.confidential

import cats.kernel.Monoid
import com.wavesenterprise.state.{ContractId, DataEntry}
import com.wavesenterprise.transaction.docker.ExecutedContractData
import cats.implicits._
import com.wavesenterprise.state.contracts.confidential.ConfidentialOutput

sealed abstract class ConfidentialDiff(val contractsData: Map[ContractId, ExecutedContractData])
case object EmptyConfidentialDiff                                                                   extends ConfidentialDiff(Map.empty)
case class ConfidentialDiffValue(override val contractsData: Map[ContractId, ExecutedContractData]) extends ConfidentialDiff(contractsData)

object ConfidentialDiff {
  def fromOutput(output: ConfidentialOutput): ConfidentialDiff = {
    if (output.entries.nonEmpty) {
      val keyValueMap  = output.entries.view.map(entry => entry.key -> entry).toMap[String, DataEntry[_]]
      val executedData = ExecutedContractData(keyValueMap)
      ConfidentialDiffValue(Map(output.contractId -> executedData))
    } else {
      EmptyConfidentialDiff
    }
  }

  implicit val confidentialMonoid: Monoid[ConfidentialDiff] = new Monoid[ConfidentialDiff] {
    override def empty: ConfidentialDiff = EmptyConfidentialDiff

    override def combine(diff1: ConfidentialDiff, diff2: ConfidentialDiff): ConfidentialDiff = (diff1, diff2) match {
      case (EmptyConfidentialDiff, EmptyConfidentialDiff)                                 => EmptyConfidentialDiff
      case (_, EmptyConfidentialDiff)                                                     => diff1
      case (EmptyConfidentialDiff, _)                                                     => diff2
      case (ConfidentialDiffValue(contractsData1), ConfidentialDiffValue(contractsData2)) => ConfidentialDiffValue(contractsData1 |+| contractsData2)
    }
  }
}
