package com.wavesenterprise.transaction.docker

import cats.Monoid
import com.wavesenterprise.state.DataEntry

/**
  * Storage for result of [[ExecutedContractTransaction]] execution
  */
case class ExecutedContractData(data: Map[String, DataEntry[_]]) {

  def filterKeys(keys: Set[String]): ExecutedContractData = {
    ExecutedContractData(data.filterKeys(keys.contains))
  }

  def toVector: Vector[DataEntry[_]] = {
    data.values.toVector
  }
}

object ExecutedContractData {

  implicit val contractDataMonoid: Monoid[ExecutedContractData] = new Monoid[ExecutedContractData] {

    override def empty: ExecutedContractData = ExecutedContractData(Map.empty)

    override def combine(x: ExecutedContractData, y: ExecutedContractData): ExecutedContractData = ExecutedContractData(x.data ++ y.data)
  }
}
