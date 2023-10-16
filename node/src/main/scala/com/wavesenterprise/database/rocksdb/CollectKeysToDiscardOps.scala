package com.wavesenterprise.database.rocksdb

import com.wavesenterprise.state.ContractId
import com.wavesenterprise.transaction.docker.ExecutedContractTransaction

import scala.collection.mutable

trait CollectKeysToDiscardOps {

  protected def collectKeysToDiscard(tx: ExecutedContractTransaction, keysToDiscard: mutable.Map[ContractId, Set[String]]): Unit = {
    val contractId = ContractId(tx.tx.contractId)
    val updated    = keysToDiscard.getOrElse(contractId, Set.empty) ++ tx.results.map(_.key)
    keysToDiscard += contractId -> updated
  }
}
