package com.wavesenterprise.utx

import cats.implicits.catsSyntaxOptionId
import com.wavesenterprise.account.Address
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.network.contracts.{ConfidentialDataId, ConfidentialDataType}
import com.wavesenterprise.state.{Blockchain, ContractId}
import com.wavesenterprise.transaction.{ConfidentialContractDataUpdate, Transaction}
import com.wavesenterprise.transaction.docker.{ConfidentialDataInCallContractSupported, ExecutableTransaction}
import com.wavesenterprise.utils.ScorexLogging
import monix.execution.Scheduler
import monix.reactive.subjects.ConcurrentSubject

trait ConfidentialOps extends ScorexLogging {

  def blockchain: Blockchain

  def nodeOwner: Address

  def confidentialScheduler: Scheduler

  protected val confidentialTransactionsInternal: ConcurrentSubject[ConfidentialContractDataUpdate, ConfidentialContractDataUpdate] =
    ConcurrentSubject.publish[ConfidentialContractDataUpdate](confidentialScheduler)

  protected def addToConfidentialTransactions(tx: Transaction): Unit = (confidentialContractInfo(tx), tx) match {
    case (Some(contractInfo), tx: ConfidentialDataInCallContractSupported)
        if nodeIsContractValidator() && contractInfo.groupParticipants.contains(nodeOwner) =>
      confidentialTransactionsInternal.onNext {
        log.trace(s"Confidential tx '${tx.id()}' has been added to the stream")
        ConfidentialContractDataUpdate(
          ConfidentialDataId(ContractId(contractInfo.contractId), tx.inputCommitment, ConfidentialDataType.Input),
          tx.timestamp.some
        )
      }
    case _ => ()
  }

  private def nodeIsContractValidator(): Boolean = blockchain.lastBlockContractValidators.contains(nodeOwner)

  private def confidentialContractInfo(tx: Transaction): Option[ContractInfo] = contractInfo(tx).filter(_.isConfidential)

  private def contractInfo(tx: Transaction): Option[ContractInfo] = tx match {
    case executableTx: ExecutableTransaction => blockchain.contract(ContractId(executableTx.contractId))
    case _                                   => None
  }

}
