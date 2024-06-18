package com.wavesenterprise.utx

import cats.implicits.catsSyntaxOptionId
import com.wavesenterprise.account.Address
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.network.contracts.{ConfidentialDataId, ConfidentialDataType}
import com.wavesenterprise.state.{Blockchain, ContractId}
import com.wavesenterprise.transaction.docker.{
  CallContractTransactionV6,
  CallContractTransactionV7,
  CreateContractTransactionV6,
  ExecutableTransaction
}
import com.wavesenterprise.transaction.{AtomicTransaction, ConfidentialContractDataUpdate, Transaction}
import com.wavesenterprise.utils.ScorexLogging
import monix.execution.Scheduler
import monix.reactive.subjects.ConcurrentSubject

// TODO: refactor heavily nested match case and if
trait ConfidentialOps extends ScorexLogging {

  def blockchain: Blockchain

  def nodeOwner: Address

  def confidentialScheduler: Scheduler

  protected val confidentialTransactionsInternal: ConcurrentSubject[ConfidentialContractDataUpdate, ConfidentialContractDataUpdate] =
    ConcurrentSubject.publish[ConfidentialContractDataUpdate](confidentialScheduler)

  private def getInputCommitment(executableTx: ExecutableTransaction): Option[Commitment] = executableTx match {
    case callTxV6: CallContractTransactionV6 => callTxV6.inputCommitment.some
    case callTxV7: CallContractTransactionV7 => callTxV7.inputCommitmentOpt
    case _                                   => None
  }

  protected def addToConfidentialTransactions(tx: Transaction): Unit = tx match {
    case exTx: ExecutableTransaction =>
      (contractInfo(tx, Set.empty[ContractInfo]), getInputCommitment(exTx)) match {
        case (Some(contractInfo), Some(inputCommitment))
            if nodeIsContractValidator() && contractInfo.groupParticipants.contains(nodeOwner) =>
          confidentialTransactionsInternal.onNext {
            log.trace(s"Confidential tx '${tx.id()}' has been added to the stream")
            ConfidentialContractDataUpdate(
              ConfidentialDataId(ContractId(contractInfo.contractId), inputCommitment, ConfidentialDataType.Input),
              tx.timestamp.some
            )
          }
        case _ => ()
      }
    case atomic: AtomicTransaction =>
      atomic.transactions.foldLeft(Set.empty[ContractInfo]) {
        case (state, tx) => (contractInfo(tx, state), tx) match {
            case (Some(contractInfo), tx: ExecutableTransaction) =>
              getInputCommitment(tx)
                .fold(state) { inputCommitment =>
                  {
                    if (nodeIsContractValidator() && contractInfo.groupParticipants.contains(nodeOwner)) {
                      confidentialTransactionsInternal.onNext {
                        log.trace(s"Confidential tx '${tx.id()}' has been added to the stream")
                        ConfidentialContractDataUpdate(
                          ConfidentialDataId(ContractId(contractInfo.contractId), inputCommitment, ConfidentialDataType.Input, Some(contractInfo)),
                          tx.timestamp.some
                        )
                      }
                    }
                    state
                  }
                }
            case (None, tx: CreateContractTransactionV6) =>
              state + ContractInfo(tx)
            case _ => state
          }
      }
    case _ => ()
  }

  private def nodeIsContractValidator(): Boolean = blockchain.lastBlockContractValidators.contains(nodeOwner)

  private def contractInfo(tx: Transaction, state: Set[ContractInfo]): Option[ContractInfo] = tx match {
    case executableTx: ExecutableTransaction =>
      blockchain.contract(ContractId(executableTx.contractId))
        .orElse(state.find(_.contractId == executableTx.contractId))
        .filter(_.isConfidential)
    case _ => None
  }

}
