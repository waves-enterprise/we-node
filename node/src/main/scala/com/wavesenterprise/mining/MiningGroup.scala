package com.wavesenterprise.mining

import com.wavesenterprise.docker.{ContractExecutor, ContractInfo}
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.transaction.docker.ExecutableTransaction
import com.wavesenterprise.transaction.{AtomicTransaction, Transaction}
import com.wavesenterprise.utils.pki.CrlCollection
import monix.eval.Task

sealed trait GroupKey {
  def groupParallelism: Int
  def description: String
}

sealed trait TransactionConfirmationSetup {
  def groupKey: GroupKey
}

case class ContractExecutionGroupKey(groupParallelism: Int) extends GroupKey {
  override def description: String = s"contract execution group"

  override def equals(that: Any): Boolean =
    that match {
      case that: ContractExecutionGroupKey => eq(that) || groupParallelism == that.groupParallelism
      case _                               => false
    }

  override def hashCode(): Int = groupParallelism.hashCode()
}

case object SimpleGroupKey extends GroupKey {
  override def groupParallelism: Int = 1
  override def description: String   = s"simple transaction group"
}

case object AtomicGroupKey extends GroupKey {
  override def groupParallelism: Int = 1
  override def description: String   = s"atomic transaction group"
}

case class ExecutableTxSetup(tx: ExecutableTransaction,
                             executor: ContractExecutor,
                             info: ContractInfo,
                             parallelism: Int,
                             maybeCertChainWithCrl: Option[(CertChain, CrlCollection)])
    extends TransactionConfirmationSetup {
  val groupKey: ContractExecutionGroupKey = ContractExecutionGroupKey(parallelism)
}

case class SimpleTxSetup(tx: Transaction, maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]) extends TransactionConfirmationSetup {
  override def groupKey: GroupKey = SimpleGroupKey
}

case class AtomicSimpleSetup(tx: AtomicTransaction, innerSetups: List[SimpleTxSetup], maybeCertChainWithCrl: Option[(CertChain, CrlCollection)])
    extends TransactionConfirmationSetup {
  override def groupKey: GroupKey = AtomicGroupKey
}

case class AtomicComplexSetup(tx: AtomicTransaction,
                              innerSetupTasks: List[Task[TransactionConfirmationSetup]],
                              maybeCertChainWithCrl: Option[(CertChain, CrlCollection)])
    extends TransactionConfirmationSetup {
  override def groupKey: GroupKey = AtomicGroupKey
}
