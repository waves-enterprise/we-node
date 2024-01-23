package com.wavesenterprise.mining

import com.wavesenterprise.ContractExecutor
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.transaction.docker.ExecutableTransaction
import com.wavesenterprise.state.contracts.confidential.ConfidentialInput
import com.wavesenterprise.transaction.docker.CallContractTransactionV6
import com.wavesenterprise.transaction.{AtomicTransaction, Transaction}
import com.wavesenterprise.utils.pki.CrlCollection
import monix.eval.Task

sealed trait GroupKey {
  def groupParallelism: Int
  def description: String
}

sealed trait TransactionConfirmationSetup {
  def groupKey: GroupKey

  def tx: Transaction
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

case object ConfidentialSmartContractGroupKey extends GroupKey {
  override def groupParallelism: Int = 1
  override def description: String   = s"confidential smart contract group"
}

case object AtomicGroupKey extends GroupKey {
  override def groupParallelism: Int = 1
  override def description: String   = s"atomic transaction group"
}

sealed trait ExecutableSetup extends TransactionConfirmationSetup {
  def tx: ExecutableTransaction
  def executor: ContractExecutor
  def info: ContractInfo
  def maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]
}

case class ConfidentialCallPermittedSetup(override val tx: CallContractTransactionV6,
                                          executor: ContractExecutor,
                                          info: ContractInfo,
                                          input: ConfidentialInput,
                                          maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]) extends ExecutableSetup {
  override def groupKey: GroupKey = ConfidentialSmartContractGroupKey
}

case class ConfidentialExecutableUnpermittedSetup(override val tx: ExecutableTransaction,
                                                  info: ContractInfo,
                                                  maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]) extends TransactionConfirmationSetup {
  override def groupKey: GroupKey = ConfidentialSmartContractGroupKey
}

case class DefaultExecutableTxSetup(override val tx: ExecutableTransaction,
                                    override val executor: ContractExecutor,
                                    override val info: ContractInfo,
                                    parallelism: Int,
                                    override val maybeCertChainWithCrl: Option[(CertChain, CrlCollection)])
    extends ExecutableSetup {
  val groupKey: ContractExecutionGroupKey = ContractExecutionGroupKey(parallelism)
}

case class SimpleTxSetup(override val tx: Transaction, maybeCertChainWithCrl: Option[(CertChain, CrlCollection)])
    extends TransactionConfirmationSetup {
  override def groupKey: GroupKey = SimpleGroupKey
}

case class AtomicSimpleSetup(override val tx: AtomicTransaction,
                             innerSetups: List[SimpleTxSetup],
                             maybeCertChainWithCrl: Option[(CertChain, CrlCollection)])
    extends TransactionConfirmationSetup {
  override def groupKey: GroupKey = AtomicGroupKey
}

case class AtomicComplexSetup(override val tx: AtomicTransaction,
                              innerSetupTasks: List[Task[TransactionConfirmationSetup]],
                              maybeCertChainWithCrl: Option[(CertChain, CrlCollection)])
    extends TransactionConfirmationSetup {
  override def groupKey: GroupKey = AtomicGroupKey
}
