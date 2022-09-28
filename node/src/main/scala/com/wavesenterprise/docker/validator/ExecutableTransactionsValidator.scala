package com.wavesenterprise.docker.validator

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.acl.Role
import com.wavesenterprise.block.Block
import com.wavesenterprise.docker._
import com.wavesenterprise.docker.validator.ExecutableTransactionsValidator.ValidationStartCause
import com.wavesenterprise.docker.validator.ExecutableTransactionsValidator.ValidationStartCause.{KeyBlockAppended, MicroBlockAppended}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider.FeatureProviderExt
import com.wavesenterprise.mining.{TransactionsAccumulatorProvider, ValidatorTransactionsConfirmatory}
import com.wavesenterprise.settings.PositiveInt
import com.wavesenterprise.state.diffs.docker.ExecutedContractTransactionDiff.ValidatingExecutor
import com.wavesenterprise.state.{Blockchain, ByteStr, MiningConstraintsHolder}
import com.wavesenterprise.utils.{ScorexLogging, Time}
import com.wavesenterprise.utx.UtxPool
import monix.execution.Scheduler
import monix.execution.cancelables.SerialCancelable

import scala.concurrent.duration.FiniteDuration

class ExecutableTransactionsValidator(
    val ownerKey: PrivateKeyAccount,
    val transactionsAccumulatorProvider: TransactionsAccumulatorProvider,
    val contractExecutionComponentsOpt: Option[ContractExecutionComponents],
    val utx: UtxPool,
    val blockchain: Blockchain with MiningConstraintsHolder,
    val time: Time,
    val pullingBufferSize: PositiveInt,
    val utxCheckDelay: FiniteDuration
)(implicit val scheduler: Scheduler)
    extends ScorexLogging {

  private[this] val txsConfirmation = SerialCancelable()

  def runValidation(height: Int, cause: ValidationStartCause): Unit = {
    val keyBlockId   = cause.keyBlock.uniqueId
    val confirmatory = initNewConfirmation(keyBlockId)

    val isValidationActivated = blockchain.isFeatureActivated(BlockchainFeature.ContractValidationsSupport, height)

    if (isValidationActivated) {
      val isValidator =
        confirmatory.transactionsAccumulator
          .permissions(ownerKey.toAddress)
          .contains(Role.ContractValidator, cause.keyBlock.timestamp)

      if (isValidator) {
        txsConfirmation := confirmatory.confirmationTask.runAsyncLogErr(scheduler)

        cause match {
          case _: KeyBlockAppended =>
            log.debug(s"Executable transaction validation has been started for key-block '$keyBlockId' on height '$height'")
          case MicroBlockAppended(_, microBlockId) =>
            log.debug(s"Executable transaction validation continues with key-block '$keyBlockId' and micro-block '$microBlockId' on height '$height'")
        }
      } else {
        log.debug(s"Current account '$ownerKey' is not a contract validator")
        cancelValidation()
      }
    }
  }

  private def initNewConfirmation(keyBlockId: ByteStr): ValidatorTransactionsConfirmatory = {
    val transactionsAccumulator = transactionsAccumulatorProvider.build(Some(blockchain), contractTxExecutor = ValidatingExecutor)
    val transactionsExecutorOpt = contractExecutionComponentsOpt.map(_.createValidatorExecutor(transactionsAccumulator, keyBlockId))
    contractExecutionComponentsOpt.foreach(_.setDelegatingState(transactionsAccumulator))

    new ValidatorTransactionsConfirmatory(
      transactionsAccumulator = transactionsAccumulator,
      transactionExecutorOpt = transactionsExecutorOpt,
      utx = utx,
      pullingBufferSize = pullingBufferSize,
      utxCheckDelay = utxCheckDelay,
      ownerKey = ownerKey
    )(scheduler)
  }

  def cancelValidation(): Unit = {
    txsConfirmation := SerialCancelable()
    log.debug("Executable transaction validation has been cancelled")
  }
}

object ExecutableTransactionsValidator {

  sealed trait ValidationStartCause {
    def keyBlock: Block
  }

  object ValidationStartCause {
    case class MicroBlockAppended(keyBlock: Block, microBlockId: ByteStr) extends ValidationStartCause
    case class KeyBlockAppended(keyBlock: Block)                          extends ValidationStartCause
  }
}
