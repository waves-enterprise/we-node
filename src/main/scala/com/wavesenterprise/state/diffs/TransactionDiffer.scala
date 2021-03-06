package com.wavesenterprise.state.diffs

import cats.syntax.either._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.metrics._
import com.wavesenterprise.settings.BlockchainSettings
import com.wavesenterprise.state.diffs.TransactionDiffer.{TransactionValidationError, stats}
import com.wavesenterprise.state.diffs.docker._
import com.wavesenterprise.state.{Diff, _}
import com.wavesenterprise.transaction.ValidationError.{AccountBalanceError, GenericError, UnsupportedTransactionType}
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.acl.PermitTransaction
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.assets.exchange.ExchangeTransaction
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}
import com.wavesenterprise.transaction.smart.{SetScriptTransaction, Verifier}
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.validation.FeeCalculator
import com.wavesenterprise.utils.ScorexLogging

import scala.concurrent.duration.FiniteDuration

case class TransactionDiffer(
    settings: BlockchainSettings,
    permissionValidator: PermissionValidator,
    prevBlockTimestamp: Option[Long],
    currentBlockTimestamp: Long,
    currentBlockHeight: Int,
    txExpireTimeout: FiniteDuration,
    blockOpt: Option[Signed] = None,
    minerOpt: Option[PublicKeyAccount] = None,
    alreadyVerified: Boolean = false,
    alreadyVerifiedTxIds: Set[ByteStr] = Set.empty
) { self =>

  def apply(blockchain: Blockchain, tx: Transaction, atomically: Boolean = false): Either[ValidationError, Diff] =
    (for {
      _            <- verify(blockchain, tx)
      _            <- validateAtomicBadge(tx, atomically)
      diff         <- createDiff(blockchain, tx)
      positiveDiff <- validateBalance(blockchain, tx, diff)
    } yield positiveDiff).leftMap(TransactionValidationError(_, tx))

  val asFunc: (Blockchain, Transaction) => Either[ValidationError, Diff] = (b, t) => self.apply(b, t)

  private def verify(blockchain: Blockchain, tx: Transaction): Either[ValidationError, Unit] = {
    if (alreadyVerified) {
      Right(())
    } else {
      for {
        _ <- stats.verification.measureForType(tx.builder.typeId)(Verifier(blockchain, currentBlockHeight, alreadyVerifiedTxIds)(tx))
        _ <- stats.commonValidation
          .measureForType(tx.builder.typeId) {
            for {
              _ <- CommonValidation.disallowTxFromFuture(currentBlockTimestamp, tx)
              _ <- CommonValidation.disallowTxFromPast(prevBlockTimestamp, tx, txExpireTimeout)
              _ <- CommonValidation.disallowBeforeActivationTime(blockchain, currentBlockHeight, tx)
              _ <- CommonValidation.disallowDuplicateIds(blockchain, tx)
              _ <- CommonValidation.disallowSendingGreaterThanBalance(blockchain, tx)
              _ <- permissionValidator.validatePermissionForTx(blockchain, tx)
              _ <- FeeCalculator(blockchain, settings.custom.functionality, settings.fees).validateTxFee(currentBlockHeight, tx)
            } yield ()
          }
      } yield ()
    }
  }

  private def validateBalance(blockchain: Blockchain, tx: Transaction, diff: Diff): Either[AccountBalanceError, Diff] = {
    if (alreadyVerified) {
      Right(diff)
    } else {
      stats.balanceValidation.measureForType(tx.builder.typeId) {
        BalanceDiffValidation(blockchain, currentBlockHeight, settings.custom.functionality)(diff)
      }
    }
  }

  private def createDiff(blockchain: Blockchain, tx: Transaction): Either[ValidationError, Diff] = {
    stats.transactionDiffValidation.measureForType(tx.builder.typeId) {
      tx match {
        // genesis txs
        case gtx: GenesisTransaction               => GenesisTransactionDiff(currentBlockHeight)(gtx)
        case gptx: GenesisPermitTransaction        => GenesisPermitTransactionDiff(settings.custom.genesis, currentBlockHeight)(gptx)
        case grntx: GenesisRegisterNodeTransaction => GenesisRegisterNodeTransactionDiff(currentBlockHeight)(grntx)

        // token txs
        case itx: IssueTransaction   => AssetTransactionsDiff.issue(currentBlockHeight)(itx)
        case rtx: ReissueTransaction => AssetTransactionsDiff.reissue(blockchain, currentBlockHeight)(rtx)
        case btx: BurnTransaction    => AssetTransactionsDiff.burn(blockchain, currentBlockHeight)(btx)
        case ttx: TransferTransaction =>
          TransferTransactionDiff(blockchain, settings.custom.functionality, currentBlockTimestamp, currentBlockHeight)(ttx)
        case mtx: MassTransferTransaction => MassTransferTransactionDiff(blockchain, currentBlockTimestamp, currentBlockHeight)(mtx)
        case ltx: LeaseTransaction        => LeaseTransactionsDiff.lease(blockchain, currentBlockHeight)(ltx)
        case ltx: LeaseCancelTransaction =>
          LeaseTransactionsDiff.leaseCancel(blockchain, settings.custom.functionality, currentBlockTimestamp, currentBlockHeight)(ltx)
        case stx: SponsorFeeTransactionV1 => AssetTransactionsDiff.sponsor(blockchain, currentBlockHeight)(stx)

        case _: ExchangeTransaction => Left(UnsupportedTransactionType)

        // alias
        case atx: CreateAliasTransaction => CreateAliasTransactionDiff(blockchain, currentBlockHeight)(atx)

        // data
        case dtx: DataTransaction => DataTransactionDiff(blockchain, currentBlockHeight)(dtx)

        // ride smart-contract txs
        case sstx: SetScriptTransaction        => SetScriptTransactionDiff(blockchain, currentBlockHeight)(sstx)
        case sstx: SetAssetScriptTransactionV1 => AssetTransactionsDiff.setAssetScript(blockchain, currentBlockHeight)(sstx)

        // acl
        case ptx: PermitTransaction          => PermitTransactionDiff(blockchain, settings.custom.genesis, currentBlockTimestamp, currentBlockHeight)(ptx)
        case rntx: RegisterNodeTransactionV1 => RegisterNodeTransactionDiff(blockchain, currentBlockHeight)(rntx)

        // docker smart-contract txs
        case ctx: CreateContractTransaction => CreateContractTransactionDiff(blockchain, blockOpt, currentBlockHeight)(ctx)
        case ctx: CallContractTransaction   => CallContractTransactionDiff(blockchain, blockOpt, currentBlockHeight)(ctx)
        case etx: ExecutedContractTransaction =>
          ExecutedContractTransactionDiff(blockchain, currentBlockTimestamp, currentBlockHeight, blockOpt, minerOpt)(etx)
        case dct: DisableContractTransaction => DisableContractTransactionDiff(blockchain, currentBlockHeight)(dct)
        case uct: UpdateContractTransaction  => UpdateContractTransactionDiff(blockchain, blockOpt, currentBlockHeight)(uct)

        // privacy transactions
        case cptx: CreatePolicyTransaction    => CreatePolicyTransactionDiff(blockchain, currentBlockHeight)(cptx)
        case uptx: UpdatePolicyTransaction    => UpdatePolicyTransactionDiff(blockchain, currentBlockHeight)(uptx)
        case pdhtx: PolicyDataHashTransaction => PolicyDataHashTransactionDiff(blockchain, currentBlockHeight)(pdhtx)

        // atomic
        case atx: AtomicTransaction => AtomicTransactionDiff(blockchain, this, currentBlockHeight, blockOpt, minerOpt)(atx)
        case _                      => Left(UnsupportedTransactionType)
      }
    }
  }

  private def validateAtomicBadge(tx: Transaction, atomically: Boolean): Either[GenericError, Unit] = {
    tx match {
      case _: ExecutedContractTransaction =>
        Right(())
      case atomicInnerTx: AtomicInnerTransaction if atomicInnerTx.atomicBadge.isDefined && !atomically =>
        Left(GenericError("Transactions with atomic badge cannot be mined outside of atomic container"))
      case _ =>
        Right(())
    }
  }
}

object TransactionDiffer extends Instrumented with ScorexLogging {

  private val stats = TxProcessingStats

  case class TransactionValidationError(cause: ValidationError, tx: Transaction) extends ValidationError
}
