package com.wavesenterprise.transaction.smart

import cats.implicits._
import com.google.common.base.Throwables
import com.wavesenterprise.crypto
import com.wavesenterprise.lang.ExprEvaluator.Log
import com.wavesenterprise.lang.v1.compiler.Terms.{EVALUATED, FALSE, TRUE}
import com.wavesenterprise.metrics._
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.ValidationError._
import com.wavesenterprise.transaction.assets.exchange.{ExchangeTransaction, Order}
import com.wavesenterprise.transaction.smart.script.{Script, ScriptRunner}
import com.wavesenterprise.transaction.{GenesisPermitTransaction, _}
import com.wavesenterprise.utils.ScorexLogging
import shapeless.{:+:, CNil, Coproduct}

import scala.util.{Failure, Success, Try}

object Verifier extends Instrumented with ScorexLogging {

  private val stats = TxProcessingStats

  import stats.TxTimerExt

  private type TxOrd       = Transaction :+: Order :+: CNil
  type ValidationResult[T] = Either[ValidationError, T]

  val AtomicTxMinerProofIndex = 1

  def apply(blockchain: Blockchain, currentBlockHeight: Int, alreadyVerifiedTxIds: Set[ByteStr] = Set.empty)(
      tx: Transaction): ValidationResult[Transaction] =
    (tx match {
      case _: GenesisTransaction             => Right(tx)
      case _: GenesisPermitTransaction       => Right(tx)
      case _: GenesisRegisterNodeTransaction => Right(tx)
      case atomicTx: AtomicTransaction       => verifyAtomicTx(blockchain, currentBlockHeight, atomicTx)
      case pt: ProvenTransaction =>
        (pt, blockchain.accountScript(pt.sender.toAddress)) match {
          case (et: ExchangeTransaction, scriptOpt) =>
            verifyExchange(et, blockchain, scriptOpt, currentBlockHeight)
          case (dt: DataTransactionV1, Some(script)) if dt.sender != dt.author =>
            stats.signatureVerification
              .measureForType(tx.builder.typeId)(verifyDataTxAuthorSignature(dt))
              .flatMap(_ =>
                stats.accountScriptExecution.measureForType(pt.builder.typeId)(
                  verifyTx(blockchain, script, currentBlockHeight, pt, isTokenScript = false)))
          case (_, Some(script)) =>
            stats.accountScriptExecution
              .measureForType(pt.builder.typeId)(verifyTx(blockchain, script, currentBlockHeight, pt, isTokenScript = false))
          case (tx, _) if alreadyVerifiedTxIds.contains(tx.id()) => Right(tx)
          case _ =>
            stats.signatureVerification
              .measureForType(tx.builder.typeId)(verifyAsEllipticCurveSignature(pt, pt.proofSourceBytes, exactlyOneProof = true))
        }
    }).flatMap(tx =>
      tx.checkedAssets
        .flatMap(assetId => blockchain.assetScript(assetId))
        .foldRight(Either.right[ValidationError, Transaction](tx)) { (script, txr) =>
          txr.right.flatMap(tx =>
            stats.assetScriptExecution
              .measureForType(tx.builder.typeId)(verifyTx(blockchain, script, currentBlockHeight, tx, isTokenScript = true)))
        })

  private def verifyAtomicTx(blockchain: Blockchain, currentBlockHeight: Int, atomicTx: AtomicTransaction): ValidationResult[AtomicTransaction] = {
    stats.signatureVerification.measureForType(atomicTx.builder.typeId) {
      verifyAsEllipticCurveSignature(atomicTx, atomicTx.proofSourceBytes, exactlyOneProof = false) >>
        atomicTx.transactions.traverse(innerTx => apply(blockchain, currentBlockHeight)(innerTx)) >>
        atomicTx.miner.fold(atomicTx.asRight[ValidationError]) { minerPublicKey =>
          val minerProofSource = AtomicUtils.extractMinerProofSource(atomicTx)
          val proofs           = atomicTx.proofs.proofs
          if (proofs.isDefinedAt(AtomicTxMinerProofIndex)) {
            val minerProof = proofs(AtomicTxMinerProofIndex)
            Either.cond(crypto.verify(minerProof.arr, minerProofSource, minerPublicKey.publicKey),
                        atomicTx,
                        GenericError(s"Invalid miner proof of atomic transaction"))
          } else {
            Left(GenericError(s"The miner proof of atomic transaction is missing"))
          }
        }
    }
  }

  def verifyDataTxAuthorSignature(pt: DataTransactionV1): Either[ValidationError, DataTransactionV1] =
    pt.proofs.proofs.headOption match {
      case Some(p) =>
        Either.cond(crypto.verify(p.arr, pt.proofSourceBytes, pt.author.publicKey), pt, GenericError(s"Proof is not valid for author for $pt"))
      case None => Left(GenericError(s"Proofs is empty for $pt"))
    }

  def verifyTx(blockchain: Blockchain, script: Script, height: Int, transaction: Transaction, isTokenScript: Boolean): ValidationResult[Transaction] =
    Try {
      logged(
        s"transaction ${transaction.id()}",
        ScriptRunner[EVALUATED](height, Coproduct[TxOrd](transaction), blockchain, script, isTokenScript)
      ) match {
        case (log, Left(execError)) => Left(ScriptExecutionError(execError, script.text, log, isTokenScript))
        case (log, Right(FALSE)) =>
          Left(TransactionNotAllowedByScript(log, script.text, isTokenScript))
        case (_, Right(TRUE)) => Right(transaction)
        case (_, Right(x))    => Left(GenericError(s"Script returned not a boolean result, but $x"))
      }
    } match {
      case Failure(e) =>
        Left(ScriptExecutionError(s"Uncaught execution error: ${Throwables.getStackTraceAsString(e)}", script.text, List.empty, isTokenScript))
      case Success(s) => s
    }

  def verifyOrder(blockchain: Blockchain, script: Script, height: Int, order: Order): ValidationResult[Order] =
    Try {
      logged(s"order ${order.idStr()}", ScriptRunner[EVALUATED](height, Coproduct[TxOrd](order), blockchain, script, isTokenScript = false)) match {
        case (log, Left(execError)) => Left(ScriptExecutionError(execError, script.text, log, isTokenScript = false))
        case (log, Right(FALSE))    => Left(TransactionNotAllowedByScript(log, script.text, isTokenScript = false))
        case (_, Right(TRUE))       => Right(order)
        case (_, Right(x))          => Left(GenericError(s"Script returned not a boolean result, but $x"))
      }
    } match {
      case Failure(e) => Left(ScriptExecutionError(s"Uncaught execution error: $e", script.text, List.empty, isTokenScript = false))
      case Success(s) => s
    }

  def verifyExchange(et: ExchangeTransaction,
                     blockchain: Blockchain,
                     matcherScriptOpt: Option[Script],
                     height: Int): ValidationResult[Transaction] = {
    val typeId    = et.builder.typeId
    val sellOrder = et.sellOrder
    val buyOrder  = et.buyOrder

    def matcherTxVerification =
      matcherScriptOpt
        .map { script =>
          if (et.version != 1) {
            stats.accountScriptExecution
              .measureForType(typeId)(verifyTx(blockchain, script, height, et, isTokenScript = false))
          } else {
            Left(GenericError("Can't process transaction with signature from scripted account"))
          }
        }
        .getOrElse(stats.signatureVerification.measureForType(typeId)(verifyAsEllipticCurveSignature(et)))

    def sellerOrderVerification =
      blockchain
        .accountScript(sellOrder.sender.toAddress)
        .map(script =>
          if (sellOrder.version != 1) {
            stats.orderValidation.measure(verifyOrder(blockchain, script, height, sellOrder))
          } else {
            Left(GenericError("Can't process order with signature from scripted account"))
          })
        .getOrElse(stats.signatureVerification.measureForType(typeId)(verifyAsEllipticCurveSignature(sellOrder)))

    def buyerOrderVerification =
      blockchain
        .accountScript(buyOrder.sender.toAddress)
        .map(script =>
          if (buyOrder.version != 1) {
            stats.orderValidation.measure(verifyOrder(blockchain, script, height, buyOrder))
          } else {
            Left(GenericError("Can't process order with signature from scripted account"))
          })
        .getOrElse(stats.signatureVerification.measureForType(typeId)(verifyAsEllipticCurveSignature(buyOrder)))

    def assetVerification(assetId: Option[AssetId], tx: ExchangeTransaction) =
      assetId.fold[ValidationResult[Transaction]](Right(tx)) { assetId =>
        blockchain.assetScript(assetId).fold[ValidationResult[Transaction]](Right(tx)) { script =>
          verifyTx(blockchain, script, height, tx, isTokenScript = true).left.map {
            case x: HasScriptType => x
            case GenericError(x)  => ScriptExecutionError(x, script.text, List.empty, isTokenScript = true)
            case x                => ScriptExecutionError(x.toString, script.text, List.empty, isTokenScript = true)
          }
        }
      }

    for {
      _ <- matcherTxVerification
      _ <- sellerOrderVerification
      _ <- buyerOrderVerification
      _ <- assetVerification(et.buyOrder.assetPair.amountAsset, et)
      _ <- assetVerification(et.buyOrder.assetPair.priceAsset, et)
    } yield et
  }

  def verifyAsEllipticCurveSignature[T <: Proven with Authorized](pt: T, exactlyOneProof: Boolean = true): ValidationResult[T] = {
    verifyAsEllipticCurveSignature(pt, pt.bodyBytes(), exactlyOneProof)
  }

  def verifyAsEllipticCurveSignature[T <: Proven with Authorized](proven: T,
                                                                  proofSourceBytes: Array[Byte],
                                                                  exactlyOneProof: Boolean): ValidationResult[T] = {
    (if (exactlyOneProof) {
       Either.cond(proven.proofs.proofs.length == 1, proven, GenericError("Transactions from non-scripted accounts must have exactly 1 proof"))
     } else {
       Right(proven)
     }) >> (proven.proofs.proofs.headOption match {
      case Some(p) =>
        Either.cond(
          crypto.verify(p.arr, proofSourceBytes, proven.sender.publicKey),
          proven,
          GenericError(s"Script doesn't exist and proof doesn't validate as signature for '$proven'")
        )
      case _ => Left(GenericError("Transactions from non-scripted accounts must have proof"))
    })
  }

  private def logged(id: => String, result: (Log, Either[String, EVALUATED])): (Log, Either[String, EVALUATED]) = {
    val (execLog, execResult) = result
    log.debug(s"Script for $id evaluated to $execResult")
    execLog.foreach {
      case (k, Right(v))  => log.debug(s"Evaluated `$k` to $v")
      case (k, Left(err)) => log.debug(s"Failed to evaluate `$k`: $err")
    }
    result
  }
}
