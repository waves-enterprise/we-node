package com.wavesenterprise.state

import cats.implicits._
import com.wavesenterprise.transaction.{AtomicTransaction, ProvenTransaction, Signed, Transaction}
import com.wavesenterprise.transaction.ValidationError.InvalidSignature
import com.wavesenterprise.transaction.smart.Verifier
import monix.eval.Task
import monix.execution.Scheduler

class SignatureValidator()(implicit scheduler: Scheduler) {

  private val processorsCount = Runtime.getRuntime.availableProcessors()

  def validateOrdered[A <: Signed](items: Seq[A]): Task[Either[InvalidSignature, List[A]]] = {
    Task
      .parTraverseN(processorsCount)(items.toList)(block => Task(Signed.validate(block)))
      .map(_.sequence)
      .executeOn(scheduler)
  }

  def validate[A <: Signed](value: A): Task[Either[InvalidSignature, A]] = {
    Task.eval(Signed.validate(value)).executeOn(scheduler)
  }

  /**
    * Performs preliminary validation of transactions
    * @return Set of valid transaction ids, which verified as elliptic curve signature
    */
  def preValidateProvenTransactions(txs: Iterable[Transaction]): Task[Set[ByteStr]] = {
    Task
      .parTraverseN(processorsCount)(txs) {
        case _: AtomicTransaction => Task.pure(None)
        case provenTx: ProvenTransaction =>
          Task(Verifier.verifyAsEllipticCurveSignature(provenTx) match {
            case Left(_)      => None
            case Right(value) => Some(value.id())
          })
        case _ => Task.pure(None)
      }
      .map(_.view.flatten.toSet)
      .executeOn(scheduler)
  }
}
