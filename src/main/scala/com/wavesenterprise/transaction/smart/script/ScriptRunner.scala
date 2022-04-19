package com.wavesenterprise.transaction.smart.script

import cats.implicits._
import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.lang.v1.compiler.Terms.EVALUATED
import com.wavesenterprise.lang.v1.evaluator.EvaluatorV1
import com.wavesenterprise.lang.{ExecutionError, ExprEvaluator}
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.assets.exchange.Order
import com.wavesenterprise.transaction.smart.NodeBlockchainContext
import monix.eval.Coeval
import shapeless._

object ScriptRunner {

  def apply[A <: EVALUATED](height: Int,
                            in: Transaction :+: Order :+: CNil,
                            blockchain: Blockchain,
                            script: Script,
                            isTokenScript: Boolean): (ExprEvaluator.Log, Either[ExecutionError, A]) = {
    script match {
      case Script.Expr(expr) =>
        val ctx = NodeBlockchainContext.build(
          script.version,
          AddressScheme.getAddressSchema.chainId,
          Coeval.evalOnce(in),
          Coeval.evalOnce(height),
          blockchain,
          isTokenScript
        )
        EvaluatorV1.applywithLogging[A](ctx, expr)

      case _ => (List.empty, "Unsupported script version".asLeft[A])
    }
  }
}
