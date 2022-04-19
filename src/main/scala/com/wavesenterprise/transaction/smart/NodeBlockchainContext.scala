package com.wavesenterprise.transaction.smart

import cats.kernel.Monoid
import com.wavesenterprise.lang.ScriptVersion
import com.wavesenterprise.lang.v1.evaluator.ctx.EvaluationContext
import com.wavesenterprise.lang.v1.evaluator.ctx.impl.waves.WavesContext
import com.wavesenterprise.lang.v1.evaluator.ctx.impl.{CryptoContext, PureContext}
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.smart.BlockchainContext.In
import monix.eval.Coeval

object NodeBlockchainContext {
  def build(version: ScriptVersion,
            nByte: Byte,
            in: Coeval[In],
            h: Coeval[Int],
            blockchain: Blockchain,
            isTokenContext: Boolean): EvaluationContext = {
    Monoid
      .combineAll(
        Seq(
          PureContext.build(version),
          CryptoContext.build(BlockchainContext.baseGlobal),
          WavesContext.build(version, new WavesEnvironment(nByte, in, h, blockchain), isTokenContext)
        ))
      .evaluationContext
  }
}
