package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.crypto
import com.wavesenterprise.database.certs.CertificatesState
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.state.Diff
import com.wavesenterprise.transaction.ValidationError

object CertChainDiff {
  def apply(certChain: CertChain, timestamp: Long, certState: CertificatesState): Either[ValidationError, Diff] = {
    crypto.validateCertChain(certChain, timestamp).leftMap(ValidationError.fromCryptoError) >>
      Diff.fromCertChain(certChain, certState)
  }

  def apply(certChains: Seq[CertChain], timestamp: Long, certState: CertificatesState): Either[ValidationError, Diff] = {
    certChains.toList.traverse(CertChainDiff(_, timestamp, certState)).map(_.combineAll)
  }
}
