package com.wavesenterprise.utx

import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.{Transaction, ValidationError}

trait UtxCertStorage {
  def putCertChain(tx: Transaction, certChain: CertChain): Either[ValidationError, Unit]
  def removeCertChain(tx: Transaction): Unit
  def getCertChain(txId: ByteStr): Option[CertChain]
}
