package com.wavesenterprise.utx

import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.{Transaction, ValidationError}

trait NopeUtxCertStorage extends UtxCertStorage {
  def putCertChain(tx: Transaction, certChain: CertChain): Either[ValidationError, Unit] = Right(())
  def removeCertChain(tx: Transaction): Unit                                             = ()
  def getCertChain(txId: ByteStr): Option[CertChain]                                     = None
}
