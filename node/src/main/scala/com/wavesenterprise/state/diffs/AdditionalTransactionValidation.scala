package com.wavesenterprise.state.diffs

import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.docker.ExecutableTransaction

trait AdditionalTransactionValidation {
  protected def additionalTransactionValidation(tx: ExecutableTransaction): Either[ValidationError, Unit] = Right(())
}
