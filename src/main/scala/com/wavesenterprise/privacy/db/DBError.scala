package com.wavesenterprise.privacy.db

import com.wavesenterprise.transaction.ValidationError

trait DBError

object DBError {
  case class ValidationErrorWrap(validationError: ValidationError) extends DBError
  case object DuplicateKey                                         extends DBError
  case object UnmappedError                                        extends DBError
}
