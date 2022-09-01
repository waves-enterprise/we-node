package com.wavesenterprise.privacy.s3

sealed trait S3Error

case object UnmappedError                                extends S3Error
case class ParseError(message: String)                   extends S3Error
case class BucketError(message: String)                  extends S3Error
case class InvalidHash(actual: String, expected: String) extends S3Error
