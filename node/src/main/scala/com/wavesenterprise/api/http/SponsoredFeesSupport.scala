package com.wavesenterprise.api.http

import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{AssetId, ValidationError}

/**
  * To be mixed in for transaction requests, that support Sponsored Fee feature
  * (which basically means, that they have an optional 'feeAssetId' field)
  */
trait SponsoredFeesSupport {
  def feeAssetId: Option[String]

  def decodeFeeAssetId(): Either[ValidationError, Option[AssetId]] =
    feeAssetId match {
      case Some(feeAssetIdString) if feeAssetIdString.nonEmpty =>
        ByteStr
          .decodeBase58(feeAssetIdString)
          .fold(ex => Left(GenericError(ex)), f => Right(Some(f)))

      case _ =>
        Right(None)
    }
}
